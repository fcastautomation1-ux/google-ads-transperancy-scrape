// Use puppeteer-extra + stealth plugin for better anti-detection
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());
const { google } = require('googleapis');
const fs = require('fs');
// NOTE: run `npm i puppeteer puppeteer-extra puppeteer-extra-plugin-stealth` to install new deps
// Optional env vars: PROXIES (semicolon-separated), CONCURRENT_PAGES, MAX_PROXY_ATTEMPTS, PROXY_RETRY_DELAY_MIN/PROXY_RETRY_DELAY_MAX, BATCH_DELAY_MIN/MAX

// ============================================
// CONFIGURATION
// ============================================
const SPREADSHEET_ID = '1beJ263B3m4L8pgD9RWsls-orKLUvLMfT2kExaiyNl7g';
const SHEET_NAME = 'App data'; // Separate sheet for this agent
const CREDENTIALS_PATH = './credentials.json';
const CONCURRENT_PAGES = parseInt(process.env.CONCURRENT_PAGES) || 3; // 3 parallel requests per batch
const MAX_WAIT_TIME = 60000;
const MAX_RETRIES = 3;
const RETRY_WAIT_MULTIPLIER = 1.5;

// Proxy rotation settings: set env var PROXIES="http://user:pass@host:port;http://..."
const PROXIES = process.env.PROXIES ? process.env.PROXIES.split(';').map(p => p.trim()).filter(Boolean) : [];
const MAX_PROXY_ATTEMPTS = parseInt(process.env.MAX_PROXY_ATTEMPTS) || Math.max(3, PROXIES.length);
const PROXY_RETRY_DELAY_MIN = parseInt(process.env.PROXY_RETRY_DELAY_MIN) || 30000; // 30s
const PROXY_RETRY_DELAY_MAX = parseInt(process.env.PROXY_RETRY_DELAY_MAX) || 90000; // 90s

// Batch delay range (in ms)
const BATCH_DELAY_MIN = parseInt(process.env.BATCH_DELAY_MIN) || 8000;
const BATCH_DELAY_MAX = parseInt(process.env.BATCH_DELAY_MAX) || 20000;

function pickProxy() {
    if (!PROXIES.length) return null;
    return PROXIES[Math.floor(Math.random() * PROXIES.length)];
}

// Simple in-memory stats to help debug blocking patterns during a run
const proxyStats = { totalBlocks: 0, perProxy: {} };

// Anti-detection: Rotating User Agents
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
];

// Anti-detection: Random viewport sizes
const VIEWPORTS = [
    { width: 1920, height: 1080 },
    { width: 1366, height: 768 },
    { width: 1536, height: 864 },
    { width: 1440, height: 900 },
    { width: 1280, height: 720 }
];

// Helper: Random delay between min and max milliseconds
const randomDelay = (min, max) => new Promise(r => setTimeout(r, min + Math.random() * (max - min)));

// ============================================
// GOOGLE SHEETS SETUP
// ============================================
async function getGoogleSheetsClient() {
    const credentials = JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
    const auth = new google.auth.GoogleAuth({
        credentials,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const authClient = await auth.getClient();
    return google.sheets({ version: 'v4', auth: authClient });
}

async function getUrlData(sheets) {
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A:C`,
    });
    const rows = response.data.values || [];

    // Find index of LAST row with data in Column B (App Link)
    let lastProcessedIndex = 0;
    for (let i = rows.length - 1; i >= 1; i--) {
        if (rows[i][1]?.trim()) { // Column B is index 1
            lastProcessedIndex = i;
            break;
        }
    }

    const toProcess = [];
    for (let i = lastProcessedIndex + 1; i < rows.length; i++) {
        const row = rows[i];
        const url = row[0]?.trim(); // Column A
        if (url) {
            toProcess.push({ url, rowIndex: i });
        }
    }
    return toProcess;
}

async function safeBatchWrite(sheets, updates) {
    if (updates.length === 0) return;

    // 1. Re-fetch current Column A (URLs) and Column B (App Link) to find where these URLs are NOW
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A:C`,
    });
    const rows = response.data.values || [];

    const data = [];
    updates.forEach(({ url, appName, storeLink, isVideo }) => {
        // 2. Find the index where URL matches AND Column B (index 1) is empty
        let foundIndex = -1;
        for (let i = 1; i < rows.length; i++) {
            if (rows[i][0]?.trim() === url && !rows[i][1]?.trim()) {
                foundIndex = i;
                break;
            }
        }

        // Fallback: If no empty row found, find the last match
        if (foundIndex === -1) {
            for (let i = rows.length - 1; i >= 1; i--) {
                if (rows[i][0]?.trim() === url) {
                    foundIndex = i;
                    break;
                }
            }
        }

        if (foundIndex !== -1) {
            const rowNum = foundIndex + 1;
            data.push({
                range: `${SHEET_NAME}!B${rowNum}`, // Column B for Link
                values: [[storeLink]]
            });
            data.push({
                range: `${SHEET_NAME}!C${rowNum}`, // Column C for Name
                values: [[appName]]
            });
            data.push({
                range: `${SHEET_NAME}!D${rowNum}`, // Column D for Format
                values: [[isVideo ? 'Video Ad' : 'Text/Image Ad']]
            });
        }
    });

    if (data.length === 0) return;
    await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        resource: { valueInputOption: 'RAW', data: data }
    });
    console.log(`  ✅ Safely wrote ${data.length / 2} results to 'App data'`);
}

// ============================================
// SELF-RESTART LOGIC
// ============================================
async function triggerSelfRestart() {
    const repo = process.env.GITHUB_REPOSITORY;
    const token = process.env.GH_TOKEN;
    if (!repo || !token) return;

    console.log(`\n🔄 Triggering auto-restart for App data...`);
    const https = require('https');
    const data = JSON.stringify({ event_type: 'app_data_trigger' });
    const options = {
        hostname: 'api.github.com',
        port: 443,
        path: `/repos/${repo}/dispatches`,
        method: 'POST',
        headers: {
            'Authorization': `token ${token}`,
            'User-Agent': 'Node.js',
            'Accept': 'application/vnd.github.v3+json',
            'Content-Type': 'application/json',
            'Content-Length': data.length
        }
    };
    const req = https.request(options);
    req.write(data);
    req.end();
}

// ============================================
// EXTRACTION LOGIC
// ============================================
async function extractAppData(url, browser, attempt = 1) {
    const page = await browser.newPage();
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    const cleanName = (name) => {
        if (!name) return 'NOT_FOUND';
        // 1. Remove invisible Unicode control characters (like U+2066, U+2069)
        let cleaned = name.replace(/[\u200B-\u200D\uFEFF\u2066-\u2069]/g, '').trim();
        // 2. Remove the Google variation separator
        cleaned = cleaned.split('!@~!@~')[0].trim();
        // 3. Robust Duplicate Remover (e.g. "A | A" or "A | B | A")
        if (cleaned.includes('|')) {
            const parts = cleaned.split('|').map(p => p.trim()).filter(p => p.length > 0);
            const uniqueParts = [...new Set(parts)];
            cleaned = uniqueParts[0]; // Take the first unique headline
        }
        return cleaned;
    };
    let result = { appName: 'NOT_FOUND', storeLink: 'NOT_FOUND', isVideo: false };

    // ANTI-DETECTION: Random User-Agent per request
    const userAgent = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
    await page.setUserAgent(userAgent);

    // ANTI-DETECTION: Random viewport per request
    const viewport = VIEWPORTS[Math.floor(Math.random() * VIEWPORTS.length)];
    await page.setViewport(viewport);

    // ANTI-DETECTION: Mask webdriver property
    await page.evaluateOnNewDocument(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    });

    try {
        console.log(`  🚀 Loading (${viewport.width}x${viewport.height}): ${url.substring(0, 50)}...`);

        // ANTI-DETECTION: Short random delay before navigation (1-2 seconds)
        await randomDelay(1000, 2000);

        // Add language header to look more like a real browser
        await page.setExtraHTTPHeaders({ 'accept-language': 'en-US,en;q=0.9' });

        const response = await page.goto(url, { waitUntil: 'networkidle0', timeout: MAX_WAIT_TIME });

        // 1. Capture the "Advertiser Name" from the main page to use as a blacklist
        // Also check if it's a video ad (checking for actual video element ONLY with valid dimensions)
        const mainPageInfo = await page.evaluate(() => {
            const topTitle = document.querySelector('h1, .advertiser-name, .ad-details-heading');

            // Helper to check for video indicators
            const checkVideo = () => {
                // 1. Strict check: Video element with dimensions
                const videoEl = document.querySelector('video');
                if (videoEl && videoEl.offsetWidth > 10 && videoEl.offsetHeight > 10) return true;

                // 2. Format text check combined with visual indicators (Play button)
                // If the text says "Format: Video", we look for a Play button to confirm it's actually a video
                const formatText = document.body.innerText;
                if (formatText.includes('Format: Video')) {
                    // Check for Play buttons or overlays
                    const playBtn = document.querySelector('[aria-label*="Play" i], .material-icons, .goog-icon');
                    if (playBtn) {
                        // Check if icon content is play_arrow or similar
                        if (playBtn.innerText.includes('play_arrow') || playBtn.innerText.includes('play_circle')) return true;
                        // Check aria label
                        const label = playBtn.getAttribute('aria-label') || '';
                        if (label.toLowerCase().includes('play')) return true;
                    }
                }
                return false;
            };

            return {
                blacklist: topTitle ? topTitle.innerText.trim().toLowerCase() : '',
                isVideo: checkVideo()
            };
        });
        const blacklistName = mainPageInfo.blacklist;
        if (mainPageInfo.isVideo) result.isVideo = true;

        const content = await page.content();
        // Extra block detection: HTTP 429 or common captcha indicators
        if ((response && response.status && response.status() === 429) ||
            content.includes('Our systems have detected unusual traffic') ||
            content.includes('Too Many Requests') ||
            content.toLowerCase().includes('captcha') ||
            content.toLowerCase().includes('g-recaptcha') ||
            content.toLowerCase().includes('verify you are human')) {
            console.error('  ⚠️ BLOCKED: Google is detecting unusual traffic or captcha.');
            await page.close();
            return { appName: 'BLOCKED', storeLink: 'BLOCKED' };
        }

        // ANTI-DETECTION: Random wait with jitter (3-5 seconds)
        const baseWait = 3000 + Math.random() * 2000;
        const attemptMultiplier = Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(baseWait * attemptMultiplier);

        // ANTI-DETECTION: Human-like scrolling with random movements
        await page.evaluate(async () => {
            const randomScroll = 600 + Math.random() * 400; // 600-1000px
            window.scrollBy(0, randomScroll);
            await new Promise(r => setTimeout(r, 300 + Math.random() * 400));
            window.scrollBy(0, -randomScroll / 2);
            await new Promise(r => setTimeout(r, 200 + Math.random() * 300));
        });

        // ANTI-DETECTION: Random mouse movement simulation
        try {
            await page.mouse.move(100 + Math.random() * 500, 100 + Math.random() * 300);
            await sleep(300 + Math.random() * 500);
        } catch (e) { }

        await randomDelay(500, 1000);

        const frames = page.frames();
        for (const frame of frames) {
            try {
                const frameData = await frame.evaluate((blacklist) => {
                    const data = { appName: null, storeLink: null, isVideo: false };
                    const root = document.querySelector('#portrait-landscape-phone') || document.body;

                    // Helper to clean/extract real link from an href
                    const cleanLink = (href) => {
                        if (!href || href.includes('javascript:')) return null;

                        // 1. Direct Store Links (Strict Check)
                        if (href.includes('play.google.com') || href.includes('itunes.apple.com')) {
                            return href;
                        }

                        // 2. Google Ad Services Redirects (extract 'adurl' and validate STRICTLY)
                        if (href.includes('googleadservices') || href.includes('/pagead/aclk')) {
                            try {
                                const m = href.match(/[\?&]adurl=([^&\s]+)/i);
                                if (m && m[1]) {
                                    const decoded = decodeURIComponent(m[1]);
                                    // STRICT: Only accept if it is a Store Link
                                    if (decoded.includes('play.google.com') || decoded.includes('itunes.apple.com')) {
                                        return decoded;
                                    }
                                }
                            } catch (e) { }
                        }
                        return null; // Return null if it's not a verified store link
                    };

                    // CLASSIFICATION STRATEGY (Based on User Structure Observation)
                    // Video Ads: Name and Link are in the SAME anchor tag (ochAppName).
                    // Text Ads: Name is just text (no link), Link is separate or missing/in a different button.

                    data.isVideo = false; // Default to Text

                    // 1. Try to find the "Video Ad" structure (Combined Name + Link)
                    const combinedSelector = 'a[data-asoch-targets*="ochAppName"]';
                    const combinedEl = root.querySelector(combinedSelector);

                    if (combinedEl) {
                        const txt = combinedEl.innerText.trim();
                        // Check if name is valid (not blacklisted)
                        if (txt && txt.length > 2 && txt.toLowerCase() !== blacklist) {
                            const extractedLink = cleanLink(combinedEl.href);

                            // If we have BOTH Name and Link in this element -> IT IS A VIDEO AD
                            if (extractedLink) {
                                data.appName = txt;
                                data.storeLink = extractedLink;
                                data.isVideo = true; // Confirmed VIDEO AD structure
                                return data; // Return immediately
                            }
                        }
                    }

                    // 2. Fallback: Separate Extraction (Likely a Text/Image Ad)
                    // If we are here, it means we didn't find the "Video Ad" structure (Combined).
                    // We will search for Name and Link separately.

                    // A. Find Name (Look for text-only headers essentially)
                    if (!data.appName) {
                        const nameSelectors = [
                            '[role="heading"]', // Common in text ads
                            '[role="link"] span',
                            '.short-app-name a', // Sometimes text ads use this but might not have link
                            'div[class*="app-name"]',
                            'span[class*="app-name"]',
                            '.app-title',
                            'a[data-asoch-targets*="AdTitle"]' // Fallback
                        ];

                        for (const sel of nameSelectors) {
                            const elements = root.querySelectorAll(sel);
                            for (const el of elements) {
                                const txt = el.innerText.trim();
                                if (txt && txt.length > 2 && txt.toLowerCase() !== blacklist) {
                                    data.appName = txt;
                                    break;
                                }
                            }
                            if (data.appName) break;
                        }
                    }

                    // B. Find Link (Separate Button/Link)
                    if (!data.storeLink) {
                        const linkSelectors = [
                            'a[data-asoch-targets*="ochInstallButton"]',
                            'a[data-asoch-targets*="ctaButton"]',
                            '.install-button-anchor',
                            'a.ns-sbqu4-e-75'
                        ];

                        for (const sel of linkSelectors) {
                            const el = root.querySelector(sel);
                            const foundLink = el ? cleanLink(el.href) : null;
                            if (foundLink) {
                                data.storeLink = foundLink;
                                break;
                            }
                        }

                        // Fallback Xpath if still not found
                        if (!data.storeLink) {
                            const xpath = '//*[@id="portrait-landscape-phone"]/div[1]/div[5]/a[2]';
                            const xpRes = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
                            if (xpRes && xpRes.href) {
                                const cleanXpLink = cleanLink(xpRes.href);
                                if (cleanXpLink) data.storeLink = cleanXpLink;
                            }
                        }
                    }

                    return data;
                }, blacklistName);

                if (frameData.storeLink && result.storeLink === 'NOT_FOUND') result.storeLink = frameData.storeLink;
                if (frameData.appName && result.appName === 'NOT_FOUND') result.appName = cleanName(frameData.appName);
                // Trust the frame's classification if it found data
                if (frameData.appName || frameData.storeLink) {
                    result.isVideo = frameData.isVideo;
                }

                if (result.storeLink !== 'NOT_FOUND' && result.appName !== 'NOT_FOUND') break;
            } catch (e) { }
        }

        // RegEx fallback for the link from full page source (STRICT MODE)
        // We only accept the fallback link if it explicitly contains play.google.com or itunes.apple.com
        if (result.storeLink === 'NOT_FOUND') {
            const pageSource = await page.content();
            // Look for ad click links
            const matches = pageSource.match(/https:\/\/www\.googleadservices\.com\/pagead\/aclk[^"'’\s]*/g);
            if (matches) {
                for (const match of matches) {
                    try {
                        const decoded = decodeURIComponent(match);
                        if (decoded.includes('play.google.com') || decoded.includes('itunes.apple.com')) {
                            result.storeLink = match; // Keep original link if it resolves to store
                            break;
                        }
                    } catch (e) { }
                }
            }
        }


        // If we still don't have appName, try meta tags/title as fallback
        if (result.appName === 'NOT_FOUND') {
            try {
                const metaOg = pageSource.match(/<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']/i);
                if (metaOg && metaOg[1]) result.appName = metaOg[1].trim();
                if (result.appName === 'NOT_FOUND') {
                    const metaTitle = pageSource.match(/<meta[^>]+name=["']title["'][^>]+content=["']([^"']+)["']/i);
                    if (metaTitle && metaTitle[1]) result.appName = metaTitle[1].trim();
                }
                if (result.appName === 'NOT_FOUND') {
                    const titleTag = pageSource.match(/<title>([^<]+)<\/title>/i);
                    if (titleTag && titleTag[1]) {
                        // strip site suffixes like " - Google Ads" or similar
                        result.appName = titleTag[1].split('|')[0].split('-')[0].trim();
                    }
                }
            } catch (e) { }
        }

        // Direct store link cleanup
        if (result.storeLink !== 'NOT_FOUND' && result.storeLink.includes('adurl=')) {
            try {
                const urlObj = new URL(result.storeLink);
                const adUrl = urlObj.searchParams.get('adurl');
                if (adUrl && adUrl.startsWith('http')) result.storeLink = adUrl;
            } catch (e) { }
        }

        await page.close();
        return result;
    } catch (err) {
        console.error(`  ❌ Error: ${err.message}`);
        await page.close();
        return { appName: 'ERROR', storeLink: 'ERROR' };
    }
}

async function extractWithRetry(url, browser) {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        const data = await extractAppData(url, browser, attempt);
        if (data.appName === 'BLOCKED') return data;
        if (data.appName !== 'NOT_FOUND' || data.storeLink !== 'NOT_FOUND') return data;
        // Random wait between retries (2-4 seconds)
        await randomDelay(2000, 4000);
    }
    return { appName: 'NOT_FOUND', storeLink: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION
// ============================================
(async () => {
    console.log(`🤖 Starting Safety App Data Agent (Sheet: App data)...\n`);
    const sessionStartTime = Date.now();
    const MAX_RUNTIME = 330 * 60 * 1000;

    const sheets = await getGoogleSheetsClient();
    const toProcess = await getUrlData(sheets);

    if (toProcess.length === 0) {
        console.log('✨ No new URLs to process in App data.');
        process.exit(0);
    }

    console.log(`📋 Found ${toProcess.length} pending URLs in App data\n`);

    console.log(PROXIES.length ? `🔁 Proxy rotation enabled (${PROXIES.length} proxies)` : '🔁 Proxy rotation disabled - running direct (no PROXIES env var)');

    for (let i = 0; i < toProcess.length; i += CONCURRENT_PAGES) {
        if (Date.now() - sessionStartTime > MAX_RUNTIME) {
            console.log('⏰ Session limit reached. Restarting...');
            await triggerSelfRestart();
            process.exit(0);
        }

        const batch = toProcess.slice(i, i + CONCURRENT_PAGES);
        console.log(`📦 Batch ${Math.floor(i / CONCURRENT_PAGES) + 1}/${Math.ceil(toProcess.length / CONCURRENT_PAGES)}`);

        let proxyAttempts = 0;
        let handled = false;

        while (!handled && proxyAttempts < MAX_PROXY_ATTEMPTS) {
            const proxy = pickProxy();
            const launchArgs = ['--autoplay-policy=no-user-gesture-required', '--disable-blink-features=AutomationControlled', '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'];
            if (proxy) launchArgs.push(`--proxy-server=${proxy}`);

            console.log(`  🌐 Launching browser (proxy: ${proxy || 'DIRECT'})`);
            const browser = await puppeteer.launch({ headless: true, args: launchArgs });

            try {
                const results = await Promise.all(batch.map(async (item) => {
                    const data = await extractWithRetry(item.url, browser);
                    return { url: item.url, ...data };
                }));

                // Debug: show extracted results before writing
                results.forEach(r => console.log(`  → ${r.url.substring(0, 80)} => ${r.storeLink} | ${r.appName}`));

                if (results.some(r => r.appName === 'BLOCKED')) {
                    console.log('  🛑 Block detected on this proxy. Closing browser and rotating proxy...');
                    // Update simple stats for diagnostics
                    proxyStats.totalBlocks++;
                    proxyStats.perProxy[proxy || 'DIRECT'] = (proxyStats.perProxy[proxy || 'DIRECT'] || 0) + 1;
                    await browser.close();
                    proxyAttempts++;
                    if (proxyAttempts >= MAX_PROXY_ATTEMPTS) {
                        console.log('  ❌ Max proxy attempts reached. Triggering self-restart.');
                        console.log('  🔍 Proxy stats:', JSON.stringify(proxyStats));
                        await triggerSelfRestart();
                        process.exit(0);
                    }
                    const wait = PROXY_RETRY_DELAY_MIN + Math.random() * (PROXY_RETRY_DELAY_MAX - PROXY_RETRY_DELAY_MIN);
                    console.log(`  ⏳ Waiting ${Math.round(wait / 1000)}s before next proxy attempt...`);
                    await new Promise(r => setTimeout(r, wait));
                    continue; // try next proxy
                }

                await safeBatchWrite(sheets, results);
                handled = true;
                await browser.close();
            } catch (err) {
                console.error(`  ❌ Batch error: ${err.message}`);
                // Track error count for this proxy
                proxyStats.perProxy[proxy || 'DIRECT'] = (proxyStats.perProxy[proxy || 'DIRECT'] || 0) + 1;
                try { await browser.close(); } catch (e) { }
                proxyAttempts++;
                if (proxyAttempts >= MAX_PROXY_ATTEMPTS) {
                    console.log('  ❌ Max proxy attempts reached due to errors. Triggering self-restart.');
                    console.log('  🔍 Proxy stats:', JSON.stringify(proxyStats));
                    await triggerSelfRestart();
                    process.exit(0);
                }
                const wait = PROXY_RETRY_DELAY_MIN + Math.random() * (PROXY_RETRY_DELAY_MAX - PROXY_RETRY_DELAY_MIN);
                console.log(`  ⏳ Waiting ${Math.round(wait / 1000)}s before next proxy attempt (error)...`);
                await new Promise(r => setTimeout(r, wait));
            }
        }

        // Longer random delay between batches to reduce detection risk
        const batchDelay = BATCH_DELAY_MIN + Math.random() * (BATCH_DELAY_MAX - BATCH_DELAY_MIN);
        console.log(`  ⏳ Waiting ${Math.round(batchDelay / 1000)}s before next batch...`);
        await new Promise(r => setTimeout(r, batchDelay));
    }

    const remaining = await getUrlData(sheets);
    if (remaining.length > 0) {
        await triggerSelfRestart();
    }

    console.log('🔍 Proxy stats:', JSON.stringify(proxyStats));
    console.log('\n🏁 Workflow complete.');
    process.exit(0);
})();
