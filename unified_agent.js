/**
 * UNIFIED GOOGLE ADS TRANSPARENCY AGENT
 * =====================================
 * Combines app_data_agent.js + agent.js in ONE VISIT per URL
 * 
 * Sheet Structure:
 *   Column A: Advertiser Name
 *   Column B: Ads URL
 *   Column C: App Link
 *   Column D: App Name
 *   Column E: Video ID
 */

// EXACT IMPORTS FROM app_data_agent.js
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());
const { google } = require('googleapis');
const fs = require('fs');

// ============================================
// CONFIGURATION
// ============================================
const SPREADSHEET_ID = '1l4JpCcA1GSkta1CE77WxD_YCgePHI87K7NtMu1Sd4Q0';
const SHEET_NAME = 'Sheet1';
const CREDENTIALS_PATH = './credentials.json';
const CONCURRENT_PAGES = parseInt(process.env.CONCURRENT_PAGES) || 5; // Increased for speed
const MAX_WAIT_TIME = 60000;
const MAX_RETRIES = 3;
const POST_CLICK_WAIT = 8000; // Reduced from 12s
const RETRY_WAIT_MULTIPLIER = 1.3; // Reduced from 1.5

const BATCH_DELAY_MIN = parseInt(process.env.BATCH_DELAY_MIN) || 5000; // Increased for accuracy
const BATCH_DELAY_MAX = parseInt(process.env.BATCH_DELAY_MAX) || 10000; // Increased for accuracy

const PROXIES = process.env.PROXIES ? process.env.PROXIES.split(';').map(p => p.trim()).filter(Boolean) : [];
const MAX_PROXY_ATTEMPTS = parseInt(process.env.MAX_PROXY_ATTEMPTS) || Math.max(3, PROXIES.length);
const PROXY_RETRY_DELAY_MIN = parseInt(process.env.PROXY_RETRY_DELAY_MIN) || 30000;
const PROXY_RETRY_DELAY_MAX = parseInt(process.env.PROXY_RETRY_DELAY_MAX) || 90000;

function pickProxy() {
    if (!PROXIES.length) return null;
    return PROXIES[Math.floor(Math.random() * PROXIES.length)];
}

const proxyStats = { totalBlocks: 0, perProxy: {} };

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
];

const VIEWPORTS = [
    { width: 1920, height: 1080 },
    { width: 1366, height: 768 },
    { width: 1536, height: 864 },
    { width: 1440, height: 900 },
    { width: 1280, height: 720 }
];

const randomDelay = (min, max) => new Promise(r => setTimeout(r, min + Math.random() * (max - min)));
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ============================================
// GOOGLE SHEETS
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
        range: `${SHEET_NAME}!A:E`,
    });
    const rows = response.data.values || [];
    const toProcess = [];

    for (let i = 1; i < rows.length; i++) {
        const row = rows[i];
        const url = row[1]?.trim() || '';
        const storeLink = row[2]?.trim() || '';
        const appName = row[3]?.trim() || '';
        const videoId = row[4]?.trim() || '';

        if (!url) continue;

        const needsMetadata = !storeLink || !appName;
        const hasValidStoreLink = storeLink &&
            storeLink !== 'NOT_FOUND' &&
            (storeLink.includes('play.google.com') || storeLink.includes('apps.apple.com'));
        const needsVideoId = hasValidStoreLink && !videoId;

        if (needsMetadata || needsVideoId) {
            toProcess.push({
                url,
                rowIndex: i,
                needsMetadata,
                needsVideoId,
                existingStoreLink: storeLink
            });
        }
    }

    return toProcess;
}

async function batchWriteToSheet(sheets, updates) {
    if (updates.length === 0) return;

    const data = [];
    updates.forEach(({ rowIndex, advertiserName, storeLink, appName, videoId }) => {
        const rowNum = rowIndex + 1;
        if (advertiserName && advertiserName !== 'SKIP') {
            data.push({ range: `${SHEET_NAME}!A${rowNum}`, values: [[advertiserName]] });
        }
        if (storeLink && storeLink !== 'SKIP') {
            data.push({ range: `${SHEET_NAME}!C${rowNum}`, values: [[storeLink]] });
        }
        if (appName && appName !== 'SKIP') {
            data.push({ range: `${SHEET_NAME}!D${rowNum}`, values: [[appName]] });
        }
        if (videoId && videoId !== 'SKIP') {
            data.push({ range: `${SHEET_NAME}!E${rowNum}`, values: [[videoId]] });
        }
    });

    if (data.length === 0) return;

    try {
        await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            resource: { valueInputOption: 'RAW', data: data }
        });
        console.log(`  ✅ Wrote ${updates.length} results to sheet`);
    } catch (error) {
        console.error(`  ❌ Write error:`, error.message);
    }
}

// ============================================
// UNIFIED EXTRACTION - ONE VISIT PER URL
// Both metadata + video ID extracted on same page
// ============================================
async function extractAllInOneVisit(url, browser, needsMetadata, needsVideoId, existingStoreLink, attempt = 1) {
    const page = await browser.newPage();
    let result = {
        advertiserName: 'SKIP',
        appName: needsMetadata ? 'NOT_FOUND' : 'SKIP',
        storeLink: needsMetadata ? 'NOT_FOUND' : 'SKIP',
        videoId: 'SKIP'
    };
    let capturedVideoId = null;

    // Clean name function from app_data_agent.js
    const cleanName = (name) => {
        if (!name) return 'NOT_FOUND';
        let cleaned = name.replace(/[\u200B-\u200D\uFEFF\u2066-\u2069]/g, '').trim();
        cleaned = cleaned.split('!@~!@~')[0].trim();
        if (cleaned.includes('|')) {
            const parts = cleaned.split('|').map(p => p.trim()).filter(p => p.length > 0);
            const uniqueParts = [...new Set(parts)];
            cleaned = uniqueParts[0];
        }
        return cleaned;
    };

    // ANTI-DETECTION from app_data_agent.js
    const userAgent = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
    await page.setUserAgent(userAgent);

    const viewport = VIEWPORTS[Math.floor(Math.random() * VIEWPORTS.length)];
    await page.setViewport(viewport);

    await page.evaluateOnNewDocument(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    });

    // VIDEO ID CAPTURE + SPEED OPTIMIZATION
    await page.setRequestInterception(true);
    page.on('request', (request) => {
        const requestUrl = request.url();

        // Capture video ID from googlevideo.com requests
        if (requestUrl.includes('googlevideo.com/videoplayback')) {
            const urlParams = new URLSearchParams(requestUrl.split('?')[1]);
            const id = urlParams.get('id');
            if (id && /^[a-f0-9]{16}$/.test(id)) {
                capturedVideoId = id;
            }
        }

        const resourceType = request.resourceType();
        // Abort more resource types for speed: image, font, media (except video id capture), and tracking
        const blockedTypes = ['image', 'font', 'other'];
        const blockedPatterns = [
            'analytics', 'google-analytics', 'doubleclick', 'pagead',
            'facebook.com', 'bing.com', 'logs', 'collect'
        ];

        if (blockedTypes.includes(resourceType) || blockedPatterns.some(p => requestUrl.includes(p))) {
            request.abort();
        } else {
            request.continue();
        }
    });

    try {
        console.log(`  🚀 Loading (${viewport.width}x${viewport.height}): ${url.substring(0, 50)}...`);

        await page.setExtraHTTPHeaders({ 'accept-language': 'en-US,en;q=0.9' });

        // Increased wait strategy for accuracy - iframes need time to render content
        const response = await page.goto(url, { waitUntil: 'networkidle2', timeout: MAX_WAIT_TIME });

        const content = await page.content();
        if ((response && response.status && response.status() === 429) ||
            content.includes('Our systems have detected unusual traffic') ||
            content.includes('Too Many Requests') ||
            content.toLowerCase().includes('captcha') ||
            content.toLowerCase().includes('g-recaptcha') ||
            content.toLowerCase().includes('verify you are human')) {
            console.error('  ⚠️ BLOCKED');
            await page.close();
            return { advertiserName: 'BLOCKED', appName: 'BLOCKED', storeLink: 'BLOCKED', videoId: 'BLOCKED' };
        }

        // Wait for dynamic elements to settle (increased for accuracy)
        const baseWait = 4000 + Math.random() * 3000;
        const attemptMultiplier = Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(baseWait * attemptMultiplier);

        // Human-like interaction
        await page.evaluate(async () => {
            window.scrollBy(0, 300);
            await new Promise(r => setTimeout(r, 400));
            window.scrollBy(0, -100);
        });

        // =====================================================
        // PHASE 1: METADATA EXTRACTION
        // =====================================================
        if (needsMetadata) {
            console.log(`  📊 Extracting metadata...`);

            const mainPageInfo = await page.evaluate(() => {
                const getSafeText = (sel) => {
                    const el = document.querySelector(sel);
                    if (!el) return null;
                    const text = el.innerText.trim();
                    const blacklistWords = ['ad details', 'google ads', 'transparency center', 'about this ad'];
                    if (!text || blacklistWords.some(word => text.toLowerCase().includes(word)) || text.length < 2) return null;
                    return text;
                };

                const advertiserSelectors = [
                    '.advertiser-name',
                    '.advertiser-name-container',
                    'h1',
                    '.creative-details-page-header-text',
                    '.ad-details-heading'
                ];

                let advertiserName = null;
                for (const sel of advertiserSelectors) {
                    advertiserName = getSafeText(sel);
                    if (advertiserName) break;
                }

                const checkVideo = () => {
                    const videoEl = document.querySelector('video');
                    if (videoEl && videoEl.offsetWidth > 10 && videoEl.offsetHeight > 10) return true;
                    return document.body.innerText.includes('Format: Video');
                };

                return {
                    advertiserName: advertiserName || 'NOT_FOUND',
                    blacklist: advertiserName ? advertiserName.toLowerCase() : '',
                    isVideo: checkVideo()
                };
            });

            const blacklistName = mainPageInfo.blacklist;
            result.advertiserName = mainPageInfo.advertiserName;

            const frames = page.frames();
            for (const frame of frames) {
                try {
                    const frameData = await frame.evaluate((blacklist) => {
                        const data = { appName: null, storeLink: null };

                        const extractStoreLink = (href) => {
                            if (!href || typeof href !== 'string') return null;
                            const isValidStoreLink = (url) => {
                                if (!url) return false;
                                return (url.includes('play.google.com/store/apps/details') && url.includes('id=')) ||
                                    ((url.includes('apps.apple.com') || url.includes('itunes.apple.com')) && url.includes('/app/'));
                            };
                            if (isValidStoreLink(href)) return href;

                            // Check for adurl in redirects
                            if (href.includes('adurl=') || href.includes('dest=') || href.includes('url=')) {
                                try {
                                    const patterns = [/[?&]adurl=([^&\s]+)/i, /[?&]dest=([^&\s]+)/i, /[?&]url=([^&\s]+)/i];
                                    for (const pattern of patterns) {
                                        const match = href.match(pattern);
                                        if (match && match[1]) {
                                            const decoded = decodeURIComponent(match[1]);
                                            if (isValidStoreLink(decoded)) return decoded;
                                        }
                                    }
                                } catch (e) { }
                            }
                            return null;
                        };

                        const cleanAppName = (text) => {
                            if (!text || typeof text !== 'string') return null;
                            let clean = text.trim().replace(/[\u200B-\u200D\uFEFF]/g, '');
                            const badWords = ['ad details', 'install', 'download', 'google ads', 'visit site', 'open', 'play'];
                            if (badWords.some(w => clean.toLowerCase().includes(w)) && clean.length < 15) return null;
                            if (clean.toLowerCase() === blacklist) return null;
                            return clean.split('|')[0].split('!@~!@~')[0].trim();
                        };

                        // 1. Scan ALL links for store patterns
                        const allLinks = Array.from(document.querySelectorAll('a'));
                        for (const link of allLinks) {
                            const linkHref = link.href;
                            const extracted = extractStoreLink(linkHref);
                            if (extracted) {
                                data.storeLink = extracted;

                                // Try to get name from common app name nearby or data-attributes
                                const nameAttr = link.getAttribute('data-asoch-targets');
                                if (nameAttr && (nameAttr.includes('AppName') || nameAttr.includes('appname'))) {
                                    const cleaned = cleanAppName(link.innerText || link.textContent);
                                    if (cleaned) data.appName = cleaned;
                                }
                            }
                        }

                        // 2. Scan structured selectors for App Name if not found
                        if (!data.appName) {
                            const nameSels = [
                                'a[data-asoch-targets*="AppName"]',
                                'a[data-asoch-targets*="appname" i]',
                                '.short-app-name',
                                'div[class*="app-name"]',
                                '[role="heading"]',
                                '.app-title'
                            ];
                            for (const sel of nameSels) {
                                const els = document.querySelectorAll(sel);
                                for (const el of els) {
                                    const cleaned = cleanAppName(el.innerText || el.textContent);
                                    if (cleaned) {
                                        data.appName = cleaned;
                                        break;
                                    }
                                }
                                if (data.appName) break;
                            }
                        }

                        return data;
                    }, blacklistName);

                    if (frameData.appName && !result.appName || result.appName === 'NOT_FOUND') {
                        if (frameData.appName) result.appName = cleanName(frameData.appName);
                    }
                    if (frameData.storeLink && (result.storeLink === 'NOT_FOUND' || result.storeLink === 'SKIP')) {
                        result.storeLink = frameData.storeLink;
                    }

                    if (result.appName !== 'NOT_FOUND' && result.storeLink !== 'NOT_FOUND') break;
                } catch (e) { }
            }

            // Final fallback from Meta/Title
            if (result.appName === 'NOT_FOUND' || result.appName === 'Ad Details') {
                try {
                    const title = await page.title();
                    if (title && !title.toLowerCase().includes('google ads')) {
                        result.appName = title.split(' - ')[0].split('|')[0].trim();
                    }
                } catch (e) { }
            }
        }

        // =====================================================
        // PHASE 2: VIDEO ID EXTRACTION (from agent.js)
        // Done on SAME page, no second visit needed
        // =====================================================
        const finalStoreLink = result.storeLink !== 'SKIP' ? result.storeLink : existingStoreLink;
        const hasValidLink = finalStoreLink &&
            finalStoreLink !== 'NOT_FOUND' &&
            (finalStoreLink.includes('play.google.com') || finalStoreLink.includes('apps.apple.com'));

        if (needsVideoId || (needsMetadata && hasValidLink)) {
            console.log(`  🎬 Extracting Video ID...`);

            // Find and click play button (EXACT from agent.js)
            const playButtonInfo = await page.evaluate(() => {
                const results = { found: false, x: 0, y: 0 };
                const searchForPlayButton = (root) => {
                    const playButton = root.querySelector('.play-button');
                    if (playButton) {
                        const rect = playButton.getBoundingClientRect();
                        if (rect.width > 0 && rect.height > 0) {
                            results.found = true;
                            results.x = rect.left + rect.width / 2;
                            results.y = rect.top + rect.height / 2;
                            return true;
                        }
                    }
                    const elements = root.querySelectorAll('*');
                    for (const el of elements) {
                        if (el.shadowRoot) {
                            const found = searchForPlayButton(el.shadowRoot);
                            if (found) return true;
                        }
                    }
                    return false;
                };

                const iframes = document.querySelectorAll('iframe');
                for (let i = 0; i < iframes.length; i++) {
                    try {
                        const iframeDoc = iframes[i].contentDocument || iframes[i].contentWindow?.document;
                        if (iframeDoc) {
                            const found = searchForPlayButton(iframeDoc);
                            if (found) break;
                        }
                    } catch (e) { }
                }
                if (!results.found) searchForPlayButton(document);

                // Fallback: click center of visible iframe
                if (!results.found) {
                    for (const iframe of iframes) {
                        const rect = iframe.getBoundingClientRect();
                        if (rect.width > 0 && rect.height > 0) {
                            results.found = true;
                            results.x = rect.left + rect.width / 2;
                            results.y = rect.top + rect.height / 2;
                            break;
                        }
                    }
                }
                return results;
            });

            if (playButtonInfo.found) {
                try {
                    const client = await page.target().createCDPSession();
                    await client.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: playButtonInfo.x, y: playButtonInfo.y });
                    await sleep(100);
                    await client.send('Input.dispatchMouseEvent', { type: 'mousePressed', x: playButtonInfo.x, y: playButtonInfo.y, button: 'left', clickCount: 1 });
                    await sleep(80);
                    await client.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x: playButtonInfo.x, y: playButtonInfo.y, button: 'left', clickCount: 1 });

                    // Wait for video to load
                    const waitTime = POST_CLICK_WAIT * Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
                    await sleep(waitTime);

                    if (capturedVideoId) {
                        result.videoId = capturedVideoId;
                        console.log(`  ✓ Video ID: ${capturedVideoId}`);
                    } else {
                        result.videoId = 'NOT_FOUND';
                        console.log(`  ⚠️ No video ID captured`);
                    }
                } catch (e) {
                    console.log(`  ⚠️ Click failed: ${e.message}`);
                    result.videoId = 'NOT_FOUND';
                }
            } else {
                result.videoId = hasValidLink ? 'NOT_FOUND' : 'SKIP';
                console.log(`  ⚠️ No play button found`);
            }
        }

        await page.close();
        return result;
    } catch (err) {
        console.error(`  ❌ Error: ${err.message}`);
        await page.close();
        return { advertiserName: 'ERROR', appName: 'ERROR', storeLink: 'ERROR', videoId: 'ERROR' };
    }
}

async function extractWithRetry(item, browser) {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        if (attempt > 1) console.log(`  🔄 Retry ${attempt}/${MAX_RETRIES}...`);

        const data = await extractAllInOneVisit(
            item.url,
            browser,
            item.needsMetadata,
            item.needsVideoId,
            item.existingStoreLink,
            attempt
        );

        if (data.storeLink === 'BLOCKED' || data.appName === 'BLOCKED') return data;

        const gotMetadata = !item.needsMetadata || (data.storeLink !== 'NOT_FOUND' || data.appName !== 'NOT_FOUND');
        const gotVideoId = !item.needsVideoId || data.videoId !== 'NOT_FOUND';

        if (gotMetadata || gotVideoId) return data;

        await randomDelay(2000, 4000);
    }
    return { advertiserName: 'NOT_FOUND', storeLink: 'NOT_FOUND', appName: 'NOT_FOUND', videoId: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION
// ============================================
(async () => {
    console.log(`🤖 Starting UNIFIED Google Ads Agent...\n`);
    console.log(`📋 Sheet: ${SHEET_NAME}`);
    console.log(`⚡ Columns: A=Advertiser, B=URL, C=App Link, D=App Name, E=Video ID\n`);

    const sessionStartTime = Date.now();
    const MAX_RUNTIME = 330 * 60 * 1000;

    const sheets = await getGoogleSheetsClient();
    const toProcess = await getUrlData(sheets);

    if (toProcess.length === 0) {
        console.log('✨ All rows complete. Nothing to process.');
        process.exit(0);
    }

    const needsMeta = toProcess.filter(x => x.needsMetadata).length;
    const needsVideo = toProcess.filter(x => x.needsVideoId).length;
    console.log(`📊 Found ${toProcess.length} rows to process:`);
    console.log(`   - ${needsMeta} need metadata`);
    console.log(`   - ${needsVideo} need video ID\n`);

    console.log(PROXIES.length ? `🔁 Proxy rotation enabled (${PROXIES.length} proxies)` : '🔁 Running direct');

    const PAGES_PER_BROWSER = 40;
    let currentIndex = 0;

    while (currentIndex < toProcess.length) {
        if (Date.now() - sessionStartTime > MAX_RUNTIME) {
            console.log('\n⏰ Time limit reached. Stopping.');
            process.exit(0);
        }

        const remainingCount = toProcess.length - currentIndex;
        const currentSessionSize = Math.min(PAGES_PER_BROWSER, remainingCount);

        console.log(`\n🏢 Starting New Browser Session (Items ${currentIndex + 1} - ${currentIndex + currentSessionSize})`);

        let launchArgs = [
            '--autoplay-policy=no-user-gesture-required',
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--no-zygote',
            '--single-process',
            '--disable-software-rasterizer',
            '--no-first-run'
        ];

        const proxy = pickProxy();
        if (proxy) launchArgs.push(`--proxy-server=${proxy}`);

        console.log(`  🌐 Browser (proxy: ${proxy || 'DIRECT'})`);

        let browser;
        try {
            browser = await puppeteer.launch({
                headless: 'new',
                args: launchArgs,
                executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || null
            });
        } catch (launchError) {
            console.error(`  ❌ Failed to launch browser: ${launchError.message}`);
            await sleep(5000);
            try {
                browser = await puppeteer.launch({ headless: 'new', args: launchArgs });
            } catch (retryError) {
                console.error(`  ❌ Failed to launch browser on retry. Exiting.`);
                process.exit(1);
            }
        }

        let sessionProcessed = 0;
        let blocked = false;

        while (sessionProcessed < currentSessionSize && !blocked) {
            const batchSize = Math.min(CONCURRENT_PAGES, currentSessionSize - sessionProcessed);
            const batch = toProcess.slice(currentIndex, currentIndex + batchSize);

            console.log(`📦 Batch ${currentIndex + 1}-${currentIndex + batchSize} / ${toProcess.length}`);

            try {
                const results = await Promise.all(batch.map(async (item) => {
                    const data = await extractWithRetry(item, browser);
                    return {
                        rowIndex: item.rowIndex,
                        advertiserName: data.advertiserName,
                        storeLink: data.storeLink,
                        appName: data.appName,
                        videoId: data.videoId
                    };
                }));

                results.forEach(r => {
                    console.log(`  → Row ${r.rowIndex + 1}: Advertiser=${r.advertiserName} | Link=${r.storeLink?.substring(0, 40) || 'SKIP'}... | Name=${r.appName} | Video=${r.videoId}`);
                });

                if (results.some(r => r.storeLink === 'BLOCKED' || r.appName === 'BLOCKED')) {
                    console.log('  🛑 Block detected. Closing browser and rotating...');
                    proxyStats.totalBlocks++;
                    proxyStats.perProxy[proxy || 'DIRECT'] = (proxyStats.perProxy[proxy || 'DIRECT'] || 0) + 1;
                    blocked = true;
                } else {
                    await batchWriteToSheet(sheets, results);
                    currentIndex += batchSize;
                    sessionProcessed += batchSize;
                }
            } catch (err) {
                console.error(`  ❌ Batch error: ${err.message}`);
                currentIndex += batchSize;
                sessionProcessed += batchSize;
            }

            if (!blocked) {
                const batchDelay = BATCH_DELAY_MIN + Math.random() * (BATCH_DELAY_MAX - BATCH_DELAY_MIN);
                console.log(`  ⏳ Waiting ${Math.round(batchDelay / 1000)}s...`);
                await sleep(batchDelay);
            }
        }

        try {
            await browser.close();
            await sleep(2000);
        } catch (e) { }

        if (blocked) {
            const wait = PROXY_RETRY_DELAY_MIN + Math.random() * (PROXY_RETRY_DELAY_MAX - PROXY_RETRY_DELAY_MIN);
            console.log(`  ⏳ Block wait: ${Math.round(wait / 1000)}s...`);
            await sleep(wait);
        }
    }

    const remaining = await getUrlData(sheets);
    if (remaining.length > 0) {
        console.log(`📈 ${remaining.length} rows remaining for next scheduled run.`);
    }

    console.log('🔍 Proxy stats:', JSON.stringify(proxyStats));
    console.log('\n🏁 Complete.');
    process.exit(0);
})();
