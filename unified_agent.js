/**
 * UNIFIED GOOGLE ADS TRANSPARENCY AGENT
 * =====================================
 * DIRECT COMBINATION of agent.js + app_data_agent.js
 * NO MODIFICATIONS - exact same code from both agents
 * 
 * Sheet Structure:
 *   Column A: Advertiser Name
 *   Column B: Ads URL
 *   Column C: App Link
 *   Column D: App Name
 *   Column E: Video ID
 */

// ============================================
// EXACT IMPORTS FROM app_data_agent.js
// ============================================
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());
const { google } = require('googleapis');
const fs = require('fs');

// ============================================
// CONFIGURATION (Combined from both agents)
// ============================================
const SPREADSHEET_ID = '1l4JpCcA1GSkta1CE77WxD_YCgePHI87K7NtMu1Sd4Q0';
const SHEET_NAME = 'Sheet1';
const CREDENTIALS_PATH = './credentials.json';
const CONCURRENT_PAGES = parseInt(process.env.CONCURRENT_PAGES) || 2;
const MAX_WAIT_TIME = 60000;
const MAX_RETRIES = 3;
const POST_CLICK_WAIT = 12000; // From agent.js
const RETRY_WAIT_MULTIPLIER = 1.5;

// From app_data_agent.js
const BATCH_DELAY_MIN = parseInt(process.env.BATCH_DELAY_MIN) || 8000;
const BATCH_DELAY_MAX = parseInt(process.env.BATCH_DELAY_MAX) || 20000;

// Proxy settings from app_data_agent.js
const PROXIES = process.env.PROXIES ? process.env.PROXIES.split(';').map(p => p.trim()).filter(Boolean) : [];
const MAX_PROXY_ATTEMPTS = parseInt(process.env.MAX_PROXY_ATTEMPTS) || Math.max(3, PROXIES.length);
const PROXY_RETRY_DELAY_MIN = parseInt(process.env.PROXY_RETRY_DELAY_MIN) || 30000;
const PROXY_RETRY_DELAY_MAX = parseInt(process.env.PROXY_RETRY_DELAY_MAX) || 90000;

function pickProxy() {
    if (!PROXIES.length) return null;
    return PROXIES[Math.floor(Math.random() * PROXIES.length)];
}

const proxyStats = { totalBlocks: 0, perProxy: {} };

// EXACT from app_data_agent.js
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
];

// EXACT from app_data_agent.js
const VIEWPORTS = [
    { width: 1920, height: 1080 },
    { width: 1366, height: 768 },
    { width: 1536, height: 864 },
    { width: 1440, height: 900 },
    { width: 1280, height: 720 }
];

// EXACT from app_data_agent.js
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
    updates.forEach(({ rowIndex, storeLink, appName, videoId }) => {
        const rowNum = rowIndex + 1;

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
// SELF-RESTART (from app_data_agent.js)
// ============================================
async function triggerSelfRestart() {
    const repo = process.env.GITHUB_REPOSITORY;
    const token = process.env.GH_TOKEN;
    if (!repo || !token) return;

    console.log(`\n🔄 Triggering auto-restart...`);
    const https = require('https');
    const data = JSON.stringify({ event_type: 'unified_agent_trigger' });
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
// EXACT extractAppData FROM app_data_agent.js (lines 167-571)
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

        // =====================================================
        // CRITICAL FIX: Find the VISIBLE ad variation first
        // On Google Ads Transparency, multiple ad variations load
        // in different iframes, but only ONE is visible at a time.
        // We need to identify and extract from ONLY the visible one.
        // =====================================================

        // Step 1: Find the visible ad container on the MAIN page
        const visibleAdInfo = await page.evaluate(() => {
            // Look for the carousel/slider that shows ads
            // The visible ad is typically in a slide that is currently displayed
            const carouselSelectors = [
                '[class*="carousel"] [class*="active"]',
                '[class*="slider"] [class*="active"]',
                '[class*="slide"]:not([class*="hidden"])',
                '[aria-hidden="false"]',
                '.ad-preview-container:not([hidden])',
                '[class*="creative-preview"]:not([style*="display: none"])',
                '[class*="ad-container"]:not([style*="display: none"])'
            ];

            let visibleContainer = null;
            for (const sel of carouselSelectors) {
                const el = document.querySelector(sel);
                if (el && el.offsetWidth > 0 && el.offsetHeight > 0) {
                    visibleContainer = sel;
                    break;
                }
            }

            // Get all iframes and check which ones are VISIBLE (have dimensions)
            const iframes = document.querySelectorAll('iframe');
            const visibleFrameUrls = [];

            iframes.forEach(iframe => {
                // Check if iframe is visible
                const rect = iframe.getBoundingClientRect();
                const style = window.getComputedStyle(iframe);
                const isVisible = rect.width > 50 && rect.height > 50 &&
                    style.display !== 'none' &&
                    style.visibility !== 'hidden' &&
                    parseFloat(style.opacity) > 0;

                // Check if any parent is hidden
                let parent = iframe.parentElement;
                let parentVisible = true;
                while (parent) {
                    const parentStyle = window.getComputedStyle(parent);
                    if (parentStyle.display === 'none' ||
                        parentStyle.visibility === 'hidden' ||
                        parseFloat(parentStyle.opacity) === 0) {
                        parentVisible = false;
                        break;
                    }
                    parent = parent.parentElement;
                }

                if (isVisible && parentVisible) {
                    visibleFrameUrls.push(iframe.src || iframe.name || 'unnamed');
                }
            });

            return {
                containerSelector: visibleContainer,
                visibleFrameUrls: visibleFrameUrls,
                totalFrames: iframes.length
            };
        });

        console.log(`  📊 Found ${visibleAdInfo.totalFrames} iframes, ${visibleAdInfo.visibleFrameUrls.length} visible`);

        // Step 2: Extract from frames, but prioritize VISIBLE ones
        const frames = page.frames();
        let foundFromVisibleFrame = false;

        for (const frame of frames) {
            try {
                // Check if this frame's URL matches a visible iframe
                const frameUrl = frame.url() || '';
                const isLikelyVisible = visibleAdInfo.visibleFrameUrls.some(vfUrl =>
                    frameUrl.includes(vfUrl) || vfUrl.includes(frameUrl) ||
                    frameUrl.includes('tpc.googlesyndication') // Common ad frame URL
                );

                const frameData = await frame.evaluate((blacklist) => {
                    const data = { appName: null, storeLink: null, isVideo: false };
                    const root = document.querySelector('#portrait-landscape-phone') || document.body;

                    // Check if this frame content is visible (has dimensions)
                    const bodyRect = document.body.getBoundingClientRect();
                    if (bodyRect.width < 50 || bodyRect.height < 50) {
                        return { ...data, isHidden: true };
                    }

                    // =====================================================
                    // ULTRA-PRECISE STORE LINK EXTRACTOR
                    // Only accepts REAL Play Store / App Store links
                    // =====================================================
                    const extractStoreLink = (href) => {
                        if (!href || typeof href !== 'string') return null;
                        if (href.includes('javascript:') || href === '#') return null;

                        const isValidStoreLink = (url) => {
                            if (!url) return false;
                            const isPlayStore = url.includes('play.google.com/store/apps') && url.includes('id=');
                            const isAppStore = (url.includes('apps.apple.com') || url.includes('itunes.apple.com')) && url.includes('/app/');
                            return isPlayStore || isAppStore;
                        };

                        if (isValidStoreLink(href)) return href;

                        if (href.includes('googleadservices.com') || href.includes('/pagead/aclk')) {
                            try {
                                const patterns = [
                                    /[?&]adurl=([^&\s]+)/i,
                                    /[?&]dest=([^&\s]+)/i,
                                    /[?&]url=([^&\s]+)/i
                                ];
                                for (const pattern of patterns) {
                                    const match = href.match(pattern);
                                    if (match && match[1]) {
                                        const decoded = decodeURIComponent(match[1]);
                                        if (isValidStoreLink(decoded)) return decoded;
                                    }
                                }
                            } catch (e) { }
                        }

                        try {
                            const playMatch = href.match(/(https?:\/\/play\.google\.com\/store\/apps\/details\?id=[a-zA-Z0-9._]+)/);
                            if (playMatch && playMatch[1]) return playMatch[1];
                            const appMatch = href.match(/(https?:\/\/(apps|itunes)\.apple\.com\/[^\s&"']+\/app\/[^\s&"']+)/);
                            if (appMatch && appMatch[1]) return appMatch[1];
                        } catch (e) { }

                        return null;
                    };

                    // =====================================================
                    // CLEAN APP NAME
                    // =====================================================
                    const cleanAppName = (text) => {
                        if (!text || typeof text !== 'string') return null;
                        let clean = text.trim();
                        clean = clean.replace(/[\u200B-\u200D\uFEFF\u2066-\u2069]/g, '');
                        clean = clean.replace(/\.[a-zA-Z][\w-]*/g, ' ');
                        clean = clean.replace(/[a-zA-Z-]+\s*:\s*[^;]+;/g, ' ');
                        clean = clean.split('!@~!@~')[0];
                        if (clean.includes('|')) {
                            const parts = clean.split('|').map(p => p.trim()).filter(p => p.length > 2);
                            if (parts.length > 0) clean = parts[0];
                        }
                        clean = clean.replace(/\s+/g, ' ').trim();
                        if (clean.length < 2) return null;
                        if (/^[\d\s\W]+$/.test(clean)) return null;
                        return clean;
                    };

                    // =====================================================
                    // EXTRACTION - Find FIRST element with BOTH name + store link
                    // =====================================================
                    const appNameSelectors = [
                        'a[data-asoch-targets*="ochAppName"]',
                        'a[data-asoch-targets*="appname" i]',
                        'a[data-asoch-targets*="rrappname" i]',
                        'a[class*="short-app-name"]',
                        '.short-app-name a'
                    ];

                    for (const selector of appNameSelectors) {
                        const elements = root.querySelectorAll(selector);
                        for (const el of elements) {
                            const rawName = el.innerText || el.textContent || '';
                            const appName = cleanAppName(rawName);
                            if (!appName || appName.toLowerCase() === blacklist) continue;

                            const storeLink = extractStoreLink(el.href);
                            if (appName && storeLink) {
                                return { appName, storeLink, isVideo: true, isHidden: false };
                            } else if (appName && !data.appName) {
                                data.appName = appName;
                            }
                        }
                    }

                    // Backup: Install button for link
                    if (data.appName && !data.storeLink) {
                        const installSels = [
                            'a[data-asoch-targets*="ochButton"]',
                            'a[data-asoch-targets*="Install" i]',
                            'a[aria-label*="Install" i]'
                        ];
                        for (const sel of installSels) {
                            const el = root.querySelector(sel);
                            if (el && el.href) {
                                const storeLink = extractStoreLink(el.href);
                                if (storeLink) {
                                    data.storeLink = storeLink;
                                    data.isVideo = true;
                                    break;
                                }
                            }
                        }
                    }

                    // Fallback for app name only
                    if (!data.appName) {
                        const textSels = ['[role="heading"]', 'div[class*="app-name"]', '.app-title'];
                        for (const sel of textSels) {
                            const elements = root.querySelectorAll(sel);
                            for (const el of elements) {
                                const rawName = el.innerText || el.textContent || '';
                                const appName = cleanAppName(rawName);
                                if (appName && appName.toLowerCase() !== blacklist) {
                                    data.appName = appName;
                                    break;
                                }
                            }
                            if (data.appName) break;
                        }
                    }

                    data.isHidden = false;
                    return data;
                }, blacklistName);

                // Skip hidden frames
                if (frameData.isHidden) continue;

                // If we found BOTH app name AND store link, use this immediately (high confidence)
                if (frameData.appName && frameData.storeLink && result.appName === 'NOT_FOUND') {
                    result.appName = cleanName(frameData.appName);
                    result.storeLink = frameData.storeLink;
                    result.isVideo = frameData.isVideo;
                    foundFromVisibleFrame = true;
                    console.log(`  ✓ Found: ${result.appName} -> ${result.storeLink.substring(0, 60)}...`);
                    break; // We have both, stop searching
                }

                // If we only found name (no link), store it but keep looking
                if (frameData.appName && !frameData.storeLink && result.appName === 'NOT_FOUND') {
                    result.appName = cleanName(frameData.appName);
                    result.isVideo = frameData.isVideo;
                    // DON'T break - continue looking for a frame with BOTH name+link
                }
            } catch (e) { }
        }

        // If we still don't have appName, try meta tags/title as fallback
        if (result.appName === 'NOT_FOUND') {
            try {
                const pageSource = await page.content();
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

// ============================================
// EXACT extractVideoId FROM agent.js (lines 87-188)
// ============================================
async function extractVideoId(url, browser, attempt = 1, baseWaitTime = POST_CLICK_WAIT) {
    const page = await browser.newPage();
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    let videoSourceId = null;

    // IMPORTANT: Only block heavy images/fonts, but allow scripts and media
    await page.setRequestInterception(true);
    page.on('request', (request) => {
        const resourceType = request.resourceType();
        const requestUrl = request.url();

        // Check for video ID in ALL requests
        if (requestUrl.includes('googlevideo.com/videoplayback')) {
            const urlParams = new URLSearchParams(requestUrl.split('?')[1]);
            const id = urlParams.get('id');
            if (id && /^[a-f0-9]{16}$/.test(id)) {
                videoSourceId = id;
            }
        }

        if (['image', 'font'].includes(resourceType)) {
            request.abort();
            return;
        }
        request.continue();
    });

    try {
        await page.goto(url, { waitUntil: 'networkidle2', timeout: MAX_WAIT_TIME });

        const initialWait = 3000 * Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(initialWait);

        // Find and click play button
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
            const client = await page.target().createCDPSession();
            await client.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: playButtonInfo.x, y: playButtonInfo.y });
            await sleep(100);
            await client.send('Input.dispatchMouseEvent', { type: 'mousePressed', x: playButtonInfo.x, y: playButtonInfo.y, button: 'left', clickCount: 1 });
            await sleep(80);
            await client.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x: playButtonInfo.x, y: playButtonInfo.y, button: 'left', clickCount: 1 });

            const waitTime = baseWaitTime * Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
            await sleep(waitTime);
        }

        await page.close();
        return videoSourceId;
    } catch (err) {
        console.error(`  ❌ Error (attempt ${attempt}): ${err.message}`);
        await page.close();
        return null;
    }
}

// EXACT extractVideoIdWithRetry FROM agent.js (lines 191-214)
async function extractVideoIdWithRetry(url, browser, rowIndex) {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        if (attempt > 1) {
            console.log(`  🔄 [${rowIndex + 1}] Retry attempt ${attempt}/${MAX_RETRIES}...`);
        }

        const videoId = await extractVideoId(url, browser, attempt, POST_CLICK_WAIT);

        if (videoId) {
            if (attempt > 1) {
                console.log(`  ✅ [${rowIndex + 1}] Video ID found on attempt ${attempt}: ${videoId}`);
            }
            return videoId;
        }

        if (attempt < MAX_RETRIES) {
            const retryDelay = 2000 * Math.pow(2, attempt - 1);
            console.log(`  ⏳ [${rowIndex + 1}] Waiting ${retryDelay}ms before retry...`);
            await new Promise(r => setTimeout(r, retryDelay));
        }
    }

    return 'NOT_FOUND';
}

// EXACT extractWithRetry FROM app_data_agent.js (lines 574-582)
async function extractAppDataWithRetry(url, browser) {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        const data = await extractAppData(url, browser, attempt);
        if (data.appName === 'BLOCKED') return data;
        if (data.appName !== 'NOT_FOUND' || data.storeLink !== 'NOT_FOUND') return data;
        await randomDelay(2000, 4000);
    }
    return { appName: 'NOT_FOUND', storeLink: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION (Pattern from app_data_agent.js)
// ============================================
(async () => {
    console.log(`🤖 Starting UNIFIED Google Ads Agent...\n`);
    console.log(`📋 Sheet: ${SHEET_NAME}`);
    console.log(`⚡ Columns: A=Advertiser Name, B=Ads URL, C=App Link, D=App Name, E=Video ID\n`);

    const sessionStartTime = Date.now();
    const MAX_RUNTIME = 330 * 60 * 1000;

    const sheets = await getGoogleSheetsClient();
    const toProcess = await getUrlData(sheets);

    if (toProcess.length === 0) {
        console.log('✨ All rows are complete. Nothing to process.');
        process.exit(0);
    }

    const needsMeta = toProcess.filter(x => x.needsMetadata).length;
    const needsVideo = toProcess.filter(x => x.needsVideoId).length;
    console.log(`📊 Found ${toProcess.length} rows to process:`);
    console.log(`   - ${needsMeta} need metadata extraction`);
    console.log(`   - ${needsVideo} need video ID extraction\n`);

    console.log(PROXIES.length ? `🔁 Proxy rotation enabled (${PROXIES.length} proxies)` : '🔁 Running direct (no PROXIES env var)');

    for (let i = 0; i < toProcess.length; i += CONCURRENT_PAGES) {
        if (Date.now() - sessionStartTime > MAX_RUNTIME) {
            console.log('\n⏰ Time limit reached. Restarting...');
            await triggerSelfRestart();
            process.exit(0);
        }

        const batch = toProcess.slice(i, i + CONCURRENT_PAGES);
        console.log(`📦 Batch ${Math.floor(i / CONCURRENT_PAGES) + 1}/${Math.ceil(toProcess.length / CONCURRENT_PAGES)}`);

        let proxyAttempts = 0;
        let handled = false;

        while (!handled && proxyAttempts < MAX_PROXY_ATTEMPTS) {
            const proxy = pickProxy();
            const launchArgs = [
                '--autoplay-policy=no-user-gesture-required',
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage'
            ];
            if (proxy) launchArgs.push(`--proxy-server=${proxy}`);

            console.log(`  🌐 Browser (proxy: ${proxy || 'DIRECT'})`);
            const browser = await puppeteer.launch({ headless: true, args: launchArgs });

            try {
                const results = await Promise.all(batch.map(async (item) => {
                    let storeLink = 'SKIP';
                    let appName = 'SKIP';
                    let videoId = 'SKIP';

                    // STEP 1: Extract metadata if needed (using app_data_agent.js logic)
                    if (item.needsMetadata) {
                        console.log(`  📊 [Row ${item.rowIndex + 1}] Extracting metadata...`);
                        const metaData = await extractAppDataWithRetry(item.url, browser);
                        storeLink = metaData.storeLink;
                        appName = metaData.appName;
                    }

                    // STEP 2: Extract video ID if needed (using agent.js logic)
                    const finalStoreLink = storeLink !== 'SKIP' ? storeLink : item.existingStoreLink;
                    const hasValidLink = finalStoreLink &&
                        finalStoreLink !== 'NOT_FOUND' &&
                        (finalStoreLink.includes('play.google.com') || finalStoreLink.includes('apps.apple.com'));

                    if (item.needsVideoId || (item.needsMetadata && hasValidLink)) {
                        console.log(`  🎬 [Row ${item.rowIndex + 1}] Extracting video ID...`);
                        videoId = await extractVideoIdWithRetry(item.url, browser, item.rowIndex);
                        console.log(`  📊 [${item.rowIndex + 1}] Video ID: ${videoId}`);
                    }

                    return { rowIndex: item.rowIndex, storeLink, appName, videoId };
                }));

                results.forEach(r => {
                    console.log(`  → Row ${r.rowIndex + 1}: Link=${r.storeLink?.substring(0, 40) || 'SKIP'}... | Name=${r.appName} | VideoID=${r.videoId}`);
                });

                if (results.some(r => r.storeLink === 'BLOCKED' || r.appName === 'BLOCKED')) {
                    console.log('  🛑 Block detected. Rotating...');
                    proxyStats.totalBlocks++;
                    proxyStats.perProxy[proxy || 'DIRECT'] = (proxyStats.perProxy[proxy || 'DIRECT'] || 0) + 1;
                    await browser.close();
                    proxyAttempts++;
                    if (proxyAttempts >= MAX_PROXY_ATTEMPTS) {
                        console.log('  ❌ Max attempts. Restarting...');
                        await triggerSelfRestart();
                        process.exit(0);
                    }
                    const wait = PROXY_RETRY_DELAY_MIN + Math.random() * (PROXY_RETRY_DELAY_MAX - PROXY_RETRY_DELAY_MIN);
                    console.log(`  ⏳ Waiting ${Math.round(wait / 1000)}s...`);
                    await new Promise(r => setTimeout(r, wait));
                    continue;
                }

                await batchWriteToSheet(sheets, results);
                handled = true;
                await browser.close();
            } catch (err) {
                console.error(`  ❌ Batch error: ${err.message}`);
                try { await browser.close(); } catch (e) { }
                proxyAttempts++;
                if (proxyAttempts >= MAX_PROXY_ATTEMPTS) {
                    await triggerSelfRestart();
                    process.exit(0);
                }
                await new Promise(r => setTimeout(r, PROXY_RETRY_DELAY_MIN + Math.random() * (PROXY_RETRY_DELAY_MAX - PROXY_RETRY_DELAY_MIN)));
            }
        }

        const batchDelay = BATCH_DELAY_MIN + Math.random() * (BATCH_DELAY_MAX - BATCH_DELAY_MIN);
        console.log(`  ⏳ Waiting ${Math.round(batchDelay / 1000)}s...\n`);
        await new Promise(r => setTimeout(r, batchDelay));
    }

    const remaining = await getUrlData(sheets);
    if (remaining.length > 0) {
        console.log(`📈 ${remaining.length} more rows. Restarting...`);
        await triggerSelfRestart();
    }

    console.log('🔍 Proxy stats:', JSON.stringify(proxyStats));
    console.log('\n🏁 Complete.');
    process.exit(0);
})();
