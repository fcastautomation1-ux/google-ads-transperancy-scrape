/**
 * UNIFIED GOOGLE ADS TRANSPARENCY AGENT - IMAGE ADS
 * ==================================================
 * Extracts data from Image Ads using hover-based extraction
 * 
 * Sheet Structure:
 *   Column A: Advertiser Name
 *   Column B: Ads URL (input)
 *   Column C: App Link (Store Link)
 *   Column D: App Name (from landscape-app-title)
 *   Column E: Image URL (from landscape-image)
 *   Column F: App Subtitle/Tagline (from landscape-app-text)
 *   Column M: Timestamp
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
const SHEET_NAME = process.env.SHEET_NAME || 'Test'; // Can be overridden via env var
const CREDENTIALS_PATH = './credentials.json';
const SHEET_BATCH_SIZE = parseInt(process.env.SHEET_BATCH_SIZE) || 1000; // Rows to load per batch
const CONCURRENT_PAGES = parseInt(process.env.CONCURRENT_PAGES) || 5; // Balanced: faster but safe
const MAX_WAIT_TIME = 60000;
const MAX_RETRIES = 2;  // Reduced from 4 to 2, increased wait time instead
const POST_CLICK_WAIT = 6000;
const RETRY_WAIT_MULTIPLIER = 1.5;  // Increased multiplier for longer waits
const PAGE_LOAD_DELAY_MIN = parseInt(process.env.PAGE_LOAD_DELAY_MIN) || 2000; // Increased from 1000
const PAGE_LOAD_DELAY_MAX = parseInt(process.env.PAGE_LOAD_DELAY_MAX) || 4000; // Increased from 3000

const BATCH_DELAY_MIN = parseInt(process.env.BATCH_DELAY_MIN) || 8000; // Increased from 5000
const BATCH_DELAY_MAX = parseInt(process.env.BATCH_DELAY_MAX) || 15000; // Increased from 10000

const PROXIES = process.env.PROXIES ? process.env.PROXIES.split(';').map(p => p.trim()).filter(Boolean) : [];
const MAX_PROXY_ATTEMPTS = parseInt(process.env.MAX_PROXY_ATTEMPTS) || Math.max(3, PROXIES.length);
const PROXY_RETRY_DELAY_MIN = parseInt(process.env.PROXY_RETRY_DELAY_MIN) || 25000;
const PROXY_RETRY_DELAY_MAX = parseInt(process.env.PROXY_RETRY_DELAY_MAX) || 75000;

function pickProxy() {
    if (!PROXIES.length) return null;
    return PROXIES[Math.floor(Math.random() * PROXIES.length)];
}

const proxyStats = { totalBlocks: 0, perProxy: {} };

// ============================================
// IMAGE AD VALIDATION - Check if ad matches expected structure
// ============================================
function isValidImageAdStructure(frameContent) {
    try {
        // Must have: image, title, and description
        const hasImage = frameContent.querySelector('img.landscape-image, img#landscape-image, .landscape-image img');
        const hasTitle = frameContent.querySelector('span.landscape-app-title, span#landscape-app-title, .landscape-app-title');
        const hasDescription = frameContent.querySelector('div.landscape-app-text, div#landscape-app-text, .landscape-app-text');

        if (!hasImage) {
            return false;
        }
        if (!hasTitle) {
            return false;
        }
        if (!hasDescription) {
            return false;
        }

        // Validate that title and description have actual text
        const titleText = (hasTitle.innerText || hasTitle.textContent || '').trim();
        const descText = (hasDescription.innerText || hasDescription.textContent || '').trim();

        if (!titleText || titleText.length < 2) {
            return false;
        }
        if (!descText || descText.length < 2) {
            return false;
        }

        return true;
    } catch (e) {
        return false;
    }
}

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
];

const VIEWPORTS = [
    { width: 1920, height: 1080 },
    { width: 1366, height: 768 },
    { width: 1536, height: 864 },
    { width: 1440, height: 900 },
    { width: 1280, height: 720 },
    { width: 1600, height: 900 },
    { width: 1920, height: 1200 },
    { width: 1680, height: 1050 }
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

async function getUrlData(sheets, batchSize = SHEET_BATCH_SIZE) {
    const toProcess = [];
    let startRow = 1; // Start from row 2 (skip header)
    let hasMoreData = true;
    let totalProcessed = 0;

    console.log(`📊 Loading data in batches of ${batchSize} rows...`);

    while (hasMoreData) {
        try {
            const endRow = startRow + batchSize - 1;
            const range = `${SHEET_NAME}!A${startRow + 1}:G${endRow + 1}`; // +1 because Google Sheets is 1-indexed

            const response = await sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: range,
            });

            const rows = response.data.values || [];

            if (rows.length === 0) {
                hasMoreData = false;
                break;
            }

            for (let i = 0; i < rows.length; i++) {
                const row = rows[i];
                const actualRowIndex = startRow + i; // Actual row number in sheet
                const url = row[1]?.trim() || '';
                const storeLink = row[2]?.trim() || '';
                const appName = row[3]?.trim() || '';
                const videoId = row[4]?.trim() || '';
                const appSubtitle = row[5]?.trim() || '';
                const imageUrl = row[6]?.trim() || '';

                if (!url) continue;

                // SKIP ONLY: Rows with Play Store link in Column C
                const hasPlayStoreLink = storeLink && storeLink.includes('play.google.com');
                if (hasPlayStoreLink) {
                    continue; // Skip - already has Play Store link
                }

                // Process all other rows
                const needsMetadata = !storeLink || !appName || !appSubtitle || !imageUrl;
                toProcess.push({
                    url,
                    rowIndex: actualRowIndex,
                    needsMetadata,
                    needsVideoId: true,
                    existingStoreLink: storeLink
                });
            }

            totalProcessed += rows.length;
            console.log(`  ✓ Processed ${totalProcessed} rows, found ${toProcess.length} to process`);

            // If we got less than batchSize rows, we've reached the end
            if (rows.length < batchSize) {
                hasMoreData = false;
            } else {
                startRow = endRow + 1;
                // Small delay between batches to avoid rate limits
                await sleep(100);
            }
        } catch (error) {
            console.error(`  ⚠️ Error loading batch starting at row ${startRow}: ${error.message}`);
            // If error, try to continue with next batch
            startRow += batchSize;
            await sleep(500); // Wait a bit longer on error
        }
    }

    console.log(`📊 Total: ${totalProcessed} rows scanned, ${toProcess.length} need processing\n`);
    return toProcess;
}

async function batchWriteToSheet(sheets, updates) {
    if (updates.length === 0) return;

    const data = [];
    updates.forEach(({ rowIndex, advertiserName, storeLink, appName, videoId, appSubtitle, imageUrl }) => {
        const rowNum = rowIndex + 1;

        // Column A: Advertiser Name (optional)
        if (advertiserName && advertiserName !== 'SKIP' && advertiserName !== 'NOT_FOUND') {
            data.push({ range: `${SHEET_NAME}!A${rowNum}`, values: [[advertiserName]] });
        }

        // Column C: Store Link (optional)
        if (storeLink && storeLink !== 'SKIP' && storeLink !== 'NOT_FOUND') {
            data.push({ range: `${SHEET_NAME}!C${rowNum}`, values: [[storeLink]] });
        }

        // Column D: App Name
        if (appName && appName !== 'SKIP') {
            data.push({ range: `${SHEET_NAME}!D${rowNum}`, values: [[appName || 'NOT_FOUND']] });
        }

        // Column E: Image URL (for image ads) - Previously Video ID
        // For image ads, we write the image URL here instead of video ID
        if (imageUrl && imageUrl !== 'SKIP' && imageUrl !== 'NOT_FOUND') {
            data.push({ range: `${SHEET_NAME}!E${rowNum}`, values: [[imageUrl]] });
        } else if (videoId && videoId !== 'SKIP' && videoId !== 'NOT_FOUND') {
            // Fallback: write video ID if it exists (for video ads)
            data.push({ range: `${SHEET_NAME}!E${rowNum}`, values: [[videoId]] });
        } else {
            data.push({ range: `${SHEET_NAME}!E${rowNum}`, values: [['NOT_FOUND']] });
        }

        // Column F: App Subtitle/Tagline
        data.push({ range: `${SHEET_NAME}!F${rowNum}`, values: [[appSubtitle || 'NOT_FOUND']] });

        // Write Timestamp to Column M (Pakistan Time)
        const timestamp = new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' });
        data.push({ range: `${SHEET_NAME}!M${rowNum}`, values: [[timestamp]] });
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
        videoId: 'SKIP',
        appSubtitle: needsMetadata ? 'NOT_FOUND' : 'SKIP',
        imageUrl: needsMetadata ? 'NOT_FOUND' : 'SKIP'
    };
    let capturedVideoId = null;

    // Clean name function - removes CSS garbage and normalizes
    const cleanName = (name) => {
        if (!name) return 'NOT_FOUND';
        let cleaned = name.trim();

        // Remove invisible unicode
        cleaned = cleaned.replace(/[\u200B-\u200D\uFEFF\u2066-\u2069]/g, '');

        // Remove CSS-like patterns
        cleaned = cleaned.replace(/[a-zA-Z-]+\s*:\s*[^;]+;?/g, ' ');
        cleaned = cleaned.replace(/\d+px/g, ' ');
        cleaned = cleaned.replace(/\*+/g, ' ');
        cleaned = cleaned.replace(/\.[a-zA-Z][\w-]*/g, ' ');

        // Remove special markers
        cleaned = cleaned.split('!@~!@~')[0];
        if (cleaned.includes('|')) {
            cleaned = cleaned.split('|')[0];
        }

        // Normalize whitespace
        cleaned = cleaned.replace(/\s+/g, ' ').trim();

        // Length check
        if (cleaned.length < 2 || cleaned.length > 80) return 'NOT_FOUND';

        // Reject if looks like CSS
        if (/:\s*\d/.test(cleaned) || cleaned.includes('height') || cleaned.includes('width') || cleaned.includes('font')) {
            return 'NOT_FOUND';
        }

        return cleaned || 'NOT_FOUND';
    };

    // ENHANCED ANTI-DETECTION - More comprehensive fingerprint masking
    const userAgent = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
    await page.setUserAgent(userAgent);

    const viewport = VIEWPORTS[Math.floor(Math.random() * VIEWPORTS.length)];
    await page.setViewport(viewport);

    // Random screen properties for more realistic fingerprint
    const screenWidth = viewport.width + Math.floor(Math.random() * 100) - 50;
    const screenHeight = viewport.height + Math.floor(Math.random() * 100) - 50;

    await page.evaluateOnNewDocument((screenW, screenH) => {
        // Remove webdriver flag
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

        // Chrome runtime
        window.chrome = { runtime: {} };

        // Plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
            configurable: true
        });

        // Languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
            configurable: true
        });

        // Platform
        Object.defineProperty(navigator, 'platform', {
            get: () => /Win/.test(navigator.userAgent) ? 'Win32' :
                /Mac/.test(navigator.userAgent) ? 'MacIntel' : 'Linux x86_64',
            configurable: true
        });

        // Hardware concurrency (randomize CPU cores)
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 4 + Math.floor(Math.random() * 4), // 4-8 cores
            configurable: true
        });

        // Device memory (randomize RAM)
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => [4, 8, 16][Math.floor(Math.random() * 3)],
            configurable: true
        });

        // Screen properties
        Object.defineProperty(screen, 'width', { get: () => screenW, configurable: true });
        Object.defineProperty(screen, 'height', { get: () => screenH, configurable: true });
        Object.defineProperty(screen, 'availWidth', { get: () => screenW, configurable: true });
        Object.defineProperty(screen, 'availHeight', { get: () => screenH - 40, configurable: true });

        // Permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );

        // Canvas fingerprint protection (add noise)
        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function () {
            const context = this.getContext('2d');
            if (context) {
                const imageData = context.getImageData(0, 0, this.width, this.height);
                for (let i = 0; i < imageData.data.length; i += 4) {
                    imageData.data[i] += Math.random() * 0.01 - 0.005; // Tiny noise
                }
                context.putImageData(imageData, 0, 0);
            }
            return originalToDataURL.apply(this, arguments);
        };
    }, screenWidth, screenHeight);

    // SPEED OPTIMIZATION - Block unnecessary resources (but allow ad images for extraction)
    await page.setRequestInterception(true);
    page.on('request', (request) => {
        const requestUrl = request.url();
        const resourceType = request.resourceType();

        // ALLOW googlesyndication images - we need them for extraction!
        if (requestUrl.includes('googlesyndication.com/simgad') ||
            requestUrl.includes('tpc.googlesyndication.com')) {
            request.continue();
            return;
        }

        // Abort other resource types for speed: font, stylesheet, and tracking
        const blockedTypes = ['font', 'other', 'stylesheet'];
        const blockedPatterns = [
            'analytics', 'google-analytics', 'doubleclick', 'pagead',
            'facebook.com', 'bing.com', 'logs', 'collect', 'securepubads'
        ];

        if (blockedTypes.includes(resourceType) || blockedPatterns.some(p => requestUrl.includes(p))) {
            request.abort();
        } else {
            request.continue();
        }
    });

    try {
        console.log(`  🚀 Loading (${viewport.width}x${viewport.height}): ${url.substring(0, 50)}...`);

        // Random mouse movement before page load (more human-like)
        try {
            const client = await page.target().createCDPSession();
            await client.send('Input.dispatchMouseEvent', {
                type: 'mouseMoved',
                x: Math.random() * viewport.width,
                y: Math.random() * viewport.height
            });
        } catch (e) { /* Ignore if CDP not ready */ }

        // Enhanced headers with randomization
        const acceptLanguages = [
            'en-US,en;q=0.9',
            'en-US,en;q=0.9,zh-CN;q=0.8',
            'en-US,en;q=0.9,fr;q=0.8',
            'en-GB,en;q=0.9',
            'en-US,en;q=0.9,es;q=0.8'
        ];
        await page.setExtraHTTPHeaders({
            'accept-language': acceptLanguages[Math.floor(Math.random() * acceptLanguages.length)],
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'sec-ch-ua': `"Not_A Brand";v="8", "Chromium";v="${120 + Math.floor(Math.random() * 2)}", "Google Chrome";v="${120 + Math.floor(Math.random() * 2)}"`,
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': `"${/Win/.test(userAgent) ? 'Windows' : /Mac/.test(userAgent) ? 'macOS' : 'Linux'}"`,
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1'
        });

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

        // Wait for dynamic elements to settle (INCREASED for reliability)
        const baseWait = 5000 + Math.random() * 3000; // Increased: 5000-8000ms for better iframe loading
        const attemptMultiplier = Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(baseWait * attemptMultiplier);

        // Additional wait specifically for iframes to render (with timeout to prevent hanging)
        try {
            const iframeWaitPromise = page.evaluate(async () => {
                const iframes = document.querySelectorAll('iframe');
                if (iframes.length > 0) {
                    await new Promise((resolve, reject) => {
                        let loaded = 0;
                        const totalIframes = iframes.length;
                        let timeoutId;

                        const checkLoaded = () => {
                            loaded++;
                            if (loaded >= totalIframes) {
                                clearTimeout(timeoutId);
                                setTimeout(resolve, 1500); // Increased from 800ms for content to render
                            }
                        };

                        // Overall timeout: max 8 seconds for all iframes (increased from 5)
                        timeoutId = setTimeout(() => {
                            resolve(); // Force resolve after 8 seconds
                        }, 8000);

                        iframes.forEach(iframe => {
                            try {
                                if (iframe.contentDocument && iframe.contentDocument.readyState === 'complete') {
                                    checkLoaded();
                                } else {
                                    iframe.onload = checkLoaded;
                                    // Timeout after 3 seconds per iframe (increased from 2)
                                    setTimeout(checkLoaded, 3000);
                                }
                            } catch (e) {
                                // Cross-origin iframe, count as loaded
                                checkLoaded();
                            }
                        });
                        // If no iframes, resolve immediately
                        if (totalIframes === 0) {
                            clearTimeout(timeoutId);
                            resolve();
                        }
                    });
                }
            });

            // Wrap with timeout to prevent hanging in page.evaluate (increased to 10 seconds)
            await Promise.race([
                iframeWaitPromise,
                new Promise((resolve) => setTimeout(resolve, 10000)) // 10 second max timeout
            ]);
        } catch (e) {
            // If iframe check fails, wait longer and continue
            await sleep(2000);
        }

        // Random mouse movements for more human-like behavior
        try {
            const client = await page.target().createCDPSession();
            const movements = 2 + Math.floor(Math.random() * 3); // 2-4 movements
            for (let i = 0; i < movements; i++) {
                await client.send('Input.dispatchMouseEvent', {
                    type: 'mouseMoved',
                    x: Math.random() * viewport.width,
                    y: Math.random() * viewport.height
                });
                await sleep(200 + Math.random() * 300);
            }
        } catch (e) { /* Ignore if CDP fails */ }

        // All ads (video, text, image) will now be processed.

        // Human-like interaction (optimized for speed while staying safe)
        await page.evaluate(async () => {
            // Quick but natural scrolling with random pauses
            for (let i = 0; i < 3; i++) {
                window.scrollBy(0, 150 + Math.random() * 100);
                await new Promise(r => setTimeout(r, 200 + Math.random() * 150));
                // Random pause sometimes (30% chance)
                if (Math.random() < 0.3) {
                    await new Promise(r => setTimeout(r, 300 + Math.random() * 200));
                }
            }
            // Scroll back up a bit
            window.scrollBy(0, -100);
            await new Promise(r => setTimeout(r, 250));
        });

        // Random pause before extraction (10-30% chance, adds randomness)
        if (Math.random() < 0.2) {
            const randomPause = 500 + Math.random() * 1000;
            await sleep(randomPause);
        }

        // =====================================================
        // IMAGE AD EXTRACTION - HOVER BASED
        // Extracts: Image URL, App Name, App Subtitle
        // Writes to: Column D (App Name), Column E (Image URL), Column F (App Subtitle)
        // =====================================================
        if (needsMetadata) {
            console.log(`  📊 Extracting Image Ad data using hover...`);

            // Find all iframes that might contain the ad
            const frames = page.frames();
            let extractedData = null;
            let validImageAdFound = false;
            let extractionAttempts = 0;
            const MAX_EXTRACTION_ATTEMPTS = 3; // Try multiple times within same page

            // Helper function to extract data from a frame
            const extractFromFrame = async (frame, useHover = true) => {
                const data = { imageUrl: null, appName: null, appSubtitle: null };

                try {
                    // Get the image element handle
                    const imgElement = await frame.$('img.landscape-image, img#landscape-image, .landscape-image img');

                    if (imgElement && useHover) {
                        // Scroll element into view first
                        try {
                            await frame.evaluate(el => {
                                if (el) el.scrollIntoView({ block: 'center', behavior: 'instant' });
                            }, imgElement);
                            await sleep(300); // Increased from 200ms

                            // Hover over the image with timeout
                            await Promise.race([
                                imgElement.hover(),
                                new Promise((_, reject) => setTimeout(() => reject(new Error('Hover timeout')), 3000)) // Increased from 2s
                            ]);
                            await sleep(500); // Increased from 300ms for content to appear
                        } catch (hoverErr) {
                            // Hover failed, continue with extraction anyway
                            console.log(`  ⚠️ Hover failed, trying direct extraction...`);
                        }
                    }

                    // Extract data from frame
                    const extractPromise = frame.evaluate(() => {
                        const result = {
                            imageUrl: null,
                            appName: null,
                            appSubtitle: null
                        };

                        // =====================================================
                        // EXTRACT IMAGE URL (from the hovered image)
                        // =====================================================
                        const imageSelectors = [
                            'img.landscape-image',
                            'img#landscape-image',
                            '.landscape-image img',
                            'img[id*="landscape-image"]',
                            'img[class*="landscape-image"]'
                        ];

                        for (const sel of imageSelectors) {
                            const img = document.querySelector(sel);
                            if (img && img.src && img.src.startsWith('http')) {
                                // Make sure it's the googlesyndication image
                                if (img.src.includes('googlesyndication.com/simgad') ||
                                    img.src.includes('tpc.googlesyndication.com')) {
                                    result.imageUrl = img.src;
                                    break;
                                }
                                // Fallback: any valid image URL
                                if (!result.imageUrl) {
                                    result.imageUrl = img.src;
                                }
                            }
                        }

                        // =====================================================
                        // EXTRACT APP NAME (App Title)
                        // =====================================================
                        const appNameSelectors = [
                            'span.landscape-app-title',
                            'span#landscape-app-title',
                            '.landscape-app-title',
                            '[id*="landscape-app-title"]',
                            '[class*="landscape-app-title"]',
                            '.landscape-title-bar span'
                        ];

                        for (const sel of appNameSelectors) {
                            const el = document.querySelector(sel);
                            if (el) {
                                const text = (el.innerText || el.textContent || '').trim();
                                if (text && text.length >= 2 && text.length <= 100) {
                                    result.appName = text;
                                    break;
                                }
                            }
                        }

                        // =====================================================
                        // EXTRACT APP SUBTITLE (App Text/Description)
                        // =====================================================
                        const appSubtitleSelectors = [
                            'div.landscape-app-text',
                            'div#landscape-app-text',
                            '.landscape-app-text',
                            '[id*="landscape-app-text"]',
                            '[class*="landscape-app-text"]'
                        ];

                        for (const sel of appSubtitleSelectors) {
                            const el = document.querySelector(sel);
                            if (el) {
                                const text = (el.innerText || el.textContent || '').trim();
                                if (text && text.length >= 2 && text.length <= 200) {
                                    result.appSubtitle = text;
                                    break;
                                }
                            }
                        }

                        return result;
                    });

                    // Wrap extraction with timeout (max 6 seconds - increased from 4)
                    const extracted = await Promise.race([
                        extractPromise,
                        new Promise((resolve) => {
                            setTimeout(() => resolve({
                                imageUrl: null,
                                appName: null,
                                appSubtitle: null
                            }), 6000);
                        })
                    ]);

                    return extracted;
                } catch (e) {
                    return data;
                }
            };

            // Try extraction multiple times if needed
            while (extractionAttempts < MAX_EXTRACTION_ATTEMPTS && !extractedData?.imageUrl) {
                extractionAttempts++;

                if (extractionAttempts > 1) {
                    console.log(`  🔄 Extraction attempt ${extractionAttempts}/${MAX_EXTRACTION_ATTEMPTS}...`);
                    await sleep(1500); // Wait before retry
                }

                for (const frame of frames) {
                    try {
                        // Validate frame has ONLY the correct image ad structure (title + image + description)
                        const isValidImageAd = await Promise.race([
                            frame.evaluate(() => {
                                const img = document.querySelector('img.landscape-image, img#landscape-image, .landscape-image img');
                                const title = document.querySelector('span.landscape-app-title, span#landscape-app-title, .landscape-app-title');
                                const desc = document.querySelector('div.landscape-app-text, div#landscape-app-text, .landscape-app-text');

                                if (!img || !title || !desc) return false;

                                // Validate they have content
                                const titleText = (title.innerText || title.textContent || '').trim();
                                const descText = (desc.innerText || desc.textContent || '').trim();

                                return titleText.length >= 2 && descText.length >= 2;
                            }),
                            new Promise((resolve) => setTimeout(() => resolve(false), 3000)) // 3s timeout for validation
                        ]);

                        if (!isValidImageAd) {
                            continue;  // Skip this frame - not a valid image ad
                        }

                        validImageAdFound = true;
                        console.log(`  ✅ Found valid image ad structure, extracting (attempt ${extractionAttempts})...`);

                        // Try with hover first
                        extractedData = await extractFromFrame(frame, true);

                        // If hover extraction didn't get all data, try without hover
                        if (!extractedData?.imageUrl || !extractedData?.appName || !extractedData?.appSubtitle) {
                            console.log(`  🔄 Partial data, trying direct extraction...`);
                            await sleep(500);
                            const directData = await extractFromFrame(frame, false);

                            // Merge results - prefer existing data but fill in missing
                            if (directData) {
                                extractedData = extractedData || {};
                                extractedData.imageUrl = extractedData.imageUrl || directData.imageUrl;
                                extractedData.appName = extractedData.appName || directData.appName;
                                extractedData.appSubtitle = extractedData.appSubtitle || directData.appSubtitle;
                            }
                        }

                        // If we found at least the image URL and app name, we're good
                        if (extractedData && extractedData.imageUrl && extractedData.appName) {
                            console.log(`  ✓ Extraction successful!`);
                            break;
                        }
                    } catch (e) {
                        // Frame access error, continue to next frame
                        continue;
                    }
                }

                // If we got data, break retry loop
                if (extractedData?.imageUrl && extractedData?.appName) {
                    break;
                }
            }

            // Apply extracted data to result
            if (extractedData) {
                if (extractedData.appName) {
                    result.appName = cleanName(extractedData.appName);
                    console.log(`  ✓ App Name (D): ${result.appName}`);
                }
                if (extractedData.imageUrl) {
                    result.imageUrl = extractedData.imageUrl;
                    console.log(`  ✓ Image URL (E): ${result.imageUrl.substring(0, 60)}...`);
                }
                if (extractedData.appSubtitle) {
                    result.appSubtitle = extractedData.appSubtitle;
                    console.log(`  ✓ App Subtitle (F): ${result.appSubtitle}`);
                }
            } else if (!validImageAdFound) {
                console.log(`  ⏭️  Skipping: Not an image ad (no matching structure)`);
                result = {
                    advertiserName: 'SKIP',
                    appName: 'SKIP',
                    storeLink: 'SKIP',
                    videoId: 'SKIP',
                    appSubtitle: 'SKIP',
                    imageUrl: 'SKIP'
                };
            } else {
                console.log(`  ⚠️ No image ad data extracted after ${extractionAttempts} attempts`);
            }

            // Set video ID to SKIP for image ads (not applicable)
            result.videoId = 'SKIP';
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

        // If explicitly skipped (not an image ad), return as-is
        if (data.appName === 'SKIP' && data.imageUrl === 'SKIP') {
            return data;
        }

        // Success criteria for IMAGE ADS:
        // We need ALL three: appName, imageUrl, and appSubtitle
        const imageAdSuccess = (data.appName && data.appName !== 'NOT_FOUND' && data.appName !== 'SKIP') &&
            (data.imageUrl && data.imageUrl !== 'NOT_FOUND' && data.imageUrl !== 'SKIP') &&
            (data.appSubtitle && data.appSubtitle !== 'NOT_FOUND' && data.appSubtitle !== 'SKIP');

        if (imageAdSuccess) {
            return data;
        } else if (attempt === 1) {
            console.log(`  ⚠️ Attempt 1 - Incomplete data. Retrying with longer wait...`);
        }

        await randomDelay(3000, 6000);
    }
    // If we're here, we exhausted retries. Return whatever we have.
    return { advertiserName: 'NOT_FOUND', storeLink: 'NOT_FOUND', appName: 'NOT_FOUND', videoId: 'NOT_FOUND', appSubtitle: 'NOT_FOUND', imageUrl: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION
// ============================================
(async () => {
    console.log(`🤖 Starting IMAGE ADS Google Ads Agent...\n`);
    console.log(`📋 Sheet: ${SHEET_NAME}`);
    console.log(`⚡ Columns: D=App Name, E=Image URL, F=App Subtitle\n`);

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

    const PAGES_PER_BROWSER = 30; // Balanced: faster but safe
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
        // Reset adaptive counter for each browser session
        consecutiveSuccessBatches = 0;

        while (sessionProcessed < currentSessionSize && !blocked) {
            const batchSize = Math.min(CONCURRENT_PAGES, currentSessionSize - sessionProcessed);
            const batch = toProcess.slice(currentIndex, currentIndex + batchSize);

            console.log(`📦 Batch ${currentIndex + 1}-${currentIndex + batchSize} / ${toProcess.length}`);

            try {
                // Stagger page loads to avoid blocks - add delay between each concurrent page
                const results = await Promise.all(batch.map(async (item, index) => {
                    // Add random delay before starting each page (staggered)
                    if (index > 0) {
                        const staggerDelay = PAGE_LOAD_DELAY_MIN + Math.random() * (PAGE_LOAD_DELAY_MAX - PAGE_LOAD_DELAY_MIN);
                        await sleep(staggerDelay * index); // Each page waits progressively longer
                    }
                    const data = await extractWithRetry(item, browser);
                    return {
                        rowIndex: item.rowIndex,
                        advertiserName: data.advertiserName,
                        storeLink: data.storeLink,
                        appName: data.appName,
                        videoId: data.videoId,
                        appSubtitle: data.appSubtitle,
                        imageUrl: data.imageUrl
                    };
                }));

                results.forEach(r => {
                    console.log(`  → Row ${r.rowIndex + 1}: Advertiser=${r.advertiserName} | Link=${r.storeLink?.substring(0, 40) || 'SKIP'}... | Name=${r.appName} | Video=${r.videoId}`);
                });

                // Separate successful results from blocked ones
                const successfulResults = results.filter(r => r.storeLink !== 'BLOCKED' && r.appName !== 'BLOCKED');
                const blockedResults = results.filter(r => r.storeLink === 'BLOCKED' || r.appName === 'BLOCKED');

                // Always write successful results to sheet (even if some were blocked)
                if (successfulResults.length > 0) {
                    await batchWriteToSheet(sheets, successfulResults);
                    console.log(`  ✅ Wrote ${successfulResults.length} successful results to sheet`);
                }

                // If any results were blocked, mark for browser rotation
                if (blockedResults.length > 0) {
                    console.log(`  🛑 Block detected (${blockedResults.length} blocked, ${successfulResults.length} successful). Closing browser and rotating...`);
                    proxyStats.totalBlocks++;
                    proxyStats.perProxy[proxy || 'DIRECT'] = (proxyStats.perProxy[proxy || 'DIRECT'] || 0) + 1;
                    blocked = true;
                    consecutiveSuccessBatches = 0; // Reset on block
                } else {
                    consecutiveSuccessBatches++; // Track successful batches
                }

                // Update index for all processed items (both successful and blocked)
                currentIndex += batchSize;
                sessionProcessed += batchSize;
            } catch (err) {
                console.error(`  ❌ Batch error: ${err.message}`);
                currentIndex += batchSize;
                sessionProcessed += batchSize;
            }

            if (!blocked) {
                // Adaptive delay: reduce delay if we're having success (faster processing)
                const adaptiveMultiplier = Math.max(0.7, 1 - (consecutiveSuccessBatches * 0.05)); // Reduce delay by 5% per successful batch, min 70%
                const adjustedMin = BATCH_DELAY_MIN * adaptiveMultiplier;
                const adjustedMax = BATCH_DELAY_MAX * adaptiveMultiplier;
                const batchDelay = adjustedMin + Math.random() * (adjustedMax - adjustedMin);
                console.log(`  ⏳ Waiting ${Math.round(batchDelay / 1000)}s... (adaptive: ${Math.round(adaptiveMultiplier * 100)}%)`);
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