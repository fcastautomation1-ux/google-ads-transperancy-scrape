const puppeteer = require('puppeteer');
const { google } = require('googleapis');
const fs = require('fs');

// ============================================
// CONFIGURATION
// ============================================
const SPREADSHEET_ID = '1beJ263B3m4L8pgD9RWsls-orKLUvLMfT2kExaiyNl7g';
const SHEET_NAME = 'Sheet1';
const CREDENTIALS_PATH = './credentials.json';
const CONCURRENT_PAGES = 3;
const MAX_WAIT_TIME = 60000;
const MAX_RETRIES = 3;
const RETRY_WAIT_MULTIPLIER = 1.5;

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
        range: `${SHEET_NAME}!A:H`,
    });
    const rows = response.data.values || [];

    // 1. Find the index of the LAST row that has data in Column G
    let lastProcessedIndex = 0;
    for (let i = rows.length - 1; i >= 1; i--) {
        if (rows[i][6]?.trim()) {
            lastProcessedIndex = i;
            break;
        }
    }

    console.log(`  🔍 Last processed row was: ${lastProcessedIndex + 1}`);

    const toProcess = [];
    // 2. Start collecting URLs ONLY from rows AFTER the last processed one
    for (let i = lastProcessedIndex + 1; i < rows.length; i++) {
        const row = rows[i];
        const url = row[0]?.trim();
        if (url) {
            toProcess.push({ url, rowIndex: i });
        }
    }
    return toProcess;
}

/**
 * SAFE WRITE SYSTEM: Finds the current row index of a URL before writing
 * This prevents mismatch if the sheet has been sorted or duplicates removed.
 */
async function safeBatchWrite(sheets, updates) {
    if (updates.length === 0) return;

    // 1. Re-fetch current Column A to find where these URLs are NOW
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A:A`,
    });
    const currentUrls = (response.data.values || []).map(r => r[0]?.trim());

    const data = [];
    updates.forEach(({ url, appName, storeLink }) => {
        // 2. Find the current position of this URL
        // We search the entire Column A to find the match
        const foundIndex = currentUrls.indexOf(url);

        if (foundIndex !== -1) {
            const rowNum = foundIndex + 1; // 1-indexed for Sheets
            data.push({
                range: `${SHEET_NAME}!G${rowNum}`,
                values: [[storeLink]]
            });
            data.push({
                range: `${SHEET_NAME}!H${rowNum}`,
                values: [[appName]]
            });
        } else {
            console.log(`  ⚠️ URL no longer found in sheet (deleted or moved): ${url.substring(0, 40)}...`);
        }
    });

    if (data.length === 0) return;

    try {
        await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            resource: {
                valueInputOption: 'RAW',
                data: data
            }
        });
        console.log(`  ✅ Safe-wrote ${updates.length} results to correct rows`);
    } catch (error) {
        console.error(`  ❌ Safe write error:`, error.message);
    }
}

// ============================================
// SELF-RESTART LOGIC
// ============================================
async function triggerSelfRestart() {
    const repo = process.env.GITHUB_REPOSITORY;
    const token = process.env.GH_TOKEN;
    if (!repo || !token) return;

    console.log(`\n🔄 Triggering auto-restart...`);
    const https = require('https');
    const data = JSON.stringify({ event_type: 'new_agent_trigger' });
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
    let result = { appName: 'NOT_FOUND', storeLink: 'NOT_FOUND' };

    // Set a realistic User-Agent to avoid early detection
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

    try {
        console.log(`  🚀 Loading: ${url.substring(0, 60)}...`);

        // Reverting to networkidle0 for maximum safety - wait for all ad resources
        await page.goto(url, { waitUntil: 'networkidle0', timeout: MAX_WAIT_TIME });

        // Check if blocked by "Sorry" page or "Too many requests"
        const content = await page.content();
        if (content.includes('Our systems have detected unusual traffic') || content.includes('Too Many Requests')) {
            console.error('  ⚠️ BLOCKED: Google is detecting unusual traffic. Stopping batch.');
            await page.close();
            return { appName: 'BLOCKED', storeLink: 'BLOCKED' };
        }

        // Wait for the main ad container or at least some basic page structure
        try {
            await page.waitForSelector('ad-creative-preview, .creative-preview-container', { timeout: 10000 });
        } catch (e) {
            console.log('  🕒 Main container slow to appear, continuing...');
        }

        // Base wait: Increase significantly for stability
        const baseWait = 8000 * Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(baseWait);

        // Robust Scroll
        await page.evaluate(async () => {
            window.scrollBy(0, 800);
            await new Promise(r => setTimeout(r, 500));
            window.scrollBy(0, -800);
        });
        await sleep(1500);

        const frames = page.frames();
        for (const frame of frames) {
            try {
                const frameData = await frame.evaluate(() => {
                    const data = { appName: null, storeLink: null };

                    // Search in the whole doc first to be safe, then try specific container
                    const root = document.querySelector('#portrait-landscape-phone') || document.body;

                    // App Link Selectors
                    const linkSelectors = [
                        'a[data-asoch-targets*="ochAppName"]',
                        'a[data-asoch-targets*="ochInstallButton"]',
                        'a.ns-sbqu4-e-75[href*="googleadservices"]',
                        'a.install-button-anchor[href*="googleadservices"]',
                        'a[href*="googleadservices.com/pagead/aclk"]',
                        'a[href*="play.google.com/store/apps/details"]'
                    ];

                    // Try XPath found in user logic
                    const xpath = '//*[@id="portrait-landscape-phone"]/div[1]/div[5]/a[2]';
                    const xpRes = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
                    if (xpRes && xpRes.href) data.storeLink = xpRes.href;

                    if (!data.storeLink) {
                        for (const sel of linkSelectors) {
                            const el = root.querySelector(sel);
                            if (el && el.href && !el.href.includes('javascript:')) {
                                data.storeLink = el.href;
                                break;
                            }
                        }
                    }

                    // App Name Selectors
                    const nameSelectors = [
                        'a[data-asoch-targets*="ochAppName"]',
                        '.short-app-name a',
                        'div[class*="app-name"]',
                        'span[class*="app-name"]',
                        '.app-title'
                    ];
                    for (const sel of nameSelectors) {
                        const el = root.querySelector(sel);
                        if (el && el.innerText.trim()) {
                            data.appName = el.innerText.trim();
                            break;
                        }
                    }
                    return data;
                });

                if (frameData.storeLink && result.storeLink === 'NOT_FOUND') result.storeLink = frameData.storeLink;
                if (frameData.appName && result.appName === 'NOT_FOUND') result.appName = frameData.appName;

                // If we found both, stop searching frames
                if (result.storeLink !== 'NOT_FOUND' && result.appName !== 'NOT_FOUND') break;
            } catch (e) { }
        }

        // RegEx fallback for the link from full page source
        if (result.storeLink === 'NOT_FOUND') {
            const pageSource = await page.content();
            const matches = pageSource.match(/https:\/\/www\.googleadservices\.com\/pagead\/aclk[^"'’\s]*/g);
            if (matches) result.storeLink = matches[0];
        }

        // Cleanup Redirects (get direct Play Store/App Store link)
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
        if (data.appName === 'BLOCKED') return data; // Return immediately to trigger restart
        if (data.appName !== 'NOT_FOUND' || data.storeLink !== 'NOT_FOUND') return data;
        await new Promise(r => setTimeout(r, 3000 + Math.random() * 2000));
    }
    return { appName: 'NOT_FOUND', storeLink: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION
// ============================================
(async () => {
    console.log(`🤖 Starting Safety-First App Info Agent...\n`);
    const sessionStartTime = Date.now();
    const MAX_RUNTIME = 330 * 60 * 1000; // 5.5 hours

    const sheets = await getGoogleSheetsClient();
    const toProcess = await getUrlData(sheets);

    if (toProcess.length === 0) {
        console.log('✨ No new URLs to process.');
        process.exit(0);
    }

    console.log(`📋 Found ${toProcess.length} pending URLs\n`);

    const browser = await puppeteer.launch({
        headless: true,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process'
        ]
    });

    for (let i = 0; i < toProcess.length; i += CONCURRENT_PAGES) {
        if (Date.now() - sessionStartTime > MAX_RUNTIME) {
            console.log('⏰ Session limit reached. Triggering restart...');
            await browser.close();
            await triggerSelfRestart();
            process.exit(0);
        }

        const batch = toProcess.slice(i, i + CONCURRENT_PAGES);
        console.log(`📦 Batch ${Math.floor(i / CONCURRENT_PAGES) + 1}/${Math.ceil(toProcess.length / CONCURRENT_PAGES)}`);

        const results = await Promise.all(batch.map(async (item) => {
            const data = await extractWithRetry(item.url, browser);
            return { url: item.url, ...data };
        }));

        // CHECK IF ANY IN BATCH WERE BLOCKED
        if (results.some(r => r.appName === 'BLOCKED')) {
            console.log('🛑 Block detected by one of the pages. Restarting to get a fresh IP...');
            await browser.close();
            await triggerSelfRestart();
            process.exit(0);
        }

        // Writing results safely to handle any row shifts
        await safeBatchWrite(sheets, results);

        // Final batch delay to breathe
        await new Promise(r => setTimeout(r, 2000));
    }

    await browser.close();

    // Re-check for new data to trigger self-restart if needed
    const remaining = await getUrlData(sheets);
    if (remaining.length > 0) {
        console.log('📈 More data found. Restarting...');
        await triggerSelfRestart();
    }

    console.log('\n🏁 Workflow complete.');
    process.exit(0);
})();
