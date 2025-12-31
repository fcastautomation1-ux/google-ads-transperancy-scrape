const puppeteer = require('puppeteer');
const { google } = require('googleapis');
const fs = require('fs');

// ============================================
// CONFIGURATION
// ============================================
const SPREADSHEET_ID = '1beJ263B3m4L8pgD9RWsls-orKLUvLMfT2kExaiyNl7g';
const SHEET_NAME = 'App data'; // Separate sheet for this agent
const CREDENTIALS_PATH = './credentials.json';
const CONCURRENT_PAGES = 3; // Reduced for stealth
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
    updates.forEach(({ url, appName, storeLink }) => {
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
    let result = { appName: 'NOT_FOUND', storeLink: 'NOT_FOUND' };

    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

    try {
        console.log(`  🚀 Loading: ${url.substring(0, 60)}...`);
        await page.goto(url, { waitUntil: 'networkidle0', timeout: MAX_WAIT_TIME });

        const content = await page.content();
        if (content.includes('Our systems have detected unusual traffic') || content.includes('Too Many Requests')) {
            console.error('  ⚠️ BLOCKED: Google is detecting unusual traffic. Stopping batch.');
            await page.close();
            return { appName: 'BLOCKED', storeLink: 'BLOCKED' };
        }

        const baseWait = 8000 * Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(baseWait);

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
                    const root = document.querySelector('#portrait-landscape-phone') || document.body;

                    const xpath = '//*[@id="portrait-landscape-phone"]/div[1]/div[5]/a[2]';
                    const xpRes = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
                    if (xpRes && xpRes.href) data.storeLink = xpRes.href;

                    // App Link Selectors (Expanded for Text Ads)
                    const linkSelectors = [
                        'a[data-asoch-targets*="ochAppName"]',
                        'a[data-asoch-targets*="ochInstallButton"]',
                        'a[data-asoch-targets*="ctaButton"]', // Common in text ads
                        'a.ns-sbqu4-e-75[href*="googleadservices"]',
                        'a.install-button-anchor[href*="googleadservices"]',
                        'a[href*="googleadservices.com/pagead/aclk"]',
                        'a[href*="play.google.com/store/apps/details"]',
                        'a[href*="itunes.apple.com"]'
                    ];

                    if (!data.storeLink) {
                        for (const sel of linkSelectors) {
                            const el = root.querySelector(sel);
                            if (el && el.href && !el.href.includes('javascript:')) {
                                data.storeLink = el.href;
                                break;
                            }
                        }
                    }

                    // Fallback Link: Any link that looks like a destination (not google internal)
                    if (!data.storeLink) {
                        const allLinks = Array.from(root.querySelectorAll('a[href]'));
                        const destLink = allLinks.find(a => {
                            const h = a.href.toLowerCase();
                            return !h.includes('google.com') &&
                                !h.includes('youtube.com') &&
                                !h.includes('javascript:') &&
                                h.startsWith('http');
                        });

                        // If no non-google link, try specific aclk link as last resort
                        if (!destLink) {
                            const aclk = allLinks.find(a => a.href.includes('googleadservices.com/pagead/aclk'));
                            if (aclk) data.storeLink = aclk.href;
                        } else {
                            data.storeLink = destLink.href;
                        }
                    }

                    // App Name / Brand Name Selectors (Expanded for Text Ads)
                    const nameSelectors = [
                        'a[data-asoch-targets*="ochAppName"]',
                        'a[data-asoch-targets*="AdTitle"]', // Text ad heading
                        '.short-app-name a',
                        'div[class*="app-name"]',
                        'span[class*="app-name"]',
                        '.app-title',
                        '.advertiser-name',
                        'h1', 'h2' // heading fallback
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
                if (result.storeLink !== 'NOT_FOUND' && result.appName !== 'NOT_FOUND') break;
            } catch (e) { }
        }

        if (result.storeLink === 'NOT_FOUND') {
            const pageSource = await page.content();
            const matches = pageSource.match(/https:\/\/www\.googleadservices\.com\/pagead\/aclk[^"'’\s]*/g);
            if (matches) result.storeLink = matches[0];
        }

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
        await new Promise(r => setTimeout(r, 3000 + Math.random() * 2000));
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

    const browser = await puppeteer.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-web-security', '--disable-features=IsolateOrigins,site-per-process']
    });

    for (let i = 0; i < toProcess.length; i += CONCURRENT_PAGES) {
        if (Date.now() - sessionStartTime > MAX_RUNTIME) {
            console.log('⏰ Session limit reached. Restarting...');
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

        if (results.some(r => r.appName === 'BLOCKED')) {
            console.log('🛑 Block detected. Restarting for fresh IP...');
            await browser.close();
            await triggerSelfRestart();
            process.exit(0);
        }

        await safeBatchWrite(sheets, results);
        await new Promise(r => setTimeout(r, 2000));
    }

    await browser.close();
    const remaining = await getUrlData(sheets);
    if (remaining.length > 0) {
        await triggerSelfRestart();
    }
    console.log('\n🏁 Workflow complete.');
    process.exit(0);
})();
