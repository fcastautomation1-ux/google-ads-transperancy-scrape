const puppeteer = require('puppeteer');
const { google } = require('googleapis');
const fs = require('fs');

// ============================================
// CONFIGURATION
// ============================================
const SPREADSHEET_ID = '1beJ263B3m4L8pgD9RWsls-orKLUvLMfT2kExaiyNl7g';
const INPUT_SHEET_NAME = 'Sheet1';
const OUTPUT_SHEET_NAME = 'Sheet6';
const CREDENTIALS_PATH = './credentials.json';
const CONCURRENT_PAGES = 3;
const MAX_WAIT_TIME = 60000;
const POST_CLICK_WAIT = 12000;
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

async function getInputsAndProcessed(sheets) {
    // Get URLs from Sheet1 Column A
    const inputResponse = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${INPUT_SHEET_NAME}!A:A`,
    });
    const inputRows = inputResponse.data.values || [];

    // Get already processed URLs from Sheet6 Column A to avoid duplicates
    const outputResponse = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${OUTPUT_SHEET_NAME}!A:A`,
    });
    const processedUrls = new Set((outputResponse.data.values || []).map(row => row[0]?.trim()).filter(Boolean));

    const toProcess = [];
    // Skip header, assuming row 0 is header
    for (let i = 1; i < inputRows.length; i++) {
        const url = inputRows[i][0]?.trim();
        if (url && !processedUrls.has(url)) {
            toProcess.push({ url, rowIndex: i });
        }
    }
    return toProcess;
}

// ============================================
// EXTRACTION LOGIC
// ============================================
async function extractAppData(url, browser, attempt = 1) {
    const page = await browser.newPage();
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    try {
        await page.goto(url, { waitUntil: 'networkidle2', timeout: MAX_WAIT_TIME });

        // Initial wait for dynamic content
        const baseWait = 5000 * Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(baseWait);

        const appData = await page.evaluate(() => {
            const results = { appName: 'NOT_FOUND', storeLink: 'NOT_FOUND' };

            const searchInDoc = (doc) => {
                // Find App Name using visual cues from user
                const nameEl = doc.querySelector('a[data-asoch-targets*="ochAppName"]') ||
                    doc.querySelector('.short-app-name a');

                if (nameEl) {
                    results.appName = nameEl.innerText.trim();
                    if (results.storeLink === 'NOT_FOUND') results.storeLink = nameEl.href;
                }

                // Look for Play Store or App Store links
                const allLinks = Array.from(doc.querySelectorAll('a'));
                for (const a of allLinks) {
                    const href = a.href || '';
                    if (href.includes('play.google.com') || href.includes('itunes.apple.com')) {
                        results.storeLink = href;
                        break;
                    }
                    // Check for install button targets
                    const targets = a.getAttribute('data-asoch-targets') || '';
                    if (targets.includes('ochAppIcon') || targets.includes('ochInstallButton')) {
                        if (results.storeLink === 'NOT_FOUND') results.storeLink = href;
                    }
                }
                return results.appName !== 'NOT_FOUND' && results.storeLink !== 'NOT_FOUND';
            };

            // 1. Search top level
            searchInDoc(document);

            // 2. Search in iframes
            const iframes = document.querySelectorAll('iframe');
            for (const iframe of iframes) {
                try {
                    const iframeDoc = iframe.contentDocument || iframe.contentWindow?.document;
                    if (iframeDoc) {
                        const found = searchInDoc(iframeDoc);
                        if (found) break;
                    }
                } catch (e) { }
            }

            return results;
        });

        // Handle googleadservices redirect
        if (appData.storeLink && appData.storeLink.includes('googleadservices.com/')) {
            try {
                const urlObj = new URL(appData.storeLink);
                const adUrl = urlObj.searchParams.get('adurl');
                if (adUrl) {
                    appData.storeLink = adUrl;
                }
            } catch (e) { }
        }

        await page.close();
        return appData;
    } catch (err) {
        console.error(`  ❌ Error on ${url.substring(0, 30)} (Attempt ${attempt}): ${err.message}`);
        await page.close();
        return { appName: 'ERROR', storeLink: 'ERROR' };
    }
}

async function extractWithRetry(url, browser) {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        const data = await extractAppData(url, browser, attempt);
        if (data.appName && data.appName !== 'ERROR' && data.appName !== 'NOT_FOUND') {
            return data;
        }
        if (attempt < MAX_RETRIES) {
            console.log(`  🔄 [${url.substring(url.length - 15)}] Retry ${attempt}/${MAX_RETRIES}...`);
            await new Promise(r => setTimeout(r, 3000));
        }
    }
    return { appName: 'NOT_FOUND', storeLink: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION
// ============================================
(async () => {
    console.log(`🤖 Starting App Info Agent (Sheet1 -> ${OUTPUT_SHEET_NAME})...\n`);

    const sheets = await getGoogleSheetsClient();
    const toProcess = await getInputsAndProcessed(sheets);

    if (toProcess.length === 0) {
        console.log('✨ No new URLs to process.');
        process.exit(0);
    }

    console.log(`📋 Found ${toProcess.length} new URLs to process\n`);

    const browser = await puppeteer.launch({
        headless: true,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--autoplay-policy=no-user-gesture-required'
        ]
    });

    for (let i = 0; i < toProcess.length; i += CONCURRENT_PAGES) {
        const batch = toProcess.slice(i, i + CONCURRENT_PAGES);
        console.log(`\n📦 Batch ${Math.floor(i / CONCURRENT_PAGES) + 1}/${Math.ceil(toProcess.length / CONCURRENT_PAGES)}`);

        const batchResults = await Promise.all(batch.map(async (item) => {
            console.log(`  🔗 Processing: ...${item.url.substring(item.url.length - 40)}`);
            const data = await extractWithRetry(item.url, browser);
            console.log(`  ✅ Result: [${data.appName}]`);
            return { url: item.url, appName: data.appName, storeLink: data.storeLink };
        }));

        // Output: Column A: URL, Column B: App Name, Column C: Playstore Link, Column D: Timestamp
        const values = batchResults.map(r => [r.url, r.appName, r.storeLink, new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' })]);

        try {
            await sheets.spreadsheets.values.append({
                spreadsheetId: SPREADSHEET_ID,
                range: `${OUTPUT_SHEET_NAME}!A:D`,
                valueInputOption: 'RAW',
                resource: { values }
            });
            console.log(`  💾 Saved ${batchResults.length} rows to Sheet6`);
        } catch (error) {
            console.error(`  ❌ Append error:`, error.message);
        }
    }

    await browser.close();
    console.log('\n🏁 Workflow complete.');
})();
