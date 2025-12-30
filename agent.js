const puppeteer = require('puppeteer');
const { google } = require('googleapis');
const fs = require('fs');

// ============================================
// CONFIGURATION
// ============================================
const SPREADSHEET_ID = '1beJ263B3m4L8pgD9RWsls-orKLUvLMfT2kExaiyNl7g';
const SHEET_NAME = 'Sheet1';
const CREDENTIALS_PATH = './credentials.json';
const CONCURRENT_PAGES = 3; // Reduced for better stability/video loading
const MAX_WAIT_TIME = 60000; // 60 seconds
const POST_CLICK_WAIT = 12000; // Give video 12 seconds to load
const MAX_RETRIES = 3; // Maximum retry attempts
const RETRY_WAIT_MULTIPLIER = 1.5; // Increase wait time by 1.5x on each retry

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
  const sheets = google.sheets({ version: 'v4', auth: authClient });

  return sheets;
}

async function getUrlsFromSheet(sheets) {
  // Get columns A to H
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A:H`,
  });

  const rows = response.data.values || [];
  const urlData = [];

  // Skip header row
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const url = row[0]?.trim();
    const existingVideoId = row[5]?.trim(); // Column F
    const existingAppLink = row[6]?.trim(); // Column G
    const existingAppName = row[7]?.trim(); // Column H

    // Process if it has a URL and is missing ANY of the target data
    if (url && (!existingVideoId || !existingAppLink || !existingAppName)) {
      urlData.push({
        url: url,
        rowIndex: i - 1
      });
    }
  }

  return urlData;
}

async function batchWriteToSheet(sheets, updates) {
  if (updates.length === 0) return;

  const data = updates.map(({ rowIndex, videoId, appLink, appName }) => ({
    range: `${SHEET_NAME}!F${rowIndex + 2}:H${rowIndex + 2}`,
    values: [[videoId, appLink, appName]]
  }));

  try {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      resource: {
        valueInputOption: 'RAW',
        data: data
      }
    });
    console.log(`  ✅ Batch wrote ${updates.length} results (F, G, H)`);
  } catch (error) {
    console.error(`  ❌ Batch write error:`, error.message);
  }
}

// ============================================
// BALANCED VIDEO ID EXTRACTOR WITH RETRY
// ============================================
async function extractAdData(url, browser, attempt = 1, baseWaitTime = POST_CLICK_WAIT) {
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

    // Extract App Name and Play Store Link
    const appInfo = await page.evaluate(() => {
      const info = { appLink: null, appName: null };

      // Look for Play Store link
      const links = Array.from(document.querySelectorAll('a'));
      const playLink = links.find(a => a.href.includes('play.google.com/store/apps/details'));
      if (playLink) {
        info.appLink = playLink.href;

        // Try to find app name near the link or in specific ad elements
        const parent = playLink.closest('div');
        if (parent) {
          // Look for text in headings or common app name classes
          const h1 = document.querySelector('h1');
          if (h1) info.appName = h1.textContent.trim();

          // Alternative: look for advertiser name or app title
          if (!info.appName) {
            const title = document.querySelector('.creative-preview-title'); // Hypothetical common class
            if (title) info.appName = title.textContent.trim();
          }
        }
      }

      // If we still don't have an app name, find the main advertiser title text
      if (!info.appName) {
        const metaTitle = document.title.split(' - ')[0]; // Often contains app name
        info.appName = metaTitle.trim();
      }

      return info;
    });

    // Find and click play button (logic remains same)
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
    return {
      videoId: videoSourceId,
      appLink: appInfo.appLink || 'NOT_FOUND',
      appName: appInfo.appName || 'NOT_FOUND'
    };
  } catch (err) {
    console.error(`  ❌ Error (attempt ${attempt}): ${err.message}`);
    await page.close();
    return null;
  }
}

// Retry wrapper function
async function extractAdDataWithRetry(url, browser, rowIndex) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    if (attempt > 1) {
      console.log(`  🔄 [${rowIndex + 1}] Retry attempt ${attempt}/${MAX_RETRIES}...`);
    }

    const adData = await extractAdData(url, browser, attempt, POST_CLICK_WAIT);

    if (adData && adData.videoId) {
      if (attempt > 1) {
        console.log(`  ✅ [${rowIndex + 1}] Data found on attempt ${attempt}`);
      }
      return adData;
    }

    if (attempt < MAX_RETRIES) {
      const retryDelay = 2000 * Math.pow(2, attempt - 1);
      console.log(`  ⏳ [${rowIndex + 1}] Waiting ${retryDelay}ms before retry...`);
      await new Promise(r => setTimeout(r, retryDelay));
    }
  }

  return { videoId: 'NOT_FOUND', appLink: 'NOT_FOUND', appName: 'NOT_FOUND' };
}

// ============================================
// CONCURRENT PROCESSING
// ============================================
async function processUrlBatch(urlData, startIndex, browser) {
  const results = await Promise.all(
    urlData.map(async (item, i) => {
      const actualIndex = startIndex + i;
      const rowIndex = item.rowIndex;
      const url = item.url;

      console.log(`[${rowIndex + 1}] Processing: ${url.substring(0, 60)}...`);

      const adData = await extractAdDataWithRetry(url, browser, rowIndex);

      console.log(`  📊 [${rowIndex + 1}] Video: ${adData.videoId.substring(0, 8)} | App: ${adData.appName.substring(0, 15)}`);
      return { rowIndex: rowIndex, ...adData };
    })
  );

  return results;
}

// ============================================
// SELF-RESTART LOGIC
// ============================================
async function triggerSelfRestart() {
  const repo = process.env.GITHUB_REPOSITORY;
  const token = process.env.GH_TOKEN;

  if (!repo || !token) {
    console.log('⚠️  Skipping auto-restart: GITHUB_REPOSITORY or GH_TOKEN missing in env.');
    return;
  }

  console.log(`\n🔄 Triggering auto-restart for ${repo}...`);

  const https = require('https');
  const data = JSON.stringify({ event_type: 'sheet_update' });

  const options = {
    hostname: 'api.github.com',
    port: 443,
    path: `/repos/${repo}/dispatches`,
    method: 'POST',
    headers: {
      'Authorization': `token ${token}`,
      'User-Agent': 'Node.js-Scraper',
      'Accept': 'application/vnd.github.v3+json',
      'Content-Type': 'application/json',
      'Content-Length': data.length
    }
  };

  return new Promise((resolve) => {
    const req = https.request(options, (res) => {
      console.log(`  📡 GitHub Response: ${res.statusCode}`);
      resolve();
    });
    req.on('error', (e) => {
      console.error(`  ❌ Auto-restart error: ${e.message}`);
      resolve();
    });
    req.write(data);
    req.end();
  });
}

// ============================================
// MAIN FUNCTION
// ============================================
(async () => {
  console.log('📊 Starting BALANCED Google Sheets + Puppeteer Integration...\n');
  const sessionStartTime = Date.now();
  const MAX_RUNTIME_MS = 330 * 60 * 1000; // 5 hours 30 minutes (safety margin)

  const sheets = await getGoogleSheetsClient();
  console.log('✅ Connected to Google Sheets\n');

  let urlData = await getUrlsFromSheet(sheets);
  console.log(`📋 Found ${urlData.length} URLs to process\n`);

  if (urlData.length === 0) {
    console.log('⚠️  No URLs found to process.');
    process.exit(0);
  }

  const browser = await puppeteer.launch({
    headless: true,
    defaultViewport: { width: 1920, height: 1080 },
    args: [
      '--autoplay-policy=no-user-gesture-required',
      '--disable-blink-features=AutomationControlled',
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage'
    ]
  });

  console.log(`🌐 Browser launched - Processing ${CONCURRENT_PAGES} URLs at a time\n`);

  // Process URLs in batches
  for (let i = 0; i < urlData.length; i += CONCURRENT_PAGES) {
    // CHECK FOR TIMEOUT
    if (Date.now() - sessionStartTime > MAX_RUNTIME_MS) {
      console.log('\n⏰ Reached 5.5 hour limit. Saving and restarting workflow...');
      await browser.close();
      await triggerSelfRestart();
      process.exit(0);
    }

    const batch = urlData.slice(i, i + CONCURRENT_PAGES);
    console.log(`\n📦 Processing batch ${Math.floor(i / CONCURRENT_PAGES) + 1}/${Math.ceil(urlData.length / CONCURRENT_PAGES)}`);

    const batchResults = await processUrlBatch(batch, i, browser);
    await batchWriteToSheet(sheets, batchResults);
  }

  await browser.close();

  // FINAL CHECK: Did more rows get added while we were running?
  console.log('\n🏁 Finished initial batch. Checking for newly added rows...');
  const remainingData = await getUrlsFromSheet(sheets);
  if (remainingData.length > 0) {
    console.log(`📈 ${remainingData.length} more links were found. Restarting workflow...`);
    await triggerSelfRestart();
  } else {
    console.log('✨ All links processed. No more pending rows.');
  }

  process.exit(0);
})();