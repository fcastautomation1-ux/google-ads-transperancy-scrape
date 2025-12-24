const puppeteer = require('puppeteer');
const { google } = require('googleapis');
const fs = require('fs');

// ============================================
// CONFIGURATION
// ============================================
const SPREADSHEET_ID = '1beJ263B3m4L8pgD9RWsls-orKLUvLMfT2kExaiyNl7g';
const SHEET_NAME = 'Sheet1';
const CREDENTIALS_PATH = './credentials.json';
const CONCURRENT_PAGES = 6; // Increased for better throughput
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
  // Get both column A (URLs) and column F (existing video IDs)
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A:F`,
  });

  const rows = response.data.values || [];
  const urlData = [];
  
  // Skip header row, process each row
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const url = row[0]?.trim();
    const existingVideoId = row[5]?.trim(); // Column F is index 5
    
    // Only include URLs that are not empty and don't have existing video ID
    if (url && !existingVideoId) {
      urlData.push({
        url: url,
        rowIndex: i - 1 // 0-based index for row (excluding header)
      });
    }
  }
  
  return urlData;
}

async function batchWriteToSheet(sheets, updates) {
  if (updates.length === 0) return;
  
  const data = updates.map(({ rowIndex, videoId }) => ({
    range: `${SHEET_NAME}!F${rowIndex + 2}`,
    values: [[videoId]]
  }));

  try {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      resource: {
        valueInputOption: 'RAW',
        data: data
      }
    });
    console.log(`  ✅ Batch wrote ${updates.length} results`);
  } catch (error) {
    console.error(`  ❌ Batch write error:`, error.message);
  }
}

// ============================================
// BALANCED VIDEO ID EXTRACTOR WITH RETRY
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
    
    // Check for video ID in ALL requests (before blocking anything)
    if (requestUrl.includes('googlevideo.com/videoplayback')) {
      const urlParams = new URLSearchParams(requestUrl.split('?')[1]);
      const id = urlParams.get('id');
      if (id && /^[a-f0-9]{16}$/.test(id)) {
        videoSourceId = id;
      }
    }
    
    // Only block large images and fonts - allow everything else
    if (['image', 'font'].includes(resourceType)) {
      request.abort();
      return;
    }
    
    request.continue();
  });

  try {
    // Use networkidle2 for better stability with slow-loading pages
    await page.goto(url, { 
      waitUntil: 'networkidle2',
      timeout: MAX_WAIT_TIME 
    });
    
    // Initial wait - increase on retries
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
        } catch (e) {}
      }

      if (!results.found) {
        searchForPlayButton(document);
      }

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
      // Simple but realistic click
      const client = await page.target().createCDPSession();
      
      await client.send('Input.dispatchMouseEvent', {
        type: 'mouseMoved',
        x: playButtonInfo.x,
        y: playButtonInfo.y
      });
      await sleep(100);
      
      await client.send('Input.dispatchMouseEvent', {
        type: 'mousePressed',
        x: playButtonInfo.x,
        y: playButtonInfo.y,
        button: 'left',
        clickCount: 1
      });
      await sleep(80);
      
      await client.send('Input.dispatchMouseEvent', {
        type: 'mouseReleased',
        x: playButtonInfo.x,
        y: playButtonInfo.y,
        button: 'left',
        clickCount: 1
      });
      
      // Wait for video to start and make network request - increase wait time on retries
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

// Retry wrapper function
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
      // Wait before retrying (exponential backoff)
      const retryDelay = 2000 * Math.pow(2, attempt - 1);
      console.log(`  ⏳ [${rowIndex + 1}] Waiting ${retryDelay}ms before retry...`);
      await new Promise(r => setTimeout(r, retryDelay));
    }
  }
  
  return null;
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
      
      const videoId = await extractVideoIdWithRetry(url, browser, rowIndex);
      
      if (videoId) {
        console.log(`  ✅ [${rowIndex + 1}] Video ID: ${videoId}`);
        return { rowIndex: rowIndex, videoId };
      } else {
        console.log(`  ⚠️  [${rowIndex + 1}] No video ID found after ${MAX_RETRIES} attempts`);
        return { rowIndex: rowIndex, videoId: 'NOT_FOUND' };
      }
    })
  );
  
  return results;
}

// ============================================
// MAIN FUNCTION
// ============================================
(async () => {
  console.log('📊 Starting BALANCED Google Sheets + Puppeteer Integration...\n');
  const startTime = Date.now();

  const sheets = await getGoogleSheetsClient();
  console.log('✅ Connected to Google Sheets\n');

  const urlData = await getUrlsFromSheet(sheets);
  console.log(`📋 Found ${urlData.length} URLs to process (skipping already scraped)\n`);

  if (urlData.length === 0) {
    console.log('⚠️  No URLs found to process (all may already be scraped)');
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
  const allResults = [];
  for (let i = 0; i < urlData.length; i += CONCURRENT_PAGES) {
    const batch = urlData.slice(i, i + CONCURRENT_PAGES);
    console.log(`\n📦 Processing batch ${Math.floor(i / CONCURRENT_PAGES) + 1}/${Math.ceil(urlData.length / CONCURRENT_PAGES)}`);
    
    const batchResults = await processUrlBatch(batch, i, browser);
    allResults.push(...batchResults);
    
    // Batch write to sheet
    await batchWriteToSheet(sheets, batchResults);
  }

  await browser.close();
  
  const endTime = Date.now();
  const totalTime = ((endTime - startTime) / 1000).toFixed(2);
  const avgTime = (totalTime / urlData.length).toFixed(2);
  
  console.log('\n✨ All done!');
  console.log(`⏱️  Total time: ${totalTime}s`);
  console.log(`⏱️  Average per URL: ${avgTime}s`);
  console.log(`✅ Found: ${allResults.filter(r => r.videoId !== 'NOT_FOUND').length}`);
  console.log(`❌ Not found: ${allResults.filter(r => r.videoId === 'NOT_FOUND').length}`);
  
  process.exit(0);
})();