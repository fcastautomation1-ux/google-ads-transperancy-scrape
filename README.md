# Google Ads Transparency Scraper

Automated scraper for extracting video IDs from Google Ads Transparency Report pages.

## Features

- ✅ Daily automated scraping via GitHub Actions (9 AM Pakistan Time)
- ✅ Concurrent processing (6 pages at a time)
- ✅ Automatic retry logic (up to 3 attempts with increasing wait times)
- ✅ Skips already scraped URLs (checks column F)
- ✅ Batch writes to Google Sheets

## Setup

### 1. Local Setup

1. Install dependencies (from project root):
```bash
npm install
```

2. Create credentials file:
   - Download your Google Service Account credentials JSON file
   - Save it as `credentials.json` in this folder

3. Update configuration in `agent.js`:
   - `SPREADSHEET_ID`: Your Google Sheets ID
   - `SHEET_NAME`: Your sheet name (default: 'Sheet1')

4. Run the scraper:
```bash
npm start
```
Or directly:
```bash
node agent.js
```

### 2. GitHub Actions Setup

To enable daily automated runs:

1. **Add GitHub Secret:**
   - Go to your repository → Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Name: `GOOGLE_CREDENTIALS`
   - Value: Paste the entire contents of your `credentials.json` file

2. **Verify Workflow:**
   - The workflow is configured to run daily at 9:00 AM Pakistan Time (4:00 AM UTC)
   - You can manually trigger it from the Actions tab → "Daily Google Ads Scraping" → "Run workflow"
   - The workflow file is located at `.github/workflows/daily-scrape.yml` in the repository root

## How It Works

1. **Reads URLs from Google Sheets:**
   - Reads URLs from column A
   - Skips rows that already have data in column F (already scraped)

2. **Extracts Video IDs:**
   - Opens each URL in a headless browser
   - Clicks the play button
   - Monitors network requests for video playback URLs
   - Extracts the 16-character hex video ID

3. **Retry Logic:**
   - If video ID is not found, retries up to 3 times
   - Each retry increases wait times (exponential backoff)
   - More time given for video to load on subsequent attempts

4. **Writes Results:**
   - Batch writes video IDs to column F in Google Sheets
   - Writes "NOT_FOUND" if video ID cannot be extracted after 3 attempts

## Configuration

Edit these constants in `agent.js`:

- `CONCURRENT_PAGES`: Number of URLs to process simultaneously (default: 6)
- `MAX_WAIT_TIME`: Maximum page load timeout in ms (default: 60000)
- `POST_CLICK_WAIT`: Initial wait time after clicking play button in ms (default: 12000)
- `MAX_RETRIES`: Maximum retry attempts (default: 3)
- `RETRY_WAIT_MULTIPLIER`: Multiplier for wait times on retries (default: 1.5)

## Google Sheets Format

Your Google Sheet should have:
- **Column A**: URLs to scrape
- **Column F**: Video IDs (automatically filled by the scraper)

The scraper will skip any rows that already have data in column F.

## Notes

- The scraper runs in headless mode
- Images and fonts are blocked to speed up loading
- Scripts and media are allowed to ensure video playback works
- Results are written in batches for efficiency

