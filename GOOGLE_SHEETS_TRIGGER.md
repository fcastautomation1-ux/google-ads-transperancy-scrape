# Google Sheets Auto-Start Setup

To make your scraping workflow start automatically when new data is added to your Google Sheet, follow these steps:

## 1. Open Script Editor
1. Open your Google Sheet.
2. Go to **Extensions** > **Apps Script**.

## 2. Add the Script
Copy and paste the following code into the script editor (replace any existing code):

```javascript
// CONFIGURATION
const GITHUB_OWNER = 'YOUR_GITHUB_USERNAME'; // e.g., 'johndoe'
const GITHUB_REPO = 'google-ads-transperancy-scrape'; // Your repository name
const GITHUB_TOKEN = 'YOUR_GITHUB_PAT_TOKEN'; // Start with ghp_...

function triggerGitHubWorkflow() {
  // 1. Check if a workflow is ALREADY running
  const statusUrl = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/runs?status=in_progress`;
  const getOptions = {
    headers: { 'Authorization': 'token ' + GITHUB_TOKEN }
  };
  
  try {
    const response = UrlFetchApp.fetch(statusUrl, getOptions);
    const data = JSON.parse(response.getContentText());
    
    if (data.total_count > 0) {
      Logger.log("⏳ A workflow is already running. No need to start a new one.");
      return; 
    }
  } catch (e) {
    Logger.log("⚠️ Could not check status, proceeding anyway...");
  }

  // 2. If nothing is running, trigger it
  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/dispatches`;
  const payload = { event_type: 'sheet_update' };
  const options = {
    method: 'post',
    headers: {
      'Authorization': 'token ' + GITHUB_TOKEN,
      'Accept': 'application/vnd.github.v3+json',
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify(payload)
  };

  UrlFetchApp.fetch(url, options);
  Logger.log('✅ Workflow triggered successfully');
}

// 1. Instantly removes duplicate URLs from Column A (Bulk Operation)
function removeDuplicates(sheet) {
  const oldLastRow = sheet.getLastRow();
  if (oldLastRow < 2) return;

  // Use built-in Google Sheets command to remove duplicates based on Column 1 (Col A)
  // This happens "all at once" and is much faster than manual looping
  const lastColumn = sheet.getLastColumn();
  const range = sheet.getRange(1, 1, oldLastRow, lastColumn);
  range.removeDuplicates([1]);
  
  const newLastRow = sheet.getLastRow();
  const count = oldLastRow - newLastRow;

  if (count > 0) {
    Logger.log("🗑️ Bulk removed " + count + " duplicate URLs.");
    SpreadsheetApp.getActiveSpreadsheet().toast(`🗑️ Removed ${count} duplicate rows at once.`, "Duplicate Cleaner", 5);
  }
}

// 2. Only trigger if new rows are added below the last processed row
function checkForChanges(e) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Sheet1"); 
  
  removeDuplicates(sheet);
  
  const lastRow = sheet.getLastRow();
  const dataF = sheet.getRange(1, 6, lastRow).getValues();
  let lastProcessedRow = 0;
  
  for (let i = dataF.length - 1; i >= 0; i--) {
    if (dataF[i][0].toString().trim() !== "") {
      lastProcessedRow = i + 1;
      break;
    }
  }

  if (lastRow > lastProcessedRow) {
    Logger.log("🚀 New rows found. Starting GitHub...");
    triggerGitHubWorkflow();
  }
}

// 3. GUARDIAN: Checks every 10-15 mins to see if the workflow stopped but links remain
function guardianCheck() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Sheet1");
  const lastRow = sheet.getLastRow();
  
  // Scans for ANY empty F cells where A has a URL
  if (lastRow < 2) return;
  const dataA = sheet.getRange(2, 1, lastRow - 1).getValues();
  const dataF = sheet.getRange(2, 6, lastRow - 1).getValues();

  let pendingWork = false;
  for (let i = 0; i < dataA.length; i++) {
    if (dataA[i][0].toString().trim() !== "" && dataF[i][0].toString().trim() === "") {
      pendingWork = true;
      break;
    }
  }

  if (pendingWork) {
    Logger.log("🛡️ Guardian: Detected pending work. Ensuring workflow is running...");
    triggerGitHubWorkflow();
  }
}
```

## 3. Set up the Triggers
You need **TWO** triggers for 24/7 reliability:

1.  **Change Trigger:**
    - Function: `checkForChanges`
    - Source: `From spreadsheet`
    - Event: `On change`
2.  **Guardian Timer (The "Restart" Logic):**
    - Function: `guardianCheck`
    - Source: `Time-driven`
    - Type: `Minutes timer`
    - Interval: `Every 10 minutes` (This ensures it starts back up if stopped manually)

## 4. Get a GitHub Token
1. Go to GitHub > Settings > Developer settings > Personal access tokens > Tokens (classic).
2. Generate a new token.
   - For **Public** Repos: Check the **`public_repo`** scope.
   - For **Private** Repos: Check the **`repo`** scope.
3. Paste this token into the `GITHUB_TOKEN` variable in the script above.
