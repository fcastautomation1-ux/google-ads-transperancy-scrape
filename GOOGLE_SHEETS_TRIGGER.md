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

// Only trigger if new rows are added below the last processed row
function checkForChanges(e) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Sheet1"); // Ensure this matches your sheet
  const lastRow = sheet.getLastRow();
  
  // Find the last row in Column F that has any value (ID or NOT_FOUND)
  const dataF = sheet.getRange(1, 6, lastRow).getValues();
  let lastProcessedRow = 0;
  
  for (let i = dataF.length - 1; i >= 0; i--) {
    if (dataF[i][0].toString().trim() !== "") {
      lastProcessedRow = i + 1;
      break;
    }
  }

  // If the sheet's total rows are more than the last processed row,
  // it means we have new URLs at the bottom!
  if (lastRow > lastProcessedRow) {
    Logger.log("🚀 New rows found at the bottom of the sheet. Starting GitHub...");
    triggerGitHubWorkflow();
  } else {
    Logger.log("ℹ️ No new rows found below row " + lastProcessedRow);
  }
}
```

## 3. Set up the Trigger
Since the script needs to connect to GitHub, it cannot run automatically without setup.

1. In the Apps Script sidebar, click on the **Triggers** icon (alarm clock).
2. Click **+ Add Trigger** (bottom right).
3. configure it as follows:
   - **Choose which function to run**: `checkForChanges`
   - **Select event source**: `From spreadsheet`
   - **Select event type**: `On change` (This covers rows added, copy-paste, etc.)
4. Click **Save**.
5. You will see a "Sign in with Google" popup.
6. Click **Advanced** > **Go to (Script Name) (unsafe)** > **Allow**.

## 4. Get a GitHub Token
1. Go to GitHub > Settings > Developer settings > Personal access tokens > Tokens (classic).
2. Generate a new token.
   - For **Public** Repos: Check the **`public_repo`** scope.
   - For **Private** Repos: Check the **`repo`** scope.
3. Paste this token into the `GITHUB_TOKEN` variable in the script above.
