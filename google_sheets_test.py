import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import re
import os
import time
from googleapiclient.errors import HttpError

# === CONFIGURATION ===
SPREADSHEET_NAME = "Scraped Data"
CREDENTIALS_FILE = "D:/Python/New_folder/premium-cipher-467309-m8-1261f7750afd.json"
HTML_FOLDER = "D:/Python/New_folder/city_html"

# List of cities and their corresponding HTML files
CITIES = {
    "Bath": "bath.html",
    "Birmingham": "birmingham.html",
    "Bradford": "bradford.html",
    "Brighton and Hove": "brighton and hove.html",
    "Bristol": "bristol.html"
}

def get_html_from_file(filename):
    """Read HTML content from file"""
    filepath = os.path.join(HTML_FOLDER, filename)
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        print(f"❌ Error reading {filename}: {e}")
        return None

def clean_address(address_html):
    """Clean and format the address from HTML"""
    soup = BeautifulSoup(address_html, "html.parser")
    address_text = soup.get_text(separator=", ", strip=True)
    address_text = re.sub(r',\s+,', ', ', address_text)  # Remove extra commas
    address_text = re.sub(r'\s+', ' ', address_text)     # Remove extra spaces
    return address_text

def parse_html_table(html):
    """Parse the HTML table and extract certification body data"""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "find_a_cb_results_table"})
    
    if not table:
        raise ValueError("Could not find the results table in HTML")
    
    data = []
    for row in table.find_all("tr")[1:]:  # Skip header row
        try:
            company = row.find("td", class_="companyName").get_text(strip=True)
            address_td = row.find("td", class_="companyAddress")
            address = clean_address(str(address_td)) if address_td else ""
            
            scheme_td = row.find("td", class_="schemeColumn")
            schemes = " ".join(span.get_text(strip=True) 
                         for span in scheme_td.find_all("span", class_="scheme-icon")) if scheme_td else ""
            
            url_td = row.find("td", class_="URL")
            website = url_td.find("a")["href"] if url_td and url_td.find("a") else ""
            
            data.append([company, address, schemes, website])
        except Exception as e:
            print(f"⚠️ Error processing row: {e}")
            continue
    
    return data

def upload_to_google_sheets(all_data, spreadsheet_name):
    """Upload data to Google Sheets with city section headers"""
    try:
        # Set up credentials
        scope = ["https://spreadsheets.google.com/feeds", 
                "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        
        # Open or create spreadsheet
        try:
            spreadsheet = client.open(spreadsheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            spreadsheet = client.create(spreadsheet_name)
        
        # Get or create first worksheet
        if spreadsheet.worksheets():
            sheet = spreadsheet.get_worksheet(0)
        else:
            sheet = spreadsheet.add_worksheet(title="Sheet1", rows="1000", cols="5")
        
        # Clear entire sheet
        sheet.clear()
        
        # Prepare headers
        headers = ["City", "Company", "Address", "Certification", "Website"]
        sheet.append_row(headers)
        
        # Format header row
        sheet.format("A1:E1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.8, "green": 0.8, "blue": 0.8}
        })
        
        # Prepare all data (city name only once as section header)
        all_values = []
        for city, data in all_data.items():
            if data:
                # Add city section header
                all_values.append([f"=== {city.upper()} ===", "", "", "", ""])
                
                # Add data rows without repeating city name
                for record in data:
                    all_values.append([""] + record)
                
                # Add empty row after each city section
                all_values.append(["", "", "", "", ""])
        
        # Upload data with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if all_values:
                    sheet.update(
                        f"A2:E{len(all_values)+1}",
                        all_values,
                        value_input_option="RAW"
                    )
                print(f"✅ Successfully uploaded data for {len(all_data)} cities")
                break
            except HttpError as e:
                if e.resp.status == 429 and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 60
                    print(f"⚠️ Rate limited - waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                raise
    except Exception as e:
        print(f"❌ Error uploading to Google Sheets: {e}")

def main():
    all_data = {}
    
    # Process all city files
    for city, filename in CITIES.items():
        html = get_html_from_file(filename)
        if html:
            try:
                data = parse_html_table(html)
                print(f"📊 Found {len(data)} certification bodies for {city}")
                all_data[city] = data
            except Exception as e:
                print(f"❌ Error processing {city} data: {e}")
    
    # Upload to Google Sheets if data exists
    if all_data:
        upload_to_google_sheets(all_data, SPREADSHEET_NAME)
    else:
        print("⚠️ No data to upload")

if __name__ == "__main__":
    os.makedirs(HTML_FOLDER, exist_ok=True)
    main()
