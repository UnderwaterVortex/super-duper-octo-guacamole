import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from twilio.rest import Client
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# --- CONFIGURATION ---
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
TWILIO_PHONE_NUMBER = os.environ.get('TWILIO_PHONE_NUMBER')
GOOGLE_CREDS_JSON = os.environ.get('GOOGLE_CREDS_JSON')

# Name of the Google Sheets workbook file
WORKBOOK_NAME = "Calling Agent Sheet" # <-- Or your actual file name
MASTER_SHEET_NAME = "master"
RETRY_DELAY_MINUTES = 30 # Set your desired retry delay in minutes

AUDIO_MAPPING = {
    "North": "https://res.cloudinary.com/dqyoxump7/video/upload/v1752949596/bulletin_audio_hindi_20250629_035112.wav_1_qifvte.wav",
    "South": "https://res.cloudinary.com/dqyoxump7/video/upload/v1752949596/bulletin_audio_hindi_20250629_035112.wav_0_nm0yqq.wav",
    "East": "https://res.cloudinary.com/dqyoxump7/video/upload/v1752950393/bulletin_audio_hindi_20250629_123802.wav_1_bozis5.wav",
    "West": "https://res.cloudinary.com/dqyoxump7/video/upload/v1752950393/bulletin_audio_hindi_20250629_123802.wav_0_evmopy.wav"
}

# --- TIMEZONE SETUP ---
IST = ZoneInfo("Asia/Kolkata")

# --- SETUP CLIENTS ---
try:
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds)
    workbook = gc.open(WORKBOOK_NAME)
    twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
except Exception as e:
    print(f"FATAL: Could not initialize clients. Error: {e}")
    exit()

# --- HELPER FUNCTIONS ---
def make_call(phone_number, audio_url):
    try:
        call = twilio_client.calls.create(
                        twiml=f'<Response><Play>{audio_url}</Play></Response>',
                        to=phone_number,
                        from_=TWILIO_PHONE_NUMBER
                    )
        print(f"Initiating call to {phone_number}. SID: {call.sid}")
        return call.sid, "Initiated"
    except Exception as e:
        print(f"Error calling {phone_number}: {e}")
        return None, "Failed"

def check_call_status(call_sid):
    try:
        call = twilio_client.calls.fetch(call_sid)
        return call.status
    except Exception as e:
        print(f"Error fetching status for SID {call_sid}: {e}")
        return "failed"

# --- MAIN SCRIPT ---
def main():
    print("--- Running Voice Agent Script ---")
    now_ist = datetime.now(IST)
    todays_sheet_name = now_ist.strftime('%d%m%Y')
    print(f"Operating on sheet: '{todays_sheet_name}' for date {now_ist.strftime('%Y-%m-%d')}")

    # 1. Get or create today's worksheet
    try:
        worksheet = workbook.worksheet(todays_sheet_name)
        print(f"Successfully opened existing sheet: '{todays_sheet_name}'")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Sheet '{todays_sheet_name}' not found. Creating and populating from '{MASTER_SHEET_NAME}'...")
        try:
            master_sheet = workbook.worksheet(MASTER_SHEET_NAME)
            master_data = master_sheet.get_all_records()
            
            worksheet = workbook.add_worksheet(title=todays_sheet_name, rows="1", cols="8")
            headers = ['Name', 'PhoneNumber', 'Location', 'CallTime', 'CallStatus', 'LastCalled', 'RetryAt', 'CallSid']
            worksheet.append_row(headers, value_input_option='USER_ENTERED')
            
            rows_to_add = []
            today_date_ist = now_ist.date()
            for record in master_data:
                try:
                    call_time_obj = datetime.strptime(record['CallTime'], '%H:%M:%S').time()
                    full_call_time = datetime.combine(today_date_ist, call_time_obj)
                    rows_to_add.append([
                        record['Name'], record['PhoneNumber'], record['Location'],
                        full_call_time.strftime('%Y-%m-%d %H:%M:%S'),
                        '', '', '', '' # Empty status columns
                    ])
                except (ValueError, KeyError) as e:
                    print(f"Skipping record {record} due to invalid format or missing key: {e}")
            
            if rows_to_add:
                worksheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
            print(f"Successfully created and populated '{todays_sheet_name}'.")
        except Exception as e:
            print(f"FATAL: Could not create or populate daily sheet. Error: {e}")
            return # Exit if we can't create the sheet

    # 2. Process calls from today's worksheet
    daily_data = worksheet.get_all_records()
    for index, row in enumerate(daily_data):
        sheet_row_index = index + 2 # +1 for header, +1 for 0-based index
        call_status = row.get('CallStatus', '')

        # Skip if call is already successfully delivered
        if call_status == 'Delivered':
            continue

        # --- Check status of initiated or retrying calls ---
        if call_status in ['Initiated', 'Retry Scheduled']:
            # For retries, first check if it's time
            if call_status == 'Retry Scheduled' and row.get('RetryAt'):
                retry_at_time = datetime.strptime(row['RetryAt'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=IST)
                if retry_at_time > now_ist:
                    continue # Not time for the retry yet

            # Fetch the latest status from Twilio if there's a CallSid
            if row.get('CallSid'):
                latest_status = check_call_status(row['CallSid'])
                if latest_status in ['completed', 'busy', 'no-answer', 'failed']:
                    print(f"Updating status for {row['PhoneNumber']} to '{latest_status}'")
                    # Expanded retry logic
                    if latest_status in ['busy', 'no-answer', 'failed']:
                        retry_time_ist = now_ist + timedelta(minutes=RETRY_DELAY_MINUTES)
                        worksheet.update_cell(sheet_row_index, 7, retry_time_ist.strftime('%Y-%m-%d %H:%M:%S')) # Col G: RetryAt
                        worksheet.update_cell(sheet_row_index, 5, "Retry Scheduled") # Col E: CallStatus
                    else: # 'completed'
                        worksheet.update_cell(sheet_row_index, 5, "Delivered")
            continue # Done with this user for this run

        # --- Initiate new calls for today ---
        if call_status == '':
            call_time_str = row.get('CallTime', '')
            if not call_time_str: continue

            call_time_ist = datetime.strptime(call_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=IST)
            if call_time_ist < now_ist:
                print(f"Initiating daily call for {row['PhoneNumber']} scheduled at {call_time_ist.strftime('%H:%M:%S')}")
                
                audio_url = AUDIO_MAPPING.get(row['Location'])
                if not audio_url:
                    print(f"Warning: No audio mapping for {row['Location']}. Skipping.")
                    continue
                
                call_sid, status = make_call(row['PhoneNumber'], audio_url)
                
                # Write timestamps back in IST
                last_called_ist_str = now_ist.strftime('%Y-%m-%d %H:%M:%S')
                worksheet.update_cell(sheet_row_index, 5, status)
                worksheet.update_cell(sheet_row_index, 6, last_called_ist_str)
                if call_sid:
                    worksheet.update_cell(sheet_row_index, 8, call_sid)

    print("--- Script Finished ---")

if __name__ == "__main__":
    main()
