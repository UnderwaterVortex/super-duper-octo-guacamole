import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from twilio.rest import Client
import pandas as pd
from datetime import datetime, timedelta, timezone # <-- Changed import
from zoneinfo import ZoneInfo # <-- Added import for timezones

# --- CONFIGURATION ---

# These will be set as GitHub Secrets
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
TWILIO_PHONE_NUMBER = os.environ.get('TWILIO_PHONE_NUMBER')

# This JSON content will be stored as a single multi-line GitHub Secret
GOOGLE_CREDS_JSON = os.environ.get('GOOGLE_CREDS_JSON')

# The name of your Google Sheet
SHEET_NAME = "19072025" # <-- IMPORTANT: Change this!

# Mapping of 'Location' column to your WAV file URLs from Cloudinary
AUDIO_MAPPING = {
    "North": "https://res.cloudinary.com/dqyoxump7/video/upload/v1752949596/bulletin_audio_hindi_20250629_035112.wav_1_qifvte.wav", # <-- CHANGE THESE
    "South": "https://res.cloudinary.com/dqyoxump7/video/upload/v1752949596/bulletin_audio_hindi_20250629_035112.wav_0_nm0yqq.wav", # <-- CHANGE THESE
    "East": "https://res.cloudinary.com/dqyoxump7/video/upload/v1752950393/bulletin_audio_hindi_20250629_123802.wav_1_bozis5.wav",   # <-- CHANGE THESE
    "West": "https://res.cloudinary.com/dqyoxump7/video/upload/v1752950393/bulletin_audio_hindi_20250629_123802.wav_0_evmopy.wav"    # <-- CHANGE THESE
}

# --- TIMEZONE SETUP ---
IST = ZoneInfo("Asia/Kolkata") # Define Indian Standard Time

# --- SETUP CLIENTS ---

# Authenticate with Twilio
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# Authenticate with Google Sheets
creds_dict = json.loads(GOOGLE_CREDS_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
gc = gspread.authorize(creds)
worksheet = gc.open(SHEET_NAME).sheet1

def make_call(phone_number, audio_url):
    """Initiates a call using Twilio."""
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
        return None, f"Failed: {e}"

def check_call_status(call_sid):
    """Fetches the status of a specific call from Twilio."""
    try:
        call = twilio_client.calls.fetch(call_sid)
        return call.status
    except Exception as e:
        print(f"Error fetching status for SID {call_sid}: {e}")
        return "failed"

def main():
    """Main function to run the process."""
    print("--- Running Voice Agent Script ---")
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    
    # Get the current time in UTC, making it "timezone-aware"
    now_utc = datetime.now(timezone.utc)
    print(f"Current UTC time: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Current IST time: {now_utc.astimezone(IST).strftime('%Y-%m-%d %H:%M:%S')}")

    # --- PROCESS 1: CHECK STATUS OF PENDING CALLS ---
    for index, row in df.iterrows():
        if row['CallStatus'] == 'Initiated' and row['CallSid']:
            status = check_call_status(row['CallSid'])
            sheet_row_index = index + 2

            if status in ['completed', 'busy', 'no-answer', 'failed']:
                print(f"Updating status for {row['PhoneNumber']} (SID: {row['CallSid']}) to '{status}'")
                
                if status in ['no-answer', 'busy']:
                    # The LastCalled time is in UTC, so we create an aware object to do math
                    last_called_utc = datetime.strptime(row['LastCalled'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    retry_time_utc = last_called_utc + timedelta(hours=1)
                    worksheet.update_cell(sheet_row_index, 7, retry_time_utc.strftime('%Y-%m-%d %H:%M:%S')) # Column G: RetryAt
                    worksheet.update_cell(sheet_row_index, 5, "Retry Scheduled") # Column E: CallStatus
                else:
                    final_status = "Delivered" if status == 'completed' else "Failed"
                    worksheet.update_cell(sheet_row_index, 5, final_status)

    # --- PROCESS 2: INITIATE NEW SCHEDULED CALLS ---
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    
    for index, row in df.iterrows():
        should_call = False
        
        # Check for initial calls scheduled in IST
        if row['CallStatus'] == '' and str(row.get('CallTime', '')):
            naive_call_time = datetime.strptime(str(row['CallTime']), '%Y-%m-%d %H:%M:%S')
            aware_call_time_ist = naive_call_time.replace(tzinfo=IST) # Tell Python this time is IST
            if aware_call_time_ist < now_utc: # Python compares the two aware times correctly
                should_call = True

        # Check for retries scheduled in UTC
        elif row['CallStatus'] == 'Retry Scheduled' and str(row.get('RetryAt', '')):
            naive_retry_time = datetime.strptime(str(row['RetryAt']), '%Y-%m-%d %H:%M:%S')
            aware_retry_time_utc = naive_retry_time.replace(tzinfo=timezone.utc) # Retry time is in UTC
            if aware_retry_time_utc < now_utc:
                should_call = True

        if should_call:
            phone_number = str(row['PhoneNumber'])
            location = row['Location']
            audio_url = AUDIO_MAPPING.get(location)
            
            if not audio_url:
                print(f"Warning: No audio mapping for location '{location}'. Skipping.")
                continue

            call_sid, status = make_call(phone_number, audio_url)
            sheet_row_index = index + 2
            
            # The 'LastCalled' and 'RetryAt' columns will store time in UTC for consistency
            last_called_utc_str = now_utc.strftime('%Y-%m-%d %H:%M:%S')

            worksheet.update_cell(sheet_row_index, 5, status)
            worksheet.update_cell(sheet_row_index, 6, last_called_utc_str)
            if call_sid:
                worksheet.update_cell(sheet_row_index, 8, call_sid)

    print("--- Script Finished ---")

if __name__ == "__main__":
    main()
