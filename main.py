import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from twilio.rest import Client
import pandas as pd
from datetime import datetime, timedelta
import json

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
    # Add all your location-to-audio mappings here
}

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
        return call.status # e.g., 'completed', 'no-answer', 'busy', 'failed'
    except Exception as e:
        print(f"Error fetching status for SID {call_sid}: {e}")
        return "failed" # Assume failed if we can't fetch it

def main():
    """Main function to run the process."""
    print("--- Running Voice Agent Script ---")
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    now = datetime.utcnow()
    print(f"Current UTC time: {now}")

    # Process 1: Check status of pending calls
    for index, row in df.iterrows():
        if row['CallStatus'] == 'Initiated' and row['CallSid']:
            status = check_call_status(row['CallSid'])
            sheet_row_index = index + 2

            if status in ['completed', 'busy', 'no-answer', 'failed']:
                print(f"Updating status for {row['PhoneNumber']} (SID: {row['CallSid']}) to '{status}'")
                
                if status in ['no-answer', 'busy']:
                    last_called_time = datetime.strptime(row['LastCalled'], '%Y-%m-%d %H:%M:%S')
                    retry_time = last_called_time + timedelta(hours=1)
                    worksheet.update_cell(sheet_row_index, 7, retry_time.strftime('%Y-%m-%d %H:%M:%S')) # Column G: RetryAt
                    worksheet.update_cell(sheet_row_index, 5, "Retry Scheduled") # Column E: CallStatus
                else:
                    final_status = "Delivered" if status == 'completed' else "Failed"
                    worksheet.update_cell(sheet_row_index, 5, final_status) # Column E: CallStatus

    # Process 2: Initiate new scheduled calls
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    
    for index, row in df.iterrows():
        should_call = False
        call_time_str = str(row.get('CallTime', ''))
        retry_time_str = str(row.get('RetryAt', ''))

        if row['CallStatus'] == '' and call_time_str:
            if datetime.strptime(call_time_str, '%Y-%m-%d %H:%M:%S') < now:
                should_call = True
        elif row['CallStatus'] == 'Retry Scheduled' and retry_time_str:
            if datetime.strptime(retry_time_str, '%Y-%m-%d %H:%M:%S') < now:
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
            current_utc_time_str = now.strftime('%Y-%m-%d %H:%M:%S')

            worksheet.update_cell(sheet_row_index, 5, status)
            worksheet.update_cell(sheet_row_index, 6, current_utc_time_str)
            if call_sid:
                worksheet.update_cell(sheet_row_index, 8, call_sid)

    print("--- Script Finished ---")

if __name__ == "__main__":
    main()
