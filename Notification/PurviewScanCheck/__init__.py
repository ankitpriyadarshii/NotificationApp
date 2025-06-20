import os
import json
import logging
import requests
import azure.functions as func
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText

load_dotenv()

# Load env vars
PURVIEW_ENDPOINT = os.getenv("PURVIEW_ENDPOINT")
DATA_SOURCE_NAME = os.getenv("DATA_SOURCE_NAME")
SCAN_NAME = os.getenv("SCAN_NAME")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))

LAST_DISCOVERY_FILE = "last_discovery.json"

def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    #headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://purview.azure.net/.default"
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

def get_latest_scan_info(token):
    url = f"{PURVIEW_ENDPOINT}/scan/datasources/{DATA_SOURCE_NAME}/scans/{SCAN_NAME}/runs?api-version=2023-09-01"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(url, headers=headers)
        logging.info(f"Purview API Status: {response.status_code}")
        logging.info(f"Purview API Response: {response.text[:300]}...")
        response.raise_for_status()

        result = response.json()

        # Assuming you're looking for the most recent scan
        return result["value"][0] if result.get("value") else None

    except Exception as e:
        logging.error(f"Failed to call Purview API or parse JSON: {e}")
        return None


def send_email_alert(subject, body):
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, [EMAIL_RECEIVER], msg.as_string())
        server.quit()

        logging.info(f"Email sent to {EMAIL_RECEIVER}")
    except Exception as e:
        logging.error(f"Email failed: {e}")

def main(mytimer: func.TimerRequest) -> None:
    logging.info("Function triggered")

    
    token = get_token()
    scan = get_latest_scan_info(token)

    if not scan:
        logging.info("No scan found.")
        return

    discovered_assets = scan.get("discoveryExecutionDetails", {}).get("statistics", {}).get("assets", {}).get("discovered", 0)
    run_id = scan.get("id", "unknown")
    run_time = scan.get("discoveryExecutionDetails", {}).get("discoveryStartTime", "unknown")

    last_data = {}
    if os.path.exists(LAST_DISCOVERY_FILE):
        with open(LAST_DISCOVERY_FILE, "r") as f:
            last_data = json.load(f)

    last_discovered = last_data.get("discoveredAssets", 0)

    if discovered_assets > last_discovered:
        new_assets = (discovered_assets - last_discovered) - 1
        message = f"{new_assets} new assets discovered in scan {run_id} at {run_time}"
        logging.warning(message)
        send_email_alert("Purview Alert: New Assets Detected", message)
    else:
        logging.info(f"No new assets. Last scan at {run_time} found {discovered_assets} assets.")

    with open(LAST_DISCOVERY_FILE, "w") as f:
        json.dump({
            "runId": run_id,
            "discoveredAssets": discovered_assets,
            "timestamp": run_time
        }, f)
