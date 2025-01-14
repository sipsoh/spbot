import logging
import requests
from azure.storage.blob import BlobServiceClient
from msal import ConfidentialClientApplication
import azure.functions as func
import os

# Azure AD credentials (from Application Settings)
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["https://graph.microsoft.com/.default"]

# SharePoint site and library info
SITE_ID = os.getenv("SITE_ID")
LIBRARY_ID = os.getenv("LIBRARY_ID")

# Form Recognizer credentials
FORM_RECOGNIZER_ENDPOINT = os.getenv("FORM_RECOGNIZER_ENDPOINT")
FORM_RECOGNIZER_KEY = os.getenv("FORM_RECOGNIZER_KEY")

# Blob Storage connection
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")

# Initialize Function App
app = func.FunctionApp()

# Function to get access token
def get_access_token():
    app = ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(SCOPES)
    return result["access_token"]

# Function to download files from SharePoint
def download_files_from_sharepoint(access_token):
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{LIBRARY_ID}/root/children"
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        files = response.json().get("value", [])
        for file in files:
            file_name = file["name"]
            download_url = file["@microsoft.graph.downloadUrl"]
            file_content = requests.get(download_url).content
            logging.info(f"Downloaded file: {file_name}")
            process_file(file_name, file_content)
    else:
        logging.error(f"Failed to fetch files: {response.status_code}, {response.text}")

# Function to process files with Form Recognizer
def process_file(file_name, file_content):
    headers = {
        "Ocp-Apim-Subscription-Key": FORM_RECOGNIZER_KEY,
        "Content-Type": "application/pdf"
    }
    
    response = requests.post(
        f"{FORM_RECOGNIZER_ENDPOINT}/formrecognizer/v2.1/layout/analyze",
        headers=headers,
        data=file_content
    )
    
    if response.status_code == 202:
        logging.info(f"Processing started for file: {file_name}")
        store_file_in_blob(file_name, file_content)
    else:
        logging.error(f"Failed to process file {file_name}: {response.status_code}, {response.text}")

# Function to store files in Blob Storage
def store_file_in_blob(file_name, file_content):
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=file_name)
    blob_client.upload_blob(file_content, overwrite=True)
    logging.info(f"Stored file: {file_name} in Blob Storage")

# Timer trigger function
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def timer_trigger1(myTimer: func.TimerRequest) -> None:
    logging.info("Azure Timer Trigger Function executed.")
    
    try:
        token = get_access_token()
        download_files_from_sharepoint(token)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
