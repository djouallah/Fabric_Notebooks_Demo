import logging
import os
import requests
from urllib.parse import urlparse
import pandas as pd
from io import BytesIO, StringIO
import obstore as obs
from obstore.store import from_url



def download_excel( folder: str, ws: str, lh:str) -> str:
    """Download files from URLs and save to object storage using obstore"""
    store = from_url(f'abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}.Lakehouse/Files')
    if not folder.endswith("/"):
        folder += "/"

    saved_paths = []

    # The page where the download link is found. We visit this first.
    landing_page_url = "https://aemo.com.au/en/energy-systems/electricity/national-electricity-market-nem/participant-information/nem-registration-and-exemption-list"

    # Use a session object to persist headers and cookies across requests
    with requests.Session() as session:
        # Set the headers for the entire session
        session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': landing_page_url,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
        })

        try:
            # STEP 1: Visit the landing page to establish the session and get cookies.
            logging.info(f"Visiting landing page to establish session: {landing_page_url}")
            session.get(landing_page_url, timeout=15)
            logging.info("Session established successfully.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Could not visit the landing page to establish a session: {e}")
            return "" # Exit if we can't establish the session

        for url in ["https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls"]:
            filename = os.path.basename(urlparse(url).path)
            if not filename:
                continue

            try:
                # STEP 2: Download the file using the established session.
                # The session will automatically send the necessary cookies.
                logging.info(f"Attempting to download file: {url}")
                response = session.get(url, timeout=30)
                response.raise_for_status()

                # --- The rest of your code is the same ---
                target_path = folder + filename
                store.put(target_path, response.content)
                saved_paths.append(target_path)
                logging.info(f"Successfully downloaded and saved {target_path}")

                if filename.lower() == "nem-registration-and-exemption-list.xls":
                    try:
                        df = pd.read_excel(BytesIO(response.content), sheet_name="PU and Scheduled Loads")
                        csv_buffer = StringIO()
                        df.to_csv(csv_buffer, index=False)
                        csv_filename = "nem-registration-and-exemption-list_PU_and_Scheduled_Loads.csv"
                        csv_path = folder + csv_filename
                        store.put(csv_path, csv_buffer.getvalue().encode('utf-8'))
                        saved_paths.append(csv_path)
                        logging.info(f"Successfully converted and saved {csv_path}")
                    except Exception as e:
                        logging.error(f"Failed to convert sheet from {filename}: {e}")

            except requests.exceptions.HTTPError as http_err:
                logging.error(f"HTTP error for {url}: {http_err} - Status: {http_err.response.status_code}")
            except Exception as e:
                logging.error(f"An unexpected error occurred for {url}: {e}")

    return "\n".join(saved_paths)
