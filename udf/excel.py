import fabric.functions as fn
import logging
import os
import requests
from urllib.parse import urlparse
import pandas as pd
from io import BytesIO, StringIO

udf = fn.UserDataFunctions()

@udf.connection(argName="myLakehouse", alias="data")
@udf.function()
def download_to_lakehouse(myLakehouse: fn.FabricLakehouseClient, urls: list[str], folder: str) -> str:
    # Ensure folder ends with slash
    if not folder.endswith("/"):
        folder += "/"

    connection = myLakehouse.connectToFiles()
    saved_paths = []

    for url in urls:
        filename = os.path.basename(urlparse(url).path)
        if not filename:
            continue  # Skip invalid URLs

        try:
            # Make the request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            # Save to Lakehouse
            target_path = folder + filename
            file_client = connection.get_file_client(target_path)
            file_client.upload_data(response.content, overwrite=True)
            file_client.close()

            saved_paths.append(target_path)

            # ✨ NEW LOGIC: Handle AEMO .xls sheet "PU and Scheduled Loads"
            if filename.lower() == "nem-registration-and-exemption-list.xls":
                try:
                    xls_bytes = BytesIO(response.content)
                    df = pd.read_excel(xls_bytes, sheet_name="PU and Scheduled Loads")

                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False)

                    csv_filename = "nem-registration-and-exemption-list_PU_and_Scheduled_Loads.csv"
                    csv_path = folder + csv_filename

                    csv_client = connection.get_file_client(csv_path)
                    csv_client.upload_data(csv_buffer.getvalue().encode('utf-8'), overwrite=True)
                    csv_client.close()

                    saved_paths.append(csv_path)
                except Exception as e:
                    logging.error(f"Failed to convert sheet 'PU and Scheduled Loads' from {filename} to CSV: {str(e)}")

        except Exception as e:
            logging.error(f"Failed to download {url}: {str(e)}")

    connection.close()
    return "\n".join(saved_paths)
