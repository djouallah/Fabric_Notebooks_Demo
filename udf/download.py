import re
import requests
from   urllib.request import urlopen
import fabric.functions as fn
import io
import zipfile
import gzip
import datetime

udf = fn.UserDataFunctions()

@udf.connection(argName="myLakehouse", alias="data")
@udf.function()
def download(myLakehouse: fn.FabricLakehouseClient, urls: list[str], folders: list[str], totalfiles: int) -> str:
    connection = myLakehouse.connectToFiles()

    summary = []

    for url, folder in zip(urls, folders):
        if not folder.endswith("/"):
            folder += "/"

        log_file_name = folder + "download_log.csv"

        # Step 1: Read existing CSV log (if exists)
        downloaded_files = []
        try:
            log_file = connection.get_file_client(log_file_name)
            download_file = log_file.download_file()
            log_content = download_file.readall().decode("utf-8")
            if log_content.strip():
                downloaded_files = [line.split(",")[0] for line in log_content.strip().splitlines()[1:]]  # Skip header
            log_file.close()
        except:
            downloaded_files = []

        def extract_week_partition(filename: str) -> str:
            match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
            if match:
                year, month, day = map(int, match.groups())
                dt = datetime.date(year, month, day)
                iso_year, iso_week, _ = dt.isocalendar()
                return f"week={iso_year}_{iso_week:02d}"
            return "week=unknown"

        uploaded_log_entries = []
        extracted_paths = []

        try:
            result = urlopen(url).read().decode('utf-8')
        except Exception:
            continue  # Skip bad URL

        pattern = re.compile(r'[\w.-]+\.zip')
        all_files = sorted(dict.fromkeys(pattern.findall(result)), reverse=True)
        new_files = sorted(list(set(all_files) - set(downloaded_files)), reverse=True)[:totalfiles]

        if not new_files:
            summary.append(f"{url} - 0 files extracted")
            continue

        for filename in new_files:
            try:
                download_url = url + filename
                with requests.get(download_url, stream=True) as resp:
                    if resp.ok:
                        zip_bytes = io.BytesIO(resp.content)
                        with zipfile.ZipFile(zip_bytes, 'r') as zf:
                            for zip_info in zf.infolist():
                                with zf.open(zip_info) as extracted:
                                    gzip_buffer = io.BytesIO()
                                    with gzip.GzipFile(filename=zip_info.filename, mode='wb', fileobj=gzip_buffer) as gz:
                                        gz.write(extracted.read())

                                    # Preserve original file name and just add .gz
                                    gz_name = zip_info.filename + ".gz"
                                    partition_folder = folder + extract_week_partition(filename) + "/"
                                    gz_filename = partition_folder + gz_name

                                    target_file = connection.get_file_client(gz_filename)
                                    target_file.upload_data(gzip_buffer.getvalue(), overwrite=True)
                                    target_file.close()

                                    uploaded_log_entries.append((filename, gz_filename))
                                    extracted_paths.append(gz_filename)
            except:
                continue

        # Step 3: Update the CSV log with two columns: zip and extracted file paths
        if uploaded_log_entries:
            try:
                log_file = connection.get_file_client(log_file_name)
                download_file = log_file.download_file()
                existing_log = download_file.readall().decode("utf-8")
                existing_lines = existing_log.strip().splitlines()[1:] if existing_log.strip() else []
            except:
                existing_lines = []

            new_lines = [f"{zipf},{gzf}" for zipf, gzf in uploaded_log_entries]
            all_log_lines = existing_lines + new_lines
            log_content = "zip_filename,extracted_filepath\n" + "\n".join(sorted(all_log_lines))

            log_file = connection.get_file_client(log_file_name)
            log_file.upload_data(log_content, overwrite=True)
            log_file.close()

        summary.append(f"{url} - {len(extracted_paths)} files extracted")

    connection.close()
    return "\n".join(summary)
