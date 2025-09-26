import re
import requests
from urllib.request import urlopen
import io
import zipfile
import gzip
import datetime
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple
import obstore
from obstore.store import from_url


def download_files(urls: List[str], folders: List[str], totalfiles: int, destination: str, max_workers: int) -> str:
    """
    Optimized download function using obstore for OneLake operations.

    Fixes:
    - Always re-reads the log from OneLake (no stale cache issue)
    - Log appends instead of overwriting order
    - Parallel downloads/uploads for speed
    - totalfiles batching preserved
    """
    store = from_url(destination)
    summary = []

    def process_url(url_folder_pair: Tuple[str, str]) -> str:
        url, folder = url_folder_pair

        clean_folder = folder.strip("/")
        if clean_folder and not clean_folder.endswith("/"):
            clean_folder += "/"

        log_file_name = clean_folder + "download_log.csv"

        # Step 1: Read log fresh from OneLake
        downloaded_files = []
        try:
            log_data = obstore.get(store, log_file_name)
            log_content = log_data.decode("utf-8").strip()
            if log_content:
                downloaded_files = [line.split(",")[0] for line in log_content.splitlines()[1:]]
        except Exception:
            downloaded_files = []

        def extract_week_partition(filename: str) -> str:
            match = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
            if match:
                year, month, day = map(int, match.groups())
                dt = datetime.date(year, month, day)
                iso_year, iso_week, _ = dt.isocalendar()
                return f"week={iso_year}_{iso_week:02d}"
            return "week=unknown"

        try:
            result = urlopen(url).read().decode("utf-8")
        except Exception:
            return f"{url} - Connection failed"

        pattern = re.compile(r"[\w.-]+\.zip")
        all_files = sorted(dict.fromkeys(pattern.findall(result)), reverse=True)

        # Only files not already in the log
        new_files = sorted(list(set(all_files) - set(downloaded_files)), reverse=True)[:totalfiles]

        if not new_files:
            return f"{url} - 0 files extracted (all up to date)"

        uploaded_log_entries = []
        batch_uploads = []

        def process_file(filename: str):
            try:
                download_url = url + filename
                with requests.get(download_url, stream=True, timeout=30) as resp:
                    if not resp.ok:
                        return None
                    zip_bytes = io.BytesIO(resp.content)
                    with zipfile.ZipFile(zip_bytes, "r") as zf:
                        results = []
                        for zip_info in zf.infolist():
                            with zf.open(zip_info) as extracted:
                                gzip_buffer = io.BytesIO()
                                with gzip.GzipFile(filename=zip_info.filename, mode="wb", fileobj=gzip_buffer) as gz:
                                    gz.write(extracted.read())
                                gz_name = zip_info.filename + ".gz"
                                partition_folder = clean_folder + extract_week_partition(filename) + "/"
                                gz_filename = partition_folder + gz_name
                                results.append((filename, gz_filename, gzip_buffer.getvalue()))
                        return results
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                return None

        # Step 2: Parallelize file-level downloads
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(process_file, fn) for fn in new_files]
            for f in as_completed(futures):
                res = f.result()
                if res:
                    for zipf, gzf, data in res:
                        uploaded_log_entries.append((zipf, gzf))
                        batch_uploads.append((gzf, data))

        # Step 3: Parallelize uploads
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(obstore.put, store, gz_filename, data) for gz_filename, data in batch_uploads]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    print(f"Error uploading: {e}")

        # Step 4: Append to log
        if uploaded_log_entries:
            try:
                existing_lines = []
                try:
                    existing_log_data = obstore.get(store, log_file_name)
                    existing_log = existing_log_data.decode("utf-8").strip()
                    existing_lines = existing_log.splitlines()[1:] if existing_log else []
                except:
                    existing_lines = []

                new_lines = [f"{zipf},{gzf}" for zipf, gzf in uploaded_log_entries]
                all_log_lines = existing_lines + new_lines
                log_content = "zip_filename,extracted_filepath\n" + "\n".join(all_log_lines)

                obstore.put(store, log_file_name, log_content.encode("utf-8"))

            except Exception as e:
                print(f"Error updating log file {log_file_name}: {e}")

        return f"{url} - {len(uploaded_log_entries)} files extracted"

    # Process URLs concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(process_url, (url, folder)): url
            for url, folder in zip(urls, folders)
        }
        for future in as_completed(future_to_url):
            try:
                result = future.result()
                summary.append(result)
            except Exception as e:
                url = future_to_url[future]
                summary.append(f"{url} - Error: {e}")

    return "\n".join(summary)
