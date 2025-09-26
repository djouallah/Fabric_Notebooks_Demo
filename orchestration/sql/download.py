import re
import requests
from   urllib.request import urlopen
import io
import zipfile
import gzip
import datetime
import os
from   concurrent.futures import ThreadPoolExecutor, as_completed
from   typing import Dict, List, Tuple
import tempfile
import obstore
from   obstore.store import from_url


def download(urls: list[str], folders: list[str], totalfiles: int,destination: str, max_workers:int) -> str:
    """
    Optimized download function using obstore for OneLake operations.
    
    Key optimizations:
    1. Native obstore operations for better OneLake performance
    2. Batched file operations (single read/write per log file)
    3. Local caching of logs during session
    4. Concurrent downloads with controlled parallelism
    5. Bulk operations using temporary storage
    """
    store = from_url(destination)   
    summary = []
    log_cache: Dict[str, List[str]] = {}  # Cache downloaded files list
    
    def process_url(url_folder_pair: Tuple[str, str]) -> str:
        url, folder = url_folder_pair
        
        # Clean folder path (no need for /lakehouse/default/Files/ prefix with obstore)
        clean_folder = folder.strip("/")
        if clean_folder and not clean_folder.endswith("/"):
            clean_folder += "/"
        
        log_file_name = clean_folder + "download_log.csv"

        # Step 1: Read existing CSV log with caching using obstore
        if log_file_name not in log_cache:
            downloaded_files = []
            try:
                # Check if log exists and read it
                log_data = obstore.get(store, log_file_name)
                log_content = log_data.decode('utf-8').strip()
                if log_content:
                    downloaded_files = [line.split(",")[0] for line in log_content.splitlines()[1:]]  # Skip header
            except Exception:
                # Log file doesn't exist or error reading - start fresh
                downloaded_files = []
            log_cache[log_file_name] = downloaded_files
        else:
            downloaded_files = log_cache[log_file_name]

        def extract_week_partition(filename: str) -> str:
            match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
            if match:
                year, month, day = map(int, match.groups())
                dt = datetime.date(year, month, day)
                iso_year, iso_week, _ = dt.isocalendar()
                return f"week={iso_year}_{iso_week:02d}"
            return "week=unknown"

        try:
            result = urlopen(url).read().decode('utf-8')
        except Exception:
            return f"{url} - Connection failed"

        pattern = re.compile(r'[\w.-]+\.zip')
        all_files = sorted(dict.fromkeys(pattern.findall(result)), reverse=True)
        new_files = sorted(list(set(all_files) - set(downloaded_files)), reverse=True)[:totalfiles]

        if not new_files:
            return f"{url} - 0 files extracted (all up to date)"

        # Create temporary directory for batch operations
        with tempfile.TemporaryDirectory() as temp_dir:
            uploaded_log_entries = []
            extracted_paths = []
            batch_uploads = []  # For bulk obstore operations

            for filename in new_files:
                try:
                    download_url = url + filename
                    with requests.get(download_url, stream=True, timeout=30) as resp:
                        if resp.ok:
                            zip_bytes = io.BytesIO(resp.content)
                            with zipfile.ZipFile(zip_bytes, 'r') as zf:
                                for zip_info in zf.infolist():
                                    with zf.open(zip_info) as extracted:
                                        # Create gzipped content
                                        gzip_buffer = io.BytesIO()
                                        with gzip.GzipFile(filename=zip_info.filename, mode='wb', fileobj=gzip_buffer) as gz:
                                            gz.write(extracted.read())

                                        # Generate final paths
                                        gz_name = zip_info.filename + ".gz"
                                        partition_folder = clean_folder + extract_week_partition(filename) + "/"
                                        gz_filename = partition_folder + gz_name

                                        # Prepare for batch upload
                                        gzip_data = gzip_buffer.getvalue()
                                        batch_uploads.append((gz_filename, gzip_data))
                                        
                                        uploaded_log_entries.append((filename, gz_filename))
                                        extracted_paths.append(gz_filename)
                except Exception as e:
                    print(f"Error processing {filename}: {e}")
                    continue

            # Batch upload all files to OneLake using obstore
            for gz_filename, gzip_data in batch_uploads:
                try:
                    obstore.put(store, gz_filename, gzip_data)
                except Exception as e:
                    print(f"Error uploading {gz_filename}: {e}")

            # Step 2: Single batch update of CSV log using obstore
            if uploaded_log_entries:
                try:
                    existing_lines = []
                    # Read existing log if it exists
                    if log_file_name in log_cache and log_cache[log_file_name]:
                        try:
                            existing_log_data = obstore.get(store, log_file_name)
                            existing_log = existing_log_data.decode('utf-8').strip()
                            existing_lines = existing_log.splitlines()[1:] if existing_log else []
                        except:
                            existing_lines = []

                    new_lines = [f"{zipf},{gzf}" for zipf, gzf in uploaded_log_entries]
                    all_log_lines = existing_lines + new_lines
                    log_content = "zip_filename,extracted_filepath\n" + "\n".join(sorted(all_log_lines))

                    # Single write operation to OneLake using obstore
                    obstore.put(store, log_file_name, log_content.encode('utf-8'))
                    
                    # Update cache
                    log_cache[log_file_name].extend([entry[0] for entry in uploaded_log_entries])
                    
                except Exception as e:
                    print(f"Error updating log file {log_file_name}: {e}")

            return f"{url} - {len(extracted_paths)} files extracted"

    # Process URLs concurrently with controlled parallelism
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all URL processing tasks
        future_to_url = {
            executor.submit(process_url, (url, folder)): url 
            for url, folder in zip(urls, folders)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_url):
            try:
                result = future.result()
                summary.append(result)
            except Exception as e:
                url = future_to_url[future]
                summary.append(f"{url} - Error: {e}")

    return "\n".join(summary)
