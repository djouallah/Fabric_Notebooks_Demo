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
import threading


def download_files(urls: List[str], folders: List[str], totalfiles: int, ws: str, lh: str, max_workers: int) -> int:
    """
    Optimized download function with enhanced GitHub ZIP support.
    Maintains HTML parsing, parallelism, logging, and OneLake upload.
    """
    store = from_url(f'abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}.Lakehouse/Files')
    summary = []
    total_files_processed = 0
    log_lock = threading.Lock()

    def resolve_github_download_url(base_url: str, filename: str) -> str:
        """
        Convert a GitHub directory-style URL + .zip filename into a valid downloadable link.
        Handles:
        - /tree/main/path/   -> convert to archive link
        - /blob/main/path/   -> invalid (can't zip single file?)
        - /releases/tag/     -> direct
        - codeload.github.com fallback
        """
        base_url = base_url.strip("/")
        
        # If already points to codeload or release asset, return as-is
        if "codeload.github.com" in base_url or "github.com" in base_url and "/releases/download/" in base_url:
            return f"{base_url}/{filename}"

        # Match github.com/user/repo[/tree/branch/subdir]
        tree_pattern = r"https?://github\.com/([^/]+)/([^/]+)(?:/tree/([^/]+)(/.*)?)?"
        match = re.match(tree_pattern, base_url)
        if match:
            user, repo, branch, subpath = match.groups()
            branch = branch or "main"  # default branch
            subpath = subpath or ""

            # Use GitHub's codeload which allows partial repo zips
            # Note: codeload does NOT support subdirectory-only zip; gives full repo
            # So we still need to filter inside zip later
            return f"https://codeload.github.com/{user}/{repo}/zip/refs/heads/{branch}"

        # Fallback: assume regular server
        return base_url.rstrip("/") + "/" + filename

    def extract_week_partition(filename: str) -> str:
        match = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
        if match:
            year, month, day = map(int, match.groups())
            dt = datetime.date(year, month, day)
            iso_year, iso_week, _ = dt.isocalendar()
            return f"week={iso_year}_{iso_week:02d}"
        return "week=unknown"

    def process_url(url_folder_pair: Tuple[str, str]) -> Tuple[str, int]:
        url, folder = url_folder_pair
        clean_folder = folder.strip("/")
        if clean_folder and not clean_folder.endswith("/"):
            clean_folder += "/"
        log_file_name = clean_folder + "download_log.csv"

        # Read log fresh
        with log_lock:
            downloaded_files = set()
            try:
                log_result = obstore.get(store, log_file_name)
                log_bytes = log_result.bytes()
                log_content = bytes(log_bytes).decode("utf-8").strip()
                if log_content:
                    lines = log_content.splitlines()
                    if len(lines) > 1:
                        for line in lines[1:]:
                            if line.strip():
                                parts = line.split(",", 1)
                                downloaded_files.add(parts[0].strip())
            except Exception as e:
                print(f"Could not read log file {log_file_name}: {e}")
                downloaded_files = set()

        try:
            response = urlopen(url)
            result = response.read().decode("utf-8")
        except Exception as e:
            return f"{url} - Connection failed: {e}", 0

        pattern = re.compile(r"[\w.-]+\.zip")
        all_files = sorted(dict.fromkeys(pattern.findall(result)), reverse=True)
        new_files = [f for f in all_files if f not in downloaded_files][:totalfiles]

        if not new_files:
            return f"{url} - 0 files extracted (all {len(all_files)} already downloaded)", 0

        uploaded_log_entries = []
        batch_uploads = []

        def process_file(filename: str):
            try:
                # ✅ Smart URL resolution (GitHub-aware)
                download_url = resolve_github_download_url(url, filename)
                print(f"Downloading from: {download_url}")

                with requests.get(download_url, stream=True, timeout=60) as resp:
                    resp.raise_for_status()
                    zip_data = io.BytesIO(resp.content)

                with zipfile.ZipFile(zip_data, "r") as zf:
                    results = []
                    for zip_info in zf.infolist():
                        if zip_info.is_dir():
                            continue

                        # For GitHub archives: strip top-level dir like "repo-main/"
                        name_parts = zip_info.filename.split("/")
                        actual_filename = "/".join(name_parts[1:])  # Skip root folder
                        if not actual_filename:
                            continue

                        with zf.open(zip_info) as extracted:
                            content = extracted.read()

                        gz_buffer = io.BytesIO()
                        with gzip.GzipFile(filename=actual_filename, mode="wb", fileobj=gz_buffer) as gz:
                            gz.write(content)

                        gz_name = actual_filename + ".gz"
                        partition_folder = clean_folder + extract_week_partition(filename) + "/"
                        gz_full_path = partition_folder + gz_name.replace("/", "_")  # Avoid nested dirs in Lakehouse?
                        results.append((filename, gz_full_path, gz_buffer.getvalue()))

                    return results

            except Exception as e:
                print(f"Error processing {filename} from {url}: {e}")
                return None

        # Parallelize extraction
        with ThreadPoolExecutor(max_workers=min(8, len(new_files))) as pool:
            futures = [pool.submit(process_file, fn) for fn in new_files]
            for f in as_completed(futures):
                res = f.result()
                if res:
                    for zipf, gzf, data in res:
                        uploaded_log_entries.append((zipf, gzf))
                        batch_uploads.append((gzf, data))

        if not batch_uploads:
            return f"{url} - No files to upload after processing", 0

        # Upload in parallel
        successful_uploads = []
        with ThreadPoolExecutor(max_workers=min(8, len(batch_uploads))) as pool:
            futures = {
                pool.submit(obstore.put, store, gz_filename, data): (gz_filename, orig_zip)
                for (gz_filename, data), (orig_zip, _) in zip(batch_uploads, uploaded_log_entries)
            }
            for future in as_completed(futures):
                gz_filename, orig_zip = futures[future]
                try:
                    future.result()
                    successful_uploads.append((orig_zip, gz_filename))
                except Exception as e:
                    print(f"Upload error {gz_filename}: {e}")

        # Update log safely
        if successful_uploads:
            with log_lock:
                try:
                    existing_lines = []
                    try:
                        result = obstore.get(store, log_file_name)
                        content = result.bytes()
                        log_str = bytes(content).decode("utf-8").strip()
                        if log_str:
                            lines = log_str.splitlines()
                            if len(lines) > 1:
                                existing_lines = lines[1:]
                    except Exception:
                        pass

                    new_lines = [f"{z},{g}" for z, g in successful_uploads]
                    final_lines = existing_lines + new_lines
                    updated_log = "zip_filename,extracted_filepath\n" + "\n".join(final_lines)
                    obstore.put(store, log_file_name, updated_log.encode("utf-8"))
                    print(f"✅ Log updated: {len(successful_uploads)} entries added")
                except Exception as e:
                    print(f"Failed to update log {log_file_name}: {e}")

        return f"{url} - {len(successful_uploads)} files processed", len(successful_uploads)

    # Process all URL-folder pairs concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(process_url, pair): pair
            for pair in zip(urls, folders)
        }
        for future in as_completed(future_to_url):
            try:
                msg, count = future.result()
                summary.append(msg)
                total_files_processed += count
            except Exception as e:
                pair = future_to_url[future]
                summary.append(f"{pair[0]} - Unexpected error: {e}")

    print("\n".join(summary))
    return 1 if total_files_processed > 0 else 0
