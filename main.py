import os
import json
import requests
import rich.progress
import concurrent.futures
from bs4 import BeautifulSoup
from urllib.parse import unquote


BASE_URL = "https://nekit.sytes.net/files/gems/"

OUTPUT_FOLDER = "output"
CACHE_FILE = "html-cache.json"
WORKERS = 3


def get_resource_urls(html: str):
    html = BeautifulSoup(html, 'html.parser')
    urls = [unquote(a.attrs['href']) for a in html.find_all('a')]
    return [url[2:] for url in urls if url.startswith('./')]


def format_output_filename(filename: str):
    return filename.replace('[AliQ] S01E', '')


def get_html(url: str) -> str:
    if not os.path.exists(CACHE_FILE):
        print('Creating cache file ...')
        with open(CACHE_FILE, 'w') as file:
            json.dump({}, file)

    cache = json.load(open(CACHE_FILE, 'r'))

    if url in cache:
        print(f'Loading \"{url}\" from cache ...')
        return cache[url]

    print(f'Getting \"{url}\" ...')
    resp = requests.get(url)
    resp.raise_for_status()

    cache[url] = resp.text

    print(f'Caching \"{url}\" ...')
    json.dump(cache, open(CACHE_FILE, 'w'), indent=4)

    return resp.text


if __name__ == '__main__':
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    html = get_html(BASE_URL)
    vids = get_resource_urls(html)

    print(f'Found {len(vids)} URLs')

    progress = rich.progress.Progress(
        rich.progress.TextColumn("{task.description}"),
        rich.progress.BarColumn(),
        rich.progress.DownloadColumn(),
        rich.progress.TransferSpeedColumn(),
        rich.progress.TimeRemainingColumn(),
    )

    progress.start()

    def download_resource(vid: str):
        vid_filename = os.path.join(OUTPUT_FOLDER, vid)
        output_vid_filename = format_output_filename(vid_filename)

        if os.path.exists(output_vid_filename):
            s = os.path.getsize(output_vid_filename)
            progress.add_task(f'[green]{vid}', total=s, completed=s)
            return

        task = progress.add_task(vid, total=0, start=False)

        try:
            resp = requests.get(BASE_URL + vid, stream=True)
            if not resp.ok or resp.headers.get('Content-Length') is None:
                progress.start_task(task)
                progress.update(task, f'[red]{vid} ({resp.status_code})')
                progress.stop_task(task)
                resp.raise_for_status()
        except Exception as e:
            progress.start_task(task)
            progress.update(task, f'[red]{vid} ({str(e)})')
            progress.stop_task(task)
            raise e

        vid_size = int(resp.headers['Content-Length'])

        progress.start_task(task)
        progress.update(task, total=vid_size)

        try:
            with open(output_vid_filename, 'wb') as file:
                for data in resp.iter_content(chunk_size=4096):
                    file.write(data)
                    progress.update(task, advance=len(data))
                progress.update(task, description=f'[green]{vid}')
        except Exception as e:
            os.remove(output_vid_filename)
            progress.update(task, description=f'[red]{vid} ({str(e)})')

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(download_resource, vid) for vid in vids]

        concurrent.futures.wait(futures)
        # for future in concurrent.futures.as_completed(futures):
        #     if future.exception() is not None:
        #         pool.shutdown(cancel_futures=True)

    progress.stop()
