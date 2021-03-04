import json
import os

import requests

from tqdm import tqdm

from warcio.archiveiterator import ArchiveIterator

from .html2text import html_to_text
from tools.file_managment import file_downloader, download_gzip, lines_in_file, extract_gzip, save_file

BASE_URL = 'https://commoncrawl.s3.amazonaws.com/'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

col_names = [
    "cc",
    "cdx",
    "directory",
    "download_url",
    "size",
    "urls"
]


def warc_index_handler(warc_index, source_file):
    warc_url = BASE_URL + warc_index['filename']
    from_stream(warc_url, filename=warc_index['digest'], file_path="data/"+source_file)


def from_stream(warc_url, filename, file_path=None):
    target_n = 0
    if file_path is None:
        file_path = os.path.join(BASE_DIR, f"{filename}.txt")
    else:
        file_path = os.path.join(file_path, f"text.txt")
    resp = requests.get(warc_url, stream=True)

    for record in tqdm(ArchiveIterator(resp.raw, arc2warc=True), desc=f"Processing {filename}"):
        if record.rec_type == 'response':
            if record.http_headers.get_header('Content-Type') == 'text/html':
                html_string = record.content_stream().read()
                result = html_to_text(html_string, language="ka")
                if result:
                    target_n += 1
                    tqdm.write(f"Target in {filename}: {target_n}")
                    save_file(result, file_path)


def chunk_index(n):
    chunk = str(n)
    index = (5 - len(chunk)) * "0" + chunk
    return index


def cdx_url_generator(cdx_index, month_index):
    chunk_url = f"{BASE_URL}cc-index/collections/CC-MAIN-{month_index}/indexes/cdx-{cdx_index}.gz "
    return chunk_url


def language_in_index(language, index, strict=True):
    if strict:
        # pages with strictly on argument language content
        return '"languages"' in index and f'"{language}"' in index.split()[-1]
    else:
        # pages with content on argument language and different languages - might be noisy
        return '"languages"' in index and language in index.split()[-1]


def cdx_line_handler(index_line):
    if language_in_index("kat", index_line):
        # returns index of warc file as dictionary
        warc = json.loads('{"url":' + index_line.split('{"url":')[1])
        source_folder = "-".join(warc['filename'].split("/")[1].split("-")[2:4])
        warc_index_handler(warc, source_folder)


def parse_index_file_by_language(source_file, pool, remove_condition=True):
    with open(source_file) as cdx_file:
        total = lines_in_file(source_file)
        with pool:
            for _ in tqdm(pool.imap(cdx_line_handler, cdx_file), total=total):
                pass

    tqdm.write(f"- ✔ Finished processing {source_file}")
    # Remove leftover cdx file
    if remove_condition:
        os.remove(source_file)
        tqdm.write("- ✔ Removed the leftover cdx")


def main_loop(month_index, pool):
    # !!!!!!! this needs optimization !!!!!!!!
    for step in range(0, 300):
        tqdm.write(f"cdx-index: {month_index}")
        current_chunk = chunk_index(step)
        # cdx_index object outline
        cdx_index = {
            "cc": month_index,
            "cdx": f"cdx-{current_chunk}",
            "directory": f"data/{month_index}/",
            "download_url": cdx_url_generator(current_chunk, month_index),
            "size": 0,
            "urls": list()
        }

        # downloading gzip
        gzip_file = download_gzip(cdx_index['download_url'], folder_path=cdx_index['directory'])
        # extracting data
        file_path = extract_gzip(*gzip_file, folder_path=cdx_index['directory'])
        # returns warc_index for specific language. def. language = "kat"
        parse_index_file_by_language(file_path, pool)

        return f"✔ {month_index} is done"


if __name__ == "__main__":
    #  parse_index_file_by_language("D:/PycharmProjects/commoncrawl/data/2021-04/cdx-00000")
    file_url = "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-04/cc-index.paths.gz"
    file = download_gzip(file_url)
    extract_gzip(*file)