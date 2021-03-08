import json
import os
import time

import requests

from tqdm import tqdm

from warcio.archiveiterator import ArchiveIterator

from .html2text import html_to_text
from tools.file_managment import file_downloader, lines_in_file, save_file, gzip_to_file, download_gzip
from .slacker import post_to_slack

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


def current_time():
    return time.strftime("%H:%M:%S", time.localtime())


def chunk_index(n):
    chunk = str(n)
    index = (5 - len(chunk)) * "0" + chunk
    return index


def cdx_url_generator(cdx_index, month_index):
    chunk_url = f"{BASE_URL}cc-index/collections/CC-MAIN-{month_index}/indexes/cdx-{cdx_index}.gz "
    return chunk_url


def notify(message):
    tqdm.write(message)
    post_to_slack(message)


def language_in_index(language, index, strict=True):
    if strict:
        # pages with strictly on argument language content
        return '"languages"' in index and f'"{language}"' in index.split()[-1]
    else:
        # pages with content on argument language and different languages - might be noisy
        return '"languages"' in index and language in index.split()[-1]


def get_wet(warc_url, file_dir=None, pool=None):
    start = time.time()

    wet_url = warc_url.replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz')
    file_path = gzip_to_file(wet_url, file_dir)
    # total_lines = lines_in_file(file_path)
    with open(file_path, 'r', encoding="utf-8") as infile:
        # with pool:
        #     for _ in tqdm(pool.imap_unordered(html_to_text, infile), total=total_lines, desc="Parallel Process"):
        #         pass
        for line in infile:
            html_to_text(line)

    os.remove(file_path)
    end = time.time()
    delta = str((end - start)/60)
    notify(f"- ✔ finished wet in {delta} min")


def from_stream(warc_url, filename, file_path=None):
    if file_path is None:
        file_path = os.path.join(BASE_DIR, f"{filename}.txt")
    else:
        file_path = os.path.join(file_path, f"text.txt")
    resp = requests.get(warc_url, stream=True)

    for record in ArchiveIterator(resp.raw, arc2warc=True):
        if record.rec_type == 'response':
            if record.http_headers.get_header('Content-Type') == 'text/html':
                html_string = record.content_stream().read()
                result = html_to_text(html_string, language="ka")
                if result:
                    save_file(result, file_path)


def cdx_line_handler(index_line, pool=None, _type="wet"):
    if language_in_index("kat", index_line):
        # returns index of warc file as dictionary
        warc = json.loads('{"url":' + index_line.split('{"url":')[1])
        source_folder = "-".join(warc['filename'].split("/")[1].split("-")[2:4])
        warc_url = BASE_URL + warc['filename']
        if _type == "warc":
            from_stream(warc_url, filename=warc['digest'], file_path="data/" + source_folder)
        if _type == "wet":
            get_wet(warc_url, pool, file_dir="data/" + source_folder)


def parse_index_file_by_language(source_file, pool, remove_condition=True):
    total = lines_in_file(source_file)
    with open(source_file) as cdx_file:
        # for index_line in tqdm(cdx_file, total=total, desc="Processing CDX"):
        #     cdx_line_handler(index_line, pool)
        with pool:
            for _ in tqdm(pool.imap_unordered(cdx_line_handler, cdx_file), total=total, desc="Parallel Process"):
                pass

    tqdm.write(f"- ✔ Finished processing {source_file}")
    # Remove leftover cdx file
    if remove_condition:
        os.remove(source_file)
        tqdm.write("- ✔ Removed the leftover cdx")


def main_loop(month_index, pool):
    # !!!!!!! this needs optimization !!!!!!!!
    cc_start = time.time()
    message = f"Started working on CC-{month_index} at {current_time()}"
    notify(message)

    for step in range(0, 300):
        current_chunk = chunk_index(step)
        # time reporting
        cdx_start = time.time()
        message = f"Started working on cdx-{current_chunk} at {current_time()}"
        notify(message)
        # cdx_index object outline

        cdx_index = {
            "cc": month_index,
            "cdx": f"cdx-{current_chunk}",
            "directory": f"data/{month_index}/",
            "download_url": cdx_url_generator(current_chunk, month_index),
            "size": 0,
            "urls": list()
        }

        file_path = gzip_to_file(cdx_index['download_url'], cdx_index['directory'])

        # returns warc_index for specific language. def. language = "kat"
        parse_index_file_by_language(file_path, pool)

        cdx_end = cdx_start - time.time()
        message = f"Ended working on cdx-{current_chunk} at {current_time()} | {cdx_end}sec"
        notify(message)

    cc_end = cc_start - time.time()
    message = f"✔ {month_index} is done at {current_time} | {cc_end}"
    notify(message)
    return message


if __name__ == "__main__":
    #  parse_index_file_by_language("D:/PycharmProjects/commoncrawl/data/2021-04/cdx-00000")
    # file_url = "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-04/cc-index.paths.gz"
    start = time.time()
    tqdm.write(start)
    line = '0,123,172,167)/index.php/legales 20210117150147 {"url": "https://167.172.123.0/index.php/legales/", ' \
           '"mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": ' \
           '"A7ZADSTR36UGBGR5Z5UUTX73OBHDGCVK", "length": "31415", "offset": "191660846", "filename": ' \
           '"crawl-data/CC-MAIN-2021-04/segments/1610703513062.16/warc/CC-MAIN-20210117143625-20210117173625' \
           '-00548.warc.gz", "charset": "UTF-8", "languages": "spa"} '

    index = json.loads('{"url":' + line.split('{"url":')[1])
    tqdm.write(index)

    url = BASE_URL + index['filename']
    get_wet(url, index['digest'])

    end = time.time()
    tqdm.write(str(end - start))
