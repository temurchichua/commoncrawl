import json
import os
import time
from pprint import pprint

from tqdm import tqdm
from tools import get_wet

BASE_URL = 'https://commoncrawl.s3.amazonaws.com/'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))





if __name__ == "__main__":
    start = time.time()
    pprint(start)
    line = '0,123,172,167)/index.php/legales 20210117150147 {"url": "https://167.172.123.0/index.php/legales/", ' \
           '"mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": ' \
           '"A7ZADSTR36UGBGR5Z5UUTX73OBHDGCVK", "length": "31415", "offset": "191660846", "filename": ' \
           '"crawl-data/CC-MAIN-2021-04/segments/1610703513062.16/warc/CC-MAIN-20210117143625-20210117173625' \
           '-00548.warc.gz", "charset": "UTF-8", "languages": "spa"} '

    index = json.loads('{"url":' + line.split('{"url":')[1])
    pprint(index)

    url = BASE_URL + index['filename']
    get_wet(url, index['digest'])

    end = time.time()
    tqdm.write(str(end - start))

# print(f"wet_url: {wet_url}, \nfile_dir: {file_dir}, \nfile_name: {file_name}, \nfile_path: {file_path}")
