import os
import gzip
import shutil
from urllib import request, error
import progressbar
import pandas as pd

from tqdm import tqdm

col_names = [
    "cc",
    "cdx",
    "directory",
    "download_url",
    "size",
    "urls"
]


class ProgressBar:
    def __init__(self):
        self.pbar = None

    def __call__(self, block_num, block_size, total_size):
        """
             block_num: currently downloaded block
             block_size: block size for each transfer
             total_size: total size of web page files
        """
        if not self.pbar:
            self.pbar = progressbar.ProgressBar(maxval=total_size)
            self.pbar.start()

        downloaded = block_num * block_size
        if downloaded < total_size:
            self.pbar.update(downloaded)
        else:
            self.pbar.finish()


def chunk_index(n):
    chunk = str(n)
    chunk_index = (5 - len(chunk)) * "0" + chunk
    return chunk_index


def cdx_url_generator(chunk_index, month_index):
    chunk_url = f"https://commoncrawl.s3.amazonaws.com/cc-index/collections/CC-MAIN-{month_index}/indexes/cdx-{chunk_index}.gz"
    return chunk_url


def download_gzip(file_url, folder_path='data/'):
    # create directory if not exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # get file name and file extension from url
    base_name = os.path.basename(file_url)
    file_name, file_extension = os.path.splitext(base_name)

    # generate local path for file
    file_path = os.path.join(folder_path, base_name)

    try:
        print(f'Downloading file: {file_name} ')
        request.urlretrieve(file_url, file_path, ProgressBar())
    except error.HTTPError as e:
        print(e)
        print(f'- ❌ Download has failed from:\n{file_url}')
        return None
    else:
        print(f'- ✔ Downloaded successfully: {file_path}')
        return file_path, file_name


def extract_gzip(gzip_path, file_name, folder_path="data/", remove_condition=True):
    result_path = os.path.join(folder_path, file_name)

    print(f"Extracting {gzip_path} > {result_path}")
    with gzip.open(gzip_path, 'rb') as f_in:
        with open(result_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            print("- ✔ Done")

    # Remove leftover gzip file
    if remove_condition:
        os.remove(gzip_path)
        print("- ✔ Removed the leftover gzip")

    return result_path


def get_num_lines(file_path):
    with open(file_path) as f:
        for i, l in enumerate(tqdm(f, desc=f"Count for {file_path}")):
            pass
    return i + 1


def language_in_index(language, index, strict=True):
    if strict:
        # pages with strictly on argument language content
        return '"languages"' in index and f'"{language}"' in index.split()[-1]
    else:
        # pages with content on argument language and different languages - might be noisy
        return '"languages"' in index and language in index.split()[-1]


def parse_index_file_by_language(source_file, lang="kat", remove_condition=True):
    urls = list()
    with open(source_file) as file:
        total = get_num_lines(source_file)
        for index in tqdm(file, total=total):
            if language_in_index(lang, index):
                # returns the url of the index
                urls.append(index.split()[3][1:-2])

    # Remove leftover cdx file
    if remove_condition:
        os.remove(source_file)
        print("- ✔ Removed the leftover cdx")
    return urls


def main_loop(month_index):
    for step in tqdm(range(5), desc=f"cdx-index: {month_index}"):
        current_chunck = chunk_index(step)
        # cdx_index object outline
        cdx_index = {
            "cc": month_index,
            "cdx": f"cdx-{current_chunck}",
            "directory": f"data/{month_index}/",
            "download_url": cdx_url_generator(current_chunck, month_index),
            "size": 0,
            "urls": list()
        }

        # downloading gzip
        file = download_gzip(cdx_index['download_url'], folder_path=cdx_index['directory'])
        # extracting data
        file_path = extract_gzip(*file, folder_path=cdx_index['directory'])
        # returns list of urls for specific language. def. language = "kat"
        # file[0] holds the path of the file
        cdx_index['urls'] = parse_index_file_by_language(file_path)
        cdx_index['size'] = len(cdx_index['urls'])

        # save progress to external csv file
        row = pd.Series(cdx_index, index=col_names)

        return row


if __name__ == "__main__":
    file_url = "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-04/cc-index.paths.gz"
    file = download_gzip(file_url)
    extract_gzip(*file)

# if base_name.endswith("tar.gz"):
#     tar = tarfile.open(base_name, "r:gz")
#     tar.extractall()
#     tar.close()
# elif base_name.endswith("tar"):
#     tar = tarfile.open(base_name, "r:")
#     tar.extractall()
#     tar.close()
