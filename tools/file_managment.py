import os
import subprocess
from sys import platform

import gzip
import shutil

from urllib import error
from urllib.request import urlretrieve

from tqdm import tqdm


class TqdmUpTo(tqdm):
    """https://gist.github.com/leimao/37ff6e990b3226c2c9670a2cd1e4a6f5
    Provides `update_to(n)` which uses `tqdm.update(delta_n)`.
    Inspired by [twine#242](https://github.com/pypa/twine/pull/242),
    [here](https://github.com/pypa/twine/commit/42e55e06).
    """

    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)  # will also set self.n = b * bsize


def file_downloader(url, save_dir):
    filename = url.split('/')[-1].strip()
    with TqdmUpTo(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=filename) as t:
        urlretrieve(url, filename=os.path.join(save_dir, filename), reporthook=t.update_to)


def download_gzip(gzip_url, folder_path='data/'):
    """
    Downloads gzip file and stores it locally
    Args:
        gzip_url (str): url to download file
        folder_path (str): path of directory to store the downloaded file
    Returns:
        (str, str) : path of the file and name of the file
    """
    # create directory if not exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # get file name and file extension from url
    base_name = os.path.basename(gzip_url)
    file_name, file_extension = os.path.splitext(base_name)

    # generate local path for file
    file_path = os.path.join(folder_path, base_name).strip()
    print(file_path)
    if os.path.exists(file_path):
        tqdm.write(f"File {file_name} has been found")
        return file_path, file_name
    try:
        tqdm.write(f'Downloading file: {file_name}')
        file_downloader(url=gzip_url, save_dir=folder_path)
    except error.HTTPError as e:
        tqdm.write(e)
        tqdm.write(f'- ❌ Download has failed from:\n{gzip_url}')
        return None
    else:
        tqdm.write(f'- ✔ Downloaded successfully: {base_name}')
        return file_path, file_name


def extract_gzip(gzip_file, out_file_name=None, gzip_folder_path="data/", folder_path="data/", remove_condition=True):
    # gzip_path = os.path.join(gzip_folder_path, gzip_file)
    # !!! needs optimization
    gzip_path = os.path.realpath(gzip_file).strip()
    # if no custom name specified file will be saved as: (original - ".gz")
    if out_file_name is None:
        out_file_name, file_extension = os.path.splitext(gzip_path)

    result_path = os.path.join(folder_path, out_file_name).strip()

    tqdm.write(f"🗄️ Extracting {gzip_path} > {result_path}")
    with gzip.open(gzip_path, 'rb') as f_in:
        with open(result_path, 'wb') as f_out:
            tqdm.write(f"⚙ Copying file(s) [might take a while, grab a coffee ☕ ]")
            shutil.copyfileobj(f_in, f_out)
            tqdm.write("- ✔ Done")

    # Remove leftover gzip file
    if remove_condition:
        os.remove(gzip_path)
        tqdm.write("- ✔ Removed the leftover gzip")

    return result_path


def lines_in_file(file_path):
    if platform == "linux" or platform == "linux2":
        result = int(subprocess.check_output(['wc', '-l', file_path]).split()[0])

    elif platform == "win32":
        with open(file_path) as f:
            result = sum(1 for line in tqdm(f, desc=f"Count for {file_path}"))

    return result


def save_file(contents, location="demo.txt"):
    print(location)
    with open(location, 'a', encoding='utf-8') as f:
        f.write(contents)

# if base_name.endswith("tar.gz"):
#     tar = tarfile.open(base_name, "r:gz")
#     tar.extractall()
#     tar.close()
# elif base_name.endswith("tar"):
#     tar = tarfile.open(base_name, "r:")
#     tar.extractall()
#     tar.close()