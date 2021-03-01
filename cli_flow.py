import os
from multiprocessing import Pool, TimeoutError

from tqdm import tqdm
import pandas as pd
from tools import (download_gzip, extract_gzip,
                   cdx_url_generator,
                   parse_index_file_by_language, chunk_index, main_loop)
# import CC indexes
from data.indexes import month_indexes

pool = Pool(2)

col_names = [
    "cc",
    "cdx",
    "directory",
    "download_url",
    "size",
    "urls"
]

dataframe = pd.DataFrame(columns=col_names)


if __name__ == "__main__":
    with pool:
        MAX_COUNT = len(month_indexes)
        tqdm.write(str(month_indexes))

        for result in tqdm(pool.imap(main_loop, month_indexes), total=MAX_COUNT, desc="CC-index"):
            dataframe = dataframe.append(result, ignore_index=True)
            dataframe.to_csv('data/result.csv', index=False)