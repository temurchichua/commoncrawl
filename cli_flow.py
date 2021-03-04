import multiprocessing as mp
from tools import main_loop

import pandas as pd
from data.indexes import month_indexes
import argparse

col_names = [
    "cc",
    "cdx",
    "directory",
    "download_url",
    "size",
    "urls"
]

dataframe = pd.DataFrame(columns=col_names)

parser = argparse.ArgumentParser("CC handler processor")
parser.add_argument("--subs",
                    default=1,
                    help="Number of multiprocess.",
                    type=int)

args = parser.parse_args()
subs = args.subs
num_workers = mp.cpu_count()

if subs >= num_workers:
    print("Reduce the size of processes and try again")
    exit()

pool = mp.Pool(subs)

if __name__ == "__main__":
    main_loop(month_indexes[0], pool)
    # for month_index in month_indexes[:1]:
    #     main_loop(month_index, pool)
