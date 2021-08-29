from datetime import datetime

import numpy as np
import polars as pl
from pathlib import Path
from polars import col, lit
import pandas as pd
from pandas.io.common import get_handle
from tqdm import tqdm

big_csv = Path("./big.csv")
def download_big_data_file():
    
    csv_url = "http://sdm.lbl.gov/fastbit/data/star2002-full.csv.gz"

    if not big_csv.exists():
        with get_handle(csv_url, compression="gzip", mode="r") as fh_in, open(big_csv, "bw") as fh_out:
            fh_out.write(fh_in.handle.buffer.read())


if __name__ == "__main__":
    download_big_data_file()
    
    edf = pl.read_csv(str(big_csv), has_headers=False)
    print(edf.filter(col("column_1") == 1).select(["column_9"]).head())
    print(edf.describe())
    # print(edf.info()) # method not found will error