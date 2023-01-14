import os, pathlib, concurrent.futures, polars as pl
from tqdm import tqdm
from typing import *
from timeit import default_timer as timer

def load_paths(path: str, show_progress = False) -> pl.DataFrame:
    """use pathlib rglob to get all files and folders in a directory"""
    def _get_size(path: str) -> int:
        """get size of a file"""
        try:
                return os.path.getsize(path) if os.path.isfile(path) else -1
        except:
            return -100 
    # pathx = abs path of path
    df = pl.DataFrame(
        {
            "PATH": [str(p) for p in pathlib.Path(path).rglob("*")],
        }
    )
    
    df = df.lazy() \
    .with_column(pl.col("PATH").str.replace_all(r"\\", "/").alias("PATH")) \
    .with_column(pl.col("PATH").apply(lambda x: _get_size(x)).alias("SIZE")) \
    .with_column(pl.col("SIZE").apply(lambda x: x == -1).alias("IS_DIR")) \
    .with_column(pl.col("PATH").apply(lambda x: len(x.split("/"))).alias("LEVEL")) \
    .with_column(pl.col("PATH").apply(lambda x: "/".join(x.split("/")[:-1])).alias("PARENT_PATH")) \
    .with_column(pl.col("PATH").apply(lambda x: x.split("/")[-1]).alias("NAME")) \
    .with_column(pl.col("SIZE").apply(lambda x: 0 if x < 0 else x).alias("SIZE")) \
    .collect()

    for lvl in range(df["LEVEL"].max() - 1, df["LEVEL"].min(), -1):
        """
        children = lvl + 1
        parent = lvl
        select the children which have a parent, and add the size of the children to the parent
        # for folders
        """


        


    return df

target = "E:/jGit/pydirstats/pydirstats"
start = timer()
df = load_paths(target)
# print(df.head(15))

for i, r in enumerate(df.iterrows()):
    print(i, r)
    

print(f"load_paths took -- {(timer()) - start} -- seconds")
# print size to 3 decimal places
print(f"Total File Count : {len(df.filter(pl.col('IS_DIR') == False))}")
print(f"Total Dirx Count : {len(df.filter(pl.col('IS_DIR') == True))}")
print(f"Total Item Count : {len(df)}")
print(f"Total Size  (KB) : {(df['SIZE'].sum() / 1000):.3f}")
print(f"Total Size  (MB) : {(df['SIZE'].sum() / 1000 ** 2):.3f}")
print(f"Total Size  (GB) : {(df['SIZE'].sum() / 1000 ** 3):.3f}")


    
