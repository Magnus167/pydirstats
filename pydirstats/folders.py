import os, pathlib
import pandas as pd
from tqdm import tqdm
from timeit import default_timer as timer
from joblib import Parallel, delayed


def get_path_df(path: str, show_progress: bool = True) -> pd.DataFrame:
    """Get a pandas dataframe with all paths in a directory recursively"""
    start = timer()
    pathglob = pathlib.Path(path).rglob("*")
    print(f"pathlib.Path(path).rglob took -- {(timer()) - start} -- seconds")
    start = timer()
    df = pd.DataFrame(data=pathglob, columns=["PATH"])
    print(f"pd.DataFrame took -- {(timer()) - start} -- seconds")
    start = timer()
    df["PATH"] = df["PATH"].apply(lambda x: str(x).replace("\\", "/"))
    print(f"df['PATH'] took -- {(timer()) - start} -- seconds")
    start = timer()
    df["NAME"] = df["PATH"].apply(lambda x: x.split("/")[-1])
    print(f"df['NAME'] took -- {(timer()) - start} -- seconds")
    start = timer()    
    df["PARENT_PATH"] = df["PATH"].apply(lambda x: "/".join(x.split("/")[:-1]))
    print(f"df['PARENT_PATH'] took -- {(timer()) - start} -- seconds")
    start = timer()
    df["LEVEL"] = df["PATH"].apply(lambda x: len(x.split("/")) - 1)
    print(f"df['LEVEL'] took -- {(timer()) - start} -- seconds")
    start = timer()
    df["IS_DIR"] = df["PATH"].apply(lambda x: os.path.isdir(x))
    print(f"df['IS_DIR']  took -- {(timer()) - start} -- seconds")
    start = timer()
    df["SIZE"] = 0
    return df


def get_sizes(df: pd.DataFrame, show_progress: bool = True) -> pd.DataFrame:
    """Gets the size of each file in a dataframe. folders are calculated using pandas masks"""
    def _get_size(path):
        try:
            return os.path.getsize(path)
        except:
            return -1

    start = timer()
    r = Parallel(n_jobs=-1, pre_dispatch="all")(
        delayed(os.path.getsize)(row["PATH"]) for i, row in tqdm(df[df["IS_DIR"] == False].iterrows(), disable=not show_progress)
    )
    
    df.loc[df["IS_DIR"] == False, "SIZE"] = r

    print(f"getting files sizes (parrallel) took -- {(timer()) - start} -- seconds")
    start = timer()

    for level in tqdm(
        range(df["LEVEL"].max() -1, df["LEVEL"].min(), -1), disable=not show_progress):
        # where level is lower than current, and path is parent of current, sum size
        df.loc[
            (df["LEVEL"] == level) & (df["PARENT_PATH"].isin(df["PATH"])),
            "SIZE",
        ] = df.loc[
            (df["LEVEL"] == level) & (df["PARENT_PATH"].isin(df["PATH"])),
            "SIZE",
        ].sum()
    
    print(f"getting folder sizes took -- {(timer()) - start} -- seconds")
    start = timer()
    rdict = {
        "PATH": "./",
        "NAME": "./",
        "LEVEL": -1,
        "IS_DIR": True,
        "SIZE": df[~df["IS_DIR"]]["SIZE"].sum(),
        "PARENT_PATH": None,
    }

    # df = pd.concat([pd.DataFrame([rdict]), df]) but ensure the root is at the very bottom
    df = pd.concat([df, pd.DataFrame([rdict])])
    df["SIZE_MB"] = df["SIZE"] / (1024 ** 2)
    # if any size mb >1000, convert to gb
    if df["SIZE_MB"].max() > 1000:
        df["SIZE_GB"] = df["SIZE_MB"] / 1024

    print(f"adding root folder and sizes col took -- {(timer()) - start} -- seconds")
        
    return df


if __name__ == "__main__":
    # target_test_folder = "/test_sim_folder/"
    target = "C:/"
    show_progress = False

    start = timer()
    df = get_path_df(target, show_progress=show_progress)
    globx = timer()
    df = get_sizes(df, show_progress=show_progress)
    end = timer()

    print(f"creating df took {globx - start} seconds")
    print(f"getting sizes took {end - globx} seconds")
    print(f"total program exectution time {end - start} seconds")
    
    print("Total items:", len(df))
    print("Total folders:", len(df[df["IS_DIR"] == True]))
    print("Total files:", len(df[df["IS_DIR"] == False]))
    print("Dir levels:", df["LEVEL"].max() - df["LEVEL"].min())
    print("Total size (MB):", df["SIZE_MB"].sum())
    print("Total size (GB):", df["SIZE_MB"].sum() / 1024)
    
    

