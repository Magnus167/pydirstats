import os, pathlib, sys, concurrent.futures, pandas as pd
from tqdm import tqdm
from typing import *
from timeit import default_timer as timer

def printx(show_progress: bool = True, *args, **kwargs):
    if show_progress:
        print(*args, **kwargs)

def get_children(path: str, pathsDF: pd.DataFrame) -> pd.DataFrame:
    """Get a pandas dataframe where parent is the path"""
    return pathsDF[pathsDF["PATH"].str == path]

def get_path_df(path: str, show_progress: bool = True) -> pd.DataFrame:
    """Get a pandas dataframe with all paths in a directory recursively"""
    start = timer()
    printx(
        show_progress,
        f"pathlib.Path(path).rglob took -- {(timer()) - start} -- seconds",
    )
    start = timer()
    df = pd.DataFrame(data=pathlib.Path(path).rglob("*"), columns=["PATH"])
    printx(show_progress, f"pd.DataFrame took -- {(timer()) - start} -- seconds")

    start = timer()
    # df["PATH"] = df["PATH"].apply(lambda x: str(x).replace("\\", "/"))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        df["PATH"] = list(
            tqdm(
                executor.map(lambda x: str(x).replace("\\", "/"), df["PATH"]),
                total=len(df),
                disable=not show_progress,
            )
        )

    printx(show_progress, f"df['PATH'] took -- {(timer()) - start} -- seconds")
    start = timer()
    # df["NAME"] = df["PATH"].apply(lambda x: x.split("/")[-1])
    df["NAME"] = pd.DataFrame(data=[p.name for p in pathlib.Path(path).rglob("*")])
    printx(show_progress, f"df['NAME'] took -- {(timer()) - start} -- seconds")
    start = timer()
    df["PARENT_PATH"] = df["PATH"].apply(lambda x: "/".join(x.split("/")[:-1]))
    printx(show_progress, f"df['PARENT_PATH'] took -- {(timer()) - start} -- seconds")
    start = timer()
    df["LEVEL"] = df["PATH"].apply(lambda x: len(x.split("/")) - 1)
    printx(show_progress, f"df['LEVEL'] took -- {(timer()) - start} -- seconds")
    start = timer()
    # df["IS_DIR"] = df["PATH"].apply(lambda x: os.path.isdir(x))
    folders = pd.Series(
        [str(p).replace("\\", "/") for p in pathlib.Path(path).rglob("./")]
    )
    df["IS_DIR"] = df["PATH"].isin(folders)

    printx(show_progress, f"df['IS_DIR']  took -- {(timer()) - start} -- seconds")
    start = timer()
    df["SIZE"] = 0
    return df


def gsc(pathsDF: pd.DataFrame, show_progress: bool = True) -> pd.DataFrame:
    def _get_size(path):
        try:
            return os.path.getsize(path)
        except:
            return 0

    with concurrent.futures.ThreadPoolExecutor() as executor:
        return pd.DataFrame(
            data=list(
                tqdm(
                    executor.map(_get_size, pathsDF["PATH"]),
                    total=len(pathsDF),
                    disable=not show_progress,
                )
            ),
            columns=["SIZE"],
        )


def get_sizes(df: pd.DataFrame, show_progress: bool = True) -> pd.DataFrame:
    """Gets the size of each file in a dataframe. folders are calculated using pandas masks"""

    start = timer()

    mask = df["IS_DIR"] == False
    df.loc[mask, "SIZE"] = gsc(df[mask], show_progress=show_progress)

    print(f"getting files sizes (parrallel) took -- {(timer()) - start} -- seconds")
    start = timer()
    df["FOLDER_SIZE"] = 0

    for level in tqdm(
        range(df["LEVEL"].max(), df["LEVEL"].min()-1, -1), disable=not show_progress
    ):
        # where ("LEVEL" == level) & ("IS_DIR" == True) set "SIZE" to
        # SUM(where ("LEVEL" == level + 1) & ("PARENT_PATH" == "PATH")
        children = df[(df["LEVEL"] == level + 1)]
        currents = df[(df["LEVEL"] == level) & (df["IS_DIR"] == True)]
        for current in currents.itertuples():
            cmask : pd.DataFrame = children[children["PARENT_PATH"] == current.PATH]
            df.loc[
                (df["PATH"] == current.PATH),
                "FOLDER_SIZE",
            ] = cmask['SIZE'].sum() + cmask['FOLDER_SIZE'].sum()

    print(f"getting folder sizes took -- {(timer()) - start} -- seconds")
    start = timer()
    df["SIZE_MB"] = df["SIZE"] / (1024**2)
    df["SIZE_GB"] = df["SIZE_MB"] / 1024
    df["FOLDER_SIZE_MB"] = df["FOLDER_SIZE"] / (1024**2)
    df["FOLDER_SIZE_GB"] = df["FOLDER_SIZE_MB"] / 1024
    rdict = {
        "PATH": "./",
        "NAME": "./",
        "LEVEL": -1,
        "IS_DIR": True,
        "FOLDER_SIZE": df["SIZE"].sum(),
        "PARENT_PATH": None,
    }
    # rdict["SIZE"] = rdict["FOLDER_SIZE"]s
    rdict["FOLDER_SIZE_MB"] = rdict["FOLDER_SIZE"] / (1024**2)
    rdict["FOLDER_SIZE_GB"] = rdict["FOLDER_SIZE_MB"] / 1024
    # where is_dir. copy size to a new column called folder_size
    df = pd.concat([df, pd.DataFrame([rdict])])

    return df


if __name__ == "__main__":
    target = "E:/jGit"

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
    print(f"Total size MB : {df['SIZE_MB'].sum():,.2f}")
    print(f"Total size GB : {df['SIZE_GB'].sum():,.2f}")
    df.to_csv("test.csv", index=False)
