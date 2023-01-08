import os, pathlib
import pandas as pd
from tqdm import tqdm
from timeit import default_timer as timer
from joblib import Parallel, delayed


def get_path_df(path: str, show_progress: bool = True) -> pd.DataFrame:
    """Get a pandas dataframe with all paths in a directory recursively"""
    pathglob = pathlib.Path(path).rglob("*")
    df = pd.DataFrame(data=pathglob, columns=["PATH"])
    df["PATH"] = df["PATH"].apply(lambda x: str(x).replace("\\", "/"))
    df["NAME"] = df["PATH"].apply(lambda x: x.split("/")[-1])
    df["PARENT_PATH"] = df["PATH"].apply(lambda x: "/".join(x.split("/")[:-1]))
    df["LEVEL"] = df["PATH"].apply(lambda x: len(x.split("/")) - 1)
    df["IS_DIR"] = df["PATH"].apply(lambda x: pathlib.Path(x).is_dir())
    df["SIZE"] = 0
    return df


def get_sizes(df: pd.DataFrame, show_progress: bool = True) -> pd.DataFrame:
    """Gets the size of each file in a dataframe. folders are calculated using pandas masks"""

    for level in tqdm(
        range(df["LEVEL"].max(), df["LEVEL"].min() - 1, -1),
        disable=not show_progress,
    ):
        curr_level = df[df["LEVEL"] == level]
        lower_level = df[df["LEVEL"] == level + 1]
        dir_df = curr_level[curr_level["IS_DIR"]]
        lower_fdf = lower_level[~lower_level["IS_DIR"]]

        df.loc[lower_fdf.index, "SIZE"] += lower_fdf["PATH"].apply(
            lambda x: os.path.getsize(x)
        )
        df.loc[dir_df.index, "SIZE"] += dir_df.apply(
            lambda x: df.loc[(df["PARENT_PATH"] == x["PATH"])]["SIZE"].sum(),
            axis=1,
        )

    last_level = df[df["LEVEL"] == df["LEVEL"].min()]
    last_level_fdf = last_level[~last_level["IS_DIR"]]
    df.loc[last_level_fdf.index, "SIZE"] = last_level_fdf["PATH"].apply(
        lambda x: os.path.getsize(x)
    )

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
    return df


if __name__ == "__main__":
    # target_test_folder = "/test_sim_folder/"
    target = "E:/jGit/"
    start = timer()
    df = get_path_df(target)
    globx = timer()
    df = get_sizes(df)
    end = timer()
    print(df)
    print(f"creating df took {globx - start} seconds")
    print(f"getting sizes took {end - globx} seconds")
    print(f"total time {end - start} seconds")
