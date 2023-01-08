import os, shutil, sys
from random import randint

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pydiskstats.folders import get_path_df



def generate_test_files(test_folders):

    for file in test_folders:
        if os.path.exists(file):
            if os.path.isdir(file):
                shutil.rmtree(file)
            else:
                os.remove(file)

        if file.endswith("/"):
            os.makedirs(file, exist_ok=True)
        else:
            s = ''.join([str(randint(0, 100)) for _ in range(randint(100, 1000))])
            with open(file, "w") as f:
                f.write(s)

def test_get_path_df(target_test_folder, test_folders):
    df = get_path_df(target_test_folder)
    # where dirsmask is true, add '/' to the end of the path
    df["PATH"] = df.apply(lambda x: x["PATH"] + '/' if x["IS_DIR"] else x["PATH"], axis=1)

    assert set(df["PATH"].to_list() + [target_test_folder]) == set(test_folders)
    return True

def run_tests():
    sp = __file__.replace("\\", "/")
    sp = sp[:sp.rfind("/")]
    target_test_folder = "/test_sim_folder/"
    test_folders = [ "",
    "1.txt",
    "2.txt",
    "3/",
    "3/5/",
    "3/4.txt",
    "3/5/6.txt",]
    test_folders = [f"{sp}{target_test_folder}{f}" for f in test_folders]
    target_test_folder = test_folders[0]

    generate_test_files(test_folders)
    assert test_get_path_df(target_test_folder, test_folders)
    print("All tests passed")

if __name__ == "__main__":
    run_tests()