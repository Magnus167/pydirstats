import shutil, os, sys, pandas as pd
from typing import *


class FStructNode:
    def get_size(self, path: str) -> shutil._ntuple_diskusage:
        self.is_dir = os.path.isdir(path)
        if self.is_dir:
            self.child_paths = [os.path.join(path, child) for child in os.listdir(path)]
            self.child_nodes = [FStructNode(self, child) for child in self.child_paths]
            self.size = sum([child.size for child in self.child_nodes])
        else:
            self.size = os.path.getsize(path)
        return self.size

    def __init__(self, parent, path):
        self.parent = parent
        self.path = path
        self.name = os.path.basename(path)
        try:
            self.size = self.get_size(self.path)
        except PermissionError:
            self.size = 0
            

    def __repr__(self):
        return f"FStructNode({self.path})"


class FStruct:
    """Class for getting and storing directory size"""

    def __init__(self, path: str):
        self.root = FStructNode(None, path)
        self._build(self.root)

    def _build(self, node: FStructNode):
        curr_dir: str = node.path
        while curr_dir is not None:
            try:
                node = FStructNode(node, curr_dir)
                if node.is_dir:
                    curr_dir = node.child_paths.pop()
                else:
                    curr_dir = None
            except PermissionError:
                curr_dir = None


    def _print(self):
        def _hprint(node: FStructNode, indent: int):
            print(f"{' '*indent}{node.name} {node.size}")
            if node.is_dir:
                for child in node.child_nodes:
                    _hprint(child, indent + 2)

        _hprint(self.root, 0)

    def _to_df(self) -> pd.DataFrame:
        # linearly flatten the tree. append each node to pandas df, with parent_path, name, size, is_dir
        def _hflatten(node: FStructNode, parent_path: str, df: pd.DataFrame):
            # add current node to df. do not use append. df is initially empty and only has header
            df.loc[len(df)] = [parent_path, node.name, node.size, node.is_dir]
            if node.is_dir:
                for child in node.child_nodes:
                    df = _hflatten(child, node.path, df)
            return df
        
        dfr  = _hflatten(self.root, "", pd.DataFrame(columns=["parent_path", "name", "size", "is_dir"]))
        dfr.loc[dfr['is_dir'], 'name'] += '/'
        return dfr

    def _print_df(self):
        print(self._to_df())


if __name__ == "__main__":
    if len(sys.argv) == 2:
        if sys.argv[1] == "-h":
            print("Usage: python f.py <path>")
            exit(0)
        else:
            path = sys.argv[1]
    else:
        path = os.getcwd()
    try:
        f = FStruct(path)
        # f._print()
        f._print_df()

    except Exception as e:
        print("Exception occurred")
        print(e)
        # print traceback
        import traceback
        traceback.print_exc()