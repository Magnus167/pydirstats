
from folders import get_path_df, get_sizes
import pandas as pd, numpy as np
import matplotlib.pyplot as plt
from typing import *
import greedypacker

class FNode:
    """ A node in the file tree"""
    def __init__(self, name : str, path : str, level : int, size : int, parent : Optional["FNode"] = None):
        self.name = name
        self.path = path
        self.level = level
        self.size = size
        self.parent = parent
        self.children: List["FNode"] = []
    def add_children(self, df : pd.DataFrame) -> None:
        """add children to the node"""
        self.children = df[df["PARENT_PATH"] == self.path].apply(
            lambda x: FNode(x["NAME"], x["PATH"], x["LEVEL"], x["SIZE"], self), axis=1
        ).to_list()

    def recurse_add_children(self, df: pd.DataFrame) -> None:
        """recursively add children to the node"""
        self.add_children(df)
        for child in self.children:
            child.recurse_add_children(df)

    
    def __repr__(self):
        return f"{self.name} ({self.size})"
    
class FTree:
    def __init__(self, root: str, struct_df: pd.DataFrame):
        self.root: str = root
        self.struct_df: pd.DataFrame = struct_df
        self.children: List = []
        self.min_level: int = self.struct_df["LEVEL"].min()
        self.max_level: int = self.struct_df["LEVEL"].max()

    def _build_tree(self : "FTree") -> None:
        """builds the tree from the root node"""
        self.root_node: FNode = FNode(name=self.root, path=self.root, level=self.struct_df["LEVEL"].min(), size=self.struct_df["SIZE"].sum() )
        self.root_node.add_children(self.struct_df)
        self.children: pd.Series = self.root_node.children
        for child in self.children:
            child.recurse_add_children(self.struct_df)
    
    def _print_tree(self, node: FNode, level: int = None) -> None:
        """prints the tree"""
        if level is None:
            level = self.min_level
        if node.level < self.min_level or node.level > self.max_level:
            return
        print("  " * (node.level - level) + str(node))
        for child in node.children:
            self._print_tree(child, level)


from timeit import default_timer as timer
start = timer()
path_df = get_path_df("E:/")
print(f"get_path_df took {(timer()) - start} seconds")
start = timer()
sizes = get_sizes(path_df)
print(f"get_sizes took {(timer()) - start} seconds")
start = timer()
ft = FTree("E:/jGit/pydirstats", path_df)
print(f"FTree took {(timer()) - start} seconds")
start = timer()
ft._build_tree()
print(f"_build_tree took {(timer()) - start} seconds")



