
from timeit import default_timer as timer
from pathlib import Path
from typing import *

class FileNode:
    def __init__(self, name: str, path: Path):
        self.name = name
        self.path = path
        self.children = []
        try:
            self.size = path.stat().st_size if not path.is_dir() else 0
        except OSError:
            self.size = -1
        self.folder_size = 0

    def add_child(self, child: "FileNode"):
        self.children.append(child)
        self.folder_size += child.size

    def update_folder_size(self):
        for child in self.children:
            if child.path.is_dir():
                child.update_folder_size()
            self.folder_size += child.size


def build_tree(root_path: Path) -> FileNode:
    root_node = FileNode(root_path.name, root_path)
    for path in root_path.rglob("*"):
        if path.is_dir():
            child_node = build_tree(path)
            root_node.add_child(child_node)
        else:
            child_node = FileNode(path.name, path)
            root_node.add_child(child_node)
    if root_path.is_dir():
        root_node.update_folder_size()
    return root_node

def traverse_tree(node: FileNode):
    r = {"name": node.name, "path": node.path, "size": node.size, "folder_size": node.folder_size}
    # print()
    for child in node.children:
        traverse_tree(child)

# root_path = Path("E:\\jGit\\pydirstats\\pydirstats")
root_path = Path("E:\\jGit\\")
t = timer()
root_node = build_tree(root_path)
print("Time to build tree:", timer() - t)
t = timer()
traverse_tree(root_node)
print("Time to traverse tree:", timer() - t)
