import os
import sys


def add_path(path):
    if path not in sys.path:
        sys.path.insert(0, path)


this_dir = os.path.dirname(__file__)

# 将common目录添加到系统路径之中
add_path(this_dir)