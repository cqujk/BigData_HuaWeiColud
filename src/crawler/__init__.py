import os
import sys


def add_path(path):
    if path not in sys.path:
        sys.path.insert(0, path)


# 添加crawler目录到系统路径中
src_dir_1 = os.path.dirname(__file__)

# 添加src目录到系统路径中
src_dir_2 = os.path.abspath(os.path.join(src_dir_1, "../"))

add_path(src_dir_1)
add_path(src_dir_2)
print(src_dir_1)
print(src_dir_2)


