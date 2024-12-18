import sys
import os
import pathlib

def delete_file(path):
    if os.path.exists(path):
        os.remove(path)
    else:
        print("Arquivo não existe.")

def load_file(path):
    with open(path, "rb") as f:
        F = bytearray(f)
        return F


