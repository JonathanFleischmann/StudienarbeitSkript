import tkinter as tk
from tkinter import filedialog

import tkinter as tk
from tkinter import filedialog
import json

import os

def get_config():
    file_path = select_config_file()
    if not file_path:
        return None

    with open(file_path, 'r') as file:
        config = json.load(file)

    return config

def select_config_file():
    root = tk.Tk()
    root.withdraw()  # Versteckt das Hauptfenster
    file_path = filedialog.askopenfilename(title="Select Config File", filetypes=[("JSON Files", "*.json")], initialdir=os.getcwd())
    return file_path