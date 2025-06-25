import unicodedata
import re
import pandas as pd


def normalize(text):
    if text is None or not isinstance(text, str):
        text = str(text) if text is not None else ""
    text = text.lower()
    return unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode('utf-8')

def clean_tokens(text):
    return re.findall(r'[a-z0-9]+', normalize(text))

def convert_config_to_indices(config_dict):
    r_nom, c_nom = cell_to_index(config_dict["cell_nom_echantillon"])
    r_param, c_param = cell_to_index(config_dict["cell_parametres"])
    r_data, c_data = cell_to_index(config_dict["cell_data_start"])

    optionnels = {
        k: cell_to_index(v)
        for k, v in config_dict.get("optionnels", {}).items()
        if isinstance(v, str) and v.strip().lower() != "none"
    }

    return {
        "nom_row": r_nom,
        "nom_col": c_nom,
        "param_row": r_param,
        "param_col": c_param,
        "data_start_row": r_data,
        "data_start_col": c_data,
        "optionnels": optionnels
    }

def cell_to_index(cell: str) -> tuple[int, int]:
    letters = ''.join([c for c in cell if c.isalpha()])
    digits = ''.join([c for c in cell if c.isdigit()])

    col = 0
    for i, char in enumerate(reversed(letters.upper())):
        col += (ord(char) - ord('A') + 1) * (26 ** i)
    col -= 1  # 0-indexed

    row = int(digits) - 1

    return row, col


def values_lq_or_none(val):
    val_str = str(val).strip().lower()
    # if nan then empty cell - meaning no analysis donee
    if pd.isna(val):
        return ""
    if val_str.startswith("<"):
        return f"<LQ ({val_str})"
    if val_str in {"n.d.", "n.d", "nd", "-", "n.d,", "n.d.."}:
        return "<LQ"
    return val_str

def is_label_all(label_info):
    return isinstance(label_info, tuple) and label_info[1] == "all"