import os
import asyncio
import re
import logging
import gc
import tempfile
from concurrent.futures import ThreadPoolExecutor

import camelot
import fitz  # PyMuPDF
import pandas as pd
from pandas import ExcelWriter
from io import BytesIO
from typing import BinaryIO

import progress

# os
os.environ["PATH"] += os.pathsep + r"C:\Program Files\gs\gs10.05.1\bin"
# logging
logging.getLogger("pdfminer").setLevel(logging.ERROR)

executor = ThreadPoolExecutor()

# === Script : EXTRACT TABLE FROM PDF ===
#
def looks_like_data(row):
    non_empty = [cell for cell in row if cell and str(cell).strip()]
    return sum(bool(re.search(r'\d', str(cell))) for cell in non_empty) >= 3

async def detect_tables_async(pdf_path, page, flavor, **kwargs):
    return await asyncio.to_thread(camelot.read_pdf, pdf_path, pages=page, flavor=flavor, **kwargs)

async def extraire_pdf_vers_excel_async(pdf_path, keywords, num_header_rows):
    doc = fitz.open(pdf_path)
    target_pages = []

    progress.progress_state["progress_count"] = 0
    progress.progress_state["total_count"] = 1

    for i in range(len(doc)):
        text = (await asyncio.to_thread(lambda: doc.load_page(i).get_text())).lower()
        for kw in keywords:
            if kw.lower() in text:
                target_pages.append((str(i + 1), kw))
                break
                
    doc.close()
    del doc
    gc.collect()
    print(f"🔍 Pages retenues : {target_pages}")

    if not target_pages:
        print("🚫 Aucun mot-clé trouvé dans le document, extraction annulée.")
        progress.progress_state["progress_count"] = 1
        return BytesIO()

    progress.progress_state["total_count"] = max(len(target_pages), 1)

    # Ghost tables to be removed if size lower than :
    min_rows, min_cols = 3, 4

    pages_sans_tableaux = []

    progress_count, total_count = 0, len(target_pages)

    temp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    writer = pd.ExcelWriter(temp_xlsx.name, engine='xlsxwriter')

    for page, keyword in target_pages:
        print(f"📄 Traitement de la page {page} - Type : {keyword}")

        # Using multiple option to detect tables
        # Try LATTICE
        print("🧪 Lecture Camelot (lattice)...")
        tables = await asyncio.to_thread(
        camelot.read_pdf,
      pdf_path,
            pages=page,
            flavor="lattice",
            line_scale=40,
            shift_text=["", ""],
            copy_text=["v"]
        )
        print(f"📊 Tables détectées (mode lattice): {len(tables)}")
        for i, t in enumerate(tables):
            print(f" → Table {i + 1} shape: {t.df.shape}")

        valid_tables = [t for t in tables if t.df.shape[0] >= min_rows and t.df.shape[1] >= min_cols]

        # Try STREAM -- only if LATTICE not valid
        if not valid_tables:
            print(f"⚠️ Re-tentative avec flavor=stream sur la page {page}")
            tables = await asyncio.to_thread(
            camelot.read_pdf,
                pdf_path,
                pages=page,
                flavor="stream",
                strip_text="\n"
            )
            print(f"📊 Tables détectées (mode stream): {len(tables)}")
            valid_tables = [t for t in tables if t.df.shape[0] >= min_rows and t.df.shape[1] >= min_cols]

        if not valid_tables:
            print(f"⚠️ Aucun tableau valide détecté sur la page {page}")
            pages_sans_tableaux.append((int(page), keyword))
        else:
            tables = valid_tables


        for i, table in enumerate(tables):
            raw_table = table.df.values.tolist()

            # Normalize the table to make sure every row got the same columns number :
            #   To keep blanks
            expected_cols = max(len(row) for row in raw_table)
            normalized = [
                row + [""] * (expected_cols - len(row)) if len(row) < expected_cols else row
                for row in raw_table
            ]

            df = pd.DataFrame(normalized)
            if df.shape[0] < min_rows or df.shape[1] < min_cols:
                print(f"🚫 Tableau ignoré (trop petit) - Page {page} Table {i+1} ({df.shape[0]} lignes, {df.shape[1]} colonnes)")
                continue


            data_start_idx = None
            for idx, row in df.iterrows():
                if looks_like_data(row):
                    data_start_idx = idx
                    break

            if data_start_idx is not None:
                df_clean = df.iloc[data_start_idx:].copy()
                sheet_name = f"Page{page}_{keyword}"[:31]

                has_group_lines = any(
                    (row[0] and all(str(cell).strip() == "" for cell in row[1:]))
                    for row in df_clean.values
                )

                if has_group_lines:
                    df_clean = df_clean[~((df_clean[0].notna()) & (df_clean.iloc[:, 1:].isna().all(axis=1)))]

                df_clean = df_clean.replace(r'\n', ' ', regex=True)



                header_rows = df.iloc[:num_header_rows].values.tolist()

                fused_headers = []
                for col_idx in range(df.shape[1]):
                    parts = []
                    for row in header_rows:
                        if col_idx < len(row):
                            val = str(row[col_idx]).strip()
                            if val and val.lower() not in ["", "nan"]:
                                parts.append(val)
                    header = " ".join(parts).strip()
                    fused_headers.append(header if header else f"col_{col_idx}")


                if any(h.startswith("col_") is False for h in fused_headers):
                    df_clean.columns = fused_headers
                else:
                    df_clean.columns = [f"col_{j}" for j in range(df_clean.shape[1])]

                df_clean.reset_index(drop=True, inplace=True)

                df_clean.to_excel(writer, sheet_name=sheet_name, index=False)
                del df_clean
                gc.collect()
            else:
                print(f"🚫 Aucune ligne de données trouvée - Page {page} Table {i + 1}")
                pages_sans_tableaux.append((int(page), keyword))

        progress_count += 1
        progress.progress_state["progress_count"] = progress_count
        progress.progress_state["total_count"] = total_count
        print(f"Progression: {progress_count}/{total_count}")
        del tables
        gc.collect()

    writer.close()
    with open(temp_xlsx.name, "rb") as f:
        final_bytes = f.read()

    # Nettoyage du fichier temporaire
    os.unlink(temp_xlsx.name)

    return BytesIO(final_bytes)


