from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse, StreamingResponse

import pandas as pd
import io
import pdfplumber
import re
import json
import statistics
from decimal import Decimal, getcontext

import progress
import traceback

getcontext().prec = 10

router = APIRouter()


def detect_y_anomalies(y_val_list, keyword):
    if len(y_val_list) < 3:
        return [v for _, v in y_val_list], []

    y_val_list = sorted(y_val_list, key=lambda x: x[0])
    y_positions = [y for y, _ in y_val_list]
    dy_list = [y2 - y1 for y1, y2 in zip(y_positions, y_positions[1:])]

    median_dy = statistics.median(dy_list)
    min_dy = Decimal(str(median_dy)) * Decimal("0.7")
    max_dy = Decimal(str(median_dy)) * Decimal("1.3")

    output = []
    red_flags = []

    for i in range(len(y_val_list) - 1):
        y1, v1 = y_val_list[i]
        y2, v2 = y_val_list[i + 1]
        dy = Decimal(str(abs(y2 - y1)))

        output.append(v1)

        if dy > max_dy:
            output.append(None)
        elif dy < min_dy:
            red_flags.append(len(output) - 1)
            red_flags.append(len(output))

    output.append(y_val_list[-1][1])
    return output, red_flags


def detect_sondage_name(words):
    pattern = re.compile(r"\bSP\d{1,4}\b")
    for w in words:
        text = w.get('text', '')
        if pattern.fullmatch(text.strip()):
            return text.strip()
    return None


def extract_values_near_keyword(words, keyword, tolerance):
    ref_word = next((w for w in words if w['text'].strip().lower() == keyword.lower()), None)
    if not ref_word:
        return []

    x_ref = (ref_word['x0'] + ref_word['x1']) / 2
    y_ref = ref_word['top']

    values = []
    for w in words:
        try:
            val = float(w['text'].replace(",", "."))
        except ValueError:
            continue

        x_c = (w['x0'] + w['x1']) / 2
        y_c = w['top']

        if (x_ref - tolerance['left'] <= x_c <= x_ref + tolerance['right']) and (y_c > y_ref + tolerance['min_dy']):
            values.append((y_c, val))

    return sorted(values, key=lambda x: x[0])


def get_keyword_x_positions(words, keywords):
    positions = {}
    for kw in keywords:
        for w in words:
            if w['text'].strip().lower() == kw.lower():
                x = (w['x0'] + w['x1']) / 2
                positions[kw] = x
                break
    return positions


def generate_depths_from_config(config):
    s = float(config['start'])
    e = float(config['end'])
    p = float(config['step'])
    return [round(s + i * p, 3) for i in range(int((e - s) / p + 1))]



# === Worker extraction pressio ===
async def extract_pressio_worker(pdf_bytes):
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as doc:
            pages = doc.pages
            total = len(pages)
            progress.progress_state["progress"] = 0
            progress.progress_state["total"] = total
            progress.progress_state["is_running"] = True

            sondages = set()
            pattern = re.compile(r"\bSP\d{1,4}\b", re.IGNORECASE)
            # BASIC PATTERN SPXXXX From SP1 to SP9999 possible, to be modified to let user choose

            for i, page in enumerate(pages):
                words = page.extract_words()
                for word in words:
                    txt = word.get("text", "").strip()
                    if pattern.fullmatch(txt):
                        sondages.add(txt)

                progress.progress_state["progress"] = i + 1
                await asyncio.sleep(0)

            progress.progress_state["is_running"] = False
            progress.progress_state["last_output_file"] = None

            return {"sondages": sorted(sondages)}

    except Exception as e:
        progress.progress_state["is_running"] = False
        progress.progress_state["last_output_file"] = None
        raise e


# === Worker process pressio ===
async def process_pressio_worker(pdf_bytes, config_data):
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as doc:
            pages = doc.pages
            total = len(pages)
            progress.progress_state["progress"] = 0
            progress.progress_state["total"] = total
            progress.progress_state["is_running"] = True

            mode = config_data['mode']
            depth_config = config_data['config']
            sondages = config_data['sondages']
            keywords = ["Pf*", "Pl*", "Module"]

            final_data = {}

            for i, page in enumerate(pages):
                words = page.extract_words()
                sondage_name = detect_sondage_name(words) or f"Page {page.page_number}"
                if sondage_name not in sondages:
                    progress.progress_state["progress"] = i + 1
                    await asyncio.sleep(0)
                    continue

                # Détection des positions
                x_positions = get_keyword_x_positions(words, keywords)
                is_combined = False
                if "Pf*" in x_positions and "Pl*" in x_positions:
                    distance = abs(x_positions["Pf*"] - x_positions["Pl*"])
                    if distance <= 15:
                        is_combined = True

                values_by_keyword = {k: extract_values_near_keyword(words, k, {
                    "left": 10, "right": 30 if k != "Module" else 54, "min_dy": 50
                }) for k in keywords}

                if is_combined:
                    pf_final, pl_final = [], []
                    pf_vals = values_by_keyword["Pf*"]
                    if len(pf_vals) % 2 != 0:
                        progress.progress_state["progress"] = i + 1
                        await asyncio.sleep(0)
                        continue
                    for idx in range(len(pf_vals) - 2, -1, -2):
                        a, b = pf_vals[idx][1], pf_vals[idx + 1][1]
                        if a < b:
                            pf_final.append((pf_vals[idx][0], a))
                            pl_final.append((pf_vals[idx + 1][0], b))
                        else:
                            pf_final.append((pf_vals[idx + 1][0], b))
                            pl_final.append((pf_vals[idx][0], a))
                    pf_final.reverse()
                    pl_final.reverse()
                else:
                    pf_final = values_by_keyword["Pf*"]
                    pl_final = values_by_keyword["Pl*"]

                em_final = values_by_keyword["Module"]

                pf_list, pf_red = detect_y_anomalies(pf_final, "Pf*")
                pl_list, pl_red = detect_y_anomalies(pl_final, "Pl*")
                em_list, em_red = detect_y_anomalies(em_final, "Module")

                if mode == "global":
                    depths = generate_depths_from_config(depth_config)
                else:
                    if sondage_name not in depth_config:
                        progress.progress_state["progress"] = i + 1
                        await asyncio.sleep(0)
                        continue
                    depths = generate_depths_from_config(depth_config[sondage_name])

                if sondage_name not in final_data:
                    final_data[sondage_name] = {
                        "Depth": [],
                        "Pf*": [],
                        "Pl*": [],
                        "Module": [],
                        "RedFlags": {
                            "Pf*": [],
                            "Pl*": [],
                            "Module": []
                        }
                    }

                if not final_data[sondage_name]["Depth"]:
                    final_data[sondage_name]["Depth"] = depths

                final_data[sondage_name]["Pf*"] += pf_list
                final_data[sondage_name]["Pl*"] += pl_list
                final_data[sondage_name]["Module"] += em_list
                final_data[sondage_name]["RedFlags"]["Pf*"] += pf_red
                final_data[sondage_name]["RedFlags"]["Pl*"] += pl_red
                final_data[sondage_name]["RedFlags"]["Module"] += em_red

                progress.progress_state["progress"] = i + 1
                await asyncio.sleep(0)

            progress.progress_state["is_running"] = False
            progress.progress_state["last_output_file"] = None

            return final_data

    except Exception as e:
        progress.progress_state["is_running"] = False
        progress.progress_state["last_output_file"] = None
        raise e




@router.post("/extract-pressio")
async def extract_pressio(pdf: UploadFile = File(...)):
    current_task = progress.progress_state.get("current_task")
    if progress.progress_state.get("is_running") and current_task and not current_task.done():
        current_task.cancel()
        try:
            await current_task
        except asyncio.CancelledError:
            pass

    content = await pdf.read()
    task = asyncio.create_task(extract_pressio_worker(content))
    progress.progress_state["current_task"] = task
    progress.progress_state["is_running"] = True
    result = await task
    return result


@router.post("/process-pressio")
async def process_pressio(pdf: UploadFile = File(...), config: str = Form(...)):
    current_task = progress.progress_state.get("current_task")
    if progress.progress_state.get("is_running") and current_task and not current_task.done():
        current_task.cancel()
        try:
            await current_task
        except asyncio.CancelledError:
            pass

    content = await pdf.read()
    config_data = json.loads(config)
    task = asyncio.create_task(process_pressio_worker(content, config_data))
    progress.progress_state["current_task"] = task
    progress.progress_state["is_running"] = True
    result = await task
    return result


@router.post("/export-pressio")
async def export_pressio(validated_data: dict):
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for sondage, data in validated_data.items():
                lengths = [
                    len(data.get("Depth", [])),
                    len(data.get("Pf*", [])),
                    len(data.get("Pl*", [])),
                    len(data.get("Module", [])),
                ]
                max_len = max(lengths)

                def pad_list(lst, length):
                    return lst + [None] * (length - len(lst)) if len(lst) < length else lst

                depths = pad_list(data.get("Depth", []), max_len)
                pf = pad_list(data.get("Pf*", []), max_len)
                pl = pad_list(data.get("Pl*", []), max_len)
                module = pad_list(data.get("Module", []), max_len)

                df = pd.DataFrame({
                    "Profondeur": depths,
                    "Pf*": pf,
                    "Pl*": pl,
                    "Module": module,
                })

                safe_sheet_name = re.sub(r'[:\\/*?[\]]', '_', sondage)[:31]

                df.to_excel(writer, sheet_name=safe_sheet_name, index=False)

        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=geotech_export.xlsx"}
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})

















#
# @router.post("/extract-pressio")
# async def extract_pressio(pdf: UploadFile = File(...)):
#     try:
#         content = await pdf.read()
#         with pdfplumber.open(io.BytesIO(content)) as doc:
#             sondages = set()
#             pattern = re.compile(r"\bSP\d{1,4}\b", re.IGNORECASE)
#             for page in doc.pages:
#                 words = page.extract_words()
#                 for word in words:
#                     txt = word.get("text", "").strip()
#                     if pattern.fullmatch(txt):
#                         sondages.add(txt)
#         return {"sondages": sorted(sondages)}
#     except Exception as e:
#         return JSONResponse(status_code=500, content={"error": str(e)})
#
#
# @router.post("/process-pressio")
# async def run_pdf_pressio_extract(pdf: UploadFile = File(...), config: str = Form(...)):
#     try:
#         config_data = json.loads(config)
#         mode = config_data['mode']
#         depth_config = config_data['config']
#         sondages = config_data['sondages']
#
#         content = await pdf.read()
#         keywords = ["Pf*", "Pl*", "Module"]
#
#         final_data = {}
#
#         with pdfplumber.open(io.BytesIO(content)) as doc:
#             for page in doc.pages:
#                 words = page.extract_words()
#                 sondage_name = detect_sondage_name(words) or f"Page {page.page_number}"
#                 if sondage_name not in sondages:
#                     continue
#
#                 # Détection des positions
#                 x_positions = get_keyword_x_positions(words, keywords)
#                 is_combined = False
#                 if "Pf*" in x_positions and "Pl*" in x_positions:
#                     distance = abs(x_positions["Pf*"] - x_positions["Pl*"])
#                     if distance <= 15:
#                         is_combined = True
#
#                 values_by_keyword = {k: extract_values_near_keyword(words, k, {
#                     "left": 10, "right": 30 if k != "Module" else 54, "min_dy": 50
#                 }) for k in keywords}
#
#                 if is_combined:
#                     pf_final, pl_final = [], []
#                     pf_vals = values_by_keyword["Pf*"]
#                     if len(pf_vals) % 2 != 0:
#                         continue
#                     for i in range(len(pf_vals) - 2, -1, -2):
#                         a, b = pf_vals[i][1], pf_vals[i + 1][1]
#                         if a < b:
#                             pf_final.append((pf_vals[i][0], a))
#                             pl_final.append((pf_vals[i + 1][0], b))
#                         else:
#                             pf_final.append((pf_vals[i + 1][0], b))
#                             pl_final.append((pf_vals[i][0], a))
#                     pf_final.reverse()
#                     pl_final.reverse()
#                 else:
#                     pf_final = values_by_keyword["Pf*"]
#                     pl_final = values_by_keyword["Pl*"]
#
#                 em_final = values_by_keyword["Module"]
#
#                 pf_list, pf_red = detect_y_anomalies(pf_final, "Pf*")
#                 pl_list, pl_red = detect_y_anomalies(pl_final, "Pl*")
#                 em_list, em_red = detect_y_anomalies(em_final, "Module")
#
#                 # Profondeur
#                 if mode == "global":
#                     depths = generate_depths_from_config(depth_config)
#                 else:
#                     if sondage_name not in depth_config:
#                         continue
#                     depths = generate_depths_from_config(depth_config[sondage_name])
#
#                 if sondage_name not in final_data:
#                     final_data[sondage_name] = {
#                         "Depth": [],
#                         "Pf*": [],
#                         "Pl*": [],
#                         "Module": [],
#                         "RedFlags": {
#                             "Pf*": [],
#                             "Pl*": [],
#                             "Module": []
#                         }
#                     }
#
#                 if not final_data[sondage_name]["Depth"]:
#                     final_data[sondage_name]["Depth"] = depths
#
#                 final_data[sondage_name]["Pf*"] += pf_list
#                 final_data[sondage_name]["Pl*"] += pl_list
#                 final_data[sondage_name]["Module"] += em_list
#                 final_data[sondage_name]["RedFlags"]["Pf*"] += pf_red
#                 final_data[sondage_name]["RedFlags"]["Pl*"] += pl_red
#                 final_data[sondage_name]["RedFlags"]["Module"] += em_red
#
#         return final_data
#
#     except Exception as e:
#         return JSONResponse(status_code=500, content={"error": str(e)})
