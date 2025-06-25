from fastapi import APIRouter, UploadFile, File, Form, Request, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.encoders import jsonable_encoder

import tempfile, shutil, json, random, re
import pandas as pd
import traceback
import os 

from services.analysis_extract import BaseExtract, ColumnsExtract, RowsExtract
from services.extract_utils import convert_config_to_indices, values_lq_or_none

router = APIRouter()

@router.post("/extract-geochem")
async def extract_geochem(
    request: Request,
    excel: UploadFile = File(...),
    keywords_json: str = Form(...),
    extraction_type: str = Form(...),
    config_json: str = Form(...),
    sheet_name: str = Form(...)
):
    print("Reception FormData :")
    form = await request.form()
    for key in form:
        print(f"🔑 {key} => {form[key]}")


    try:
        keywords = json.loads(keywords_json)
        config_raw = json.loads(config_json)
        config = convert_config_to_indices(config_raw)
    except Exception as e:
        print(f"❌ Erreur de parsing ou de conversion : {e}")
        return JSONResponse(content={"error": f"Erreur parsing JSON : {e}"}, status_code=400)

    # Save excel temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_excel:
        shutil.copyfileobj(excel.file, tmp_excel)
        tmp_path = tmp_excel.name

    try:
        df = pd.read_excel(tmp_path, sheet_name=sheet_name, header=None)

        if extraction_type.lower() == "colonnes":
            labels = df.iloc[config["param_row"]]
        elif extraction_type.lower() == "lignes":
            labels = df.iloc[:, config["param_col"]]
        else:
            raise ValueError("Type d'extraction inconnu")

        matched, multiple_matches = BaseExtract.get_matching_columns(labels, keywords)


        input_zone_gauche = []

        for kw in sorted(multiple_matches):
            input_zone_gauche.append(f"{kw} → all")

        for kw, correspondances in matched.items():
            for idx, vrai_nom in correspondances:
                input_zone_gauche.append(f"{kw} → ({idx}, {vrai_nom})")

        for kw in matched:
            if not matched[kw]:
                input_zone_gauche.append(kw)

        return {
            "matched_columns": matched,
            "input_zone_gauche": input_zone_gauche,
            "sheet_name": sheet_name,
            "type": extraction_type,
            "config": config_raw # Cells
        }

    except Exception as e:
        return JSONResponse(content={"error": f"Erreur lors de l'extraction : {e}"},
                            status_code=500)





@router.post("/randomize-geochem")
async def randomize_geochem(
    excel: UploadFile = File(...),
    matched_columns_json: str = Form(...),
    config_json: str = Form(...),
    extraction_type: str = Form(...),
    sheet_name: str = Form(...)
):
    extraction_type = extraction_type.lower()
    if extraction_type == "colonnes":
        axis = "columns"
    elif extraction_type == "lignes":
        axis = "rows"
    else:
        raise ValueError(f"Type d'extraction inconnu : {extraction_type}")

    try:
        matched_columns = json.loads(matched_columns_json)
        config_raw = json.loads(config_json)
        config = convert_config_to_indices(config_raw)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_excel:
            shutil.copyfileobj(excel.file, tmp_excel)
            tmp_path = tmp_excel.name

        # Chargement de l'extracteur
        if axis == "columns":
            extractor = ColumnsExtract(tmp_path, None, sheet_name, col_config=config)
        else:
            extractor = RowsExtract(tmp_path, None, sheet_name, row_config=config)
        extractor.load_data()

        # Construction des correspondances (kw → (index, nom))
        correspondances_input = {
            f"{kw} → ({idx}, {nom})": [(idx, nom)]
            for kw, correspondances in matched_columns.items()
            for idx, nom in correspondances
        }

        # Tirage d’un mot-clé aléatoire et extraction de son index (ligne ou colonne)
        kw_display = random.choice(list(correspondances_input.keys()))
        match = re.search(r"\((\d+),", kw_display)
        if not match:
            raise ValueError(f"Impossible d’extraire l’indice depuis : {kw_display}")
        index_param = int(match.group(1))  # ligne (lignes) ou colonne (colonnes)

        # Extraction d’un échantillon aléatoire
        if axis == "columns":
            noms_ref = list(extractor.df.iloc[config["param_row"]])
            min_row = config["nom_row"] + 1
            max_row = extractor.df.shape[0] - 1
            if min_row > max_row:
                raise ValueError(f"Aucune ligne disponible pour tirage (min_row={min_row}, max_row={max_row})")
            idx_random = random.randint(min_row, max_row)

        elif axis == "rows":
            noms_ref = list(extractor.df.iloc[config["param_row"]:, config["param_col"]])
            min_col = config["data_start_col"] + 1
            max_col = extractor.df.shape[1] - 1
            if min_col > max_col:
                raise ValueError(f"Aucune colonne disponible pour tirage (min_col={min_col}, max_col={max_col})")
            idx_random = random.randint(min_col, max_col)

        else:
            raise ValueError(f"Type d'extraction inconnu : {axis}")

        # Extraction valeur
        val = extractor.extract_values(
            item=kw_display,
            df=extractor.df,
            idx=idx_random,
            noms_reference=noms_ref,
            correspondances_input=correspondances_input,
            axis=axis
        )
        if axis == "columns":
            print("🧪 Cellule brute [row, col] :", idx_random, index_param)
            print("🧪 Valeur dans df :", extractor.df.iat[idx_random, index_param])
        else:
            print("🧪 Cellule brute [row, col] :", index_param, idx_random)
            print("🧪 Valeur dans df :", extractor.df.iat[index_param, idx_random])

        # Code Artelia (identifiant)
        if axis == "columns":
            code = extractor.df.iat[idx_random, config["nom_col"]]
        else:
            code = extractor.df.iat[config["nom_row"], idx_random]

        # Nettoyage du libellé d’analyse
        kw_split = kw_display.split("→", 1)[-1].strip()
        kw_final = kw_split.replace("- (mg/kg M.S.)", "")

        # Coordonnées pour surlignage
        if axis == "columns":
            code_row = val_row = idx_random
            code_col = config["nom_col"]
            val_col = index_param
            val_row = idx_random
            val_raw = extractor.df.iat[val_row, val_col]
            kw_row = config["param_row"]
            kw_col = index_param
        else:
            code_row = config["nom_row"]
            code_col = idx_random
            val_row = index_param
            val_col = idx_random
            val_raw = extractor.df.iat[val_row, val_col]
            kw_row = index_param
            kw_col = config["param_col"]

        def safe_str(v):
            if pd.isna(v):
                return "None"
            return str(v)

        val = safe_str(val_raw)

        return {
            "message": f"Code : {code} | Analyse : {kw_final} | Valeur : {val}",
            "code": code,
            "kw_final": kw_final,
            "val": val,
            "code_row": code_row,
            "code_col": code_col,
            "kw_row": kw_row,
            "kw_col": kw_col,
            "val_row": val_row,
            "val_col": val_col
        }

    except Exception as e:
        print("❌ Erreur lors du randomize :", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Erreur randomize: " + str(e))







@router.post("/export-geochem")
async def export_geochem_excel(
    excel: UploadFile,
    extraction_type: str = Form(...),
    sheet_name: str = Form(...),
    config_json: str = Form(...),
    selection_json: str = Form(...),
    replace_lq_with: str = Form(None)
):

    try:
        print("🔥 Début export-geochem")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            file_bytes = await excel.read()
            print("📩 Taille fichier Excel :", len(file_bytes))
            tmp.write(file_bytes)
            tmp_path = tmp.name

        config_data = json.loads(config_json)
        selection_data = json.loads(selection_json)

        config_extraction = convert_config_to_indices(config_data)
        temp_json_path = tmp_path.replace(".xlsx", "_config.json")
        with open(temp_json_path, "w", encoding="utf-8") as f:
            json.dump(selection_data, f, indent=2, ensure_ascii=False)

        if extraction_type.lower() == "colonnes":
            extractor = ColumnsExtract(tmp_path, temp_json_path, sheet_name, col_config=config_extraction)
        else:
            extractor = RowsExtract(tmp_path, temp_json_path, sheet_name, row_config=config_extraction)

        # New matched column
        matched_columns = {}
        for item in selection_data["keywords_valides"]:
            if "→ (" in item and "," in item:
                base_kw, cible = item.split("→", 1)
                base_kw = base_kw.strip()
                match = re.search(r"\((\d+),\s*(.+?)\)", cible.strip())
                if match:
                    idx = int(match.group(1))
                    nom = match.group(2).strip()
                    matched_columns.setdefault(base_kw, []).append((idx, nom))

        for group_members in selection_data.get("groupes_personnalises", {}).values():
            for item in group_members:
                if "→ (" in item and "," in item:
                    base_kw, cible = item.split("→", 1)
                    base_kw = base_kw.strip()
                    match = re.search(r"\((\d+),\s*(.+?)\)", cible.strip())
                    if match:
                        idx = int(match.group(1))
                        nom = match.group(2).strip()
                        matched_columns.setdefault(base_kw, []).append((idx, nom))


        extractor.matched_columns = matched_columns
        extractor.load_keywords_ui2()
        extractor.load_data()

        try:
            extractor.extract()
            output_path = extractor.export()

            # PART FOR CONDITIONAL LQ VALUE INSIDE FRONTEND
            if replace_lq_with is not None:
                print(f"🔁 Remplacement <LQ par : '{replace_lq_with}'")
                df = pd.read_excel(output_path)
                df = df.applymap(lambda v: replace_lq_with if isinstance(v, str) and (
                        v.strip().lower().startswith("<") or v.strip().lower() in {"n.d.", "n.d", "nd", "-", "n.d,",
                                                                                   "n.d.."})
                else v)
                df.to_excel(output_path, index=False)

        except Exception as e:
            print("❌ ERREUR dans extractor.extract() :", e)
            print(traceback.format_exc())
            return JSONResponse(content={"error": f"Erreur lors de l’extract() : {e}"}, status_code=500)

        print("✅ EXTRACT FAIT")

        df = pd.read_excel(output_path)
        print("📏 Taille résultat :", df.shape)
        print("📋 Colonnes résultat :", list(df.columns))
        print("📑 Head résultat :\n", df.head(3))
        print("📁 Fichier généré :", output_path)

        return FileResponse(output_path, filename=os.path.basename(output_path))


    except Exception as e:
        print("❌ ERREUR dans export-geochem :", traceback.format_exc())
        return JSONResponse(content={"error": f"Erreur export : {e}"}, status_code=500)

@router.post("/preview-geochem")
async def preview_geochem_excel(
    excel: UploadFile = File(...),
    extraction_type: str = Form(...),
    sheet_name: str = Form(...),
    config_json: str = Form(...),
    selection_json: str = Form(...)
):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            file_bytes = await excel.read()
            tmp.write(file_bytes)
            tmp_path = tmp.name

        config_data = json.loads(config_json)
        selection_data = json.loads(selection_json)

        config_extraction = convert_config_to_indices(config_data)

        # Sauvegarde temporaire du JSON de sélection
        temp_json_path = tmp_path.replace(".xlsx", "_config.json")
        with open(temp_json_path, "w", encoding="utf-8") as f:
            json.dump(selection_data, f, indent=2, ensure_ascii=False)

        # Création de l'extracteur
        if extraction_type.lower() == "colonnes":
            extractor = ColumnsExtract(tmp_path, temp_json_path, sheet_name, col_config=config_extraction)
        else:
            extractor = RowsExtract(tmp_path, temp_json_path, sheet_name, row_config=config_extraction)

        extractor.load_keywords_ui2()
        extractor.load_data()
        extractor.extract()

        # Limiter à 5 lignes
        df_preview = pd.DataFrame.from_dict(extractor.resultats, orient="index")
        df_preview.reset_index(inplace=True)  # ajoute index en colonne
        preview_data = df_preview.head(5).to_dict(orient="records")

        return JSONResponse(content=jsonable_encoder({"preview_resultats": preview_data}))

    except Exception as e:
        import traceback
        print("❌ ERREUR dans preview-geochem :", traceback.format_exc())
        return JSONResponse(content={"error": f"Erreur dans preview : {e}"}, status_code=500)