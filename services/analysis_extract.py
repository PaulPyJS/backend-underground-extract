import pandas as pd
import os
import json
from .extract_utils import clean_tokens, values_lq_or_none

# === Script : EXTRACT VALUE WITH KEYWORD IN AN EXCEL FORMAT - TABLEURS MULTIPLE PAR CLASSES ===
# = v1.0 : Test import from Excel raw DF-Excel and keyword-based extract
        # = v1.05 : Multiple keywords test
    # = v1.2 : Normalizing text and splitting data into keywords based on value or sum
    # = v1.3 : Extraction validated for general keyword with random pick
    # = v1.4 : Adapting the script to allow HAP to be separated from Naphtalene/HAP
    # = v1.5 : DEBUG
    # = v1.6 : Adding SUM calculation based on JSON file to allow local memory
    # = v1.7 : Link to UI and using json for keywords
# = v2.0 : PASSAGE FORMAT CLASSES DEPUIS EUROFINS_EXTRACT.PY
    # = v2.1 : Adding class type AGROLAB
    # = v2.2 : Using Rows and Columns from user to configure type of table - suppressing Agrolab/Eurofins type
#
class BaseExtract:
    def __init__(self, excel_path, json_config_path, sheet_name, config, input_zone_gauche = None):
        self.excel_path = excel_path
        self.json_config_path = json_config_path
        self.sheet_name = sheet_name
        self.config = config
        self.df = None
        self.resultats = {}
        self.keywords_valides = []
        self.groupes_personnalises = {}
        self.input_zone_gauche = input_zone_gauche or []



    # INPUT:
    #   path_config (str): Path to JSON file containing a simple list of keywords.
    # OUTPUT:
    #   keywords (list[str]): List of base keywords, unchanged (not normalized here).
    @staticmethod
    def load_keywords_ui1(path_config: str) -> list[str]:
        with open(path_config, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError("Expected a JSON list of keywords as input.")

        return [kw.strip() for kw in data if isinstance(kw, str)]



    # INPUT:
    #   columns (list[str]): List of column names from a row in Excel.
    #   keywords (list[str]): List of base keywords to match against columns.
    # OUTPUT:
    #   matched (dict[str, list[tuple[int, str]]]):
    #       Dict of matches: {keyword → list of (index, column_name) found}.
    #   multiple_matches: list[str] → keywords with multiple column matches
    @staticmethod
    def get_matching_columns(columns: list[str], keywords: list[str]) -> tuple[dict[str, list[tuple[int, str]]], list[str]]:
        matched = {kw: [] for kw in keywords}
        multiple_matches = []

        for i, col in enumerate(columns):
            if "%" in str(col):
                continue
            tokens_col = clean_tokens(str(col))
            for kw in keywords:
                tokens_kw = clean_tokens(kw)
                if all(tok in tokens_col for tok in tokens_kw):
                    if (i, col) not in matched[kw]:
                        matched[kw].append((i, col))

        # NEW LIST FOR :  → all if multiple match for 1 kw
        for kw, matches in matched.items():
            if len(matches) > 1:
                multiple_matches.append(kw)

        return matched, multiple_matches



    # INPUT:
    #   item (str): Keyword or group item to extract from the DataFrame
    #   df (pd.DataFrame): Excel data as a pandas DataFrame
    #   noms_reference (list[str]): List of column names or row names used as reference to resolve indirect mappings
    #   correspondances_input (dict[str, list[tuple[int, str]]])
    #       Dictionary mapping keywords (with → or exact) to list of positions and names in the DataFrame
    #   axis (str): "rows" or "columns", specifies the orientation of extraction
    #   idx (int | None): Index of the row or column, depending on axis, to locate the value to extract
    # OUTPUT:
    #   str: Extracted value from the DataFrame (processed with `values_lq_or_none`), or empty string if not found or invalid
    def extract_values(self, item, df, noms_reference, correspondances_input, axis, idx=None):
        # STEP 1 try : → all
        # On item : item is membre for groupes_personnalises or kw for keyword_valides
        idx_col, idx_ligne = (idx, None) if axis == "rows" else (None, idx)

        if "→ all" in item:
            # Reminder : correspondance_input = {KW → all : [(idx, nom),(idx, nom),()]}
            #            item is membre for groups & kw for base  = example : "toluene → (68, Toluène)"
            #                                                              or "toluene → all"
            #
            match_possibles = correspondances_input.get(item.strip(), [])
            print(f"\nTraitement '{item}' avec colonnes possibles :", match_possibles)

            valeurs_possibles = []
            # Using valeur_possible to take multiples ones but using the first [0]
            for idx_possibles, nom in match_possibles:
                try:
                    if axis == "rows":
                        idx_possibles = idx_possibles + self.config["param_row"]
                        val = df.iat[idx_possibles, idx_col]
                        if pd.notna(val) and str(val).strip() != "":
                            valeurs_possibles.append(val)
                    elif axis == "columns":
                        val = df.iat[idx_ligne, idx_possibles]
                        if pd.notna(val) and str(val).strip() != "":
                            valeurs_possibles.append(val)
                except Exception as e:
                    print(f"Erreur accès {axis} '{nom}' : {e}")
                    continue
            # Using first value found
            return values_lq_or_none(valeurs_possibles[0]) if valeurs_possibles else ""

        # STEP 2 : → + real column name
        elif "→" in item:
            try:
                _, cible = map(str.strip, item.split("→", 1))
                if cible.startswith("(") and "," in cible:
                    idx_str, _ = cible.strip("()").split(",", 1)
                    idx_possible = int(idx_str.strip())
                    val = df.iat[idx_ligne, idx_possible] if axis == "columns" else df.iat[idx_possible, idx_col]
                    return values_lq_or_none(val)
                else:
                    idx_possible = noms_reference.index(cible)
                    val = df.iat[idx_ligne, idx_possible] if axis == "columns" else df.iat[idx_possible, idx_col]
                    return values_lq_or_none(val)
            except Exception as e:
                print(f"Erreur sur item '{item}' : {e}")
                return ""

        # STEP 3 : No → in data = " " securize the ransomize
        elif "(" not in item:
                return ""

        # STEP 4 : No → in data but ( ok = Part from the randomiz
        else:
            correspondances = correspondances_input.get(item, [])
            if len(correspondances) == 1:
                idx_ref, nom = correspondances[0]
                try:
                    val = df.iat[idx_ligne, idx_ref] if axis == "columns" else df.iat[idx_ref, idx_col]
                    return values_lq_or_none(val)
                except Exception as e:
                    print(f"Erreur fallback simple sur '{item}' : {e}")
                    return ""
            else:
                print(f"'{item}' sans → ignoré car correspondances multiples ou absentes : {correspondances}")
                return ""


    def load_data(self):
        self.df = pd.read_excel(self.excel_path, sheet_name=self.sheet_name, header=None)



    # INPUT:
    #   None (uses self.json_config_path as input file path).
    # OUTPUT:
    #   self.keywords_valides (list[str]): List of selected keywords to extract.
    #   self.groupes_personnalises (dict[str, list[str]]): Custom groups of keywords to sum, defined by user.
    #   self.ordre_colonnes (list[str]): Ordered list of parameters (keywords and groups) selected by the user in UI2.
    def load_keywords_ui2(self):
        with open(self.json_config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        print("LOAD_KEYWORD_UI2 : Contenu JSON chargé :", data)

        self.keywords_valides = data.get("keywords_valides", [])
        self.groupes_personnalises = data.get("groupes_personnalises", {})
        self.ordre_colonnes = data.get(
            "ordre_selection",
            self.keywords_valides + list(self.groupes_personnalises.keys())
        )
        print("LOAD_KEYWORD_UI2 : Groupes chargés :", self.groupes_personnalises)
        print("LOAD_KEYWORD_UI2 : Ordre colonnes :", self.ordre_colonnes)



    # INPUT:
    #   self.resultats (dict[str, dict[str, Any]]): Dictionary of extracted results, indexed by "Code Artelia".
    #   self.excel_path (str): Path to the original Excel file, used to generate the output filename.
    #   self.ordre_colonnes (list[str]): Ordered list of columns to include in the export (final user selection).
    # OUTPUT:
    #   output_path (str): Full path to the generated Excel file containing the exported results.
    def export(self):
        dossier = os.path.dirname(self.excel_path)
        nom_base = os.path.splitext(os.path.basename(self.excel_path))[0]
        horodatage = pd.Timestamp.today().strftime('%Y%m%d_%H%M')
        output_path = os.path.join(dossier, f"{nom_base}_résumé_extraction_{horodatage}.xlsx")

        if not self.resultats:
            print("LOAD UI2 : Aucun résultat à exporter.")
            return

        df_export = pd.DataFrame.from_dict(self.resultats, orient="index")

        if "Nom échantillon" in df_export.columns:
            df_export = df_export.drop(columns=["Nom échantillon"])

        # Tri explicite selon l'ordre souhaité (zone droite)
        colonnes_finales = [col for col in self.ordre_colonnes if col in df_export.columns]
        df_export = df_export[colonnes_finales]

        df_export.index.name = "Nom échantillon"
        df_export.to_excel(output_path)
        return output_path

    def format_lq(self, val):
        val_str = str(val).strip().lower()
        if pd.isna(val):
            return ""
        if hasattr(self, "replace_lq_with_minus_one") and self.replace_lq_with_minus_one:
            if val_str.startswith("<") or val_str in {"n.d.", "n.d", "nd", "-", "n.d,", "n.d.."}:
                return -1
        return values_lq_or_none(val)

# ======================================================================================= #
# ====================================== COLUMNS ======================================== #
# ======================================================================================= #

class ColumnsExtract(BaseExtract):
    def __init__(self, excel_path, json_config_path, sheet_name, col_config):
        super().__init__(excel_path, json_config_path, sheet_name, col_config)
        self.col_config = col_config

    # INPUT:
    #   self.df (pd.DataFrame): Loaded Excel sheet containing the data to extract.
    #   self.col_config (dict[str, int]): Configuration dictionary with indices for name column, name row, and parameter row:
    #       - "nom_row": starting index for data rows.
    #       - "nom_col": column index containing the sample code ("Nom échantillon").
    #       - "param_row": row index where parameter names (column headers) are located.
    #   self.keywords_valides (list[str]): List of selected keywords (including "→ all" cases) to extract.
    #   self.groupes_personnalises (dict[str, list[str]]): Custom groups of keywords to aggregate by summation.
    # OUTPUT:
    #   self.resultats (dict[str, dict[str, Any]]): Extracted values per sample code, with individual and grouped parameters.
    def extract(self):
        self.resultats = {}
        df = self.df
        cfg = self.col_config

        nom_row = cfg["nom_row"]
        nom_col = cfg["nom_col"]


        # STEP 0 : Using [output_zone_droite] to recalculate based on [input_zone_gauche]
        #           (just (matched) to avoid all)
        all_correspondances = {}

        base_keywords = [
            kw.split("→")[0].strip()
            for kw in self.keywords_valides
            if "→ all" in kw
        ]

        for membres in self.groupes_personnalises.values():
            base_keywords.extend([
                m.split("→")[0].strip()
                for m in membres
                if "→ all" in m
            ])
        base_keywords = list(set(base_keywords))# Security

        # New detection
        noms_colonnes = list(df.iloc[self.col_config["param_row"]]) # all the line, !!! : absolute index
        matched, _ = self.get_matching_columns(noms_colonnes, base_keywords)

        for kw, correspondances in matched.items():
            print(f"  {kw} → {[nom for _, nom in correspondances]}")

        # From matched
        for kw, correspondances in matched.items():
            all_correspondances[f"{kw} → all"] = [(col_idx, col) for col_idx, col in correspondances]

        # To nom_echantillon
        for idx_ligne in range(nom_row, len(df)):
            nom_echantillon = df.iat[idx_ligne, nom_col]
            if not isinstance(nom_echantillon, str) or not nom_echantillon.strip():
                continue

            self.resultats[nom_echantillon] = {}

            # STEP 1 : Groups to be processed independantly
            for nom_groupe, membres in self.groupes_personnalises.items():
                valeurs_groupe = []
                lq_detected = False
                for membre in membres:
                    val_membre = self.extract_values(
                        item=membre,
                        df=df,
                        idx=idx_ligne,
                        noms_reference=noms_colonnes,
                        correspondances_input=all_correspondances,
                        axis="columns"
                    )
                    self.resultats[nom_echantillon][membre] = val_membre
                    val_str = str(val_membre).strip().lower()
                    if val_str.startswith("<") or "lq" in val_str:
                        lq_detected = True
                        continue

                    try:
                        valeurs_groupe.append(float(str(val_membre).replace(",", ".")))
                    except:
                        continue

                if valeurs_groupe:
                    somme = sum(valeurs_groupe)
                    self.resultats[nom_echantillon][nom_groupe] = self.format_lq(somme)
                elif lq_detected:
                    self.resultats[nom_echantillon][nom_groupe] = self.format_lq("<LQ")
                else:
                    self.resultats[nom_echantillon][nom_groupe] = ""

            # STEP 2 : Simple match : keyword_valides part
            for kw in self.keywords_valides:
                if kw in self.groupes_personnalises:
                    continue
                val_kw = self.extract_values(
                    item=kw,
                    df=df,
                    idx=idx_ligne,
                    noms_reference=noms_colonnes,
                    correspondances_input=all_correspondances,
                    axis="columns"
                )
                self.resultats[nom_echantillon][kw] = self.format_lq(val_kw)



# ======================================================================================= #
# ======================================== ROWS ========================================= #
# ======================================================================================= #

class RowsExtract(BaseExtract):
    def __init__(self, excel_path, json_config_path, sheet_name, row_config):
        super().__init__(excel_path, json_config_path, sheet_name, row_config)
        self.row_config = row_config  # Exemple: {"col_nom_param": 1, "col_valeur": 2, "start_row": 8}

    def extract(self):
        self.resultats = {}
        df = self.df
        cfg = self.row_config

        nom_row = cfg["nom_row"]
        param_col = cfg["param_col"]
        param_row = cfg["param_row"]
        data_start_col = cfg["data_start_col"]

        # STEP 1 : Extracting the "→ all" needed
        #
        all_correspondances = {}
        base_keywords = [
            kw.split("→")[0].strip()
            for kw in self.keywords_valides
            if "→ all" in kw
        ]

        for membres in self.groupes_personnalises.values():
            base_keywords.extend([
                m.split("→")[0].strip()
                for m in membres
                if "→ all" in m
            ])

        base_keywords = list(set(base_keywords))
        noms_parametres = df.iloc[param_row:, param_col].tolist() # all cells from param_col from nom_row
                                                                    # !!! : relative index
        matched, _ = self.get_matching_columns(noms_parametres, base_keywords)

        # all_correspondances output - {KW → all : [(idx, nom),(idx, nom),()]}
        for kw, correspondances in matched.items():
            all_correspondances[f"{kw} → all"] = [(idx, nom) for idx, nom in correspondances]


        # STEP 2 : Looking for code and values & creating list of results
        #
        for idx_col in range(data_start_col, df.shape[1]):
            nom_echantillon = df.iloc[nom_row, idx_col]
            if not isinstance(nom_echantillon, str) or not nom_echantillon.strip():
                continue
            self.resultats[nom_echantillon] = {}

            # STEP 2.1 : Looking for values for each kind in keywords_finals
            # GROUPS
            for nom_groupe, membres in self.groupes_personnalises.items():
                valeurs_groupe = []
                lq_detected = False
                for membre in membres:
                    val_membre = self.extract_values(
                        item=membre,
                        df=df,
                        idx=idx_col,
                        noms_reference=noms_parametres,
                        correspondances_input=all_correspondances,
                        axis="rows",
                    )
                    self.resultats[nom_echantillon][membre] = val_membre
                    val_str = str(val_membre).strip().lower()
                    if val_str.startswith("<") or "lq" in val_str:
                        lq_detected = True
                        continue

                    try:
                        valeurs_groupe.append(float(str(val_membre).replace(",", ".")))
                    except:
                        continue

                if valeurs_groupe:
                    somme = sum(valeurs_groupe)
                    self.resultats[nom_echantillon][nom_groupe] = self.format_lq(somme)
                elif lq_detected:
                    self.resultats[nom_echantillon][nom_groupe] = self.format_lq("<LQ")
                else:
                    self.resultats[nom_echantillon][nom_groupe] = ""

            # SIMPLE
            for kw in self.keywords_valides:
                if kw in self.groupes_personnalises:
                    continue
                val_kw = self.extract_values(
                    item=kw,
                    df=df,
                    idx=idx_col,
                    noms_reference=noms_parametres,
                    correspondances_input=all_correspondances,
                    axis="rows",
                )
                self.resultats[nom_echantillon][kw] = self.format_lq(val_kw)












# ========================================== DEBUGGING TEST =========================================================
# ========================================== DEBUGGING TEST =========================================================
# ========================================== DEBUGGING TEST =========================================================
# ========================================== DEBUGGING TEST =========================================================
# ========================================== DEBUGGING TEST =========================================================
# ========================================== DEBUGGING TEST =========================================================
# ========================================== DEBUGGING TEST =========================================================
# ========================================== DEBUGGING TEST =========================================================
# ========================================== DEBUGGING TEST =========================================================
# ========================================== DEBUGGING TEST =========================================================
# if __name__ == "__main__":
#     file_path = os.path.join(os.path.dirname(__file__), ".xlsm")
