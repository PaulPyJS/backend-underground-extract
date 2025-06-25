import tkinter as tk
from tkinter import filedialog, messagebox
import json
import os
import sys
import re

from .extract_utils import convert_config_to_indices, values_lq_or_none
from .analysis_extract import RowsExtract, ColumnsExtract

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DOSSIER_DATA = os.path.join(BASE_DIR, "00_Cache")
os.makedirs(DOSSIER_DATA, exist_ok=True)

FICHIER_LAST_CONFIG = os.path.join(DOSSIER_DATA, "last_config_extract.json")
temp_json = os.path.join(DOSSIER_DATA, "final_keywords.json")


def save_last_config(path):
    with open(FICHIER_LAST_CONFIG, "w") as f:
        json.dump({"last_config": path}, f)


def load_last_config():
    if os.path.exists(FICHIER_LAST_CONFIG):
        with open(FICHIER_LAST_CONFIG, "r") as f:
            return json.load(f).get("last_config", "")
    return ""

# INPUT:
#   matched_columns (dict[str, list[str]]): Mapping of keywords to matching column names from initial detection.
#   extraction_type (str): Type of extraction, either "colonnes" or "lignes".
#   excel_file (str): Path to the original Excel file.
#   resultats_artelia (object): Object containing the global mapping and potential shared attributes.
#   sheet_name (str): Name of the Excel sheet to work on.
#   df (pd.DataFrame): The loaded Excel sheet as a pandas DataFrame.
#   mapping_all (dict): Global mapping of all keyword-column matches (can be overwritten inside function).
#   config_extraction (dict | None): Optional extraction configuration containing cell indices (col/row positions).
#   input_zone_gauche (list[str] | None): List of available keywords/columns to be displayed in the left UI zone.
# OUTPUT:
#   None (opens an interactive UI for selection, editing, and exporting parameters, and may write a config JSON file).
def ouvrir_ui_post_extract(matched_columns, extraction_type, excel_file, sheet_name, config_extraction=None, input_zone_gauche=None):
    affichage_mapping = {}
    libelles_formates = input_zone_gauche.copy()
    config_kw = []

    for kw, colonnes in matched_columns.items():
        for col in colonnes:
            label_affiche = f"{kw} → {col}"
            affichage_mapping[label_affiche] = kw

    def charger_config():
        nonlocal config_kw
        path = filedialog.askopenfilename(filetypes=[("JSON files", "*.json")])
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as file:
                data = json.load(file)
                # keyword_valides part
                config_kw.clear()
                config_kw.extend(data.get("keywords_valides", []))
                # groupes_personalises part
                groupes.clear()
                groupes.update(data.get("groupes_personnalises", {}))

                config_path.set(path)
                save_last_config(path)

                # Affichage des zones
                zone_droite.delete(0, tk.END)
                zone_gauche.delete(0, tk.END)

                if "Code Artelia" not in zone_droite.get(0, tk.END):
                    zone_droite.insert(0, "Code Artelia")

                # Affiche tout à gauche par défaut
                for label_zone in libelles_formates:
                    ref = affichage_mapping.get(label_zone, label_zone)
                    if isinstance(ref, tuple):
                        ref_str = f"{ref[0]} → {ref[1]}"
                    else:
                        ref_str = ref

                    if ref_str in config_kw:
                        zone_droite.insert(tk.END, label_zone)
                    else:
                        zone_gauche.insert(tk.END, label_zone)

                # Ajout des groupes
                for group_name  in groupes:
                    if group_name  not in zone_droite.get(0, tk.END):
                        zone_droite.insert(tk.END, group_name )

                afficher_groupes()

        except Exception as err:
            messagebox.showerror("Erreur", f"Impossible de charger le fichier :\n{err}")

    def ajouter_mots():
        for i in zone_gauche.curselection()[::-1]:
            kw = zone_gauche.get(i)
            zone_droite.insert(tk.END, kw)
            zone_gauche.delete(i)

    def retirer_mots():
        for i in zone_droite.curselection()[::-1]:
            kw = zone_droite.get(i)
            if kw == "Code Artelia":
                continue
            zone_gauche.insert(tk.END, kw)
            zone_droite.delete(i)

    def generer_config():
        mots = list(zone_droite.get(0, tk.END))
        if not mots:
            messagebox.showwarning("Aucun mot-clé", "Veuillez sélectionner au moins un paramètre.")
            return

        output_dict = {
            "keywords_valides": [],
            "groupes_personnalises": groupes
        }

        # Exclure les groupes déjà connus pour ne garder que les vrais paramètres
        for mot in mots:
            if mot not in groupes and mot != "Code Artelia":
                output_dict["keywords_valides"].append(mot)

        path = filedialog.asksaveasfilename(defaultextension=".json", initialfile="config_extract.json")
        if not path:
            return

        with open(path, "w", encoding="utf-8") as f:
            json.dump(output_dict, f, indent=2, ensure_ascii=False)

        config_path.set(path)
        save_last_config(path)
        messagebox.showinfo("Succès", f"Configuration sauvegardée dans :\n{path}")

    def extraire_en_excel():
        selection = list(zone_droite.get(0, tk.END))
        if not selection:
            messagebox.showwarning("Aucun paramètre", "Veuillez sélectionner au moins un paramètre à exporter.")
            return

        output_zone_droite = {
            "keywords_valides": [item for item in selection if item not in groupes],
            "groupes_personnalises": groupes,
            "ordre_selection": selection
        }

        # Sauvegarde JSON temporaire
        with open(temp_json, "w", encoding="utf-8") as f:
            json.dump(output_zone_droite, f, indent=2, ensure_ascii=False)

        # Suite : config + lancement extraction
        config = None
        lq_to_minus_one = replace_lq_var.get()
        try:
            if extraction_type.lower() in ["colonnes", "lignes"]:
                config = convert_config_to_indices(config_extraction)

            # Lancement de l'extraction
            try:
                if extraction_type.lower() == "colonnes":
                    extractor = ColumnsExtract(excel_file, temp_json, sheet_name, col_config=config)
                else:
                    extractor = RowsExtract(excel_file, temp_json, sheet_name, row_config=config)

                print("Extractor created:", extractor)
            except Exception as e:
                messagebox.showerror("Erreur instanciation extracteur", f"{e}")
                return

            extractor.load_keywords_ui2()
            extractor.load_data()
            extractor.replace_lq_with_minus_one = lq_to_minus_one
            extractor.extract()
            output_path = extractor.export()
            messagebox.showinfo("Export terminé", f"Le fichier a été généré avec succès :\n{output_path}")

        except Exception as e:
            messagebox.showerror("Erreur config_extraction", f"Erreur :\n{e}")

    # GROUPING DATA SUM FUNCTION
    #
    def editer_groupe(nom=None):
        def valider():
            nom_groupe = entry_nom.get().strip()
            if not nom_groupe:
                messagebox.showwarning("Nom manquant", "Veuillez entrer un nom de groupe.")
                return

            selection = listbox.curselection()
            mots_selectionnes = [listbox.get(i) for i in selection]

            if not mots_selectionnes:
                messagebox.showwarning("Aucun mot-clé", "Sélectionnez au moins un mot-clé.")
                return

            groupes[nom_groupe] = mots_selectionnes
            fenetre.destroy()
            afficher_groupes()






        fenetre = tk.Toplevel()
        fenetre.title("Créer / Modifier un groupe")
        tk.Label(fenetre, text="Nom du groupe :").pack(pady=5)
        entry_nom = tk.Entry(fenetre)
        entry_nom.pack(pady=5)
        if nom:
            entry_nom.insert(0, nom)

        tk.Label(fenetre, text="Mots-clés disponibles :").pack()
        listbox = tk.Listbox(fenetre, selectmode=tk.MULTIPLE, width=40, height=10)
        listbox.pack(padx=10, pady=5)

        reverse_mapping = {}
        libelles_groupables = []

        # Crée les libellés groupables à partir de input_zone_gauche
        for label in input_zone_gauche:
            if "→" in label:
                kw, col = map(str.strip, label.split("→", 1))
                libelles_groupables.append(label)
                reverse_mapping[label] = (kw, col)
            else:
                libelles_groupables.append(label)
                reverse_mapping[label] = label

        # Ajout des libellés à la listbox
        for label in libelles_groupables:
            listbox.insert(tk.END, label)

        # Memory for reopening
        if nom and nom in groupes:
            mots_du_groupe = groupes[nom]
            for i, label in enumerate(listbox.get(0, tk.END)):
                val = reverse_mapping.get(label)
                val_str = f"{val[0]} → {val[1]}" if isinstance(val, tuple) else str(val)
                if val_str in mots_du_groupe:
                    listbox.selection_set(i)

        tk.Button(fenetre, text="Valider", command=valider).pack(pady=10)


    def supprimer_groupe(nom):
        if messagebox.askyesno("Confirmer suppression", f"Supprimer le groupe « {nom} » ?"):
            groupes.pop(nom, None)
            afficher_groupes()

    def ajouter_groupe_a_selection(nom_groupe):
        if nom_groupe in groupes:
            if nom_groupe not in zone_droite.get(0, tk.END):
                zone_droite.insert(tk.END, nom_groupe)
            if nom_groupe in zone_gauche.get(0, tk.END):
                idx = zone_gauche.get(0, tk.END).index(nom_groupe)
                zone_gauche.delete(idx)

    def afficher_groupes():
        # Efface tout dans la vraie zone liste
        for widget in frame_groupes_liste.winfo_children():
            widget.destroy()

        if not groupes:
            lbl = tk.Label(frame_groupes_liste, text="Aucun groupe défini", fg="gray")
            lbl.pack()
            return

        for nom in groupes:
            row = tk.Frame(frame_groupes_liste)
            row.pack(fill="x", pady=1)
            tk.Label(row, text=nom, width=25, anchor="w").pack(side=tk.LEFT)
            tk.Button(row, text="✏️", command=lambda n=nom: editer_groupe(n)).pack(side=tk.LEFT, padx=2)
            tk.Button(row, text="❌", command=lambda n=nom: supprimer_groupe(n)).pack(side=tk.LEFT, padx=2)
            tk.Button(row, text="➕", command=lambda n=nom: ajouter_groupe_a_selection(n)).pack(side=tk.LEFT, padx=10)


    # = MAIN WINDOW
    #
    fenetre = tk.Toplevel()

    fenetre.title("Sélection des paramètres à extraire")
    fenetre.geometry("550x700")

    config_path = tk.StringVar(value="config_extract.json")
    groupes = {}

    tk.Label(fenetre, text="CONFIGURATION :", font=("Segoe UI", 10, "bold")).pack(pady=2)
    frame_conf = tk.Frame(fenetre)
    frame_conf.pack()
    tk.Label(frame_conf, textvariable=config_path, fg="blue").pack(side=tk.LEFT)
    tk.Button(frame_conf, text="SÉLECTIONNER", command=charger_config).pack(side=tk.LEFT, padx=5)

    # PARAMETERS AREA TO SELECT FROM LISTS
    #
    frame_zones = tk.Frame(fenetre)
    frame_zones.pack(pady=15, fill="both", expand=True)

    tk.Label(frame_zones, text="PARAMÈTRES DISPONIBLES", font=("Segoe UI", 9, "bold")).grid(row=0, column=0, padx=10,
                                                                                            pady=(0, 5))
    tk.Label(frame_zones, text="PARAMÈTRES À EXPORTER", font=("Segoe UI", 9, "bold")).grid(row=0, column=2, padx=10,
                                                                                           pady=(0, 5))
    zone_gauche = tk.Listbox(frame_zones, selectmode=tk.EXTENDED, width=40, height=15)
    zone_droite = tk.Listbox(frame_zones, selectmode=tk.EXTENDED, width=40, height=15)
    zone_gauche.grid(row=1, column=0, padx=10)
    zone_droite.grid(row=1, column=2, padx=10)

    # CHECK BOX FOR LQ into -1
    replace_lq_var = tk.BooleanVar(value=False)

    # Checkbox
    frame_checkbox = tk.Frame(fenetre)
    frame_checkbox.pack(pady=(0, 5))
    tk.Checkbutton(
        frame_checkbox,
        text="Remplacer les valeurs <LQ par -1",
        variable=replace_lq_var
    ).pack()

    # DRAG AND DROP ON RIGHT AREA TO BE ABLE TO MODIFY THE JSON DIRECTLY INTO THE UI
    #
    def on_start_drag(event):
        widget = event.widget
        widget.drag_start_index = widget.nearest(event.y)

    def on_drag_motion(event):
        widget = event.widget
        current_index = widget.nearest(event.y)
        start_index = getattr(widget, "drag_start_index", None)

        if start_index is not None and current_index != start_index:
            items = list(widget.get(0, tk.END))
            item = items.pop(start_index)
            items.insert(current_index, item)

            widget.delete(0, tk.END)
            for i in items:
                widget.insert(tk.END, i)

            widget.drag_start_index = current_index

    # Bind drag & drop à la zone de droite
    zone_droite.bind("<Button-1>", on_start_drag)
    zone_droite.bind("<B1-Motion>", on_drag_motion)

    frame_btn = tk.Frame(frame_zones)
    frame_btn.grid(row=1, column=1)
    tk.Button(frame_btn, text="→", command=ajouter_mots).pack(pady=10)
    tk.Button(frame_btn, text="←", command=retirer_mots).pack(pady=10)

    # RANDOM VERIFICATION AREA
    #
    frame_verification = tk.LabelFrame(fenetre, text="🔎 Vérification d'une ligne extraite",
                                       font=("Segoe UI", 9, "bold"))
    frame_verification.pack(pady=10, fill="x", padx=15)

    text_resultat = tk.Text(frame_verification, height=2, width=80, font=("Courier", 9), wrap="word", bg="#f0f0f0",
                            state="disabled")
    text_resultat.pack(pady=5, padx=10)


    def randomize_values():
        import random
        # Same extract as extract_to_excel : not the best practice but ok with this kind of data
        try:
            if extraction_type.lower() in ["colonnes", "lignes"]:
                config = convert_config_to_indices(config_extraction)
                correspondances_input = {
                    f"{kw} → ({idx}, {nom})": [(idx, nom)]
                    for kw, correspondances in matched_columns.items()
                    for idx, nom in correspondances
                }

                if extraction_type.lower() == "colonnes":
                    extractor = ColumnsExtract(excel_file, temp_json, sheet_name, col_config=config)
                    axis = "columns"
                    extractor.load_data()
                    noms_ref = list(extractor.df.iloc[config["param_row"]])
                    idx_random = random.randint(config["nom_row"] + 1, extractor.df.shape[0] - 1)

                else:
                    extractor = RowsExtract(excel_file, temp_json, sheet_name, row_config=config)
                    axis = "rows"
                    extractor.load_data()
                    noms_ref = list(extractor.df.iloc[config["param_row"]:, config["param_col"]])
                    idx_random = random.randint(config["data_start_col"] + 1, extractor.df.shape[1] - 1)

                kw = random.choice(list(correspondances_input.keys()))
                val = values_lq_or_none(extractor.extract_values(
                    item=kw,
                    df=extractor.df,
                    idx=idx_random,
                    noms_reference=noms_ref,
                    correspondances_input=correspondances_input,
                    axis=axis
                ))
                if axis == "columns":
                    code = extractor.df.iat[idx_random, config["nom_col"]]
                else:
                    code = extractor.df.iat[config["nom_row"], idx_random]

                val = "None" if val == "" else val
                match_val = re.search(r'\(([^()]+)\)', val)
                if match_val:
                    val = match_val.group(1)
                else:
                    val = val
                kw_split = kw.split('→', 1)[-1].strip()
                kw_final = kw_split.replace("- (mg/kg M.S.)", "")
                texte = f"Code : {code} | {kw_final} | {val}"

            else:
                texte = "⚠️ Type d'extraction non reconnu."
        except Exception as e:
            texte = f"❌ Erreur : {e}"

        text_resultat.configure(state="normal")
        text_resultat.delete("1.0", tk.END)
        text_resultat.insert(tk.END, texte)
        text_resultat.configure(state="disabled")

    tk.Button(frame_verification, text="Randomize", command=randomize_values).pack(pady=5)





    # GROUP AREA TO GATHER PARAMETERS TO PREPARE SUM
    #
    frame_groupes = tk.LabelFrame(fenetre, text="🔄 GROUPES DE SOMME PERSONNALISÉS", font=("Segoe UI", 9, "bold"))
    frame_groupes.pack(pady=10, fill="x", padx=15)

    frame_groupes_liste = tk.Frame(frame_groupes)
    frame_groupes_liste.pack(anchor="w", padx=10, pady=5)

    tk.Button(frame_groupes, text="+ Créer un groupe", command=lambda: editer_groupe()).pack(pady=5)

    frame_bottom = tk.Frame(fenetre)
    frame_bottom.pack(pady=15)
    tk.Button(frame_bottom, text="💾 GÉNÉRER JSON", width=20, command=generer_config).pack(side=tk.LEFT, padx=20)
    tk.Button(frame_bottom, text="📤 EXTRAIRE EN EXCEL", width=20, command=extraire_en_excel).pack(side=tk.LEFT, padx=20)

    tk.Label(fenetre, text="© Paul Ancian – 2025", font=("Segoe UI", 7), fg="gray") \
        .pack(side="bottom", pady=(5, 10))


    # == INIT ==
    config_kw = []
    affichage_mapping.clear()
    last_path = load_last_config()

    for label in input_zone_gauche:
        if label not in libelles_formates:
            if "→" not in label:
                affichage_mapping[label] = label
            elif "→ all" in label:
                kw = label.split("→")[0].strip()
                affichage_mapping[label] = (kw, "all")
            libelles_formates.append(label)

    if last_path:
        config_path.set(last_path)
        try:
            with open(last_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            config_kw.clear()
            config_kw.extend(data.get("keywords_valides", []))
            groupes.clear()
            groupes.update(data.get("groupes_personnalises", {}))
        except Exception as e:
            print(f"Erreur au chargement du dernier fichier config : {e}")

    # Affichage des zones
    zone_droite.delete(0, tk.END)
    zone_gauche.delete(0, tk.END)

    if "Code Artelia" not in zone_droite.get(0, tk.END):
        zone_droite.insert(tk.END, "Code Artelia")

    for label in libelles_formates:
        ref = affichage_mapping.get(label, label)
        if isinstance(ref, tuple):
            ref_str = f"{ref[0]} → {ref[1]}"
        else:
            ref_str = ref

        if ref_str in config_kw:
            zone_droite.insert(tk.END, label)
        else:
            zone_gauche.insert(tk.END, label)

    for nom_groupe in groupes:
        if nom_groupe not in zone_droite.get(0, tk.END):
            zone_droite.insert(tk.END, nom_groupe)

    afficher_groupes()