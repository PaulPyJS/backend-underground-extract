import os
import shutil
import json
import asyncio
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware



import progress
from services.pdf_table_extract import extraire_pdf_vers_excel_async

app = FastAPI()
origins = os.getenv("FRONTEND_URLS", "http://localhost:3000").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


from routes import extract_geochem, extract_geotech
app.include_router(extract_geochem.router)
app.include_router(extract_geotech.router)




UPLOAD_DIR = "uploads"
OUTPUT_DIR = "outputs"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("🌍 FRONTEND_URLS autorisées :", origins)

@app.get("/")
def read_root():
    return {"message": "Backend opérationnel ✅"}

@app.get("/download/{filename}")
async def download_extracted_file(filename: str):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(file_path):
        return JSONResponse(content={"error": "Fichier non prêt ou inexistant"}, status_code=404)

    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename
    )

@app.get("/progress")
async def progress_stream(request: Request):
    print("👂 Nouveau client connecté au /progress")

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    print("❌ Client déconnecté")
                    break

                if "progress_count" not in progress.progress_state or "total_count" not in progress.progress_state:
                    await asyncio.sleep(0.1)
                    continue

                progress_count = progress.progress_state["progress_count"]
                total_count = progress.progress_state["total_count"]
                percentage_count = round(progress_count / total_count, 4)

                print(f"🔁 Envoi SSE : {percentage_count}")
                yield f"data: {json.dumps({'progress_count': percentage_count})}\n\n"
                await asyncio.sleep(0.3)

        except asyncio.CancelledError:
            print("🛑 Connexion SSE annulée proprement")
            raise

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/progress-latest")
async def latest_output_file():
    return {"last_output_file": progress.progress_state.get("last_output_file")}

@app.post("/progress/reset")
async def reset_progress():
    progress.progress_state["progress_count"] = 0
    progress.progress_state["total_count"] = 1
    progress.progress_state["last_output_file"] = None
    return {"status": "reset"}







# = PDF EXTRACTION

@app.post("/extract-pdf")
async def extract_pdf(
    pdf: UploadFile = File(...),
    keywords_json: str = Form(...),
    num_header_rows: int = Form(3),
    custom_name: Optional[str] = Form(None)
):
    print("🛬 Appel reçu : extract-pdf")
    print("Nom du fichier :", pdf.filename)
    print("Keywords reçus :", keywords_json)
    print("Nombre de lignes d’en-tête :", num_header_rows)

    current_task = progress.progress_state.get("current_task")
    if progress.progress_state.get("is_running") and current_task and not current_task.done():
        print("🛑 Annulation de la tâche en cours...")
        current_task.cancel()
        try:
            await current_task
            print("✔️ Ancienne tâche annulée avec succès")
        except asyncio.CancelledError:
            print("✔️ Tâche annulée")

    # Enregistre temporairement le fichier PDF
    pdf_path = os.path.join(UPLOAD_DIR, pdf.filename)
    with open(pdf_path, "wb") as f:
        shutil.copyfileobj(pdf.file, f)

    # Décode les mots-clés
    try:
        keywords = json.loads(keywords_json)
    except Exception as e:
        return JSONResponse(content={"error": f"Mauvais format de keywords: {e}"}, status_code=400)

    new_task = asyncio.create_task(
        pdf_extraction_worker(pdf_path, keywords, num_header_rows, custom_name, pdf.filename)
    )
    progress.progress_state["current_task"] = new_task
    progress.progress_state["is_running"] = True

    print("✅ Nouvelle tâche lancée")
    return JSONResponse(content={"status": "started"}, status_code=202)


async def pdf_extraction_worker(pdf_path, keywords, num_header_rows, custom_name, original_filename):
    try:
        output_buffer = await extraire_pdf_vers_excel_async(pdf_path, keywords, num_header_rows)

        # Sauvegarde le fichier
        output_filename = f"{custom_name}.xlsx" if custom_name else f"{os.path.splitext(original_filename)[0]}_extraction.xlsx"
        output_path = os.path.join(OUTPUT_DIR, output_filename)

        with open(output_path, "wb") as f:
            f.write(output_buffer.read())
        progress.progress_state["last_output_file"] = output_filename
        print(f"✅ Fichier d’extraction enregistré : {output_filename}")

        progress.progress_state["is_running"] = False
        progress.progress_state["current_task"] = None

    except asyncio.CancelledError:
        print("❌ Tâche extraction annulée proprement")
        progress.progress_state["is_running"] = False
        progress.progress_state["current_task"] = None
        raise


    except Exception as e:
        print(f"❌ Erreur dans extraction_worker : {e}")
        progress.progress_state["is_running"] = False
        progress.progress_state["current_task"] = None


