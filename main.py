from __future__ import annotations

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

import sqlite3
import csv
import io
import os
import sys
from datetime import datetime, date
import json
import base64
import requests

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload


APP_TITLE = "Ype Kramer Tellingen"
DB_PATH = "/tmp/app.db"
CONFIG_PATH = "config.json"

SERVICE_ACCOUNT_FILE = "/etc/secrets/service_account.json"
DRIVE_FOLDER_NAME = "Voorraadtellen"

VESTIGINGEN = ["Leeuwarden", "Sneek", "Drachten"]


# -------------------- Helpers --------------------

def resource_path(rel_path: str) -> str:
    base_path = getattr(sys, "_MEIPASS", os.path.abspath("."))
    return os.path.join(base_path, rel_path)


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def read_config():
    if not os.path.exists(CONFIG_PATH):
        return {}
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def get_drive_service():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)

def get_drive_folder_id(service):

    results = service.files().list(
        q=f"name='{DRIVE_FOLDER_NAME}' and trashed=false",
        fields="files(id, name)",
        pageSize=10,
    ).execute()

    folders = results.get("files", [])

    if not folders:
        print("MAP NIET GEVONDEN:", DRIVE_FOLDER_NAME)
        return None

    print("MAP GEVONDEN


def find_file_in_drive(service, filename: str, folder_id: str | None = None):
    if folder_id:
        q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    else:
        q = f"name='{filename}' and trashed=false"

    results = service.files().list(
        q=q,
        fields="files(id, name)",
        pageSize=10,
    ).execute()

    files = results.get("files", [])
    if not files:
        return None
    return files[0]


def download_file_from_drive(filename: str):
    try:
        service = get_drive_service()
        folder_id = get_drive_folder_id(service)

        file_info = find_file_in_drive(service, filename, folder_id)
        if not file_info:
            return None

        request = service.files().get_media(fileId=file_info["id"])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        return fh.getvalue()

    except Exception as e:
        print("DOWNLOAD DRIVE FOUT:", e)
        return None


def upload_file_to_drive(local_path: str, filename: str):
    try:
        if not os.path.exists(local_path):
            print("UPLOAD DRIVE FOUT: bestand ontbreekt lokaal")
            return

        service = get_drive_service()
        folder_id = get_drive_folder_id(service)

        media = MediaFileUpload(local_path, mimetype="application/octet-stream")

        existing = find_file_in_drive(service, filename, folder_id)

        if existing:
            service.files().update(
                fileId=existing["id"],
                media_body=media
            ).execute()
        else:
            body = {"name": filename}
            if folder_id:
                body["parents"] = [folder_id]

            service.files().create(
                body=body,
                media_body=media,
                fields="id"
            ).execute()

        print(f"BESTAND GEUPLOAD NAAR DRIVE: {filename}")

    except Exception as e:
        print("UPLOAD DRIVE FOUT:", e)


def load_db_from_drive():
    content = download_file_from_drive("app.db")
    if content:
        with open(DB_PATH, "wb") as f:
            f.write(content)
        print("DATABASE GELADEN UIT GOOGLE DRIVE")
    else:
        print("GEEN DATABASE GEVONDEN IN GOOGLE DRIVE, NIEUWE WORDT GEMAAKT")


def upload_db_to_drive():
    upload_file_to_drive(DB_PATH, "app.db")


def get_historie_counts() -> dict:
    historie = {}
    conn = db()
    cur = conn.cursor()

    for v in VESTIGINGEN:
        cur.execute("SELECT COUNT(1) FROM counted WHERE vestiging=?", (v,))
        historie[v] = cur.fetchone()[0]

    conn.close()
    return historie


# -------------------- Database --------------------

def init_db():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vestiging TEXT NOT NULL,
            artikelcode TEXT NOT NULL,
            locatie TEXT,
            voorraad INTEGER,
            omschrijving TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS counted (
            vestiging TEXT NOT NULL,
            artikelcode TEXT NOT NULL,
            geteld INTEGER,
            locatie TEXT,
            counted_date TEXT NOT NULL,
            PRIMARY KEY (vestiging, artikelcode)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS selections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vestiging TEXT NOT NULL,
            selection_id TEXT NOT NULL,
            artikelcode TEXT NOT NULL,
            locatie TEXT,
            voorraad INTEGER,
            omschrijving TEXT
        )
    """)

    conn.commit()
    conn.close()


load_db_from_drive()
init_db()
upload_db_to_drive()


# -------------------- Mail via Resend --------------------

def send_mail(csv_bytes: bytes, vestiging: str):
    api_key = os.getenv("RESEND_API_KEY")
    if not api_key:
        print("GEEN RESEND_API_KEY GEVONDEN")
        return

    encoded = base64.b64encode(csv_bytes).decode()

    payload = {
        "from": os.getenv("RESEND_FROM", "onboarding@resend.dev"),
        "to": [os.getenv("RESEND_TO", "ypekramertellen@gmail.com")],
        "subject": f"Voorraad afwijkingen {vestiging}",
        "html": "<p>Zie bijlage met voorraad afwijkingen.</p>",
        "attachments": [
            {
                "filename": f"afwijkingen_{vestiging}_{date.today().isoformat()}.csv",
                "content": encoded
            }
        ]
    }

    r = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )

    print("MAIL STATUS:", r.status_code)
    print("MAIL RESPONSE:", r.text)


# -------------------- FastAPI Setup --------------------

app = FastAPI(title=APP_TITLE)
templates = Jinja2Templates(directory=resource_path("templates"))
app.mount("/static", StaticFiles(directory=resource_path("static")), name="static")


# -------------------- CSV Parser --------------------

def ingest_csv(content: bytes):
    text = content.decode("utf-8-sig", errors="ignore")
    reader = csv.reader(io.StringIO(text), delimiter=";")

    rows = []
    for i, r in enumerate(reader):
        if i == 0:
            continue
        if not r:
            continue

        art = (r[0] if len(r) > 0 else "").strip()
        if not art:
            continue

        loc = (r[1] if len(r) > 1 else "").strip()

        try:
            qty = int((r[2] if len(r) > 2 else "0").strip() or 0)
        except Exception:
            qty = 0

        desc = (r[3] if len(r) > 3 else "").strip()

        rows.append((art, loc, qty, desc))

    return rows


# -------------------- Routes --------------------

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    historie = get_historie_counts()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": APP_TITLE,
            "historie": historie,
            "error": None
        },
    )


@app.post("/upload")
async def upload(
    request: Request,
    vestiging: str = Form(...),
    aantal: int = Form(25),
):
    filename = f"{vestiging}.csv"
    content = download_file_from_drive(filename)

    if not content:
        historie = get_historie_counts()
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "title": APP_TITLE,
                "error": f"Bestand {filename} niet gevonden in Google Drive",
                "historie": historie,
            },
        )

    rows = ingest_csv(content)

    if not rows:
        historie = get_historie_counts()
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "title": APP_TITLE,
                "error": f"Bestand {filename} bevat geen regels",
                "historie": historie,
            },
        )

    selection_id = datetime.now().strftime("%Y%m%d%H%M%S")

    conn = db()
    cur = conn.cursor()

    cur.execute("DELETE FROM stock WHERE vestiging=?", (vestiging,))
    cur.executemany(
        "INSERT INTO stock(vestiging, artikelcode, locatie, voorraad, omschrijving) VALUES (?,?,?,?,?)",
        [(vestiging, r[0], r[1], r[2], r[3]) for r in rows],
    )

    cur.execute("""
        SELECT s.artikelcode, s.locatie, s.voorraad, s.omschrijving
        FROM stock s
        LEFT JOIN counted c
          ON s.artikelcode = c.artikelcode
         AND s.vestiging = c.vestiging
        WHERE s.vestiging = ?
          AND c.artikelcode IS NULL
        ORDER BY RANDOM()
        LIMIT ?
    """, (vestiging, int(aantal)))

    picked = cur.fetchall()

    cur.execute("DELETE FROM selections WHERE selection_id=?", (selection_id,))

    for r in picked:
        cur.execute(
            """
            INSERT INTO selections(
                vestiging, selection_id, artikelcode, locatie, voorraad, omschrijving
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                vestiging,
                selection_id,
                r["artikelcode"],
                r["locatie"],
                r["voorraad"],
                r["omschrijving"],
            ),
        )

    conn.commit()
    conn.close()

    upload_db_to_drive()

    return RedirectResponse(url=f"/tellen/{selection_id}", status_code=303)


@app.get("/tellen/{selection_id}", response_class=HTMLResponse)
def tellen(selection_id: str, request: Request):
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            s.id,
            s.vestiging,
            s.artikelcode,
            s.locatie,
            s.voorraad,
            s.omschrijving,
            c.geteld as last_geteld,
            c.locatie as last_locatie
        FROM selections s
        LEFT JOIN counted c
          ON s.artikelcode = c.artikelcode
         AND s.vestiging = c.vestiging
        WHERE s.selection_id=?
        ORDER BY COALESCE(s.locatie,''), s.artikelcode
    """, (selection_id,))

    rows = cur.fetchall()
    conn.close()

    return templates.TemplateResponse(
        "tellen.html",
        {
            "request": request,
            "selection_id": selection_id,
            "rows": rows,
            "n": len(rows),
        },
    )


@app.post("/verwerk/{selection_id}")
async def verwerk(selection_id: str, request: Request):
    form = await request.form()

    conn = db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, vestiging, artikelcode, locatie, voorraad, omschrijving
        FROM selections
        WHERE selection_id=?
    """, (selection_id,))

    rows = cur.fetchall()

    if not rows:
        conn.close()
        return RedirectResponse("/", status_code=303)

    vestiging = rows[0]["vestiging"]

    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow([
        "Artikelcode",
        "Oude locatie",
        "Nieuwe locatie",
        "Systeem",
        "Geteld",
        "Verschil"
    ])

    for r in rows:
        raw_geteld = (form.get(f"geteld_{r['id']}") or "").strip()
        raw_locatie_scan = (form.get(f"locatie_scan_{r['id']}") or "").strip()

        try:
            geteld = int(raw_geteld) if raw_geteld != "" else int(r["voorraad"] or 0)
        except Exception:
            geteld = int(r["voorraad"] or 0)

        systeem = int(r["voorraad"] or 0)
        oude_locatie = (r["locatie"] or "").strip()
        nieuwe_locatie = raw_locatie_scan if raw_locatie_scan else oude_locatie

        if geteld != systeem or nieuwe_locatie != oude_locatie:
            w.writerow([
                r["artikelcode"],
                oude_locatie,
                nieuwe_locatie,
                systeem,
                geteld,
                geteld - systeem
            ])

        cur.execute("""
            INSERT INTO counted(vestiging, artikelcode, geteld, locatie, counted_date)
            VALUES (?,?,?,?,?)
            ON CONFLICT(vestiging, artikelcode)
            DO UPDATE SET
                geteld=excluded.geteld,
                locatie=excluded.locatie,
                counted_date=excluded.counted_date
        """, (
            vestiging,
            r["artikelcode"],
            geteld,
            nieuwe_locatie,
            date.today().isoformat()
        ))

        cur.execute("""
            UPDATE stock
            SET locatie=?
            WHERE vestiging=? AND artikelcode=?
        """, (nieuwe_locatie, vestiging, r["artikelcode"]))

        cur.execute("""
            UPDATE selections
            SET locatie=?
            WHERE id=?
        """, (nieuwe_locatie, r["id"]))

    conn.commit()
    conn.close()

    upload_db_to_drive()

    csv_bytes = buf.getvalue().encode("utf-8-sig")
    send_mail(csv_bytes, vestiging)

    return templates.TemplateResponse(
        "verwerkt.html",
        {"request": request, "selection_id": selection_id},
    )
