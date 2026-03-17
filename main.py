from __future__ import annotations

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import base64
import csv
import io
import json
import os
import sqlite3
import sys
import uuid
from datetime import date

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload


APP_TITLE = "Ype Kramer Tellingen"
DB_PATH = "/tmp/app.db"
CONFIG_PATH = "config.json"

SERVICE_ACCOUNT_FILE = "/etc/secrets/service_account.json"
DRIVE_FOLDER_ID = "1IC63Pk55dcwW3SwkVqmJaU85xT2y6a52"

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


# -------------------- Google Drive --------------------

def get_drive_service():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def find_file_in_drive(service, filename: str, folder_id: str | None = None):
    if folder_id:
        q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    else:
        q = f"name='{filename}' and trashed=false"

    results = service.files().list(
        q=q,
        fields="files(id, name, parents)",
        pageSize=10,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()

    files = results.get("files", [])
    if not files:
        return None
    return files[0]


def download_file_from_drive(filename: str):
    try:
        service = get_drive_service()

        print("ZOEK BESTAND IN DRIVE:", filename, "MAP:", DRIVE_FOLDER_ID)
        file_info = find_file_in_drive(service, filename, DRIVE_FOLDER_ID)

        if not file_info:
            print("BESTAND NIET GEVONDEN IN DRIVE:", filename)
            return None

        request = service.files().get_media(fileId=file_info["id"])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        print("BESTAND GEDOWNLOAD UIT DRIVE:", filename)
        return fh.getvalue()

    except Exception as e:
        print("DOWNLOAD DRIVE FOUT:", e)
        return None


def upload_file_to_drive(local_path: str, filename: str):
    try:
        if not os.path.exists(local_path):
            print("UPLOAD DRIVE FOUT: bestand ontbreekt lokaal:", local_path)
            return

        service = get_drive_service()
        media = MediaFileUpload(local_path, mimetype="application/octet-stream")

        existing = find_file_in_drive(service, filename, DRIVE_FOLDER_ID)

        if existing:
            service.files().update(
                fileId=existing["id"],
                media_body=media,
                supportsAllDrives=True,
            ).execute()
            print("BESTAND UPDATED IN DRIVE:", filename)
        else:
            service.files().create(
                body={
                    "name": filename,
                    "parents": [DRIVE_FOLDER_ID],
                },
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()
            print("BESTAND AANGEMAAKT IN DRIVE:", filename)

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
        CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_unique
        ON stock(vestiging, artikelcode)
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


def get_historie_counts() -> dict:
    historie = {}
    conn = db()
    cur = conn.cursor()

    for v in VESTIGINGEN:
        cur.execute("SELECT COUNT(1) FROM counted WHERE vestiging=?", (v,))
        historie[v] = cur.fetchone()[0]

    conn.close()
    return historie


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


def replace_stock_for_vestiging(vestiging: str, rows: list[tuple[str, str, int, str]]):
    conn = db()
    cur = conn.cursor()

    cur.execute("DELETE FROM stock WHERE vestiging=?", (vestiging,))

    cur.executemany("""
        INSERT INTO stock (vestiging, artikelcode, locatie, voorraad, omschrijving)
        VALUES (?, ?, ?, ?, ?)
    """, [
        (vestiging, art, loc, qty, desc)
        for art, loc, qty, desc in rows
    ])

    conn.commit()
    conn.close()


def create_selection_for_vestiging(vestiging: str):
    selection_id = uuid.uuid4().hex[:12]

    conn = db()
    cur = conn.cursor()

    cur.execute("DELETE FROM selections WHERE vestiging=?", (vestiging,))

    cur.execute("""
        INSERT INTO selections (vestiging, selection_id, artikelcode, locatie, voorraad, omschrijving)
        SELECT vestiging, ?, artikelcode, locatie, voorraad, omschrijving
        FROM stock
        WHERE vestiging=?
        ORDER BY locatie, artikelcode
    """, (selection_id, vestiging))

    conn.commit()
    conn.close()

    return selection_id


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
                "content": encoded,
            }
        ],
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


# -------------------- Startup --------------------

load_db_from_drive()
init_db()
upload_db_to_drive()


# -------------------- FastAPI Setup --------------------

app = FastAPI(title=APP_TITLE)
templates = Jinja2Templates(directory=resource_path("templates"))
app.mount("/static", StaticFiles(directory=resource_path("static")), name="static")


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
            "error": None,
        },
    )


@app.api_route("/", methods=["HEAD"])
def home_head():
    return HTMLResponse(status_code=200)


@app.post("/upload", response_class=HTMLResponse)
async def upload_csv(
    request: Request,
    vestiging: str = Form(...),
    file: UploadFile = File(...),
):
    try:
        if vestiging not in VESTIGINGEN:
            historie = get_historie_counts()
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "title": APP_TITLE,
                    "historie": historie,
                    "error": "Ongeldige vestiging gekozen",
                },
                status_code=400,
            )

        content = await file.read()
        rows = ingest_csv(content)

        if not rows:
            historie = get_historie_counts()
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "title": APP_TITLE,
                    "historie": historie,
                    "error": "CSV bevat geen geldige regels",
                },
                status_code=400,
            )

        replace_stock_for_vestiging(vestiging, rows)
        upload_db_to_drive()

        selection_id = create_selection_for_vestiging(vestiging)
        upload_db_to_drive()

        return RedirectResponse(url=f"/selectie/{selection_id}", status_code=303)

    except Exception as e:
        print("UPLOAD FOUT:", e)
        historie = get_historie_counts()
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "title": APP_TITLE,
                "historie": historie,
                "error": f"Upload fout: {e}",
            },
            status_code=500,
        )


@app.get("/selectie/{selection_id}", response_class=HTMLResponse)
def selectie(request: Request, selection_id: str):
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, vestiging, artikelcode, locatie, voorraad, omschrijving
        FROM selections
        WHERE selection_id=?
        ORDER BY locatie, artikelcode
    """, (selection_id,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return RedirectResponse("/", status_code=303)

    vestiging = rows[0]["vestiging"]

    return templates.TemplateResponse(
        "selectie.html",
        {
            "request": request,
            "title": APP_TITLE,
            "selection_id": selection_id,
            "vestiging": vestiging,
            "rows": rows,
        },
    )


@app.post("/verwerk/{selection_id}", response_class=HTMLResponse)
async def verwerk(selection_id: str, request: Request):
    form = await request.form()

    conn = db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, vestiging, artikelcode, locatie, voorraad, omschrijving
        FROM selections
        WHERE selection_id=?
        ORDER BY locatie, artikelcode
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
        "Verschil",
        "Locatie gewijzigd",
    ])

    for r in rows:
        raw_geteld = (form.get(f"geteld_{r['id']}") or "").strip()
        raw_locatie_correctie = (form.get(f"locatie_correctie_{r['id']}") or "").strip()

        oude_locatie = (r["locatie"] or "").strip()

        try:
            geteld = int(raw_geteld) if raw_geteld != "" else int(r["voorraad"] or 0)
        except Exception:
            geteld = int(r["voorraad"] or 0)

        systeem = int(r["voorraad"] or 0)

        if raw_locatie_correctie:
            nieuwe_locatie = raw_locatie_correctie
        else:
            nieuwe_locatie = oude_locatie

        locatie_gewijzigd = nieuwe_locatie != oude_locatie
        voorraad_gewijzigd = geteld != systeem

        if voorraad_gewijzigd or locatie_gewijzigd:
            w.writerow([
                r["artikelcode"],
                oude_locatie,
                nieuwe_locatie,
                systeem,
                geteld,
                geteld - systeem,
                "JA" if locatie_gewijzigd else "NEE",
            ])

        cur.execute("""
            INSERT INTO counted(vestiging, artikelcode, geteld, locatie, counted_date)
            VALUES (?, ?, ?, ?, ?)
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
            date.today().isoformat(),
        ))

        if locatie_gewijzigd:
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
        {
            "request": request,
            "selection_id": selection_id,
            "vestiging": vestiging,
        },
    )
