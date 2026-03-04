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
import smtplib
from email.message import EmailMessage
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build


APP_TITLE = "Ype Kramer Tellingen"
DB_PATH = "app.db"
CONFIG_PATH = "config.json"

SERVICE_ACCOUNT_FILE = "/etc/secrets/service_account.json"


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

def download_csv_from_drive(filename):

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )

    service = build("drive", "v3", credentials=credentials)

    results = service.files().list(
        q=f"name='{filename}' and trashed=false",
        fields="files(id, name)"
    ).execute()

    files = results.get("files", [])

    if not files:
        return None

    file_id = files[0]["id"]

    request = service.files().get_media(fileId=file_id)

    return request.execute()


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


init_db()


# -------------------- Mail --------------------

def send_mail(csv_bytes: bytes, vestiging: str):
    cfg = read_config()

    smtp_server = cfg.get("smtp_server", "smtp.gmail.com")
    smtp_port = int(cfg.get("smtp_port", 587))
    smtp_user = cfg.get("smtp_user", "")
    smtp_pass = cfg.get("smtp_pass", "")
    mail_from = cfg.get("mail_from", smtp_user)
    mail_to = cfg.get("mail_to", "")

    if not smtp_user or not smtp_pass or not mail_to:
        print("Mail config ontbreekt.")
        return

    msg = EmailMessage()
    msg["Subject"] = f"Voorraad afwijkingen - {vestiging}"
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg.set_content(f"Afwijkingen vestiging {vestiging} in bijlage.")

    msg.add_attachment(
        csv_bytes,
        maintype="text",
        subtype="csv",
        filename=f"afwijkingen_{vestiging}_{date.today().isoformat()}.csv",
    )

    with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


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

        art = r[0].strip()
        loc = r[1].strip() if len(r) > 1 else ""

        try:
            qty = int(r[2]) if len(r) > 2 else 0
        except:
            qty = 0

        desc = r[3].strip() if len(r) > 3 else ""

        rows.append((art, loc, qty, desc))

    return rows


# -------------------- Routes --------------------

@app.get("/", response_class=HTMLResponse)
def home(request: Request):

    conn = db()
    cur = conn.cursor()

    vestigingen = ["Leeuwarden", "Sneek", "Drachten"]

    historie = {}

    for v in vestigingen:
        cur.execute("SELECT COUNT(1) FROM counted WHERE vestiging=?", (v,))
        historie[v] = cur.fetchone()[0]

    conn.close()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": APP_TITLE,
            "historie": historie
        },
    )


@app.post("/upload")
async def upload(
    request: Request,
    vestiging: str = Form(...),
    aantal: int = Form(25),
):

    filename = f"{vestiging}.csv"

    content = download_csv_from_drive(filename)

if not content:

    conn = db()
    cur = conn.cursor()

    vestigingen = ["Leeuwarden","Sneek","Drachten"]
    historie = {}

    for v in vestigingen:
        cur.execute("SELECT COUNT(1) FROM counted WHERE vestiging=?", (v,))
        historie[v] = cur.fetchone()[0]

    conn.close()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": APP_TITLE,
            "error": f"Bestand {filename} niet gevonden in Google Drive",
            "historie": historie
        },
    )

    rows = ingest_csv(content)

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

    for r in picked:
        cur.execute(
            "INSERT INTO selections(vestiging, selection_id, artikelcode, locatie, voorraad, omschrijving) VALUES (?,?,?,?,?,?)",
            (vestiging, selection_id, r["artikelcode"], r["locatie"], r["voorraad"], r["omschrijving"]),
        )

    conn.commit()
    conn.close()

    return RedirectResponse(url=f"/tellen/{selection_id}", status_code=303)


@app.get("/tellen/{selection_id}", response_class=HTMLResponse)
def tellen(selection_id: str, request: Request):

    conn = db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, vestiging, artikelcode, locatie, voorraad, omschrijving
        FROM selections
        WHERE selection_id=?
        ORDER BY locatie
    """, (selection_id,))

    rows = cur.fetchall()

    conn.close()

    return templates.TemplateResponse(
        "tellen.html",
        {
            "request": request,
            "selection_id": selection_id,
            "rows": rows,
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

    w.writerow(["Artikelcode", "Locatie", "Systeem", "Geteld", "Verschil"])

    for r in rows:

        raw = (form.get(f"geteld_{r['id']}") or "").strip()

        try:
            geteld = int(raw) if raw else int(r["voorraad"])
        except:
            geteld = int(r["voorraad"])

        systeem = int(r["voorraad"])

        if geteld != systeem:

            w.writerow([
                r["artikelcode"],
                r["locatie"],
                systeem,
                geteld,
                geteld - systeem
            ])

        cur.execute("""
            INSERT INTO counted(vestiging, artikelcode, counted_date)
            VALUES (?,?,?)
            ON CONFLICT(vestiging, artikelcode)
            DO UPDATE SET counted_date=excluded.counted_date
        """, (vestiging, r["artikelcode"], date.today().isoformat()))

    conn.commit()
    conn.close()

    csv_bytes = buf.getvalue().encode("utf-8-sig")

    send_mail(csv_bytes, vestiging)

    return templates.TemplateResponse(
        "verwerkt.html",
        {"request": request, "selection_id": selection_id},
    )
