from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import psycopg2
import psycopg2.extras

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(
        host="localhost", database="iotdb",
        user="admin", password="admin", port="5432"
    )

# ── MODÈLES PYDANTIC ──────────────────────────────────────────

class MachineCreate(BaseModel):
    id: str
    nom: str
    type: str
    seuil_temp: Optional[float] = None
    seuil_vib: Optional[float] = None

# ── ROUTES ────────────────────────────────────────────────────

@app.get("/")
def home():
    return {"message": "API RaffinIA active"}

@app.get("/predictions")
def predictions():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT p.timestamp, p.machine_id AS machine,
               p.temperature, p.vibration, p.probabilite,
               COALESCE(a.prediction_panne,'NORMAL') AS prediction_panne,
               COALESCE(a.score_risque,0) AS score_risque,
               a.valeur_predite_30min
        FROM predictions_pannes p
        LEFT JOIN (
            SELECT DISTINCT ON (machine_id)
                machine_id, prediction_panne, score_risque,
                valeur_predite_30min, timestamp
            FROM alertes_30min
            ORDER BY machine_id, timestamp DESC
        ) a ON p.machine_id = a.machine_id
        ORDER BY p.timestamp DESC LIMIT 50
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

@app.get("/alertes")
def alertes():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp, machine_id, type_capteur,
               valeur_actuelle, score_risque, prediction_panne,
               valeur_predite_30min, message_alerte
        FROM alertes_30min
        ORDER BY timestamp DESC LIMIT 100
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

@app.get("/stats")
def stats():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT COUNT(*) as total FROM alertes_30min")
    total = cur.fetchone()["total"]
    cur.execute("SELECT COUNT(*) as c FROM alertes_30min WHERE prediction_panne='CRITIQUE'")
    critiques = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM alertes_30min WHERE prediction_panne='WARNING'")
    warnings = cur.fetchone()["c"]
    cur.close(); conn.close()
    return {"total": total, "critiques": critiques, "warnings": warnings}

# ── MACHINES ──────────────────────────────────────────────────

@app.get("/machines")
def get_machines():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM machines ORDER BY created_at DESC")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

@app.get("/machines/{machine_id}/live")
def get_machine_live(machine_id: str):
    """
    Retourne les dernières mesures + prédictions à +10s pour une machine.
    Une ligne par type_capteur (temperature, vibration).
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT DISTINCT ON (type_capteur)
            type_capteur,
            valeur_actuelle,
            valeur_predite_30min        AS valeur_predite_10s,
            timestamp                   AS mesure_a,
            timestamp + interval '10 seconds' AS predit_a,
            ROUND(score_risque::numeric * 100, 1) AS score_risque_pct,
            prediction_panne,
            message_alerte
        FROM alertes_30min
        WHERE machine_id = %s
        ORDER BY type_capteur, timestamp DESC
    """, (machine_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

@app.post("/machines")
def add_machine(m: MachineCreate):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO machines (id, nom, type, seuil_temp, seuil_vib)
               VALUES (%s, %s, %s, %s, %s)""",
            (m.id, m.nom, m.type, m.seuil_temp, m.seuil_vib)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cur.close(); conn.close()
    return {"message": "Machine ajoutée", "id": m.id}

@app.delete("/machines/{machine_id}")
def delete_machine(machine_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM machines WHERE id = %s", (machine_id,))
    conn.commit()
    cur.close(); conn.close()
    return {"message": f"Machine {machine_id} supprimée"}