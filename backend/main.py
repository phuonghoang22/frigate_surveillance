import json
import os
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional
import requests

app = FastAPI(title="Camera Giám Sát API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "frigate_db")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "password123")
DB_PORT = os.getenv("DB_PORT", "5432")
FRIGATE_URL = os.getenv("FRIGATE_URL", "http://172.16.0.201:5000")
CONFIG_FILE = "/opt/frigate_mqtt/camera_config.json"

def get_db():
    return psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT, cursor_factory=RealDictCursor
    )

class CameraConfig(BaseModel):
    camera_name: str
    rtsp_url: str
    min_score: float = 0.7
    objects: List[str] = ["person", "car", "motorcycle", "dog"]

@app.get("/api/events")
def get_events(
    date: Optional[str] = Query(None),
    label: Optional[str] = Query(None),
    limit: int = Query(60, ge=1, le=200)
):
    try:
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
            
        start_time = f"{date} 00:00:00"
        end_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        end_time = f"{end_date} 00:00:00"

        conn = get_db()
        cur = conn.cursor()

        query = """
            SELECT 
                COALESCE(payload->'after'->>'id', payload->>'id') AS event_id,
                COALESCE(payload->'after'->>'camera', payload->>'camera', 'cam_test') AS camera,
                COALESCE(payload->'after'->>'label', payload->>'label', 'person') AS label,
                COALESCE(payload->'after'->>'top_score', payload->'after'->>'score', '0.85') AS raw_score,
                payload->'after'->>'start_time' AS ev_start,
                payload->'after'->>'end_time' AS ev_end,
                payload->'after'->>'frame_time' AS ev_frame,
                created_at
            FROM mqtt_events
            WHERE created_at >= %s::timestamp 
              AND created_at <  %s::timestamp
              AND COALESCE(payload->'after'->>'id', payload->>'id') IS NOT NULL
            ORDER BY id DESC
            LIMIT 400;
        """
        cur.execute(query, (start_time, end_time))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Gộp các bản tin trùng event_id và tính thời lượng sự kiện
        seen = {}
        results = []
        for r in rows:
            eid = r["event_id"]
            if not eid or eid in seen:
                continue
            seen[eid] = True

            lbl = r["label"]
            if label and label not in ("all", "") and lbl != label:
                continue

            try:
                score_val = round(float(r.get("raw_score") or 0.85) * 100)
            except:
                score_val = 85

            # Tính thời lượng (giây)
            duration_sec = 15
            try:
                st = float(r["ev_start"]) if r.get("ev_start") else None
                et = float(r["ev_end"]) if r.get("ev_end") else (float(r["ev_frame"]) if r.get("ev_frame") else None)
                if st and et and et > st:
                    duration_sec = max(1, int(round(et - st)))
            except:
                duration_sec = 12

            cam_raw = r.get("camera") or "cam_test"
            cam_display = "Hẻm 2A" if cam_raw == "cam_test" else cam_raw

            results.append({
                "event_id": eid,
                "camera": cam_display,
                "label": lbl,
                "score": score_val,
                "duration_sec": duration_sec,
                "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
                "clip_url": f"/api/events/{eid}/clip",
                "snapshot_url": f"/api/events/{eid}/snapshot"
            })
            if len(results) >= limit:
                break

        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/stats")
def get_stats():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM get_event_stats();")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"status": "success", "data": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/events/{event_id}/clip")
def get_event_clip(event_id: str):
    url = f"{FRIGATE_URL}/api/events/{event_id}/clip.mp4"
    try:
        req = requests.get(url, stream=True, timeout=10)
        if req.status_code != 200:
            raise HTTPException(status_code=404, detail="Không tìm thấy file video clip trên Frigate storage")
        return StreamingResponse(req.iter_content(chunk_size=1024*1024), media_type="video/mp4")
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Lỗi kết nối Frigate NVR: {str(e)}")

@app.get("/api/events/{event_id}/snapshot")
def get_event_snapshot(event_id: str):
    for ext, mime in [("snapshot.jpg", "image/jpeg"), ("snapshot.webp", "image/webp"), ("thumbnail.webp", "image/webp")]:
        try:
            req = requests.get(f"{FRIGATE_URL}/api/events/{event_id}/{ext}", stream=True, timeout=4)
            if req.status_code == 200:
                return StreamingResponse(req.iter_content(chunk_size=1024*64), media_type=mime)
        except:
            pass
    raise HTTPException(status_code=404, detail="Không tìm thấy snapshot")

@app.get("/api/config/camera")
def get_camera_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {
        "camera_name": "Hẻm 2A (cam_test)",
        "rtsp_url": "rtsp://172.16.10.125:8554/f6b39beeb1a84dfcd3",
        "min_score": 0.7,
        "objects": ["person", "car", "motorcycle", "dog"]
    }

@app.post("/api/config/camera")
def update_camera_config(config: CameraConfig):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(config.dict(), f, indent=4)
        return {"status": "success", "message": "Đã lưu cấu hình camera", "config": config.dict()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if os.path.exists("/opt/frigate_mqtt/backend/static"):
    app.mount("/", StaticFiles(directory="/opt/frigate_mqtt/backend/static", html=True), name="static")
