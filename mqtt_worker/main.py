import os
import json
import time
import psycopg2
import paho.mqtt.client as mqtt

MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "frigate/#")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "frigate_db")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "password123")

def init_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
            )
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS mqtt_events (
                    id SERIAL PRIMARY KEY,
                    topic VARCHAR(255) NOT NULL,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            cursor.close()
            conn.close()
            print("-> Bảng DB 'mqtt_events' đã sẵn sàng!")
            break
        except Exception as e:
            print(f"Chưa kết nối được DB, thử lại sau 3s... ({e})")
            time.sleep(3)

def save_to_db(topic, payload_str):
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cursor = conn.cursor()
        try:
            payload_json = json.loads(payload_str)
        except Exception:
            payload_json = {"raw_text": payload_str}
            
        sql = "INSERT INTO mqtt_events (topic, payload) VALUES (%s, %s);"
        cursor.execute(sql, (topic, json.dumps(payload_json)))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[LƯU SUCCESS] Topic: {topic}")
    except Exception as e:
        print(f"[LỖI DB] {e}")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"-> Kết nối MQTT Broker thành công! Subscribing: {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"-> Kết nối MQTT thất bại mã: {rc}")

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode('utf-8')
    except Exception:
        payload_str = str(msg.payload)
    save_to_db(msg.topic, payload_str)

if __name__ == "__main__":
    print("=== Khởi chạy Python MQTT Worker ===")
    init_db()
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message
    
    connected = False
    while not connected:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            connected = True
        except Exception as e:
            print(f"Chưa kết nối được MQTT Broker, thử lại sau 3s... ({e})")
            time.sleep(3)
            
    client.loop_forever()
