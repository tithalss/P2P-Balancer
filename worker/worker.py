import socket
import json
import time
import uuid

WORKER_ID = str(uuid.uuid4())
HOST = socket.gethostbyname(socket.gethostname())
PORT = 6000

CURRENT_MASTER = None  # ex: "ip:porta"
SIMULATED_TASK_TIME = 3

# ---------------- FUNÇÕES ----------------

def send_json(sock, obj):
    sock.sendall(json.dumps(obj).encode("utf-8"))

def register_to_master(master_address):
    global CURRENT_MASTER
    CURRENT_MASTER = master_address
    host, port = master_address.split(":")
    port = int(port)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            msg = {"type": "register_worker", "worker_id": WORKER_ID}
            send_json(s, msg)
            print(f"[REGISTRO] Worker registrado no master {master_address}")
    except Exception as e:
        print(f"[ERRO] Registro no master {master_address}: {e}")

def process_task(payload):
    task_id = payload.get("task_id", str(uuid.uuid4()))
    print(f"[TAREFA] Executando {task_id}")
    time.sleep(SIMULATED_TASK_TIME)
    print(f"[TAREFA] Concluída {task_id}")

def reconnect_to_master(new_master_address):
    print(f"[RECONFIG] Mudando master para {new_master_address}")
    register_to_master(new_master_address)

# ---------------- SERVIDOR ----------------

def start_worker_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"[WORKER] Ativo em {HOST}:{PORT} (ID: {WORKER_ID})")

    while True:
        conn, addr = server.accept()
        try:
            data = conn.recv(4096).decode("utf-8")
            if not data:
                conn.close()
                continue
            msg = json.loads(data)
            msg_type = msg.get("type")
            payload = msg.get("payload", {})

            if msg_type == "assign_task":
                process_task(payload)

            elif msg_type == "change_master":
                new_master = payload.get("new_master")
                if new_master:
                    reconnect_to_master(new_master)

        except Exception as e:
            print(f"[ERRO] {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    # inicia servidor em background
    import threading
    threading.Thread(target=start_worker_server, daemon=True).start()

    # registra no master inicial
    INITIAL_MASTER = "10.62.217.16:5000"  # <-- ajuste seu master
    time.sleep(1)
    register_to_master(INITIAL_MASTER)

    # mantém servidor rodando
    while True:
        time.sleep(1)
