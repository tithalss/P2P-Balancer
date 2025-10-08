import socket
import json
import threading
import time
import uuid

WORKER_ID = str(uuid.uuid4())
HOST = socket.gethostbyname(socket.gethostname())
PORT = 5000  # porta do worker

# Quando o worker é informado sobre qual master deve ajudar,
# ele se conecta ao master e envia a mensagem "worker_ready".

def send_json(sock, obj):
    sock.sendall(json.dumps(obj).encode("utf-8"))

def recv_json(sock):
    data = sock.recv(4096).decode("utf-8")
    if not data:
        return None
    return json.loads(data)

def connect_to_master(master_host, master_port, request_id=None):
    """Worker conecta ao master e envia 'worker_ready'"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((master_host, master_port))
            msg = {
                "type": "worker_ready",
                "request_id": request_id or str(uuid.uuid4()),
                "payload": {
                    "worker_id": WORKER_ID,
                    "address": f"{HOST}:{PORT}",
                    "status": "ready"
                }
            }
            send_json(s, msg)
            print(f"[ENVIADO] worker_ready -> {master_host}:{master_port}")

            # espera ACK do master
            response_data = s.recv(4096).decode("utf-8")
            if response_data:
                resp = json.loads(response_data)
                print(f"[RESPOSTA] do master: {resp}")
    except Exception as e:
        print(f"[ERRO] Falha ao notificar master {master_host}:{master_port} - {e}")