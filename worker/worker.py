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

def handle_connection(conn, addr):
    """Lida com mensagens recebidas pelo worker."""
    try:
        data = conn.recv(4096).decode("utf-8")
        if not data:
            return
        message = json.loads(data)
        msg_type = message.get("type")
        payload = message.get("payload", {})
        request_id = message.get("request_id")

        if msg_type == "assign_worker":
            target_master = payload.get("target_master")  # Ex: "10.62.217.16:5000"
            if target_master:
                master_host, master_port = target_master.split(":")
                print(f"[RECEBIDO] Pedido para ajudar master {target_master}")
                threading.Thread(
                    target=connect_to_master,
                    args=(master_host, int(master_port), request_id),
                    daemon=True
                ).start()

                # responde ao master que enviou o assign_worker
                response = {
                    "type": "ack_assign_received",
                    "request_id": request_id,
                    "payload": {"status": "notifying_target"}
                }
                send_json(conn, response)
            else:
                print("[ERRO] assign_worker sem target_master")

        elif msg_type == "ping":
            response = {
                "type": "pong",
                "request_id": request_id,
                "payload": {"status": "alive"}
            }
            send_json(conn, response)
            print(f"[PING] Pong enviado para {addr}")

        else:
            print(f"[WARN] Tipo de mensagem desconhecido: {msg_type}")

    except Exception as e:
        print(f"[ERRO] Conexão com {addr}: {e}")
    finally:
        conn.close()

def start_worker_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"[START] Worker ativo em {HOST}:{PORT} (ID: {WORKER_ID})")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_worker_server()
