import socket
import json
import threading
import time
import uuid

MASTER_ID = str(uuid.uuid4())
# HOST = '10.62.217.16'
HOST = socket.gethostbyname(socket.gethostname())
PORT = 5000

OTHER_MASTERS = [
    ("10.62.217.209", 5000),
    ("10.62.217.199", 5000),
    ("10.62.217.212", 5000),
    ("10.62.217.203", 5000),
    ("10.62.217.22", 5000)
]


def start_master_server():
    def handle_connection(conn, addr):
        try:
            data = conn.recv(4096).decode('utf-8')
            if not data:
                return
            message = json.loads(data)
            task = message.get("TASK")
            sender_id = message.get("SERVER_ID")
            if task == "HEARTBEAT":
                print(f"[RECEBIDO] HEARTBEAT de {sender_id} ({addr})")
                response = {
                    "SERVER_ID": MASTER_ID,
                    "TASK": "HEARTBEAT",
                    "RESPONSE": "ALIVE",
                    "SENDER_IP": addr[0],
                    "SENDER_PORT": addr[1],
                    "SENDER_ID": sender_id
                }
                conn.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            print(f"[ERRO] Conexão com {addr}: {e}")
        finally:
            conn.close()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"[START] Master ativo em {HOST}:{PORT} (ID: {MASTER_ID})")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()


def heartbeat():
    while True:
        for host, port in OTHER_MASTERS:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(3)
                    s.connect((host, port))
                    heartbeat_msg = {
                        "SERVER_ID": MASTER_ID,
                        "TASK": "HEARTBEAT"
                    }
                    s.sendall(json.dumps(heartbeat_msg).encode('utf-8'))
                    response_data = s.recv(4096).decode('utf-8')
                    response = json.loads(response_data)
                    if response.get("RESPONSE") == "ALIVE":
                        print(f"[ALIVE] {host}:{port} -> ID: {response.get('SERVER_ID')}")
                    else:
                        print(f"[RESPOSTA INESPERADA] {host}:{port}: {response}")
            except Exception as e:
                print(f"[SEM RESPOSTA] {host}:{port} - {e}")
        time.sleep(5)


if __name__ == '__main__':
    threading.Thread(target=start_master_server, daemon=True).start()
    heartbeat()
