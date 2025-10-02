import socket
import json
import threading
import time
import uuid

# -------------------------
# Configurações do servidor
# -------------------------
MASTER_ID = str(uuid.uuid4())          # ID único de cada servidor
HOST = '10.62.217.16'                  # IP local
PORT = 5000                            # Porta de escuta

# Lista de peers (outros masters)
OTHER_MASTERS = [
    ("10.62.217.209", 5000),  # Grupo 4 - Kawê, André, Thiago Machado
    ("10.62.217.199", 5000),  # Grupo 1 - João, Arthur, Carlos
    ("10.62.217.212", 5000),  # Grupo 2 - Rodrigo, Sergio, Cassia, Eduardo
    ("10.62.217.203", 5000),  # Grupo 5 - Braz
    ("10.62.217.22", 5000)    # Extra
]

# -------------------------
# Função 1: Servidor Master
# -------------------------
def start_master_server():
    """Inicia o servidor que escuta e responde mensagens JSON."""

    def handle_connection(conn, addr):
        """Trata cada conexão recebida."""
        try:
            data = conn.recv(4096).decode('utf-8')
            if not data:
                return  # conexão vazia -> ignora

            message = json.loads(data)
            task = message.get("TASK")
            sender_id = message.get("SERVER_ID")

            # Se recebeu um heartbeat, responde "ALIVE"
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

    # Cria socket servidor
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"[START] Master ativo em {HOST}:{PORT} (ID: {MASTER_ID})")

    # Loop infinito de aceitação de conexões
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()

# -------------------------
# Função 2: Heartbeat
# -------------------------
def master_heartbeat():
    """Envia heartbeats periódicos para os outros masters."""
    while True:
        for host, port in OTHER_MASTERS:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(3)
                    s.connect((host, port))

                    # Estrutura da mensagem de heartbeat
                    heartbeat_msg = {
                        "SERVER_ID": MASTER_ID,
                        "TASK": "HEARTBEAT"
                    }

                    # Envia JSON para o peer
                    s.sendall(json.dumps(heartbeat_msg).encode('utf-8'))

                    # Aguarda resposta
                    response_data = s.recv(4096).decode('utf-8')
                    response = json.loads(response_data)

                    if response.get("RESPONSE") == "ALIVE":
                        print(f"[ALIVE] {host}:{port} -> ID: {response.get('SERVER_ID')}")
                    else:
                        print(f"[RESPOSTA INESPERADA] {host}:{port}: {response}")

            except Exception as e:
                print(f"[SEM RESPOSTA] {host}:{port} - {e}")

        time.sleep(5)  # espera 5s antes do próximo ciclo

# -------------------------
# Main
# -------------------------
if __name__ == '__main__':
    # Servidor roda em paralelo em uma thread
    threading.Thread(target=start_master_server, daemon=True).start()

    # Loop de heartbeat roda na main thread
    master_heartbeat()


"""Thread passiva (listen e aguarda heartbeat B -> A) e thread ativa 
(pergunta se o irmao está vivo, manda protocolo
do server A pro server B / A -> B)"""