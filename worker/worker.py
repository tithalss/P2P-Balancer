# worker.py
# Worker que mantém conexão persistente com um Master, aceita TASK/REDIRECT/COMMAND_RELEASE
import socket
import json
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--id", "-i", required=True, help="worker id (ex: W1)")
parser.add_argument("--master-ip", default="127.0.0.1", help="IP do master inicial")
parser.add_argument("--master-port", type=int, default=5001, help="Porta do master inicial")
args = parser.parse_args()

WORKER_ID = args.id
home_master = {"IP": args.master_ip, "PORT": args.master_port}
current_master = dict(home_master)

def conectar_e_ouvir(host, port):
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            print(f"[WORKER {WORKER_ID}] conectado a {host}:{port}")
            # apresentar-se
            msg = {"WORKER": "ALIVE", "WORKER_ID": WORKER_ID}
            s.sendall((json.dumps(msg) + "\n").encode())
            buffer = b""
            while True:
                data = s.recv(4096)
                if not data:
                    print("[WORKER] conexão fechada pelo master")
                    s.close()
                    break
                buffer += data
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    try:
                        j = json.loads(line.decode().strip())
                    except:
                        print("[WORKER] recebido não json:", line)
                        continue

                    # TASK QUERY
                    if j.get("TASK") == "QUERY":
                        user = j.get("USER", "unknown")
                        print(f"[WORKER {WORKER_ID}] recebendo TASK QUERY para {user}")
                        # simula processamento
                        time.sleep(2)
                        resp = {"WORKER": WORKER_ID, "WORKER_ID": WORKER_ID, "STATUS": "OK", "TASK": "QUERY", "SALDO": 100}
                        s.sendall((json.dumps(resp) + "\n").encode())
                        print(f"[WORKER {WORKER_ID}] resposta enviada")

                    # REDIRECT: conectar a master alvo
                    elif j.get("TASK") == "REDIRECT":
                        target = j.get("TARGET_MASTER", {})
                        orig = j.get("HOME_MASTER", {})
                        print(f"[WORKER {WORKER_ID}] recebendo REDIRECT para {target}")
                        s.close()
                        # atualizar home_master para orig (se fornecido)
                        if orig:
                            home_master["IP"] = orig.get("IP", home_master["IP"])
                            home_master["PORT"] = orig.get("PORT", home_master["PORT"])
                        # conectar no target
                        host_t = target.get("IP")
                        port_t = target.get("PORT")
                        current_master["IP"] = host_t
                        current_master["PORT"] = port_t
                        # pequeno delay antes de reconectar
                        time.sleep(0.5)
                        print(f"[WORKER {WORKER_ID}] reconectando a {host_t}:{port_t}")
                        host, port = host_t, port_t
                        break  # sai do loop interno para reconectar
                    elif j.get("TASK") == "COMMAND_RELEASE" or j.get("TASK") == "command_release":
                        # voltar ao home_master
                        orig = j.get("payload", {}).get("original_master_address") or j.get("HOME_MASTER")
                        print(f"[WORKER {WORKER_ID}] recebendo COMMAND_RELEASE para retornar a {orig}")
                        s.close()
                        time.sleep(0.3)
                        host, port = orig.get("IP"), orig.get("PORT")
                        current_master["IP"], current_master["PORT"] = host, port
                        break
                    else:
                        # outros comandos (ignore)
                        pass
        except Exception as e:
            print(f"[WORKER] erro conexão: {e} — tentando novamente em 1s")
            time.sleep(1)
            continue

        # loop externo reconecta com os valores atuais
        host = current_master.get("IP")
        port = current_master.get("PORT")


if __name__ == "__main__":
    # inicializa valores
    current_master = dict(home_master)
    conectar_e_ouvir(current_master["IP"], current_master["PORT"])
