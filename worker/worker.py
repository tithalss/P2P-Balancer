import socket
import json
import threading
import time
import uuid
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

WORKER_ID = str(uuid.uuid4())
HOST = "127.0.0.1"
PORT = 6000
MASTER_HOST = "127.0.0.1"  # Master atual (pode mudar dinamicamente)
MASTER_PORT = 5000         # Porta do master atual (pode mudar dinamicamente)
ORIGINAL_MASTER_UUID = None  # UUID do master original (quando emprestado)
active_tasks = 0
MAX_CONCURRENT_TASKS = 2  # Máximo de tarefas simultâneas
lock = threading.Lock()


def send_json(sock, obj):
    data = json.dumps(obj) + "\n"  # Adiciona delimitador \n
    sock.sendall(data.encode())


def request_task():
    """Worker solicita tarefa ao master (modelo pull) - Payload 2.1 ou 2.1b"""
    global ORIGINAL_MASTER_UUID
    
    try:
        with socket.socket() as s:
            s.settimeout(5)
            logging.debug(f"[DEBUG] Tentando conectar em {MASTER_HOST}:{MASTER_PORT} (ORIGINAL_MASTER_UUID={ORIGINAL_MASTER_UUID})")
            s.connect((MASTER_HOST, MASTER_PORT))
            
            # 2.1 | Pedir Tarefa (normal) ou 2.1b (emprestado)
            if ORIGINAL_MASTER_UUID:
                payload = {
                    "WORKER": "ALIVE",
                    "WORKER_UUID": WORKER_ID,
                    "SERVER_UUID": ORIGINAL_MASTER_UUID,
                    "port": PORT
                }
            else:
                payload = {
                    "WORKER": "ALIVE",
                    "WORKER_UUID": WORKER_ID,
                    "port": PORT
                }
            
            send_json(s, payload)
            response = recv_json(s)
            
            if response:
                # 2.2 | Entregar Tarefa
                if response.get("TASK") == "QUERY":
                    workload = response.get("USER")
                    logging.info(f"[TASK] Tarefa recebida: {workload}")
                    # Processar tarefa
                    threading.Thread(
                        target=process_task,
                        args=(workload,),
                        daemon=True
                    ).start()
                    
                # 2.3 | Sem Tarefa
                elif response.get("TASK") == "NO_TASK":
                    logging.info(f"[NO_TASK] Sem tarefas disponíveis no momento")
                    
    except Exception as e:
        logging.error(f"[REQUEST_TASK] Erro ao solicitar tarefa em {MASTER_HOST}:{MASTER_PORT}: {e}")


def recv_json(sock):
    """Recebe JSON com delimitador \n"""
    sock.settimeout(5)
    buf = b""
    try:
        while b"\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
    except socket.timeout:
        return None
    except Exception:
        return None
    
    if not buf:
        return None
    
    try:
        raw = buf.decode(errors="ignore")
        return json.loads(raw.split("\n")[0])
    except Exception:
        return None


def register_master():
    """Registro inicial e loop de solicitação de tarefas"""
    logging.info(f"[REGISTER] Conectando ao master {MASTER_HOST}:{MASTER_PORT}")
    
    # Loop de trabalho: solicita tarefas continuamente
    while True:
        with lock:
            current_tasks = active_tasks
        
        # Só solicita tarefa se não estiver no limite
        if current_tasks < MAX_CONCURRENT_TASKS:
            request_task()
        
        time.sleep(2)  # Aguarda antes de solicitar novamente


def report_result(task_status, task_query):
    """Reporta resultado da tarefa ao master - Payload 2.4"""
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect((MASTER_HOST, MASTER_PORT))
            
            # 2.4 | Reportar Status (Worker → Servidor)
            payload = {
                "STATUS": task_status,  # "OK" ou "NOK"
                "TASK": task_query,
                "WORKER_UUID": WORKER_ID
            }
            send_json(s, payload)
            
            # Aguarda 2.5 | Confirmar Status
            response = recv_json(s)
            if response and response.get("STATUS") == "ACK":
                logging.info(f"[STATUS] Master confirmou recebimento com ACK")
            
    except Exception as e:
        logging.error(f"[REPORT_RESULT] Erro ao reportar resultado: {e}")


def process_task(workload):
    """Processa uma tarefa"""
    global active_tasks
    
    with lock:
        if active_tasks >= MAX_CONCURRENT_TASKS:
            logging.warning(f"[WORKER] Já no limite de {MAX_CONCURRENT_TASKS} tarefas simultâneas")
            return
        active_tasks += 1

    logging.info(f"[TASK] Processando workload={workload}")
    
    try:
        # Simula processamento
        time.sleep(6)
        
        # Reporta sucesso
        report_result("OK", "QUERY")
        logging.info(f"[TASK] Finalizada com sucesso")
        
    except Exception as e:
        # Reporta falha
        report_result("NOK", "QUERY")
        logging.error(f"[TASK] Falhou: {e}")
        
    finally:
        with lock:
            active_tasks -= 1


def handle_redirect_command(msg):
    """Processa comando REDIRECT (2.6) - muda para novo master temporário"""
    global MASTER_HOST, MASTER_PORT, ORIGINAL_MASTER_UUID
    
    server_redirect = msg.get("SERVER_REDIRECT", {})
    new_host = server_redirect.get("ip")
    new_port = server_redirect.get("port")
    
    if new_host and new_port:
        # Salva o UUID do master original se ainda não tiver
        if not ORIGINAL_MASTER_UUID:
            # Tenta obter do master atual antes de mudar
            ORIGINAL_MASTER_UUID = f"{MASTER_HOST}:{MASTER_PORT}"
        
        logging.info(f"[REDIRECT] Redirecionando para master {new_host}:{new_port}")
        MASTER_HOST = new_host
        MASTER_PORT = int(new_port)
        
        # Aguarda um pouco antes de conectar ao novo master
        time.sleep(1)
        logging.info(f"[REDIRECT] Agora trabalhando para master temporário {MASTER_HOST}:{MASTER_PORT}")


def handle_return_command(msg):
    """Processa comando RETURN (2.7) - retorna para master original"""
    global MASTER_HOST, MASTER_PORT, ORIGINAL_MASTER_UUID
    
    server_return = msg.get("SERVER_RETURN", {})
    return_host = server_return.get("ip")
    return_port = server_return.get("port")
    
    if return_host and return_port:
        logging.info(f"[RETURN] Retornando para master original {return_host}:{return_port}")
        MASTER_HOST = return_host
        MASTER_PORT = int(return_port)
        ORIGINAL_MASTER_UUID = None  # Limpa flag de emprestado
        
        # Aguarda um pouco antes de se reconectar
        time.sleep(1)
        logging.info(f"[RETURN] Reconectado ao master original {MASTER_HOST}:{MASTER_PORT}")


def worker_server():
    """Servidor para receber comandos do master (REDIRECT e RETURN)"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(5)
    logging.info(f"[WORKER] Servidor ativo em {HOST}:{PORT}")
    
    while True:
        conn, addr = s.accept()
        try:
            # Lê mensagem com delimitador \n
            buf = b""
            while b"\n" not in buf:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buf += chunk
            
            if buf:
                raw = buf.decode(errors="ignore")
                msg = json.loads(raw.split("\n")[0])
                logging.info(f"[RECV] {addr} -> {msg}")
                
                # 2.6 | Comando: Redirecionar
                if msg.get("TASK") == "REDIRECT":
                    threading.Thread(
                        target=handle_redirect_command,
                        args=(msg,),
                        daemon=True
                    ).start()
                
                # 2.7 | Comando: Retornar
                elif msg.get("TASK") == "RETURN":
                    threading.Thread(
                        target=handle_return_command,
                        args=(msg,),
                        daemon=True
                    ).start()
                    
        except Exception as e:
            logging.error(f"[WORKER] Erro ao processar mensagem: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    # Inicia servidor para receber comandos do master
    threading.Thread(target=worker_server, daemon=True).start()
    time.sleep(1)
    
    # Inicia loop de solicitação de tarefas
    register_master()