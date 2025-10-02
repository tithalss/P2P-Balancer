# serverA.py
# Master A - heartbeat, simulação de carga, negociação de workers
import asyncio
import json
import time
import uuid

SERVER_ID = "A"
HOST = "127.0.0.1"
PORT = 5000

PEERS = {
    # peer_id: {ip, port, last_seen, status}
    "B": {"ip": "127.0.0.1", "port": 5001, "last_seen": 0, "status": "UNKNOWN"}
}

HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 15

THRESHOLD = 3              # limite de requisições para considerar saturado
REQUEST_BATCH = 2         # número máximo de workers pedidos por negociação
LOAD_RATE = 2             # gera 1 requisição a cada LOAD_RATE segundos (simulação)

# estado runtime
workers = {}  # worker_id -> {writer, status:'idle'|'busy'|'borrowed', home:{"IP","PORT","ID"}, borrowed_from}
borrowed_workers = set()
pending = asyncio.Queue()


# ---------- utilidades ----------
def send_json_sync(writer, obj):
    writer.write((json.dumps(obj) + "\n").encode())


# ---------- connection handler (workers or other masters) ----------
async def handle_connection(reader, writer):
    peer = writer.get_extra_info("peername")
    while True:
        data = await reader.readline()
        if not data:
            # connection closed
            # try to find which worker had this writer and cleanup
            to_remove = [wid for wid, w in workers.items() if w.get("writer") is writer]
            for wid in to_remove:
                print(f"[INFO] conexão perdida com worker {wid}")
                workers.pop(wid, None)
                borrowed_workers.discard(wid)
            break
        try:
            msg = json.loads(data.decode().strip())
        except Exception:
            continue

        # ---- heartbeat / server-server messages ----
        if msg.get("TASK") == "HEARTBEAT":
            sid = msg.get("SERVER_ID")
            if sid in PEERS:
                PEERS[sid]["last_seen"] = time.time()
                PEERS[sid]["status"] = "ALIVE"
            else:
                PEERS[sid] = {"ip": peer[0], "port": peer[1], "last_seen": time.time(), "status": "ALIVE"}
            # reply:
            resp = {"SERVER_ID": SERVER_ID, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
            send_json_sync(writer, resp)

        elif msg.get("TASK") == "WORKER_REQUEST":
            # outro master pedindo workers
            needed = int(msg.get("WORKERS_NEEDED", 0))
            requester = msg.get("REQUESTER_ADDRESS", {"IP": peer[0], "PORT": msg.get("REQUESTER_PORT")})
            # pick idle workers
            idle = [wid for wid, w in workers.items() if w.get("status") == "idle" and not w.get("borrowed_from")]
            offer = idle[:needed]
            if not offer:
                resp = {"TASK": "WORKER_RESPONSE", "STATUS": "NACK", "WORKERS": []}
                send_json_sync(writer, resp)
                print(f"[INFO] recusei pedido de workers (nenhum idle)")
            else:
                # prepare ACK with worker ids
                resp = {"TASK": "WORKER_RESPONSE", "STATUS": "ACK",
                        "WORKERS": [{"WORKER_ID": wid} for wid in offer]}
                send_json_sync(writer, resp)
                print(f"[INFO] ACK para WORKER_REQUEST, emprestando {len(offer)} workers")
                # instruct workers to redirect
                for wid in offer:
                    w = workers.get(wid)
                    if not w:
                        continue
                    try:
                        cmd = {
                            "TASK": "REDIRECT",
                            "TARGET_MASTER": {"IP": requester["IP"], "PORT": requester["PORT"]},
                            "HOME_MASTER": {"ID": SERVER_ID, "IP": HOST, "PORT": PORT}
                        }
                        send_json_sync(w["writer"], cmd)
                        w["status"] = "borrowed"
                        w["borrowed_from"] = SERVER_ID
                    except Exception as e:
                        print(f"[WARN] falha ao redirecionar worker {wid}: {e}")

        elif msg.get("TASK") == "WORKER_RESPONSE":
            # response to our previous request (if we're the requester)
            # We'll handle that where we sent the request (synchronous waiting)
            pass

        elif msg.get("TASK") == "notify_worker_returned":
            wid = msg.get("payload", {}).get("worker_id")
            if wid:
                print(f"[INFO] notificação: worker {wid} devolvido ao peer")

        # ---- worker messages ----
        elif msg.get("WORKER"):
            # worker connected / announced
            wid = msg.get("WORKER_ID") or msg.get("WORKER") or str(uuid.uuid4())[:8]
            workers[wid] = {
                "writer": writer,
                "status": "idle",
                "home": msg.get("HOME_MASTER") or {"ID": SERVER_ID, "IP": HOST, "PORT": PORT},
                "borrowed_from": msg.get("ORIGINAL_MASTER_ID"),  # present if this is a borrowed worker
                "last_seen": time.time()
            }
            print(f"[INFO] worker conectado: {wid} (home={workers[wid]['home']})")

            # if there are pending tasks, assign one immediately
            if not pending.empty() and workers[wid]["status"] == "idle":
                task = await pending.get()
                cmd = {"TASK": "QUERY", "USER": task}
                send_json_sync(writer, cmd)
                workers[wid]["status"] = "busy"

        elif msg.get("STATUS"):
            # worker finished a task or reported status
            wid = msg.get("WORKER_ID") or msg.get("WORKER")
            status = msg.get("STATUS")
            if wid and wid in workers:
                workers[wid]["last_seen"] = time.time()
                workers[wid]["status"] = "idle" if status == "OK" else "idle"
                print(f"[INFO] worker {wid} reportou {status}")

        elif msg.get("TASK") == "register_temporary_worker" or msg.get("type") == "register_temporary_worker":
            # when a borrowed worker registers here
            payload = msg.get("payload", {})
            wid = payload.get("worker_id") or msg.get("WORKER_ID")
            if wid:
                # mark as borrowed here
                workers[wid] = {"writer": writer, "status": "idle", "home": payload.get("original_master_address", {}), "borrowed_from": payload.get("original_master_address", {}).get("ID"), "last_seen": time.time()}
                borrowed_workers.add(wid)
                print(f"[INFO] worker temporário registrado: {wid} (de {workers[wid]['borrowed_from']})")

        elif msg.get("TASK") == "COMMAND_RELEASE" or msg.get("TASK") == "command_release":
            # Master instructed: a worker should return to original
            # This is usually sent to a worker, not to master; ignore if master receives
            pass

    # end loop
    try:
        writer.close()
        await writer.wait_closed()
    except:
        pass


# ---------- send WORKER_REQUEST to peer ----------
async def request_workers_from_peer(peer_id, n_required):
    info = PEERS.get(peer_id)
    if not info:
        return []
    try:
        reader, writer = await asyncio.open_connection(info["ip"], info["port"])
        payload = {
            "TASK": "WORKER_REQUEST",
            "WORKERS_NEEDED": n_required,
            "REQUESTER_ADDRESS": {"IP": HOST, "PORT": PORT},
            "REQUEST_ID": str(uuid.uuid4())
        }
        send_json_sync(writer, payload)
        # wait for response
        data = await asyncio.wait_for(reader.readline(), timeout=5)
        if not data:
            writer.close()
            await writer.wait_closed()
            return []
        resp = json.loads(data.decode().strip())
        await asyncio.sleep(0.1)
        writer.close()
        await writer.wait_closed()
        if resp.get("TASK") == "WORKER_RESPONSE" and resp.get("STATUS") == "ACK":
            workers_list = [w.get("WORKER_ID") for w in resp.get("WORKERS", [])]
            print(f"[INFO] peer {peer_id} ACK com workers {workers_list}")
            return workers_list
        else:
            print(f"[INFO] peer {peer_id} NACK ou sem workers")
            return []
    except Exception as e:
        print(f"[WARN] falha ao requisitar workers de {peer_id}: {e}")
        return []


# ---------- dispatcher: envia tarefas para workers quando disponíveis ----------
async def dispatcher():
    while True:
        if pending.empty():
            await asyncio.sleep(0.5)
            continue
        # find an idle worker
        idle = None
        for wid, w in workers.items():
            if w.get("status") == "idle":
                idle = wid
                break
        if idle:
            task = await pending.get()
            w = workers[idle]
            try:
                send_json_sync(w["writer"], {"TASK": "QUERY", "USER": task})
                w["status"] = "busy"
                print(f"[INFO] tarefa '{task}' enviada para worker {idle}")
            except Exception as e:
                print(f"[WARN] falha ao enviar para worker {idle}: {e}")
                # put back the task
                await pending.put(task)
        else:
            await asyncio.sleep(0.5)


# ---------- simulate clients (gera carga) ----------
async def simulate_load():
    i = 0
    while True:
        i += 1
        task = f"user_{i}"
        await pending.put(task)
        print(f"[SIM] nova requisição: {task} | backlog={pending.qsize()}")
        await asyncio.sleep(LOAD_RATE)


# ---------- monitor saturation ----------
async def saturation_monitor():
    while True:
        backlog = pending.qsize()
        if backlog > THRESHOLD:
            needed = min(backlog - THRESHOLD, REQUEST_BATCH)
            print(f"[ALERT] saturado! backlog={backlog} > threshold={THRESHOLD}. solicitando {needed} workers")
            # try each peer until some workers are obtained
            for peer_id in list(PEERS.keys()):
                got = await request_workers_from_peer(peer_id, needed)
                if got:
                    # wait a bit for redirected workers to register (they are redirected by peer)
                    wait_for = 5
                    t0 = time.time()
                    while time.time() - t0 < wait_for:
                        # check if new workers registered here
                        for wid in got:
                            if wid in workers:
                                print(f"[INFO] worker {wid} já registrado neste master")
                        await asyncio.sleep(0.5)
                    break
        await asyncio.sleep(2)


# ---------- heartbeat sender ----------
async def send_heartbeat_forever():
    while True:
        for sid, info in list(PEERS.items()):
            try:
                r, w = await asyncio.open_connection(info["ip"], info["port"])
                send_json_sync(w, {"SERVER_ID": SERVER_ID, "TASK": "HEARTBEAT"})
                try:
                    data = await asyncio.wait_for(r.readline(), timeout=3)
                    if data:
                        resp = json.loads(data.decode().strip())
                        if resp.get("RESPONSE") == "ALIVE":
                            info["last_seen"] = time.time()
                            info["status"] = "ALIVE"
                except asyncio.TimeoutError:
                    pass
                w.close()
                await w.wait_closed()
            except Exception:
                last = info.get("last_seen", 0)
                if last and time.time() - last > HEARTBEAT_TIMEOUT:
                    if info.get("status") != "DEAD":
                        info["status"] = "DEAD"
                        print(f"[WARN] peer {sid} marcado DEAD (heartbeat)")
        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def main():
    server = await asyncio.start_server(handle_connection, HOST, PORT)
    print(f"[START] Master {SERVER_ID} rodando em {HOST}:{PORT}")
    await asyncio.gather(
        server.serve_forever(),
        send_heartbeat_forever(),
        simulate_load(),
        dispatcher(),
        saturation_monitor()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP] encerrando master")
