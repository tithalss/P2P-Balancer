# P2P-Balancer
Projeto de balanceamento de carga dinâmico.
Protocolo Servidor -> Servidor
SERVIDOR - SERVIDOR
| 1 | Servidor A → Servidor B | `{"SERVER": "ALIVE", "TASK": "REQUEST"}` | Enviar um sinal de vida (heartbeat). |

| 2 | Servidor B → Servidor A | `{"SERVER": "ALIVE" ,"TASK":"RECIEVE"}` | Recebe um sinal de vida (heartbeat). |

| 3 | Servidor A → Servidor B | `{"TASK": "WORKER_REQUEST", "WORKERS_NEEDED": 5}` | Enviar um pedido de trabalhadores emprestado. |

| 4.1 | Servidor B → Servidor A | `{"TASK": "WORKER_RESPONSE", "STATUS": "ACK", "MASTER_UUID":"UUID",  "WORKERS": ["WORKER_UUID": ...] }` | Enviar uma resposta positiva de pedido de trabalhadores emprestado. |

| 4.2 | Servidor B → Servidor A | `{"TASK": "WORKER_RESPONSE", "STATUS": "NACK",  "WORKERS": [] }` | Enviar uma resposta negativa de pedido de trabalhadores emprestado. |

| 4.3 | Worker (Emprestado) → Servidor A | `{"WORKER": "ALIVE", "WORKER_UUID":"..."}` | Worker emprestado envia uma conexão para o servidor saturado. |

# P2P-Balancer: Sistema Distribuído com Balanceamento de Carga Dinâmico

Este projeto implementa um sistema distribuído autônomo baseado na arquitetura **Master-Worker Peer-to-Peer (P2P)**, com capacidade de balanceamento de carga horizontal através de empréstimo dinâmico de Workers.

---

## 1. Arquitetura e Objetivos

O sistema é composto por:
* **Nó Master:** Gerencia sua própria *Farm* de Workers, monitora a carga e negocia recursos com Masters vizinhos quando saturado.
* **Nó Worker:** Executa as tarefas delegadas pelo seu Master atual e deve ser capaz de se redirecionar para outro Master temporariamente.

O objetivo principal é demonstrar o **Protocolo de Conversa Concensual** para que Masters possam coordenar o empréstimo de Workers via comunicação TCP/Sockets, garantindo a autonomia e interoperabilidade entre as Farms.

---

## 2. Protocolo de Conversa Concensual: Empréstimo de Worker

O fluxo a seguir detalha o protocolo de negociação entre dois Masters (Master A - Saturado, Master B - Ocioso) e o Worker emprestado (Worker B1), incluindo a gestão de conexões TCP e o ciclo de vida da tarefa.

O diagrama foi construído usando a sintaxe Mermaid e é renderizado nativamente pelo GitHub.



## 📝 Diagrama da Sprint 3: Protocolo de Conversa Consensual

Este diagrama de sequência detalha a negociação e migração de Workers entre Masters, seguindo rigorosamente o Protocolo de Comunicação Padrão exigido (baseado em TASK e RESPONSE).

```mermaid
sequenceDiagram
    participant MA as Master A
    participant MB as Master B
    participant W as Worker B1

    Note over MA: MA detecta saturação (Limiar 10 tasks) e inicia negociação.

    MA->>MB: 1. Solicita Ajuda {TASK: "WORKER_REQUEST"}
    activate MB

    alt Master B disponível (Sucesso)
        MB-->>MA: 2. Resposta Positiva {RESPONSE: "AVAILABLE", WORKERS: ["uuid_B1"]}
        MB->>W: 3. Ordena Redirecionamento {TASK: "REDIRECT", MASTER_REDIRECT: "ID_A"}
        deactivate MB
        Note over W: Worker B1 desconecta do Master B.
        
        W->>MA: 4. Conecta/Registra {WORKER: "ALIVE", MASTER_ORIGIN: "ID_B"}
        activate MA
        Note over MA,W: Worker B1 agora trabalha para Master A.
        MA-->>W: 5. Confirma Conexão (Implícito)
        deactivate MA
        
    else Master B indisponível (Falha)
        MB-->>MA: 2. Resposta Negativa {RESPONSE: "UNAVAILABLE"}
        Note over MB: Conexão encerrada após resposta.
        MA->>MA: Tenta Outro Master Vizinho
    end
