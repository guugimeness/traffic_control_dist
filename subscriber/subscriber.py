"""
Subscriber (semáforo inteligente) com suporte ao Módulo 4:

- eleição de líder pelo algoritmo de Bully;
- verificação de quórum e prevenção de split-brain;
- heartbeat do líder;
- detecção de falhas dos publishers;
- fila durável para dados de tráfego;
- checkpoint local atômico;
- restauração após reinício abrupto;
- buffer causal persistente;
- deduplicação de mensagens;
- recuperação transparente após `docker kill`.

A imagem Docker deve conter:

    /app/
    ├── subscriber.py
    └── recover/
        ├── __init__.py
        ├── checkpoint.py
        └── failure_detector.py
"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
from collections import deque
from typing import Any

import pika

from recover import (
    AtomicCheckpointStore,
    CheckpointCorruptedError,
    CheckpointError,
    FailureDetector,
    NodeStatus,
)


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

NODE_ID = int(os.getenv("NODE_ID", "1"))
TOTAL_NODES = int(os.getenv("TOTAL_NODES", "4"))

HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "5"))
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "25"))
ELECTION_TIMEOUT = float(os.getenv("ELECTION_TIMEOUT", "20"))
QUORUM_TIMEOUT = float(os.getenv("QUORUM_TIMEOUT", "20"))

PUBLISHER_HEARTBEAT_TIMEOUT = float(
    os.getenv("PUBLISHER_HEARTBEAT_TIMEOUT", "15")
)
PUBLISHER_SUSPICION_TIMEOUT = float(
    os.getenv("PUBLISHER_SUSPICION_TIMEOUT", "9")
)

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/data")
CHECKPOINT_INTERVAL = float(os.getenv("CHECKPOINT_INTERVAL", "5"))
MAX_PROCESSED_IDS = int(os.getenv("MAX_PROCESSED_IDS", "10000"))
RECONNECT_DELAY = float(os.getenv("RECONNECT_DELAY", "5"))

TRAFFIC_EXCHANGE = "traffic_data"
ELECTION_EXCHANGE = "election"
NODE_HEARTBEAT_EXCHANGE = "node_heartbeats"

FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"
SAFE_MODE = "SAFE_MODE"


class SmartTrafficLight:
    """Semáforo inteligente com eleição, persistência e recuperação."""

    CHECKPOINT_VERSION = 1

    def __init__(self) -> None:
        self.node_id = NODE_ID
        self.total_nodes = TOTAL_NODES
        self.quorum = (self.total_nodes // 2) + 1

        self.stop_event = threading.Event()

        # Estado de eleição.
        self.state = FOLLOWER
        self.leader_id: int | None = None
        self.state_lock = threading.RLock()

        # Relógio vetorial local.
        self.local_vc: dict[str, int] = {}
        self.vc_lock = threading.RLock()

        # Buffer causal persistente.
        self.causal_buffer: list[dict[str, Any]] = []
        self.buffer_lock = threading.RLock()

        # Deduplicação persistente.
        self.processed_ids: set[str] = set()
        self.processed_order: deque[str] = deque()
        self.processed_lock = threading.RLock()

        # Heartbeat e eleição.
        self.last_heartbeat = time.time()
        self.hb_lock = threading.RLock()

        self.election_in_progress = False
        self.election_lock = threading.RLock()

        self.ok_received = False
        self.ok_lock = threading.RLock()

        self.quorum_responders: set[int] = set()
        self.quorum_ack_lock = threading.RLock()

        self.heartbeat_sender_active = False
        self.heartbeat_sender_lock = threading.RLock()

        # Estatísticas persistentes.
        self.total_processed = 0
        self.last_processed_message_id: str | None = None
        self.stats_lock = threading.RLock()

        # Conexões AMQP.
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.adapters.blocking_connection.BlockingChannel | None = None

        self.pub_connection: pika.BlockingConnection | None = None
        self.pub_channel: pika.adapters.blocking_connection.BlockingChannel | None = None
        self.pub_lock = threading.RLock()

        self.traffic_queue = f"traffic_sub_{self.node_id}"
        self.direct_queue = f"sub_{self.node_id}"
        self.election_queue: str | None = None
        self.publisher_heartbeat_queue: str | None = None

        checkpoint_path = os.path.join(
            CHECKPOINT_DIR,
            f"subscriber_{self.node_id}.json",
        )
        self.checkpoint_store = AtomicCheckpointStore(checkpoint_path)

        # Detector dos publishers. O heartbeat de liderança continua integrado
        # ao algoritmo de eleição, enquanto este detector acompanha os sensores.
        self.publisher_detector = FailureDetector(
            heartbeat_timeout=PUBLISHER_HEARTBEAT_TIMEOUT,
            suspicion_timeout=PUBLISHER_SUSPICION_TIMEOUT,
            check_interval=1.0,
            on_status_change=self._on_publisher_status_change,
        )

        self._restore_checkpoint()

        print(
            f"[INIT] Semáforo {self.node_id} inicializado | "
            f"Nós={self.total_nodes} | Quórum={self.quorum} | "
            f"Estado restaurado={self.state}"
        )

    # ------------------------------------------------------------------
    # Checkpoint e recuperação
    # ------------------------------------------------------------------

    def _build_checkpoint_state(self) -> dict[str, Any]:
        """
        Cria uma fotografia consistente do estado.

        Os locks são adquiridos sempre na mesma ordem para evitar deadlocks.
        """
        with self.state_lock:
            state = self.state
            leader_id = self.leader_id

        with self.vc_lock:
            local_vc = self.local_vc.copy()

        with self.buffer_lock:
            causal_buffer = [dict(msg) for msg in self.causal_buffer]

        with self.processed_lock:
            processed_order = list(self.processed_order)

        with self.stats_lock:
            total_processed = self.total_processed
            last_processed_message_id = self.last_processed_message_id

        return {
            "checkpoint_version": self.CHECKPOINT_VERSION,
            "node_id": self.node_id,
            "state": state,
            "leader_id": leader_id,
            "local_vc": local_vc,
            "causal_buffer": causal_buffer,
            "processed_order": processed_order,
            "total_processed": total_processed,
            "last_processed_message_id": last_processed_message_id,
            "publisher_detector": self.publisher_detector.snapshot(),
        }

    def save_checkpoint(self) -> None:
        try:
            self.checkpoint_store.save(self._build_checkpoint_state())
        except CheckpointError as error:
            print(f"[CHECKPOINT-ERRO] Nó {self.node_id}: {error}")

    def _restore_checkpoint(self) -> None:
        try:
            checkpoint = self.checkpoint_store.load(default=None)
        except CheckpointCorruptedError as error:
            print(
                f"[RECOVERY-ERRO] Nó {self.node_id}: "
                f"checkpoint irrecuperável: {error}"
            )
            print("[RECOVERY] O nó iniciará com estado novo.")
            return
        except CheckpointError as error:
            print(f"[RECOVERY-ERRO] Nó {self.node_id}: {error}")
            return

        if checkpoint is None:
            print(f"[RECOVERY] Nó {self.node_id} sem checkpoint anterior.")
            return

        if int(checkpoint.get("node_id", -1)) != self.node_id:
            print(
                "[RECOVERY-ERRO] Checkpoint pertence a outro nó. "
                "O arquivo será ignorado."
            )
            return

        restored_state = str(checkpoint.get("state", FOLLOWER))

        # Um processo reiniciado nunca assume automaticamente que ainda é
        # líder. Ele precisa confirmar a situação pela rede ou por nova eleição.
        if restored_state in (LEADER, CANDIDATE):
            restored_state = FOLLOWER

        restored_leader = checkpoint.get("leader_id")
        if restored_leader is not None:
            restored_leader = int(restored_leader)

        restored_vc = checkpoint.get("local_vc", {})
        if not isinstance(restored_vc, dict):
            restored_vc = {}

        restored_buffer = checkpoint.get("causal_buffer", [])
        if not isinstance(restored_buffer, list):
            restored_buffer = []

        restored_order = checkpoint.get("processed_order", [])
        if not isinstance(restored_order, list):
            restored_order = []

        restored_order = [str(value) for value in restored_order]
        restored_order = restored_order[-MAX_PROCESSED_IDS:]

        with self.state_lock:
            self.state = restored_state
            self.leader_id = restored_leader

        with self.vc_lock:
            self.local_vc = {
                str(node): int(value)
                for node, value in restored_vc.items()
            }

        with self.buffer_lock:
            self.causal_buffer = [
                msg for msg in restored_buffer if isinstance(msg, dict)
            ]

        with self.processed_lock:
            self.processed_order = deque(restored_order)
            self.processed_ids = set(restored_order)

        with self.stats_lock:
            self.total_processed = int(
                checkpoint.get("total_processed", 0)
            )
            last_id = checkpoint.get("last_processed_message_id")
            self.last_processed_message_id = (
                str(last_id) if last_id is not None else None
            )

        detector_snapshot = checkpoint.get("publisher_detector", {})
        if isinstance(detector_snapshot, dict):
            self.publisher_detector.restore_snapshot(detector_snapshot)

        # O tempo de heartbeat não deve ser restaurado como confiável.
        with self.hb_lock:
            self.last_heartbeat = time.time()

        print(
            f"[RECOVERY] Nó {self.node_id} restaurado | "
            f"Estado={self.state} | Líder anterior={self.leader_id} | "
            f"VC={self.local_vc} | Buffer={len(self.causal_buffer)} | "
            f"Processadas={self.total_processed}"
        )

    def _checkpoint_worker(self) -> None:
        while not self.stop_event.wait(CHECKPOINT_INTERVAL):
            self.save_checkpoint()

    # ------------------------------------------------------------------
    # Conexão com RabbitMQ
    # ------------------------------------------------------------------

    @staticmethod
    def _build_params() -> pika.ConnectionParameters:
        credentials = pika.PlainCredentials(
            RABBITMQ_USER,
            RABBITMQ_PASS,
        )

        return pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=120,
            retry_delay=5,
            connection_attempts=10,
            socket_timeout=30,
        )

    def connect_rabbitmq(self) -> None:
        self._close_connections()

        while not self.stop_event.is_set():
            try:
                print(
                    f"[REDE] Nó {self.node_id} tentando conectar "
                    f"ao Broker {RABBITMQ_HOST}..."
                )

                params = self._build_params()

                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()

                self.pub_connection = pika.BlockingConnection(params)
                self.pub_channel = self.pub_connection.channel()

                self._declare_topology(self.channel)
                self._declare_publish_topology(self.pub_channel)

                self.channel.basic_qos(prefetch_count=20)
                self.pub_channel.confirm_delivery()

                print(
                    f"[REDE] Nó {self.node_id} conectado ao RabbitMQ."
                )
                return

            except Exception as error:
                print(
                    f"[FALHA-REDE] Nó {self.node_id}: "
                    f"{type(error).__name__}: {error}. "
                    f"Retentando em {RECONNECT_DELAY:.1f}s."
                )
                self._close_connections()
                self.stop_event.wait(RECONNECT_DELAY)

        raise RuntimeError("Encerramento solicitado durante a conexão.")

    def _declare_topology(
        self,
        channel: pika.adapters.blocking_connection.BlockingChannel,
    ) -> None:
        # Dados de tráfego precisam sobreviver à queda do subscriber.
        channel.exchange_declare(
            exchange=TRAFFIC_EXCHANGE,
            exchange_type="fanout",
            durable=True,
        )

        # Mensagens de eleição são transitórias.
        channel.exchange_declare(
            exchange=ELECTION_EXCHANGE,
            exchange_type="fanout",
            durable=False,
        )

        # Heartbeats também são transitórios.
        channel.exchange_declare(
            exchange=NODE_HEARTBEAT_EXCHANGE,
            exchange_type="fanout",
            durable=True,
        )

        # Fila durável de tráfego por subscriber.
        channel.queue_declare(
            queue=self.traffic_queue,
            durable=True,
        )
        channel.queue_bind(
            exchange=TRAFFIC_EXCHANGE,
            queue=self.traffic_queue,
        )

        # Caixa postal durável para mensagens diretas do Bully.
        channel.queue_declare(
            queue=self.direct_queue,
            durable=True,
        )

        # Broadcasts de eleição antigos não devem ser recuperados.
        result = channel.queue_declare(
            queue="",
            exclusive=True,
            auto_delete=True,
        )
        self.election_queue = result.method.queue
        channel.queue_bind(
            exchange=ELECTION_EXCHANGE,
            queue=self.election_queue,
        )

        # Fila transitória de heartbeat dos publishers.
        hb_result = channel.queue_declare(
            queue="",
            exclusive=True,
            auto_delete=True,
        )
        self.publisher_heartbeat_queue = hb_result.method.queue
        channel.queue_bind(
            exchange=NODE_HEARTBEAT_EXCHANGE,
            queue=self.publisher_heartbeat_queue,
        )

    def _declare_publish_topology(
        self,
        channel: pika.adapters.blocking_connection.BlockingChannel,
    ) -> None:
        channel.exchange_declare(
            exchange=TRAFFIC_EXCHANGE,
            exchange_type="fanout",
            durable=True,
        )
        channel.exchange_declare(
            exchange=ELECTION_EXCHANGE,
            exchange_type="fanout",
            durable=False,
        )
        channel.exchange_declare(
            exchange=NODE_HEARTBEAT_EXCHANGE,
            exchange_type="fanout",
            durable=True,
        )

        # As filas diretas precisam existir antes de qualquer eleição.
        for node_id in range(1, self.total_nodes + 1):
            channel.queue_declare(
                queue=f"sub_{node_id}",
                durable=True,
            )

    def _ensure_publish_channel_locked(self) -> None:
        if (
            self.pub_connection is not None
            and self.pub_connection.is_open
            and self.pub_channel is not None
            and self.pub_channel.is_open
        ):
            return

        self.pub_connection = pika.BlockingConnection(self._build_params())
        self.pub_channel = self.pub_connection.channel()
        self._declare_publish_topology(self.pub_channel)
        self.pub_channel.confirm_delivery()

    def _close_connections(self) -> None:
        for channel in (self.channel, self.pub_channel):
            try:
                if channel is not None and channel.is_open:
                    channel.close()
            except Exception:
                pass

        for connection in (self.connection, self.pub_connection):
            try:
                if connection is not None and connection.is_open:
                    connection.close()
            except Exception:
                pass

        self.channel = None
        self.connection = None
        self.pub_channel = None
        self.pub_connection = None

    # ------------------------------------------------------------------
    # Publicação de controle
    # ------------------------------------------------------------------

    def _publish_control(
        self,
        routing_key: str,
        payload: dict[str, Any],
    ) -> bool:
        with self.pub_lock:
            try:
                self._ensure_publish_channel_locked()

                published = self.pub_channel.basic_publish(
                    exchange="",
                    routing_key=routing_key,
                    body=json.dumps(payload),
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=2,
                        type=str(payload.get("type", "CONTROL")),
                        app_id=f"subscriber_{self.node_id}",
                    ),
                )

                return published is not False

            except Exception as error:
                print(
                    f"[PUB-ERRO] Nó {self.node_id}: "
                    f"mensagem direta não enviada: {error}"
                )
                self._reset_publish_connection_locked()
                return False

    def _broadcast_election(self, payload: dict[str, Any]) -> bool:
        with self.pub_lock:
            try:
                self._ensure_publish_channel_locked()

                published = self.pub_channel.basic_publish(
                    exchange=ELECTION_EXCHANGE,
                    routing_key="",
                    body=json.dumps(payload),
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=1,
                        type=str(payload.get("type", "ELECTION")),
                        app_id=f"subscriber_{self.node_id}",
                    ),
                )

                return published is not False

            except Exception as error:
                print(
                    f"[PUB-ERRO] Nó {self.node_id}: "
                    f"broadcast não enviado: {error}"
                )
                self._reset_publish_connection_locked()
                return False

    def _reset_publish_connection_locked(self) -> None:
        try:
            if self.pub_connection is not None and self.pub_connection.is_open:
                self.pub_connection.close()
        except Exception:
            pass

        self.pub_channel = None
        self.pub_connection = None

    # ------------------------------------------------------------------
    # Ordenação causal e deduplicação
    # ------------------------------------------------------------------

    @staticmethod
    def _message_id(msg: dict[str, Any]) -> str:
        explicit_id = msg.get("message_id")
        if explicit_id is not None:
            return str(explicit_id)

        sensor_id = str(msg.get("sensor_id", "unknown"))
        vc = msg.get("vector_clock", {})
        sequence = vc.get(sensor_id, "unknown") if isinstance(vc, dict) else "unknown"
        return f"{sensor_id}:{sequence}"

    def _already_processed(self, message_id: str) -> bool:
        with self.processed_lock:
            return message_id in self.processed_ids

    def _buffer_contains(self, message_id: str) -> bool:
        with self.buffer_lock:
            return any(
                self._message_id(message) == message_id
                for message in self.causal_buffer
            )

    def _remember_processed(self, message_id: str) -> None:
        with self.processed_lock:
            if message_id in self.processed_ids:
                return

            self.processed_ids.add(message_id)
            self.processed_order.append(message_id)

            while len(self.processed_order) > MAX_PROCESSED_IDS:
                oldest = self.processed_order.popleft()
                self.processed_ids.discard(oldest)

    def _merge_vc(self, remote_vc: dict[str, Any]) -> None:
        with self.vc_lock:
            for key, value in remote_vc.items():
                node = str(key)
                clock = int(value)
                self.local_vc[node] = max(
                    self.local_vc.get(node, 0),
                    clock,
                )

    def _is_causally_ready(
        self,
        msg_vc: dict[str, Any],
        sender_id: str,
    ) -> bool:
        with self.vc_lock:
            for raw_node, raw_clock in msg_vc.items():
                node = str(raw_node)
                clock = int(raw_clock)

                if node == sender_id:
                    if self.local_vc.get(node, 0) < clock - 1:
                        return False
                elif self.local_vc.get(node, 0) < clock:
                    return False

        return True

    def _buffer_traffic_message(self, msg: dict[str, Any]) -> None:
        message_id = self._message_id(msg)

        if self._already_processed(message_id):
            print(
                f"[DUPLICADA] Nó {self.node_id}: "
                f"{message_id} já havia sido processada."
            )
            return

        if self._buffer_contains(message_id):
            print(
                f"[DUPLICADA] Nó {self.node_id}: "
                f"{message_id} já está no buffer causal."
            )
            return

        with self.buffer_lock:
            self.causal_buffer.append(msg)
            buffer_size = len(self.causal_buffer)

        # Persistimos antes de confirmar a mensagem ao RabbitMQ.
        self.save_checkpoint()

        print(
            f"[CAUSAL-BUFFER] ID={message_id} | "
            f"Sensor={msg.get('sensor_id')} | "
            f"VC={msg.get('vector_clock')} | "
            f"Buffer={buffer_size}"
        )

        self._try_flush_buffer()

    def _try_flush_buffer(self) -> None:
        while not self.stop_event.is_set():
            ready_message: dict[str, Any] | None = None

            with self.buffer_lock:
                for message in self.causal_buffer:
                    sender = str(message.get("sensor_id"))
                    vc = message.get("vector_clock", {})

                    if (
                        isinstance(vc, dict)
                        and self._is_causally_ready(vc, sender)
                    ):
                        ready_message = message
                        break

            if ready_message is None:
                return

            self._commit_processed_message(ready_message)

    def _commit_processed_message(self, msg: dict[str, Any]) -> None:
        """
        Atualiza o histórico local antes do ACK definitivo já realizado no
        callback. A deduplicação torna uma eventual reentrega segura.
        """
        message_id = self._message_id(msg)
        vc = msg.get("vector_clock", {})

        if self._already_processed(message_id):
            with self.buffer_lock:
                self.causal_buffer = [
                    item
                    for item in self.causal_buffer
                    if self._message_id(item) != message_id
                ]
            self.save_checkpoint()
            return

        self._process_traffic(msg)

        if isinstance(vc, dict):
            self._merge_vc(vc)

        self._remember_processed(message_id)

        with self.buffer_lock:
            self.causal_buffer = [
                item
                for item in self.causal_buffer
                if self._message_id(item) != message_id
            ]

        with self.stats_lock:
            self.total_processed += 1
            self.last_processed_message_id = message_id

        self.save_checkpoint()

    def _process_traffic(self, msg: dict[str, Any]) -> None:
        with self.state_lock:
            current_state = self.state
            leader = self.leader_id

        if current_state == SAFE_MODE:
            role = "SAFE_MODE"
        elif current_state == LEADER:
            role = "LÍDER"
        elif current_state == CANDIDATE:
            role = f"CANDIDATO (líder={leader})"
        else:
            role = f"FOLLOWER (líder={leader})"

        print(
            f"[PROCESSADO] [{role}] "
            f"ID={self._message_id(msg)} | "
            f"Sensor={msg.get('sensor_id')} | "
            f"Fluxo={msg.get('fluxo_veiculos')} veíc/min | "
            f"VC={msg.get('vector_clock')} | "
            f"Tempo={float(msg.get('physical_timestamp', 0.0)):.6f}"
        )

    # ------------------------------------------------------------------
    # Eleição de líder e quórum
    # ------------------------------------------------------------------

    def _check_quorum(self) -> bool:
        print(
            f"[QUÓRUM] Nó {self.node_id} verificando quórum "
            f"({self.quorum}/{self.total_nodes})."
        )

        with self.quorum_ack_lock:
            self.quorum_responders = {self.node_id}

        self._broadcast_election(
            {
                "type": "QUORUM_CHECK",
                "sender_id": self.node_id,
                "timestamp": time.time(),
            }
        )

        self.stop_event.wait(QUORUM_TIMEOUT)

        with self.quorum_ack_lock:
            alive = len(self.quorum_responders)

        print(
            f"[QUÓRUM] {alive}/{self.total_nodes} nós responderam."
        )

        if alive >= self.quorum:
            return True

        self._enter_safe_mode()
        return False

    def start_election(self) -> None:
        with self.election_lock:
            if self.election_in_progress or self.stop_event.is_set():
                return
            self.election_in_progress = True

        try:
            with self.state_lock:
                self.state = CANDIDATE
            self.save_checkpoint()

            print(f"[ELEIÇÃO] Nó {self.node_id} iniciou eleição.")

            if not self._check_quorum():
                return

            higher_nodes = [
                node_id
                for node_id in range(1, self.total_nodes + 1)
                if node_id > self.node_id
            ]

            if not higher_nodes:
                self._declare_leader()
                return

            with self.ok_lock:
                self.ok_received = False

            for node_id in higher_nodes:
                print(
                    f"[ELEIÇÃO] Enviando ELECTION para nó {node_id}."
                )
                self._publish_control(
                    f"sub_{node_id}",
                    {
                        "type": "ELECTION",
                        "sender_id": self.node_id,
                        "timestamp": time.time(),
                    },
                )

            self.stop_event.wait(ELECTION_TIMEOUT)

            with self.ok_lock:
                received = self.ok_received

            if not received:
                self._declare_leader()
            else:
                print(
                    "[ELEIÇÃO] OK recebido. "
                    "Aguardando anúncio do coordenador."
                )
                with self.state_lock:
                    self.state = FOLLOWER
                with self.hb_lock:
                    self.last_heartbeat = time.time() + QUORUM_TIMEOUT
                self.save_checkpoint()

        finally:
            with self.election_lock:
                self.election_in_progress = False

    def _declare_leader(self) -> None:
        with self.state_lock:
            self.state = LEADER
            self.leader_id = self.node_id

        with self.hb_lock:
            self.last_heartbeat = time.time()

        self.save_checkpoint()

        print(
            f"[LÍDER] Nó {self.node_id} declarado líder."
        )

        self._broadcast_election(
            {
                "type": "COORDINATOR",
                "sender_id": self.node_id,
                "timestamp": time.time(),
            }
        )

        with self.heartbeat_sender_lock:
            if not self.heartbeat_sender_active:
                self.heartbeat_sender_active = True
                threading.Thread(
                    target=self._heartbeat_sender,
                    name=f"leader-heartbeat-{self.node_id}",
                    daemon=True,
                ).start()

    def _enter_safe_mode(self) -> None:
        with self.state_lock:
            self.state = SAFE_MODE
            self.leader_id = None

        self.save_checkpoint()

        print(
            f"[SAFE-MODE] Nó {self.node_id}: quórum insuficiente. "
            "Comandos de atuação ficam congelados."
        )

    # ------------------------------------------------------------------
    # Handlers de eleição
    # ------------------------------------------------------------------

    def _handle_election_msg(self, msg: dict[str, Any]) -> None:
        message_type = msg.get("type")
        sender_id = int(msg.get("sender_id", -1))

        if sender_id == self.node_id:
            return

        if message_type == "QUORUM_CHECK":
            self._publish_control(
                f"sub_{sender_id}",
                {
                    "type": "QUORUM_ACK",
                    "sender_id": self.node_id,
                    "timestamp": time.time(),
                },
            )

        elif message_type == "COORDINATOR":
            with self.state_lock:
                if self.state in (LEADER, CANDIDATE) and sender_id < self.node_id:
                    return
                self.state = FOLLOWER
                self.leader_id = sender_id

            with self.hb_lock:
                self.last_heartbeat = time.time()

            with self.election_lock:
                self.election_in_progress = False

            self.save_checkpoint()

            print(
                f"[LÍDER] Nó {sender_id} reconhecido como coordenador."
            )

        elif message_type == "HEARTBEAT":
            changed = False

            with self.hb_lock:
                self.last_heartbeat = time.time()

            with self.state_lock:
                if self.state in (LEADER, CANDIDATE) and sender_id < self.node_id:
                    return

                if (
                    self.state == SAFE_MODE
                    or self.leader_id != sender_id
                ):
                    self.state = FOLLOWER
                    self.leader_id = sender_id
                    changed = True

            if changed:
                self.save_checkpoint()
                print(
                    f"[HEARTBEAT] Líder {sender_id} reconhecido. "
                    "Nó voltou ao estado FOLLOWER."
                )

    def _handle_direct_msg(self, msg: dict[str, Any]) -> None:
        message_type = msg.get("type")
        sender_id = int(msg.get("sender_id", -1))

        if message_type == "QUORUM_ACK":
            with self.quorum_ack_lock:
                self.quorum_responders.add(sender_id)
                total = len(self.quorum_responders)

            print(
                f"[QUÓRUM] ACK do nó {sender_id}. Total={total}."
            )

        elif message_type == "ELECTION":
            print(
                f"[ELEIÇÃO] Solicitação recebida do nó {sender_id}."
            )

            self._publish_control(
                f"sub_{sender_id}",
                {
                    "type": "OK",
                    "sender_id": self.node_id,
                    "timestamp": time.time(),
                },
            )

            threading.Thread(
                target=self.start_election,
                name=f"counter-election-{self.node_id}",
                daemon=True,
            ).start()

        elif message_type == "OK":
            with self.ok_lock:
                self.ok_received = True

            print(
                f"[ELEIÇÃO] OK recebido do nó {sender_id}."
            )

    # ------------------------------------------------------------------
    # Heartbeats
    # ------------------------------------------------------------------

    def _heartbeat_sender(self) -> None:
        print(
            f"[HEARTBEAT] Nó {self.node_id} enviando heartbeat "
            f"a cada {HEARTBEAT_INTERVAL:.1f}s."
        )

        try:
            while not self.stop_event.is_set():
                with self.state_lock:
                    if self.state != LEADER:
                        return

                self._broadcast_election(
                    {
                        "type": "HEARTBEAT",
                        "sender_id": self.node_id,
                        "timestamp": time.time(),
                    }
                )

                self.stop_event.wait(HEARTBEAT_INTERVAL)

        finally:
            with self.heartbeat_sender_lock:
                self.heartbeat_sender_active = False

    def _heartbeat_monitor(self) -> None:
        print(
            f"[HEARTBEAT] Nó {self.node_id} monitorando líder "
            f"(timeout={HEARTBEAT_TIMEOUT:.1f}s)."
        )

        check_interval = max(1.0, HEARTBEAT_TIMEOUT / 3.0)

        while not self.stop_event.wait(check_interval):
            with self.state_lock:
                current_state = self.state
                current_leader = self.leader_id

            if current_state in (LEADER, CANDIDATE):
                continue

            with self.hb_lock:
                elapsed = time.time() - self.last_heartbeat

            if elapsed <= HEARTBEAT_TIMEOUT:
                continue

            print(
                f"[HEARTBEAT] Timeout de {elapsed:.1f}s. "
                f"Líder {current_leader} presumido falho."
            )

            with self.election_lock:
                already_running = self.election_in_progress

            if not already_running:
                threading.Thread(
                    target=self.start_election,
                    name=f"timeout-election-{self.node_id}",
                    daemon=True,
                ).start()

    def _on_publisher_status_change(
        self,
        publisher_id: Any,
        status: NodeStatus,
    ) -> None:
        if status == NodeStatus.SUSPECTED:
            print(
                f"[DETECTOR] Publisher {publisher_id} está SUSPEITO."
            )
        elif status == NodeStatus.FAILED:
            print(
                f"[DETECTOR] Publisher {publisher_id} foi considerado FALHO."
            )
        elif status == NodeStatus.RECOVERED:
            print(
                f"[DETECTOR] Publisher {publisher_id} se RECUPEROU."
            )

    # ------------------------------------------------------------------
    # Callbacks RabbitMQ
    # ------------------------------------------------------------------

    def _on_traffic_message(
        self,
        channel: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        del properties

        try:
            msg = json.loads(body)

            if not isinstance(msg, dict):
                raise ValueError("A mensagem de tráfego não é um objeto JSON.")

            self._buffer_traffic_message(msg)

            # O buffer e o histórico já foram persistidos antes do ACK.
            channel.basic_ack(method.delivery_tag)

        except (json.JSONDecodeError, ValueError, KeyError, TypeError) as error:
            print(
                f"[TRÁFEGO-ERRO] Mensagem inválida descartada: {error}"
            )
            channel.basic_nack(
                method.delivery_tag,
                requeue=False,
            )

        except Exception as error:
            print(
                f"[TRÁFEGO-ERRO] Falha transitória: {error}"
            )
            channel.basic_nack(
                method.delivery_tag,
                requeue=True,
            )

    def _on_election_message(
        self,
        channel: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        del properties

        try:
            msg = json.loads(body)
            self._handle_election_msg(msg)
            channel.basic_ack(method.delivery_tag)
        except Exception as error:
            print(f"[ELEIÇÃO-ERRO] {error}")
            channel.basic_nack(
                method.delivery_tag,
                requeue=False,
            )

    def _on_direct_message(
        self,
        channel: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        del properties

        try:
            msg = json.loads(body)
            self._handle_direct_msg(msg)
            channel.basic_ack(method.delivery_tag)
        except Exception as error:
            print(f"[CONTROLE-ERRO] {error}")
            channel.basic_nack(
                method.delivery_tag,
                requeue=False,
            )

    def _on_publisher_heartbeat(
        self,
        channel: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        del properties

        try:
            msg = json.loads(body)

            if (
                msg.get("type") == "HEARTBEAT"
                and msg.get("node_kind") == "publisher"
            ):
                publisher_id = str(msg["node_id"])
                self.publisher_detector.record_heartbeat(publisher_id)

            channel.basic_ack(method.delivery_tag)

        except Exception as error:
            print(f"[PUBLISHER-HB-ERRO] {error}")
            channel.basic_nack(
                method.delivery_tag,
                requeue=False,
            )

    # ------------------------------------------------------------------
    # Execução e encerramento
    # ------------------------------------------------------------------

    def _initial_election(self) -> None:
        startup_wait = 10 + self.node_id

        print(
            f"[INIT] Aguardando {startup_wait}s para eleição inicial."
        )

        if self.stop_event.wait(startup_wait):
            return

        with self.state_lock:
            leader_known = self.leader_id is not None

        if not leader_known:
            self.start_election()
        else:
            print(
                f"[RECOVERY] Líder anterior={self.leader_id}. "
                "Aguardando heartbeat antes de nova eleição."
            )

    def request_shutdown(self, reason: str) -> None:
        if self.stop_event.is_set():
            return

        print(f"\n[SHUTDOWN] Nó {self.node_id}: {reason}")
        self.stop_event.set()
        self.save_checkpoint()
        self.publisher_detector.stop()
        self._close_connections()

    def run(self) -> None:
        self.connect_rabbitmq()

        if (
            self.channel is None
            or self.election_queue is None
            or self.publisher_heartbeat_queue is None
        ):
            raise RuntimeError("Topologia RabbitMQ não inicializada.")

        self.channel.basic_consume(
            queue=self.traffic_queue,
            on_message_callback=self._on_traffic_message,
            auto_ack=False,
        )
        self.channel.basic_consume(
            queue=self.election_queue,
            on_message_callback=self._on_election_message,
            auto_ack=False,
        )
        self.channel.basic_consume(
            queue=self.direct_queue,
            on_message_callback=self._on_direct_message,
            auto_ack=False,
        )
        self.channel.basic_consume(
            queue=self.publisher_heartbeat_queue,
            on_message_callback=self._on_publisher_heartbeat,
            auto_ack=False,
        )

        self.publisher_detector.start()

        threads = [
            threading.Thread(
                target=self._heartbeat_monitor,
                name=f"heartbeat-monitor-{self.node_id}",
                daemon=True,
            ),
            threading.Thread(
                target=self._checkpoint_worker,
                name=f"checkpoint-{self.node_id}",
                daemon=True,
            ),
            threading.Thread(
                target=self._initial_election,
                name=f"initial-election-{self.node_id}",
                daemon=True,
            ),
        ]

        for thread in threads:
            thread.start()

        # Tenta processar mensagens recuperadas do checkpoint.
        self._try_flush_buffer()

        print(
            f"[RUN] Nó {self.node_id} aguardando mensagens | "
            f"Fila durável={self.traffic_queue}"
        )

        try:
            self.channel.start_consuming()
        finally:
            self.request_shutdown("consumo encerrado")


def main() -> int:
    node = SmartTrafficLight()

    def handle_signal(signum: int, frame: Any) -> None:
        del frame
        node.request_shutdown(f"sinal {signum} recebido")

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        node.run()
        return 0

    except KeyboardInterrupt:
        node.request_shutdown("interrupção pelo usuário")
        return 0

    except Exception as error:
        print(
            f"[ERRO-FATAL] Nó {NODE_ID}: "
            f"{type(error).__name__}: {error}"
        )
        node.save_checkpoint()
        return 1


if __name__ == "__main__":
    sys.exit(main())

