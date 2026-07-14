\
"""
Detector de falhas baseado em heartbeats.

Este módulo não envia mensagens pela rede. Ele mantém o registro do último
heartbeat recebido de cada nó e executa callbacks quando um nó muda de estado.

A camada de comunicação, por exemplo RabbitMQ, deve chamar:

    detector.record_heartbeat(node_id)

sempre que receber um heartbeat.

O monitor interno classifica os nós como:
- UNKNOWN: ainda não houve heartbeat suficiente;
- ALIVE: heartbeat recebido dentro do limite;
- SUSPECTED: heartbeat atrasado, mas a falha ainda não foi confirmada;
- FAILED: ausência de heartbeat acima do timeout;
- RECOVERED: nó anteriormente falho voltou a enviar heartbeat.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Hashable


NodeId = Hashable
StatusCallback = Callable[[NodeId, "NodeStatus"], None]


class NodeStatus(str, Enum):
    UNKNOWN = "UNKNOWN"
    ALIVE = "ALIVE"
    SUSPECTED = "SUSPECTED"
    FAILED = "FAILED"
    RECOVERED = "RECOVERED"


@dataclass
class HeartbeatRecord:
    node_id: NodeId
    last_seen_monotonic: float
    last_seen_wall_time: float
    status: NodeStatus = NodeStatus.UNKNOWN
    heartbeat_count: int = 0


class FailureDetector:
    """
    Detector de falhas por timeout de heartbeat.

    Parameters
    ----------
    heartbeat_timeout:
        Tempo máximo, em segundos, sem heartbeat antes de considerar o nó
        como FAILED.

    suspicion_timeout:
        Tempo sem heartbeat antes de marcar o nó como SUSPECTED.
        Deve ser menor que heartbeat_timeout.

    check_interval:
        Intervalo entre verificações internas.

    on_status_change:
        Callback opcional executado quando o estado de um nó muda.
        Assinatura: callback(node_id, novo_status)
    """

    def __init__(
        self,
        heartbeat_timeout: float = 15.0,
        suspicion_timeout: float | None = None,
        check_interval: float = 1.0,
        on_status_change: StatusCallback | None = None,
    ):
        if heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout deve ser maior que zero.")

        if check_interval <= 0:
            raise ValueError("check_interval deve ser maior que zero.")

        if suspicion_timeout is None:
            suspicion_timeout = heartbeat_timeout * 0.60

        if suspicion_timeout <= 0:
            raise ValueError("suspicion_timeout deve ser maior que zero.")

        if suspicion_timeout >= heartbeat_timeout:
            raise ValueError(
                "suspicion_timeout deve ser menor que heartbeat_timeout."
            )

        self.heartbeat_timeout = float(heartbeat_timeout)
        self.suspicion_timeout = float(suspicion_timeout)
        self.check_interval = float(check_interval)
        self.on_status_change = on_status_change

        self._records: dict[NodeId, HeartbeatRecord] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def register_node(self, node_id: NodeId) -> None:
        """
        Registra um nó antes do primeiro heartbeat.

        O relógio começa no instante do registro. Caso o nó não envie
        heartbeat, ele será posteriormente marcado como suspeito e falho.
        """
        now_mono = time.monotonic()
        now_wall = time.time()

        with self._lock:
            if node_id not in self._records:
                self._records[node_id] = HeartbeatRecord(
                    node_id=node_id,
                    last_seen_monotonic=now_mono,
                    last_seen_wall_time=now_wall,
                    status=NodeStatus.UNKNOWN,
                )

    def unregister_node(self, node_id: NodeId) -> None:
        """Remove um nó do monitoramento."""
        with self._lock:
            self._records.pop(node_id, None)

    def record_heartbeat(self, node_id: NodeId) -> None:
        """
        Registra a chegada de um heartbeat.

        Se o nó estava FAILED, seu estado passa brevemente para RECOVERED.
        Na próxima verificação, será classificado como ALIVE.
        """
        now_mono = time.monotonic()
        now_wall = time.time()
        callback_status: NodeStatus | None = None

        with self._lock:
            record = self._records.get(node_id)

            if record is None:
                record = HeartbeatRecord(
                    node_id=node_id,
                    last_seen_monotonic=now_mono,
                    last_seen_wall_time=now_wall,
                )
                self._records[node_id] = record

            previous_status = record.status
            record.last_seen_monotonic = now_mono
            record.last_seen_wall_time = now_wall
            record.heartbeat_count += 1

            if previous_status == NodeStatus.FAILED:
                record.status = NodeStatus.RECOVERED
            else:
                record.status = NodeStatus.ALIVE

            if record.status != previous_status:
                callback_status = record.status

        if callback_status is not None:
            self._notify(node_id, callback_status)

    def get_status(self, node_id: NodeId) -> NodeStatus:
        """Retorna o estado atual do nó."""
        with self._lock:
            record = self._records.get(node_id)
            return record.status if record else NodeStatus.UNKNOWN

    def seconds_since_last_heartbeat(
        self,
        node_id: NodeId,
    ) -> float | None:
        """Retorna há quantos segundos o último heartbeat foi recebido."""
        with self._lock:
            record = self._records.get(node_id)

            if record is None:
                return None

            return max(
                0.0,
                time.monotonic() - record.last_seen_monotonic,
            )

    def alive_nodes(self) -> list[NodeId]:
        """Lista nós considerados vivos ou recém-recuperados."""
        with self._lock:
            return [
                node_id
                for node_id, record in self._records.items()
                if record.status in (
                    NodeStatus.ALIVE,
                    NodeStatus.RECOVERED,
                )
            ]

    def failed_nodes(self) -> list[NodeId]:
        """Lista nós atualmente considerados falhos."""
        with self._lock:
            return [
                node_id
                for node_id, record in self._records.items()
                if record.status == NodeStatus.FAILED
            ]

    def snapshot(self) -> dict[str, dict[str, object]]:
        """
        Retorna um estado serializável em JSON.

        Pode ser incluído em um checkpoint do subscriber.
        """
        with self._lock:
            return {
                str(node_id): {
                    "last_seen_wall_time": record.last_seen_wall_time,
                    "status": record.status.value,
                    "heartbeat_count": record.heartbeat_count,
                }
                for node_id, record in self._records.items()
            }

    def restore_snapshot(
        self,
        snapshot: dict[str, dict[str, object]],
        node_id_parser: Callable[[str], NodeId] | None = None,
    ) -> None:
        """
        Restaura metadados salvos anteriormente.

        Por segurança, tempos monotônicos não são restaurados entre processos.
        Todos os nós restaurados começam com o instante atual e precisam voltar
        a enviar heartbeat para permanecerem vivos.

        node_id_parser pode ser usado para converter as chaves, por exemplo:

            detector.restore_snapshot(snapshot, node_id_parser=int)
        """
        parser = node_id_parser or (lambda value: value)
        now_mono = time.monotonic()

        with self._lock:
            for raw_node_id, data in snapshot.items():
                node_id = parser(raw_node_id)

                raw_status = str(
                    data.get("status", NodeStatus.UNKNOWN.value)
                )
                try:
                    status = NodeStatus(raw_status)
                except ValueError:
                    status = NodeStatus.UNKNOWN

                self._records[node_id] = HeartbeatRecord(
                    node_id=node_id,
                    last_seen_monotonic=now_mono,
                    last_seen_wall_time=float(
                        data.get("last_seen_wall_time", time.time())
                    ),
                    status=status,
                    heartbeat_count=int(data.get("heartbeat_count", 0)),
                )

    def start(self) -> None:
        """Inicia a thread de monitoramento, caso ainda não esteja ativa."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return

            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._monitor_loop,
                name="failure-detector",
                daemon=True,
            )
            self._thread.start()

    def stop(self, join_timeout: float = 3.0) -> None:
        """Solicita o encerramento da thread de monitoramento."""
        self._stop_event.set()

        thread = self._thread
        if (
            thread is not None
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=join_timeout)

    def _monitor_loop(self) -> None:
        while not self._stop_event.wait(self.check_interval):
            now = time.monotonic()
            changes: list[tuple[NodeId, NodeStatus]] = []

            with self._lock:
                for node_id, record in self._records.items():
                    elapsed = now - record.last_seen_monotonic
                    old_status = record.status

                    if elapsed >= self.heartbeat_timeout:
                        new_status = NodeStatus.FAILED
                    elif elapsed >= self.suspicion_timeout:
                        new_status = NodeStatus.SUSPECTED
                    else:
                        new_status = NodeStatus.ALIVE

                    if old_status == NodeStatus.RECOVERED:
                        new_status = NodeStatus.ALIVE

                    if new_status != old_status:
                        record.status = new_status
                        changes.append((node_id, new_status))

            for node_id, status in changes:
                self._notify(node_id, status)

    def _notify(self, node_id: NodeId, status: NodeStatus) -> None:
        callback = self.on_status_change

        if callback is None:
            return

        try:
            callback(node_id, status)
        except Exception as error:
            print(
                f"[FAILURE-DETECTOR] Erro no callback do nó "
                f"{node_id}: {error}"
            )

    def __enter__(self) -> "FailureDetector":
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()
