"""Publisher resiliente com relógio vetorial, Cristian, heartbeat e checkpoint."""

from __future__ import annotations

import json
import os
import random
import signal
import sys
import threading
import time
from typing import Any

import pika

from recover import AtomicCheckpointStore, CheckpointCorruptedError, CheckpointError

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
SENSOR_ID = str(os.getenv("SENSOR_ID", random.randint(1, 100)))

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/data")
CHECKPOINT_INTERVAL = float(os.getenv("CHECKPOINT_INTERVAL", "5"))
PUBLISH_MIN_INTERVAL = float(os.getenv("PUBLISH_MIN_INTERVAL", "2"))
PUBLISH_MAX_INTERVAL = float(os.getenv("PUBLISH_MAX_INTERVAL", "5"))
SENSOR_HEARTBEAT_INTERVAL = float(os.getenv("SENSOR_HEARTBEAT_INTERVAL", "5"))
CRISTIAN_SYNC_INTERVAL = float(os.getenv("CRISTIAN_SYNC_INTERVAL", "15"))
CRISTIAN_RESPONSE_TIMEOUT = float(os.getenv("CRISTIAN_RESPONSE_TIMEOUT", "10"))
RECONNECT_DELAY = float(os.getenv("RECONNECT_DELAY", "5"))

TRAFFIC_EXCHANGE = "traffic_data"
HEARTBEAT_EXCHANGE = "node_heartbeats"
TIME_REQUEST_QUEUE = "time_requests"


class TrafficSensor:
    CHECKPOINT_VERSION = 1

    def __init__(self, sensor_id: str):
        self.sensor_id = str(sensor_id)
        self.state_lock = threading.RLock()
        self.stop_event = threading.Event()

        self.vector_clock: dict[str, int] = {self.sensor_id: 0}
        self.drift_rate = random.uniform(0.90, 1.10)
        self.sync_offset = 0.0
        self.clock_origin_real = time.time()
        self.clock_origin_physical = self.clock_origin_real
        self.last_physical_time = self.clock_origin_real

        self.pending_message: dict[str, Any] | None = None
        self.total_confirmed_messages = 0
        self.last_confirmed_message_id: str | None = None

        self.connection: pika.BlockingConnection | None = None
        self.channel = None

        checkpoint_path = os.path.join(
            CHECKPOINT_DIR,
            f"publisher_{self.sensor_id}.json",
        )
        self.checkpoint_store = AtomicCheckpointStore(checkpoint_path)
        self._restore_checkpoint()

        print(
            f"[INIT] Sensor {self.sensor_id} | "
            f"Deriva={self.drift_rate:.4f}x | VC={self.vector_clock}"
        )

    # --------------------------- Checkpoint ---------------------------

    def _checkpoint_state_locked(self) -> dict[str, Any]:
        return {
            "checkpoint_version": self.CHECKPOINT_VERSION,
            "sensor_id": self.sensor_id,
            "vector_clock": self.vector_clock.copy(),
            "drift_rate": self.drift_rate,
            "sync_offset": self.sync_offset,
            "last_physical_time": self.last_physical_time,
            "pending_message": self.pending_message,
            "total_confirmed_messages": self.total_confirmed_messages,
            "last_confirmed_message_id": self.last_confirmed_message_id,
        }

    def save_checkpoint(self) -> None:
        with self.state_lock:
            state = self._checkpoint_state_locked()

        try:
            self.checkpoint_store.save(state)
        except CheckpointError as error:
            print(f"[CHECKPOINT-ERRO] Sensor {self.sensor_id}: {error}")

    def _restore_checkpoint(self) -> None:
        try:
            state = self.checkpoint_store.load(default=None)
        except (CheckpointCorruptedError, CheckpointError) as error:
            print(f"[RECOVERY-ERRO] Sensor {self.sensor_id}: {error}")
            print("[RECOVERY] Iniciando com estado novo.")
            return

        if state is None:
            print(f"[RECOVERY] Sensor {self.sensor_id} sem checkpoint anterior.")
            return

        if str(state.get("sensor_id")) != self.sensor_id:
            print("[RECOVERY-ERRO] Checkpoint de outro sensor; ignorado.")
            return

        restored_vc = state.get("vector_clock", {})
        if not isinstance(restored_vc, dict):
            restored_vc = {}

        with self.state_lock:
            self.vector_clock = {
                str(node): int(value) for node, value in restored_vc.items()
            }
            self.vector_clock.setdefault(self.sensor_id, 0)
            self.drift_rate = float(state.get("drift_rate", self.drift_rate))
            self.sync_offset = float(state.get("sync_offset", 0.0))

            restored_last = float(state.get("last_physical_time", time.time()))
            now = time.time()
            self.clock_origin_real = now
            self.clock_origin_physical = max(now, restored_last)
            self.last_physical_time = self.clock_origin_physical

            pending = state.get("pending_message")
            self.pending_message = pending if isinstance(pending, dict) else None
            self.total_confirmed_messages = int(
                state.get("total_confirmed_messages", 0)
            )
            last_id = state.get("last_confirmed_message_id")
            self.last_confirmed_message_id = (
                str(last_id) if last_id is not None else None
            )

        print(
            f"[RECOVERY] Sensor {self.sensor_id} restaurado | "
            f"VC={self.vector_clock} | "
            f"Pendente={'sim' if self.pending_message else 'não'}"
        )

    def checkpoint_worker(self) -> None:
        while not self.stop_event.wait(CHECKPOINT_INTERVAL):
            self.save_checkpoint()

    # -------------------------- Relógio físico ------------------------

    def local_physical_time(self) -> float:
        with self.state_lock:
            elapsed = time.time() - self.clock_origin_real
            calculated = (
                self.clock_origin_physical
                + elapsed * self.drift_rate
                + self.sync_offset
            )
            if calculated <= self.last_physical_time:
                calculated = self.last_physical_time + 0.000001
            self.last_physical_time = calculated
            return calculated

    # ----------------------------- RabbitMQ ---------------------------

    @staticmethod
    def _build_connection_parameters() -> pika.ConnectionParameters:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        return pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=120,
            connection_attempts=10,
            retry_delay=5,
            socket_timeout=30,
        )

    @staticmethod
    def _declare_common_topology(channel) -> None:
        channel.exchange_declare(
            exchange=TRAFFIC_EXCHANGE,
            exchange_type="fanout",
            durable=True,
        )
        channel.exchange_declare(
            exchange=HEARTBEAT_EXCHANGE,
            exchange_type="fanout",
            durable=True,
        )
        channel.queue_declare(queue=TIME_REQUEST_QUEUE, durable=True)

    def connect_rabbitmq(self) -> None:
        self.close_main_connection()

        while not self.stop_event.is_set():
            try:
                print(f"[REDE] Sensor {self.sensor_id} conectando ao RabbitMQ...")
                self.connection = pika.BlockingConnection(
                    self._build_connection_parameters()
                )
                self.channel = self.connection.channel()
                self._declare_common_topology(self.channel)
                self.channel.confirm_delivery()
                print(f"[REDE] Sensor {self.sensor_id} conectado.")
                return
            except Exception as error:
                print(
                    f"[FALHA-REDE] {type(error).__name__}: {error}. "
                    f"Nova tentativa em {RECONNECT_DELAY:.1f}s."
                )
                self.close_main_connection()
                self.stop_event.wait(RECONNECT_DELAY)

        raise RuntimeError("Encerramento solicitado durante a conexão.")

    def close_main_connection(self) -> None:
        channel = self.channel
        connection = self.connection
        self.channel = None
        self.connection = None

        try:
            if channel is not None and channel.is_open:
                channel.close()
        except Exception:
            pass

        try:
            if connection is not None and connection.is_open:
                connection.close()
        except Exception:
            pass

    # --------------------------- Outbox local -------------------------

    def _create_pending_message(self) -> dict[str, Any]:
        with self.state_lock:
            self.vector_clock[self.sensor_id] += 1
            sequence = self.vector_clock[self.sensor_id]
            payload = {
                "message_id": f"{self.sensor_id}:{sequence}",
                "type": "TRAFFIC_DATA",
                "sensor_id": self.sensor_id,
                "fluxo_veiculos": random.randint(5, 50),
                "vector_clock": self.vector_clock.copy(),
                "physical_timestamp": self.local_physical_time(),
                "created_at": time.time(),
            }
            self.pending_message = payload

        # Persistir antes do envio permite recuperar e republicar após kill.
        self.save_checkpoint()
        return payload

    def _get_or_create_pending_message(self) -> dict[str, Any]:
        with self.state_lock:
            pending = self.pending_message

        if pending is not None:
            print(
                f"[OUTBOX] Republicando mensagem pendente "
                f"{pending.get('message_id')}."
            )
            return pending

        return self._create_pending_message()

    def _publish_pending_message(self, payload: dict[str, Any]) -> None:
        if self.channel is None or self.channel.is_closed:
            raise pika.exceptions.AMQPConnectionError(
                "Canal de publicação indisponível."
            )

        published = self.channel.basic_publish(
            exchange=TRAFFIC_EXCHANGE,
            routing_key="",
            body=json.dumps(payload, ensure_ascii=False),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=str(payload["message_id"]),
                timestamp=int(time.time()),
                type="TRAFFIC_DATA",
                app_id=f"sensor_{self.sensor_id}",
            ),
        )

        if published is False:
            raise pika.exceptions.NackError(
                f"Broker rejeitou {payload['message_id']}."
            )

        with self.state_lock:
            current_id = (
                self.pending_message.get("message_id")
                if self.pending_message
                else None
            )
            if current_id == payload["message_id"]:
                self.pending_message = None
            self.total_confirmed_messages += 1
            self.last_confirmed_message_id = str(payload["message_id"])

        self.save_checkpoint()
        print(
            f"[PUBLISH] Sensor={self.sensor_id} | "
            f"ID={payload['message_id']} | "
            f"Fluxo={payload['fluxo_veiculos']} | "
            f"Vetor={payload['vector_clock']} | "
            f"Tempo={payload['physical_timestamp']:.6f}"
        )

    def publish_data(self) -> None:
        while not self.stop_event.is_set():
            try:
                if (
                    self.connection is None
                    or self.connection.is_closed
                    or self.channel is None
                    or self.channel.is_closed
                ):
                    self.connect_rabbitmq()

                payload = self._get_or_create_pending_message()
                self._publish_pending_message(payload)
                self.stop_event.wait(
                    random.uniform(PUBLISH_MIN_INTERVAL, PUBLISH_MAX_INTERVAL)
                )

            except (pika.exceptions.AMQPError, OSError) as error:
                print(
                    f"[ERRO-REDE] Sensor {self.sensor_id}: "
                    f"{type(error).__name__}: {error}"
                )
                self.close_main_connection()
                self.stop_event.wait(RECONNECT_DELAY)
            except Exception as error:
                print(
                    f"[ERRO-PUBLISH] Sensor {self.sensor_id}: "
                    f"{type(error).__name__}: {error}"
                )
                self.close_main_connection()
                self.stop_event.wait(RECONNECT_DELAY)

    # -------------------------- Heartbeat -----------------------------

    def heartbeat_worker(self) -> None:
        connection = None
        channel = None

        while not self.stop_event.is_set():
            try:
                if (
                    connection is None
                    or connection.is_closed
                    or channel is None
                    or channel.is_closed
                ):
                    connection = pika.BlockingConnection(
                        self._build_connection_parameters()
                    )
                    channel = connection.channel()
                    self._declare_common_topology(channel)

                with self.state_lock:
                    sequence = self.vector_clock.get(self.sensor_id, 0)

                heartbeat = {
                    "type": "HEARTBEAT",
                    "node_kind": "publisher",
                    "node_id": self.sensor_id,
                    "vector_sequence": sequence,
                    "timestamp": time.time(),
                }

                channel.basic_publish(
                    exchange=HEARTBEAT_EXCHANGE,
                    routing_key="",
                    body=json.dumps(heartbeat),
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=1,
                        expiration=str(int(SENSOR_HEARTBEAT_INTERVAL * 3000)),
                        type="HEARTBEAT",
                        app_id=f"sensor_{self.sensor_id}",
                    ),
                )
                self.stop_event.wait(SENSOR_HEARTBEAT_INTERVAL)

            except Exception as error:
                print(f"[HEARTBEAT-ERRO] Sensor {self.sensor_id}: {error}")
                try:
                    if connection is not None and connection.is_open:
                        connection.close()
                except Exception:
                    pass
                connection = None
                channel = None
                self.stop_event.wait(RECONNECT_DELAY)

        try:
            if connection is not None and connection.is_open:
                connection.close()
        except Exception:
            pass

    # ---------------------- Algoritmo de Cristian ---------------------

    def cristian_time_server_worker(self) -> None:
        connection = None

        while not self.stop_event.is_set():
            try:
                connection = pika.BlockingConnection(
                    self._build_connection_parameters()
                )
                channel = connection.channel()
                self._declare_common_topology(channel)
                channel.basic_qos(prefetch_count=10)

                def on_time_request(ch, method, properties, body) -> None:
                    try:
                        request = json.loads(body)
                        client_id = str(request["client_id"])
                        reply_queue = (
                            properties.reply_to
                            or f"time_responses_{client_id}"
                        )
                        response = {
                            "type": "TIME_RESPONSE",
                            "server_id": self.sensor_id,
                            "client_id": client_id,
                            "server_time": time.time(),
                            "request_id": request.get("request_id"),
                        }
                        ch.basic_publish(
                            exchange="",
                            routing_key=reply_queue,
                            body=json.dumps(response),
                            properties=pika.BasicProperties(
                                content_type="application/json",
                                correlation_id=properties.correlation_id,
                                delivery_mode=1,
                            ),
                        )
                        ch.basic_ack(method.delivery_tag)
                        print(
                            f"[NTP-SERVER] Resposta enviada ao Sensor "
                            f"{client_id}."
                        )
                    except Exception as error:
                        print(f"[NTP-SERVER-ERRO] {error}")
                        ch.basic_nack(method.delivery_tag, requeue=False)

                channel.basic_consume(
                    queue=TIME_REQUEST_QUEUE,
                    on_message_callback=on_time_request,
                    auto_ack=False,
                )
                print("[NTP-SERVER] Sensor 0 aguardando requisições.")

                while not self.stop_event.is_set() and connection.is_open:
                    connection.process_data_events(time_limit=1)

            except Exception as error:
                print(f"[NTP-SERVER-ERRO] {error}")
                self.stop_event.wait(RECONNECT_DELAY)
            finally:
                try:
                    if connection is not None and connection.is_open:
                        connection.close()
                except Exception:
                    pass
                connection = None

    def sync_physical_clock_cristian(self) -> None:
        while not self.stop_event.wait(CRISTIAN_SYNC_INTERVAL):
            connection = None
            try:
                connection = pika.BlockingConnection(
                    self._build_connection_parameters()
                )
                channel = connection.channel()
                self._declare_common_topology(channel)

                response_queue = f"time_responses_{self.sensor_id}"
                channel.queue_declare(
                    queue=response_queue,
                    durable=False,
                    auto_delete=False,
                )

                request_id = f"{self.sensor_id}-{time.time_ns()}"
                t0 = self.local_physical_time()
                request = {
                    "type": "TIME_REQUEST",
                    "client_id": self.sensor_id,
                    "request_id": request_id,
                }

                channel.basic_publish(
                    exchange="",
                    routing_key=TIME_REQUEST_QUEUE,
                    body=json.dumps(request),
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        reply_to=response_queue,
                        correlation_id=request_id,
                        delivery_mode=1,
                        expiration=str(int(CRISTIAN_RESPONSE_TIMEOUT * 1000)),
                    ),
                )

                deadline = time.monotonic() + CRISTIAN_RESPONSE_TIMEOUT
                response_body = None

                while (
                    not self.stop_event.is_set()
                    and time.monotonic() < deadline
                ):
                    method, properties, body = channel.basic_get(
                        queue=response_queue,
                        auto_ack=False,
                    )
                    if method is None:
                        connection.process_data_events(time_limit=0.2)
                        continue

                    channel.basic_ack(method.delivery_tag)
                    if properties.correlation_id == request_id and body:
                        response_body = body
                        break

                if response_body is None:
                    print(
                        f"[CRISTIAN-FALHA] Sensor {self.sensor_id}: timeout."
                    )
                    continue

                response = json.loads(response_body)
                t1 = float(response["server_time"])
                t2 = self.local_physical_time()
                rtt = max(0.0, t2 - t0)
                estimated_server_time = t1 + rtt / 2.0
                correction = estimated_server_time - t2

                with self.state_lock:
                    self.sync_offset += correction
                    current_offset = self.sync_offset

                self.save_checkpoint()
                print(
                    f"[CRISTIAN-SYNC] Sensor={self.sensor_id} | "
                    f"RTT={rtt:.6f}s | Correção={correction:.6f}s | "
                    f"Offset={current_offset:.6f}s"
                )

            except Exception as error:
                print(f"[CRISTIAN-ERRO] Sensor {self.sensor_id}: {error}")
            finally:
                try:
                    if connection is not None and connection.is_open:
                        connection.close()
                except Exception:
                    pass

    # ----------------------- Inicialização/saída ----------------------

    def request_shutdown(self, reason: str) -> None:
        if self.stop_event.is_set():
            return
        print(f"\n[SHUTDOWN] Sensor {self.sensor_id}: {reason}")
        self.stop_event.set()
        self.save_checkpoint()
        self.close_main_connection()

    def run(self) -> None:
        workers = [
            threading.Thread(
                target=self.checkpoint_worker,
                name=f"checkpoint-{self.sensor_id}",
                daemon=True,
            ),
            threading.Thread(
                target=self.heartbeat_worker,
                name=f"heartbeat-{self.sensor_id}",
                daemon=True,
            ),
        ]

        if self.sensor_id == "0":
            workers.append(
                threading.Thread(
                    target=self.cristian_time_server_worker,
                    name="cristian-server",
                    daemon=True,
                )
            )
        else:
            workers.append(
                threading.Thread(
                    target=self.sync_physical_clock_cristian,
                    name=f"cristian-client-{self.sensor_id}",
                    daemon=True,
                )
            )

        for worker in workers:
            worker.start()

        try:
            self.publish_data()
        finally:
            self.request_shutdown("loop principal encerrado")


def main() -> int:
    sensor = TrafficSensor(SENSOR_ID)

    def handle_signal(signum: int, frame: Any) -> None:
        del frame
        sensor.request_shutdown(f"sinal {signum} recebido")

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        sensor.run()
        return 0
    except KeyboardInterrupt:
        sensor.request_shutdown("interrupção pelo usuário")
        return 0
    except Exception as error:
        print(
            f"[ERRO-FATAL] Sensor {SENSOR_ID}: "
            f"{type(error).__name__}: {error}"
        )
        sensor.save_checkpoint()
        return 1


if __name__ == "__main__":
    sys.exit(main())
