from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


CONTAINERS = ["sub_1", "sub_2", "sub_3", "sub_4"]

CHECKPOINTS = {
    "sub_1": "/data/subscriber_1.json",
    "sub_2": "/data/subscriber_2.json",
    "sub_3": "/data/subscriber_3.json",
    "sub_4": "/data/subscriber_4.json",
}

MAX_EXAMPLES_PER_CONTAINER = 10
MAX_LIST_ITEMS = 30


RE_DOCKER_TS = re.compile(r"^(\S+)\s+(.*)$")

RE_BUFFER = re.compile(
    r"\[CAUSAL-BUFFER\]\s+"
    r"ID=(?P<msg_id>\S+)\s+\|\s+"
    r"Sensor=(?P<sensor>\S+)\s+\|\s+"
    r"VC=(?P<vc>\{.*?\})\s+\|\s+"
    r"Buffer=(?P<buffer>\d+)"
)

RE_OUTPUT = re.compile(
    r"\[(?P<result>PROCESSADO|IGNORADO)\]\s+"
    r"\[(?P<role>[^\]]+)\]\s+"
    r"ID=(?P<msg_id>\S+)\s+\|\s+"
    r"Sensor=(?P<sensor>\S+)\s+\|\s+"
    r".*?\|\s+"
    r"VC=(?P<vc>\{.*?\})\s+\|\s+"
    r"Tempo=(?P<tempo>[\d.]+)"
)


@dataclass(frozen=True)
class MessageKey:
    sensor: str
    sequence: int

    def __str__(self) -> str:
        return f"{self.sensor}:{self.sequence}"


@dataclass
class Event:
    container: str
    timestamp: datetime
    message_id: str
    sensor: str
    sequence: int
    vector_clock: dict[str, int]
    raw_line: str

    @property
    def key(self) -> MessageKey:
        return MessageKey(self.sensor, self.sequence)


@dataclass
class ConfirmedProof:
    dependent_arrival: Event
    prerequisite_arrival: Event
    prerequisite_processed: Event
    dependent_processed: Event
    dependency_type: str


@dataclass
class ProvenViolation:
    previous_event: Event
    current_event: Event
    reason: str


@dataclass
class CausalWarning:
    event: Event
    missing_dependencies: list[MessageKey]
    simulated_vc_before: dict[str, int]


@dataclass
class ContainerResult:
    container: str
    baseline_vc: dict[str, int] = field(default_factory=dict)

    arrivals: dict[MessageKey, Event] = field(default_factory=dict)
    processed: dict[MessageKey, Event] = field(default_factory=dict)
    processed_order: list[Event] = field(default_factory=list)
    ignored: list[Event] = field(default_factory=list)

    confirmed_proofs: list[ConfirmedProof] = field(default_factory=list)
    proven_violations: list[ProvenViolation] = field(default_factory=list)
    causal_warnings: list[CausalWarning] = field(default_factory=list)

    duplicate_processed: list[Event] = field(default_factory=list)
    pending: list[Event] = field(default_factory=list)

    parse_errors: int = 0


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
    except FileNotFoundError:
        print(
            "[ERRO] O comando 'docker' não foi encontrado. "
            "Confirme que o Docker Desktop está aberto."
        )
        sys.exit(1)


def container_running(container: str) -> bool:
    result = run_command(
        [
            "docker",
            "inspect",
            "-f",
            "{{.State.Running}}",
            container,
        ]
    )

    return (
        result.returncode == 0
        and result.stdout.strip().lower() == "true"
    )


def parse_timestamp(raw: str) -> datetime | None:
    try:
        value = raw.rstrip("Z")

        if "." in value:
            base, fraction = value.split(".", 1)
            fraction = fraction[:6].ljust(6, "0")
            value = f"{base}.{fraction}"

        return datetime.fromisoformat(value).replace(
            tzinfo=timezone.utc
        )

    except ValueError:
        return None


def parse_vector_clock(raw: str) -> dict[str, int] | None:
    try:
        value = ast.literal_eval(raw)

        if not isinstance(value, dict):
            return None

        return {
            str(node): int(clock)
            for node, clock in value.items()
        }

    except (ValueError, SyntaxError, TypeError):
        return None


def own_sequence(
    vector_clock: dict[str, int],
    sensor: str,
) -> int | None:
    value = vector_clock.get(sensor)
    return int(value) if value is not None else None


def fetch_logs(
    container: str,
    since_iso: str,
) -> str:
    result = run_command(
        [
            "docker",
            "logs",
            "--timestamps",
            "--since",
            since_iso,
            container,
        ]
    )

    if result.returncode != 0:
        print(
            f"[AVISO] Não foi possível ler os logs de "
            f"{container}: {result.stderr.strip()}"
        )

    return (result.stdout or "") + "\n" + (result.stderr or "")


def read_checkpoint_vc(container: str) -> dict[str, int]:
    """
    Lê o VC persistido no início do teste.

    Esse checkpoint pode estar alguns segundos atrasado em relação à
    memória real. Por isso, ele é usado somente para gerar alertas
    diagnósticos, nunca para declarar uma falha definitiva.
    """
    checkpoint = CHECKPOINTS[container]

    code = (
        "import json;"
        f"d=json.load(open('{checkpoint}'));"
        "s=d.get('state', d);"
        "print(json.dumps(s.get('local_vc', {})))"
    )

    result = run_command(
        [
            "docker",
            "exec",
            container,
            "python",
            "-c",
            code,
        ]
    )

    if result.returncode != 0:
        print(
            f"[AVISO] Não foi possível ler o checkpoint de "
            f"{container}. Usando VC inicial vazio."
        )
        return {}

    try:
        value = json.loads(result.stdout.strip())

        if not isinstance(value, dict):
            return {}

        return {
            str(node): int(clock)
            for node, clock in value.items()
        }

    except (json.JSONDecodeError, TypeError, ValueError):
        return {}


def verify_environment() -> bool:
    required = CONTAINERS + ["rabbitmq", "chaos"]

    missing = [
        container
        for container in required
        if not container_running(container)
    ]

    if missing:
        print(
            "[ERRO] Estes containers não estão ativos: "
            + ", ".join(missing)
        )
        return False

    result = run_command(
        [
            "docker",
            "logs",
            "--tail=30",
            "chaos",
        ]
    )

    text = (
        (result.stdout or "")
        + "\n"
        + (result.stderr or "")
    )

    if (
        "[CHAOS] Latência:" not in text
        or "Perda: 5%" not in text
    ):
        print(
            "[ERRO] O container chaos está ativo, mas não encontrei "
            "evidência recente da latência variável e da perda de 5%."
        )
        return False

    print(
        "[AMBIENTE] RabbitMQ, subscribers e chaos estão ativos."
    )
    print(
        "[AMBIENTE] Latência variável e perda de 5% confirmadas."
    )

    return True


def parse_container_logs(
    container: str,
    baseline_vc: dict[str, int],
    log_text: str,
) -> ContainerResult:
    result = ContainerResult(
        container=container,
        baseline_vc=baseline_vc.copy(),
    )

    for raw_line in log_text.splitlines():
        timestamp_match = RE_DOCKER_TS.match(raw_line)

        if not timestamp_match:
            continue

        timestamp_raw, message = timestamp_match.groups()
        timestamp = parse_timestamp(timestamp_raw)

        if timestamp is None:
            continue

        buffer_match = RE_BUFFER.search(message)

        if buffer_match:
            sensor = buffer_match.group("sensor")
            vc = parse_vector_clock(buffer_match.group("vc"))

            if vc is None:
                result.parse_errors += 1
                continue

            sequence = own_sequence(vc, sensor)

            if sequence is None:
                result.parse_errors += 1
                continue

            event = Event(
                container=container,
                timestamp=timestamp,
                message_id=buffer_match.group("msg_id"),
                sensor=sensor,
                sequence=sequence,
                vector_clock=vc,
                raw_line=message.strip(),
            )

            result.arrivals.setdefault(event.key, event)
            continue

        output_match = RE_OUTPUT.search(message)

        if output_match:
            sensor = output_match.group("sensor")
            vc = parse_vector_clock(output_match.group("vc"))

            if vc is None:
                result.parse_errors += 1
                continue

            sequence = own_sequence(vc, sensor)

            if sequence is None:
                result.parse_errors += 1
                continue

            event = Event(
                container=container,
                timestamp=timestamp,
                message_id=output_match.group("msg_id"),
                sensor=sensor,
                sequence=sequence,
                vector_clock=vc,
                raw_line=message.strip(),
            )

            if output_match.group("result") == "IGNORADO":
                result.ignored.append(event)
                continue

            if event.key in result.processed:
                result.duplicate_processed.append(event)
                continue

            result.processed[event.key] = event
            result.processed_order.append(event)

    result.processed_order.sort(
        key=lambda event: event.timestamp
    )

    return result


def required_dependencies(event: Event) -> list[MessageKey]:
    dependencies: list[MessageKey] = []

    for node, clock in event.vector_clock.items():
        required_clock = (
            clock - 1
            if node == event.sensor
            else clock
        )

        if required_clock > 0:
            dependencies.append(
                MessageKey(node, required_clock)
            )

    return dependencies


def missing_dependencies(
    event: Event,
    simulated_vc: dict[str, int],
) -> list[MessageKey]:
    missing: list[MessageKey] = []

    for node, clock in event.vector_clock.items():
        required_clock = (
            clock - 1
            if node == event.sensor
            else clock
        )

        if simulated_vc.get(node, 0) < required_clock:
            missing.append(
                MessageKey(node, required_clock)
            )

    return missing


def merge_vc(
    local_vc: dict[str, int],
    remote_vc: dict[str, int],
) -> None:
    for node, clock in remote_vc.items():
        local_vc[node] = max(
            local_vc.get(node, 0),
            clock,
        )


def detect_proven_violations(
    result: ContainerResult,
) -> None:
    """
    Detecta apenas violações que os próprios logs conseguem provar.

    Para cada sensor, a sequência processada deve ser estritamente
    crescente no tempo.

    Exemplo comprovadamente inválido:
        A:83 processada antes de A:82.

    Essa verificação não depende de checkpoint nem de reconstrução
    aproximada do estado interno.
    """
    by_sensor: dict[str, list[Event]] = {}

    for event in result.processed_order:
        by_sensor.setdefault(
            event.sensor,
            [],
        ).append(event)

    for sensor_events in by_sensor.values():
        previous: Event | None = None

        for event in sensor_events:
            if (
                previous is not None
                and event.sequence <= previous.sequence
            ):
                result.proven_violations.append(
                    ProvenViolation(
                        previous_event=previous,
                        current_event=event,
                        reason=(
                            "Sequência do mesmo sensor não foi "
                            "processada em ordem estritamente crescente."
                        ),
                    )
                )

            previous = event


def detect_diagnostic_warnings(
    result: ContainerResult,
) -> None:
    """
    Reconstrói aproximadamente o VC usando checkpoint + logs.

    Como o checkpoint pode estar atrasado em relação à memória, uma
    divergência aqui é apenas um ALERTA, e nunca um FAIL definitivo.
    """
    simulated_vc = result.baseline_vc.copy()

    for event in result.processed_order:
        missing = missing_dependencies(
            event,
            simulated_vc,
        )

        if missing:
            result.causal_warnings.append(
                CausalWarning(
                    event=event,
                    missing_dependencies=missing,
                    simulated_vc_before=simulated_vc.copy(),
                )
            )

        merge_vc(
            simulated_vc,
            event.vector_clock,
        )


def detect_confirmed_proofs(
    result: ContainerResult,
) -> None:
    """
    Confirma casos em que:

    1. a mensagem dependente chegou fisicamente antes da predecessora;
    2. ambas foram processadas;
    3. a predecessora foi processada antes da dependente.

    Esse é o núcleo da prova pedida no PDF.
    """
    seen: set[
        tuple[MessageKey, MessageKey]
    ] = set()

    for dependent_key, dependent_processed in (
        result.processed.items()
    ):
        dependent_arrival = result.arrivals.get(
            dependent_key
        )

        if dependent_arrival is None:
            continue

        for prerequisite_key in required_dependencies(
            dependent_processed
        ):
            prerequisite_arrival = result.arrivals.get(
                prerequisite_key
            )
            prerequisite_processed = result.processed.get(
                prerequisite_key
            )

            if (
                prerequisite_arrival is None
                or prerequisite_processed is None
            ):
                continue

            arrived_out_of_order = (
                dependent_arrival.timestamp
                < prerequisite_arrival.timestamp
            )

            processed_in_logical_order = (
                prerequisite_processed.timestamp
                < dependent_processed.timestamp
            )

            proof_key = (
                prerequisite_key,
                dependent_key,
            )

            if (
                arrived_out_of_order
                and processed_in_logical_order
                and proof_key not in seen
            ):
                dependency_type = (
                    "intra-sensor"
                    if prerequisite_key.sensor
                    == dependent_key.sensor
                    else "inter-sensor"
                )

                result.confirmed_proofs.append(
                    ConfirmedProof(
                        dependent_arrival=dependent_arrival,
                        prerequisite_arrival=(
                            prerequisite_arrival
                        ),
                        prerequisite_processed=(
                            prerequisite_processed
                        ),
                        dependent_processed=(
                            dependent_processed
                        ),
                        dependency_type=dependency_type,
                    )
                )

                seen.add(proof_key)


def analyze_container(result: ContainerResult) -> None:
    detect_proven_violations(result)
    detect_diagnostic_warnings(result)
    detect_confirmed_proofs(result)

    result.pending = sorted(
        (
            event
            for key, event in result.arrivals.items()
            if key not in result.processed
        ),
        key=lambda event: event.timestamp,
    )


def wait_with_progress(seconds: int) -> None:
    remaining = seconds

    while remaining > 0:
        interval = min(10, remaining)
        time.sleep(interval)
        remaining -= interval

        print(
            f"[TESTE-A] Faltam aproximadamente "
            f"{remaining}s de coleta..."
        )


def run_test(
    duration: int,
) -> tuple[list[ContainerResult], datetime]:
    baselines = {
        container: read_checkpoint_vc(container)
        for container in CONTAINERS
    }

    start_time = datetime.now(timezone.utc)

    print(
        f"[TESTE-A] Coletando por {duration}s a partir de "
        f"{start_time.isoformat()}."
    )
    print(
        "[TESTE-A] Não interrompa os containers durante a coleta."
    )

    wait_with_progress(duration)

    results: list[ContainerResult] = []

    for container in CONTAINERS:
        logs = fetch_logs(
            container,
            start_time.isoformat(),
        )

        result = parse_container_logs(
            container=container,
            baseline_vc=baselines[container],
            log_text=logs,
        )

        analyze_container(result)
        results.append(result)

    return results, start_time


def container_status(result: ContainerResult) -> str:
    if result.proven_violations:
        return "FAIL"

    if not result.processed:
        return "BLOQUEADO"

    if not result.confirmed_proofs:
        return "INCONCLUSIVO"

    if (
        result.causal_warnings
        or result.pending
        or result.ignored
    ):
        return "PASS COM ALERTA"

    return "PASS"


def overall_status(
    results: list[ContainerResult],
) -> str:
    if any(
        result.proven_violations
        for result in results
    ):
        return "FAIL"

    if any(
        not result.processed
        for result in results
    ):
        return "PASS PARCIAL" if any(
            result.confirmed_proofs
            for result in results
        ) else "INCONCLUSIVO"

    if not any(
        result.confirmed_proofs
        for result in results
    ):
        return "INCONCLUSIVO"

    all_have_proof = all(
        result.confirmed_proofs
        for result in results
    )

    has_alerts = any(
        result.causal_warnings
        or result.pending
        or result.ignored
        for result in results
    )

    if not all_have_proof:
        return "PASS PARCIAL"

    if has_alerts:
        return "PASS COM ALERTA"

    return "PASS COMPLETO"


def print_summary(results: list[ContainerResult]) -> None:
    print("\n" + "=" * 78)
    print("RESUMO — RESTRIÇÃO A: ORDENAÇÃO CAUSAL")
    print("=" * 78)

    total_proofs = 0
    total_intra = 0
    total_inter = 0
    total_proven_violations = 0
    total_warnings = 0
    total_pending = 0
    total_ignored = 0

    for result in results:
        intra = sum(
            proof.dependency_type == "intra-sensor"
            for proof in result.confirmed_proofs
        )

        inter = sum(
            proof.dependency_type == "inter-sensor"
            for proof in result.confirmed_proofs
        )

        total_proofs += len(result.confirmed_proofs)
        total_intra += intra
        total_inter += inter
        total_proven_violations += len(
            result.proven_violations
        )
        total_warnings += len(
            result.causal_warnings
        )
        total_pending += len(result.pending)
        total_ignored += len(result.ignored)

        received_sensors = sorted(
            {
                event.sensor
                for event in result.arrivals.values()
            }
        )

        processed_sensors = sorted(
            {
                event.sensor
                for event in result.processed.values()
            }
        )

        print(f"\nContainer: {result.container}")
        print(
            f"  Resultado: {container_status(result)}"
        )
        print(
            f"  VC inicial persistido: "
            f"{result.baseline_vc}"
        )
        print(
            f"  Mensagens recebidas: "
            f"{len(result.arrivals)}"
        )
        print(
            f"  Mensagens processadas: "
            f"{len(result.processed)}"
        )
        print(
            f"  Sensores recebidos: "
            f"{received_sensors or '-'}"
        )
        print(
            f"  Sensores processados: "
            f"{processed_sensors or '-'}"
        )
        print(
            f"  Provas confirmadas: "
            f"{len(result.confirmed_proofs)} "
            f"(intra={intra}, inter={inter})"
        )
        print(
            f"  Violações comprovadas: "
            f"{len(result.proven_violations)}"
        )
        print(
            f"  Alertas diagnósticos inter-sensor: "
            f"{len(result.causal_warnings)}"
        )
        print(
            f"  Pendentes ao fim da janela: "
            f"{len(result.pending)}"
        )
        print(
            f"  Mensagens ignoradas: "
            f"{len(result.ignored)}"
        )

        if result.parse_errors:
            print(
                f"  Linhas incompatíveis: "
                f"{result.parse_errors}"
            )

    status = overall_status(results)

    print("\n" + "-" * 78)
    print(
        f"Provas de inversão física corrigida: "
        f"{total_proofs}"
    )
    print(
        f"  Dependências intra-sensor: "
        f"{total_intra}"
    )
    print(
        f"  Dependências inter-sensor: "
        f"{total_inter}"
    )
    print(
        f"Violações comprovadas: "
        f"{total_proven_violations}"
    )
    print(
        f"Alertas diagnósticos não conclusivos: "
        f"{total_warnings}"
    )
    print(
        f"Mensagens pendentes: {total_pending}"
    )
    print(
        f"Mensagens ignoradas: {total_ignored}"
    )

    print("\nVEREDITO:")

    if status == "FAIL":
        print(
            "[FAIL] Os logs provaram que mensagens do mesmo "
            "sensor foram processadas fora da ordem lógica."
        )

    elif status == "INCONCLUSIVO":
        print(
            "[INCONCLUSIVO] Não foi encontrada uma inversão "
            "física corrigida de ponta a ponta nesta janela."
        )

    elif status == "PASS PARCIAL":
        print(
            "[PASS PARCIAL] Há provas válidas de reordenação, "
            "mas nem todos os subscribers produziram uma prova "
            "completa nesta janela."
        )

    elif status == "PASS COM ALERTA":
        print(
            "[PASS COM ALERTA] Todos os subscribers apresentaram "
            "provas válidas de reordenação e não houve violação "
            "comprovada. Há alertas diagnósticos ou mensagens "
            "pendentes que não constituem falha comprovada."
        )

    else:
        print(
            "[PASS COMPLETO] Todos os subscribers apresentaram "
            "inversões físicas corrigidas e nenhuma violação lógica "
            "foi comprovada."
        )

    print("=" * 78)


def compact_list(values: list[str]) -> str:
    if len(values) <= MAX_LIST_ITEMS:
        return str(values)

    shown = values[:MAX_LIST_ITEMS]
    omitted = len(values) - len(shown)

    return f"{shown} ... (+{omitted} outros)"


def event_text(event: Event) -> str:
    return (
        f"{event.timestamp.isoformat()} | "
        f"{event.message_id} | "
        f"VC={event.vector_clock}"
    )


def save_report(
    results: list[ContainerResult],
    start_time: datetime,
    path: Path,
) -> None:
    status = overall_status(results)

    lines: list[str] = [
        "# Relatório — Restrição A: Ordenação Causal",
        "",
        f"**Início da coleta:** `{start_time.isoformat()}`",
        "",
        f"**Resultado geral:** **{status}**",
        "",
        "## Critério utilizado",
        "",
        (
            "O teste confirma uma prova quando uma mensagem "
            "causalmente dependente chega antes da predecessora, "
            "mas o subscriber processa primeiro a predecessora e "
            "somente depois a dependente."
        ),
        "",
        (
            "Uma falha definitiva somente é declarada quando os "
            "próprios logs mostram que mensagens do mesmo sensor "
            "foram processadas em sequência não crescente."
        ),
        "",
        (
            "A reconstrução de dependências inter-sensor por meio "
            "do checkpoint é apresentada apenas como diagnóstico, "
            "pois o checkpoint persistido pode estar atrasado em "
            "relação ao estado que estava na memória."
        ),
        "",
    ]

    for result in results:
        intra = sum(
            proof.dependency_type == "intra-sensor"
            for proof in result.confirmed_proofs
        )

        inter = sum(
            proof.dependency_type == "inter-sensor"
            for proof in result.confirmed_proofs
        )

        lines.extend(
            [
                f"## {result.container}",
                "",
                f"- Resultado: **{container_status(result)}**",
                f"- VC inicial persistido: `{result.baseline_vc}`",
                f"- Recebidas: **{len(result.arrivals)}**",
                f"- Processadas: **{len(result.processed)}**",
                (
                    f"- Provas confirmadas: "
                    f"**{len(result.confirmed_proofs)}** "
                    f"(intra={intra}, inter={inter})"
                ),
                (
                    f"- Violações comprovadas: "
                    f"**{len(result.proven_violations)}**"
                ),
                (
                    f"- Alertas diagnósticos: "
                    f"**{len(result.causal_warnings)}**"
                ),
                f"- Pendentes: **{len(result.pending)}**",
                f"- Ignoradas: **{len(result.ignored)}**",
                "",
                "### Provas confirmadas",
                "",
            ]
        )

        proofs = result.confirmed_proofs[
            :MAX_EXAMPLES_PER_CONTAINER
        ]

        if not proofs:
            lines.append(
                "_Nenhuma prova confirmada nesta janela._"
            )

        for proof in proofs:
            lines.extend(
                [
                    (
                        f"- Tipo: **{proof.dependency_type}**. "
                        f"`{proof.dependent_arrival.message_id}` "
                        "chegou antes de "
                        f"`{proof.prerequisite_arrival.message_id}`."
                    ),
                    (
                        f"  - Chegada da dependente: "
                        f"`{event_text(proof.dependent_arrival)}`"
                    ),
                    (
                        f"  - Chegada da predecessora: "
                        f"`{event_text(proof.prerequisite_arrival)}`"
                    ),
                    (
                        f"  - Processamento da predecessora: "
                        f"`{event_text(proof.prerequisite_processed)}`"
                    ),
                    (
                        f"  - Processamento da dependente: "
                        f"`{event_text(proof.dependent_processed)}`"
                    ),
                    "",
                ]
            )

        omitted = (
            len(result.confirmed_proofs)
            - len(proofs)
        )

        if omitted > 0:
            lines.append(
                f"_Foram omitidas {omitted} provas adicionais._"
            )

        lines.extend(
            [
                "",
                "### Violações comprovadas",
                "",
            ]
        )

        violations = result.proven_violations[
            :MAX_EXAMPLES_PER_CONTAINER
        ]

        if not violations:
            lines.append("_Nenhuma._")

        for violation in violations:
            lines.extend(
                [
                    f"- **VIOLAÇÃO:** {violation.reason}",
                    (
                        f"  - Evento anterior: "
                        f"`{event_text(violation.previous_event)}`"
                    ),
                    (
                        f"  - Evento posterior: "
                        f"`{event_text(violation.current_event)}`"
                    ),
                    "",
                ]
            )

        lines.extend(
            [
                "",
                "### Alertas diagnósticos inter-sensor",
                "",
            ]
        )

        warnings = result.causal_warnings[
            :MAX_EXAMPLES_PER_CONTAINER
        ]

        if not warnings:
            lines.append("_Nenhum._")

        for warning in warnings:
            missing = [
                str(dependency)
                for dependency in warning.missing_dependencies
            ]

            lines.extend(
                [
                    (
                        f"- Possível dependência não observada antes de "
                        f"`{warning.event.message_id}`."
                    ),
                    (
                        f"  - Dependências: `{missing}`"
                    ),
                    (
                        f"  - VC reconstruído: "
                        f"`{warning.simulated_vc_before}`"
                    ),
                    (
                        "  - Classificação: alerta não conclusivo, "
                        "pois o checkpoint pode estar atrasado."
                    ),
                    "",
                ]
            )

        omitted_warnings = (
            len(result.causal_warnings)
            - len(warnings)
        )

        if omitted_warnings > 0:
            lines.append(
                f"_Foram omitidos {omitted_warnings} "
                "alertas adicionais._"
            )

        lines.extend(
            [
                "",
                "### Pendentes ao fim da coleta",
                "",
            ]
        )

        if not result.pending:
            lines.append("_Nenhuma._")
        else:
            pending_ids = [
                event.message_id
                for event in result.pending
            ]
            lines.append(
                f"`{compact_list(pending_ids)}`"
            )

        lines.extend(
            [
                "",
                "### Mensagens ignoradas",
                "",
            ]
        )

        if not result.ignored:
            lines.append("_Nenhuma._")
        else:
            ignored_ids = [
                event.message_id
                for event in result.ignored
            ]
            lines.append(
                f"`{compact_list(ignored_ids)}`"
            )

        lines.append("")

    path.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

    print(
        f"\n[TESTE-A] Relatório salvo em: "
        f"{path.resolve()}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Teste da Restrição A: ordenação causal "
            "sob rede caótica."
        )
    )

    parser.add_argument(
        "--duracao",
        type=int,
        default=180,
        help="Duração da coleta em segundos. Padrão: 180.",
    )

    args = parser.parse_args()

    if args.duracao <= 0:
        print(
            "[ERRO] --duracao deve ser maior que zero."
        )
        return 3

    if not verify_environment():
        return 3

    results, start_time = run_test(
        args.duracao
    )

    print_summary(results)

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    report_path = Path(
        f"relatorio_restricaoA_{timestamp}.md"
    )

    save_report(
        results=results,
        start_time=start_time,
        path=report_path,
    )

    status = overall_status(results)

    if status in (
        "PASS COMPLETO",
        "PASS COM ALERTA",
    ):
        return 0

    if status == "FAIL":
        return 1

    if status == "INCONCLUSIVO":
        return 2

    # PASS PARCIAL
    return 4


if __name__ == "__main__":
    sys.exit(main())