#!/bin/bash

set -u

INTERFACE="${CHAOS_INTERFACE:-eth0}"
PACKET_LOSS="${CHAOS_PACKET_LOSS:-5%}"
CHANGE_INTERVAL="${CHAOS_CHANGE_INTERVAL:-15}"
START_DELAY="${CHAOS_START_DELAY:-10}"

echo "[CHAOS] Contêiner de injeção de falhas iniciado."
echo "[CHAOS] Aguardando RabbitMQ iniciar por ${START_DELAY}s..."

sleep "$START_DELAY"

cleanup() {
    echo "[CHAOS] Removendo regras de latência e perda..."
    tc qdisc del dev "$INTERFACE" root 2>/dev/null || true
}

shutdown() {
    echo "[CHAOS] Encerramento solicitado."
    cleanup
    exit 0
}

trap shutdown SIGTERM SIGINT
trap cleanup EXIT

while ! ip link show "$INTERFACE" >/dev/null 2>&1
do
    echo "[CHAOS] Interface $INTERFACE ainda não disponível. Aguardando..."
    sleep 2
done

echo "[CHAOS] Interface $INTERFACE encontrada."

while true
do
    # Gera um valor entre 10 ms e 4000 ms.
    LATENCY=$(( RANDOM % 3991 + 10 ))

    if tc qdisc replace dev "$INTERFACE" root netem \
        delay "${LATENCY}ms" \
        loss "$PACKET_LOSS"
    then
        echo "[CHAOS] Latência: ${LATENCY} ms | Perda: $PACKET_LOSS"
    else
        echo "[CHAOS-ERRO] Não foi possível aplicar tc/netem em $INTERFACE."
    fi

    sleep "$CHANGE_INTERVAL"
done
