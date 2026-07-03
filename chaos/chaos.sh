#!/bin/bash

echo "Iniciando Chaos"

echo "Aguardando RabbitMQ iniciar..."
sleep 10

while true
do
    LATENCY=$(( RANDOM % 3991 + 10 ))
    tc qdisc replace dev eth0 root netem delay ${LATENCY}ms loss 5%

    echo "[CHAOS] Latência: ${LATENCY} ms | Perda: 5%"
    
    sleep 15
done
