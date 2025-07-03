#!/bin/bash

echo "matando procesos previos"
pkill -f uvicorn
echo "eliminados todos los apps"
sleep 2

echo "matando todos los cloudflare"
pkill -f cloudflared
sleep 2

echo "activando el entorno virtual"
# para activar el entorno virtual
source ~/env/bin/activate
sleep 2
echo "entorno virtual activado"

echo "lanzando la app para leer el puerto 8000"
#generando la app lanzada
uvicorn main:app --host 0.0.0.0 --port 8000 --reload > api.log 2>&1 &

echo "app lanzada"
sleep 10

#g
