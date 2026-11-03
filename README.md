docker compose --progress quiet up --build --quiet-build --menu=false --remove-orphans 2>&1 | tee ejecucion.log

python tools/instancias.py client 1 --compose-file docker-compose.yaml

python3 scripts/download_wikipedia.py --articles 100000

python3 tools/verificar.py --referencia output/referencia.json output/output_0.bin


Ejecucion serial: python3 tools/serial.py datasets/wikipedia-250000.jsonl --output test/referencia-250000.json