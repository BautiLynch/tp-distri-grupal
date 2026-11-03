docker compose --progress quiet up --build --quiet-build --menu=false --remove-orphans 2>&1 | tee ejecucion.log

python tools/instancias.py client 1 --compose-file docker-compose.yaml

python3 scripts/download_wikipedia.py --articles 100000