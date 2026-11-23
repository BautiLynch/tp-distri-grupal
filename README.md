# Wikipedia Article Analysis

Facultad de Ingeniería de la Universidad de Buenos Aires (FIUBA)

| Nombre | Padrón | Email |
| --- | --- | --- |
| Bautista Lynch | 108791 | blynch@fi.uba.ar |
| Joaquín Batemarco | 110222 | jbatemarco@fi.uba.ar |
| Benicio Braunstein | 110126 | bbraunstein@fi.uba.ar |

## Requisitos

- Docker con Docker Compose
- Make y Bash
- Python 3.11 o superior, disponible como `python3` (las herramientas no usan paquetes externos)

## Preparar los datos

```bash
make setup GB=1.5
```

Descarga una muestra del dataset del tamaño indicado y calcula los resultados esperados con una ejecución serial. Genera:

- `datasets/1.jsonl`, `2.jsonl` y `3.jsonl`: una copia del dataset para cada uno de los 3 clientes.
- `test/referencia.json`: resultados esperados para la verificación.

## Ejecutar

```bash
make test
```

Levanta el sistema, muestra los logs, espera a que terminen los clientes, verifica sus resultados contra la referencia y baja los contenedores. El resultado queda en `verificacion.log` y los logs en `ejecucion.log`.

Para activar los logs de debug, ejecutá `make test LOG_LEVEL=DEBUG`.

Cada cliente guarda su salida en `output/cliente_<n>/`:

- `q1.json`, `q3.json`, `q4.json` y `q5.json`: resultados de cada query en JSON.
- `q2/q2.json`: artículos de la Q2. El campo `image` contiene el nombre del archivo de imagen o `null` si no se pudo obtener.
- `q2/img_*`: todas las imágenes recibidas, guardadas en archivos individuales y listas para abrir.

### Paso a paso

```bash
make up
```

```bash
make logs
```

```bash
make verify
```

```bash
make down
```

Ejecutá `make verify` cuando hayan terminado los clientes.

### Chaos monkey

Con el sistema levantado, en otra terminal:

```bash
make chaos
```

Mata workers al azar cada `CHAOS_INTERVAL` segundos para probar la tolerancia a fallas. Se configura con las variables `CHAOS_*` del `Makefile`. Ver [tools/chaos_monkey.md](tools/chaos_monkey.md).

## Escalar

Para cambiar la cantidad de instancias de un worker o de clientes:

```bash
python3 tools/instancias.py <servicio> <cantidad> --compose-file docker-compose.yaml
```

`<servicio>` puede ser `filter`, `image_getter`, `splitter`, `filter_language`, `aggregation`, `filter_year`, `average`, `filter_average`, `client` o `todos` (todos los workers, sin clientes).

- `filter_language` y `aggregation` siempre se escalan juntos.
- `rabbitmq`, `gateway`, `handler` y `join` tienen una sola instancia.
- El comando modifica `docker-compose.yaml`, pero no inicia contenedores.
- Los clientes que se agregan copian la configuración de `client_0`, así que leen `datasets/1.jsonl`.
