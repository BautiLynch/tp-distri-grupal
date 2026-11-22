# Chaos Monkey

Script que mata servicios de workers al azar cada cierto tiempo, para probar que el sistema tolera caídas: el monitor tiene que detectar que el worker murió, levantarlo de nuevo y el procesamiento tiene que terminar con los mismos resultados que sin fallas.

## Uso

Con el sistema levantado, en otra terminal:

```bash
make chaos
```

Corre hasta que se corta con `Ctrl+C`.

Ejemplo de prueba completa con la corrida larga (~5 min), para que haya caídas mientras se procesan datos:

```bash
docker compose -f docker-compose.yaml -f docker-compose.largo.yaml up --build --detach
make chaos                       # en otra terminal
python3 tools/verificar.py --referencia test/referencia-largo.json --compose docker-compose.yaml --wait
```

Si la verificación da OK con workers muriendo en el medio, la tolerancia a fallas funciona.

## Cómo funciona

El script no habla con Docker directamente sino con Docker Compose, que ya sabe qué servicios son del TP. En cada ronda:

1. **Lista servicios corriendo**: `docker compose ps --services --status running` devuelve los nombres de los servicios del compose que están vivos (`filter_0`, `join`, `rabbitmq`...).
2. **Filtra por objetivo**: se queda con los que pertenecen a algún grupo de `CHAOS_TARGETS`. Para eso le saca el número de réplica al nombre (`filter_year_0` → `filter_year`). Así el grupo `filter` incluye `filter_0`, `filter_1`, etc., pero **no** `filter_year_0`.
3. **Elige víctimas**: elige al azar `CHAOS_KILLS_PER_ROUND` servicios entre los candidatos (o todos, si hay menos).
4. **Las mata con `SIGKILL`**: `docker compose kill -s SIGKILL <servicios>`. Es una caída abrupta, sin cierre ordenado, y el contenedor queda con código de salida `137`.

Espera `CHAOS_INTERVAL` segundos y repite.

### Recuperación

El chaos monkey solo mata. La recuperación la hace el monitor:

```
make chaos ──kill──▶ filter_1          (deja de mandar heartbeats)
                        │
monitor_0 ◀── heartbeats ┘  pasado HEARTBEAT_INTERVAL × HEARTBEAT_ALLOWED_MISSES
    │                       sin heartbeats, lo da por caído
    └──restart──▶ filter_1  (vuelve a levantar y retoma desde su storage
                             y los mensajes sin ack de RabbitMQ)
```

Con los valores actuales del monitor (`HEARTBEAT_INTERVAL=5`, `HEARTBEAT_ALLOWED_MISSES=3`), un worker caído se reinicia a los ~15 segundos.

Por eso solo conviene poner como objetivo servicios que **mandan heartbeats**: si se mata un servicio que el monitor no vigila, queda caído para siempre.

| Servicio | ¿Manda heartbeats? | ¿Es objetivo por defecto? |
| --- | --- | --- |
| `filter`, `filter_language`, `filter_year`, `filter_average`, `image_getter`, `splitter`, `aggregation`, `average`, `join` | Sí | Sí |
| `gateway`, `handler`, `client_*` | No | No |
| `rabbitmq` | No | No |
| `monitor_*` | No (todavía no hay varios monitores que se revivan entre sí) | No |

## Configuración

Las variables están al principio del [`Makefile`](../Makefile):

```makefile
export CHAOS_INTERVAL ?= 15
export CHAOS_KILLS_PER_ROUND ?= 1
export CHAOS_START_DELAY ?= 0
export CHAOS_TARGETS ?= filter,filter_language,filter_year,filter_average,image_getter,splitter,aggregation,average,join
export CHAOS_SEED ?=
```

| Variable | Default | Descripción |
| --- | --- | --- |
| `CHAOS_INTERVAL` | `15` | Segundos entre rondas. Con `0` (o menos) no mata nada. |
| `CHAOS_KILLS_PER_ROUND` | `1` | Cantidad de servicios a matar en cada ronda. Con `0` (o menos) no mata nada. |
| `CHAOS_START_DELAY` | `0` | Segundos a esperar antes de la primera ronda. |
| `CHAOS_TARGETS` | todos los workers + `join` | Grupos de servicios que puede matar, separados por coma. También acepta una réplica puntual, como `filter_year_1`. |
| `CHAOS_SEED` | vacío | Semilla del generador aleatorio. Con la misma semilla y los mismos servicios vivos, elige las mismas víctimas; sirve para reproducir una corrida. |

Se pueden cambiar en el Makefile o pisar al invocar, sin editar nada:

```bash
# Más agresivo: 2 servicios cada 5 segundos
make chaos CHAOS_INTERVAL=5 CHAOS_KILLS_PER_ROUND=2

# Solo splitters y aggregations
make chaos CHAOS_TARGETS=splitter,aggregation

# Una réplica puntual
make chaos CHAOS_TARGETS=filter_year_1
```

> Si se baja `CHAOS_INTERVAL` por debajo del tiempo de detección del monitor (~15s), se van a acumular varios workers caídos al mismo tiempo. Puede servir como prueba de estrés, pero el sistema va a avanzar más lento.

## Ver qué está pasando

El script loguea cada ronda en su propia terminal:

```
Chaos monkey started: interval=15.0s kills_per_round=1 start_delay=0.0s targets=[...]
Killed filter_1
Killed join
```

Y los reinicios del lado del monitor:

```bash
docker compose logs -f monitor_0 | grep -v "Received heartbeat"
```

```
filter_1 heartbeat exceeded
Restarting filter_1 (container 58d9616e4555)
```
