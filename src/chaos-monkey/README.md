# Chaos Monkey

Servicio que mata contenedores de workers al azar cada cierto tiempo, para probar que el sistema tolera caídas: el monitor tiene que detectar que el worker murió, levantarlo de nuevo y el procesamiento tiene que terminar con los mismos resultados que sin fallas.

## Cómo funciona

El chaos monkey corre en su propio contenedor (`chaos_monkey` en el `docker-compose.yaml`) y monta el socket de Docker (`/var/run/docker.sock`), igual que el monitor. Eso le permite listar y matar otros contenedores usando la librería `docker` de Python.

En cada ronda:

1. **Lista candidatos**: toma los contenedores en estado `running` del mismo proyecto de Compose que él (label `com.docker.compose.project`). Nunca toca contenedores de otros proyectos.
2. **Filtra por objetivo**: se queda con los que tienen un nombre de servicio (label `com.docker.compose.service`) que pertenece a algún grupo de `CHAOS_TARGETS`. Un grupo `filter` matchea `filter`, `filter_0`, `filter_1`, etc., pero **no** `filter_year_0`.
3. **Elige víctimas**: elige al azar `CHAOS_KILLS_PER_ROUND` contenedores entre los candidatos (o todos, si hay menos).
4. **Las mata con `SIGKILL`** (`docker kill`). Es una caída abrupta, sin cierre ordenado, y el contenedor queda con código de salida `137`.

Espera `CHAOS_INTERVAL` segundos y repite. Al recibir `SIGTERM` (por ejemplo con `docker compose down`) termina enseguida.

### Recuperación

El chaos monkey solo mata. La recuperación la hace el monitor:

```
chaos_monkey ──kill──▶ filter_1          (deja de mandar heartbeats)
                          │
monitor_0 ◀── heartbeats ─┘  pasado HEARTBEAT_INTERVAL × HEARTBEAT_ALLOWED_MISSES
    │                        sin heartbeats, lo da por caído
    └──restart──▶ filter_1   (vuelve a levantar y retoma desde su storage
                              y los mensajes sin ack de RabbitMQ)
```

Con los valores actuales del monitor (`HEARTBEAT_INTERVAL=5`, `HEARTBEAT_ALLOWED_MISSES=3`), un worker caído se reinicia a los ~15 segundos.

## Configuración

Se configura con variables de entorno en el servicio `chaos_monkey` del `docker-compose.yaml`:

| Variable | Default | Descripción |
| --- | --- | --- |
| `CHAOS_INTERVAL` | `15` | Segundos entre rondas. Con `0` (o menos) el chaos monkey queda deshabilitado. |
| `CHAOS_KILLS_PER_ROUND` | `1` | Cantidad de contenedores a matar en cada ronda. |
| `CHAOS_START_DELAY` | `10` | Segundos a esperar antes de la primera ronda, para que el sistema termine de levantar. |
| `CHAOS_TARGETS` | todos los workers + `join` | Grupos de servicios que puede matar, separados por coma. |
| `CHAOS_SEED` | sin definir | Semilla del generador aleatorio. Con la misma semilla y los mismos contenedores, elige las mismas víctimas; sirve para reproducir una corrida. |
| `LOG_LEVEL` | `INFO` | Nivel de log. |

Configuración actual en el compose:

```yaml
  chaos_monkey:
    build:
      context: ./src
      dockerfile: chaos-monkey/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
        required: true
    environment:
      CHAOS_INTERVAL: "15"
      CHAOS_KILLS_PER_ROUND: "1"
      CHAOS_START_DELAY: "10"
      CHAOS_TARGETS: filter,filter_language,filter_year,filter_average,image_getter,splitter,aggregation,average,join
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
      PYTHONUNBUFFERED: "1"
    volumes:
      - source: /var/run/docker.sock
        target: /var/run/docker.sock
        type: bind
```

### Ejemplos

Más agresivo: matar 2 contenedores cada 5 segundos.

```yaml
      CHAOS_INTERVAL: "5"
      CHAOS_KILLS_PER_ROUND: "2"
```

Matar solo splitters y aggregations:

```yaml
      CHAOS_TARGETS: splitter,aggregation
```

Matar solo una réplica puntual (el nombre exacto del servicio también funciona como grupo):

```yaml
      CHAOS_TARGETS: filter_year_1
```

Deshabilitarlo sin sacarlo del compose:

```yaml
      CHAOS_INTERVAL: "0"
```

> Si se baja mucho `CHAOS_INTERVAL` por debajo del tiempo de detección del monitor (~15s), se van a acumular varios workers caídos al mismo tiempo. Puede servir como prueba de estrés, pero el sistema va a avanzar más lento.

## Uso

Se levanta junto con el resto del sistema, no hace falta nada extra:

```bash
make
```

Para ver solo lo que hace:

```bash
docker compose logs -f chaos_monkey
```

```
Chaos monkey started: project=tp-distri-grupal interval=15.0s kills_per_round=1 start_delay=10.0s targets=[...]
Killed filter_1 (container 58d9616e4555)
Killed join (container 7742ebbf8ac6)
```

Y para ver los reinicios correspondientes del lado del monitor:

```bash
docker compose logs -f monitor_0 | grep -v "Received heartbeat"
```

```
58d9616e4555 heartbeat exceeded
Restarting container 58d9616e4555
```

Para confirmar que el sistema toleró las caídas, `make` compara la salida de los clientes contra `test/referencia.json` igual que sin chaos monkey: si la verificación pasa con contenedores muriendo, la tolerancia a fallas funciona.

## Archivos

- [`main.py`](main.py): lógica del chaos monkey.
- [`Dockerfile`](Dockerfile): imagen `python:3-alpine` con la librería `docker`.
- Servicio `chaos_monkey` en [`docker-compose.yaml`](../../docker-compose.yaml).

Si se regenera el compose con `tools/instancias.py`, el servicio `chaos_monkey` se conserva tal cual (solo se replican los workers y los clientes).
