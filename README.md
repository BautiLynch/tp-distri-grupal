# tp-distri-grupal

## Correr el programa

### 0. Limpiar contenedores

```shell
docker compose --profile dos down -v --remove-orphans
```

### 1. Build + levantar el sistema

```shell
docker compose up -d --build
docker compose ps
```

### 2. Esperar a los 4 clientes

```shell
docker compose wait client_v1_a client_v1_b client_v2_a client_v2_b
```

### 3. Verificar resultados contra los datasets

```shell
py -3 tools/verificar.py --dataset datasets/wikipedia-1000.jsonl    output/v1_cliente_a.bin output/v1_cliente_b.bin

py -3 tools/verificar.py --dataset datasets/wikipedia-1000-v2.jsonl output/v2_cliente_a.bin output/v2_cliente_b.bin
```

### 4. Ver duraciones / estado de la corrida

```shell
docker compose logs --no-color client_v1_a client_v1_b client_v2_a client_v2_b | Select-String "duration|status"
docker compose logs --no-color 2>&1 | Select-String "Traceback|ERROR|CRITICAL"
```

### 5. Bajar todo al terminar

```shell
docker compose down -v
```

## Variante

### Corrida de 2 clientes (profile "dos"): client_0 y client_1

```shell
docker compose --profile dos up -d --build client_0 client_1
docker compose wait client_0 client_1
```
