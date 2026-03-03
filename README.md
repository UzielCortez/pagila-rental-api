# pagila-rental-api
Proyecto base para trabajar con la base de datos de ejemplo **Pagila** usando **PostgreSQL en Docker**.
Actualmente este repositorio deja listo el entorno de base de datos para desarrollo local y, después, un set de consultas SQL para análisis.

## Requisitos
- Docker Desktop
- Docker Compose (incluido en Docker Desktop)
## Estructura del proyecto
```text
pagila-rental-api/
├── app/
│   ├── db.py
│   ├── main.py
│   ├── models.py
│   └── requirements.txt
├── docker/
│   ├── pagila-schema.sql
│   └── pagila-data.sql
├── sql/
│   └── queries.sql
├── docker-compose.yml
└── README.md
```

## Levantar la base de datos
Desde la raíz del proyecto:
```bash
docker compose up -d
```

Esto inicia un contenedor PostgreSQL con:
- Usuario: `postgres`
- Contraseña: `postgres`
- Base de datos: `pagila`
- Puerto local: `5434`

## Inicialización automática de Pagila
Al iniciar por primera vez, Docker ejecuta automáticamente los scripts montados en `docker-entrypoint-initdb.d`:
1. `docker/pagila-schema.sql` → crea la estructura (tablas, relaciones, etc.)
2. `docker/pagila-data.sql` → carga los datos de ejemplo

> Importante: estos scripts se ejecutan automáticamente solo cuando el volumen de datos está vacío.

## Conexión a PostgreSQL
Puedes conectarte con cualquier cliente SQL usando:
- Host: `localhost`
- Puerto: `5434`
- Usuario: `postgres`
- Contraseña: `postgres`
- Base de datos: `pagila`

Ejemplo con `psql`:
```bash
psql -h localhost -p 5434 -U postgres -d pagila
```

## Consultas SQL incluidas (`sql/queries.sql`)
Una vez levantada e inicializada la base con Docker, el archivo `sql/queries.sql` permite ejecutar consultas de análisis, auditoría e integridad sobre Pagila:
1. **Q1**: Top 10 clientes por gasto total (usa `RANK()`)
2. **Q2**: Top 3 películas más rentadas por tienda (usa `ROW_NUMBER()`)
3. **Q3**: Inventario disponible por tienda (CTE)
4. **Q4**: Rentas tardías por categoría y promedio de días de retraso (CTE)
5. **Q5**: Auditoría de pagos sospechosos/duplicados el mismo día
6. **Q6**: Clientes con riesgo por múltiples devoluciones tardías
7. **Q7**: Detección de inventarios con más de una renta activa simultánea

### Ejecutar todas las consultas
Desde la raíz del proyecto:
```bash
docker compose exec -T db psql -U postgres -d pagila < sql/queries.sql
```
Alternativa entrando al contenedor y pegando consultas manualmente:
```bash
docker compose exec db psql -U postgres -d pagila
```

### Ejecutar una consulta específica
1. Abre conexión:
```bash
psql -h localhost -p 5434 -U postgres -d pagila
```

2. Copia y ejecuta solo el bloque `Q1`, `Q2`, etc., según lo que quieras analizar.

## Comandos útiles
Detener servicios:
```bash
docker compose down
```
Detener y eliminar también el volumen de datos (reinicio completo):
```bash
docker compose down -v
```
Ver logs de la base de datos:
```bash
docker compose logs -f db
```

