Necesito que crees un proyecto Python completo llamado **personal-health-ledger**.
Primera etapa (MVP): consolidar, auditar y comparar datos de **weight** exportados por la app **Health Sync** en **Google Drive** (carpeta “Health Sync Weight”), integrando pares de archivos **CSV** y **FIT**.

El proyecto debe ser **ejecutable**, **modular**, con arquitectura por capas (DDD-ish), **tipado estático** (mypy/pyright), **docstrings** en todos los módulos/clases/funciones, **PEP8** (máx 100 chars/line) y **sin parámetros hardcodeados** (usar un `ParameterLoader`).
Incluir tests con **pytest** (prohibido `assert`; usar `if ...: raise AssertionError("...")`).

### Visión futura (diseño extensible, sin implementarlo ahora)

El diseño debe permitir crecer a otros dominios (`activities`, `sleep`) y nuevas fuentes/apps (no solo Drive/Health Sync).
Por eso, separa claramente:

* **domain**: modelos canónicos + metadata + reglas
* **infrastructure**: clientes externos (Drive), parsers (CSV/FIT)
* **services**: pipeline, consolidación, comparación
* **cli**: orquestación
* **utils**: config, logging, hashing, timezones, errores

---

# REQUISITO CRÍTICO DE TRAZABILIDAD / DATA LINEAGE (NO OMITIR)

Todo dataset resultante **DEBE** persistir **lineage explícito** por registro.

Cada registro consolidado debe incluir como mínimo:

* `record_id`: clave determinística
* `source_files`: lista de nombres completos de archivos origen que contribuyeron al registro
* `source_types`: subconjunto de `{"csv","fit"}` usados
* `drive_file_ids`: lista de IDs de Drive de los archivos fuente
* `ingestion_timestamp`: timestamp (tz-aware) del procesamiento
* `field_sources`: mapa `{field_name: "csv"|"fit"|"merged"|"conflict"}` (o equivalente)
* Si hay conflicto CSV vs FIT:

  * `conflicting_fields`: lista
  * `chosen_source`: `"csv"|"fit"|null` (según política configurable)
  * conservar ambos valores en columnas separadas `*_csv` y `*_fit`

Esta trazabilidad debe preservarse en:

* `output/weight_consolidated.csv`
* `output/weight_consolidated.parquet`
* `output/conflicts.csv`
* `output/comparison_summary.json`

Nota importante: CSV no soporta listas/dicts nativos. Para `output/*.csv`:

* serializa `source_files`, `source_types`, `drive_file_ids`, `conflicting_fields` como JSON string
* `field_sources` también como JSON string
  En Parquet deben persistir como tipos nativos si es posible (list/map) o como JSON si se prefiere uniformidad.

---

# Contexto de datos (primera etapa: weight)

En Google Drive existe una carpeta `"Health Sync Weight"` con exportaciones mensuales:

* CSV: `"Peso <m>-<yyyy> Huawei Health.csv"`
* FIT: `"Peso <m>-<yyyy> Huawei Health.fit"`

CSV: columnas variables por mes/idioma. Ejemplos:

* `Fecha`, `Hora`, `Peso`, `Porcentaje de grasa corporal`, `Masa de grasa corporal`,
  `Porcentaje libre de grasa`, `Masa libre de grasa`, `Porcentaje de músculo esquelético`,
  `Masa de músculo esquelético`, `Porcentaje de masa muscular`, `Masa muscular`,
  `La masa ósea`, `Agua corporal total`, `Tasa metabólica base`

FIT: contiene mínimo `timestamp` y `weight` (quizá más campos).

Puede haber duplicados, faltantes y discrepancias CSV vs FIT.

---

# Objetivos funcionales (MVP weight)

## 1) Integración Google Drive (infra/drive_client)

Implementar autenticación:

* OAuth2 installed app
* Service Account (seleccionable por config)

Configurable vía `config/config.yaml`:

* `folder_id` o `folder_name`
* `scopes`
* paths de credenciales
* directorio de cache local
* output paths
* opción recursivo (por defecto false)

Descargar a `data/raw/` y guardar metadata por archivo:

* `drive_file_id`, `name`, `mimeType`, `modifiedTime`, `md5Checksum`
  Evitar redescargas si el `md5Checksum` coincide con lo ya descargado.
  Mantener un índice local (ej. `data/raw/index.json`) para mapear `drive_file_id -> local_path + checksum + modifiedTime`.

## 2) Ingesta y normalización (parsers + domain schema)

### Canonical schema (dominio weight)

Normalizar a columnas en inglés (float32 cuando aplique):

* `timestamp` (tz-aware)
* `weight_kg`
* `body_fat_pct`
* `fat_mass_kg`
* `fat_free_pct`
* `fat_free_mass_kg`
* `skeletal_muscle_pct`
* `skeletal_muscle_mass_kg`
* `muscle_pct`
* `muscle_mass_kg`
* `bone_mass_kg`
* `body_water`
* `bmr_kcal`

CSV parser robusto:

* detectar separador (`,` o `;`)
* soportar UTF-8 y latin-1
* normalizar nombres de columnas (incluye español)
* convertir numéricos de forma segura (manejar coma decimal)
* construir `timestamp` tz-aware desde `Fecha` + `Hora` cuando aplique
* si ya viene timestamp, preferirlo

FIT parser:

* usar `fitparse` (inclúyela en requirements)
* extraer registros de peso y mapear al schema canónico
* `timestamp` tz-aware

Cada fila parseada **DEBE** incluir metadata base:

* `source_file_name`
* `source_file_id`
* `source_type` en `{"csv","fit"}`
  (esto luego se agregará en el consolidado final como lineage por registro)

## 3) Consolidación + dedupe (services/consolidation)

* Generar `record_id` determinístico configurable:

  * default: hash de (`timestamp_rounded_to_minute`, `weight_kg`, `source_types_set`)
  * permitir configurar rounding, campos y algoritmo (sha256)
* Merge CSV + FIT por timestamp con tolerancia configurable (default 60s):

  * si un campo está solo en uno, conservarlo
  * si está en ambos y es igual (con tolerancia numérica configurable), marcar `field_sources[field]="merged"` o “verified”
  * si difiere, registrar conflicto:

    * mantener `field_csv` y `field_fit`
    * set `field_sources[field]="conflict"`
    * agregar a `conflicting_fields`
    * elegir `chosen_source` según política (configurable) solo si aplica a una salida “resolved”; en el dataset principal conservar ambos

El dataset consolidado final debe incluir:

* campos de dominio canónicos
* columnas `*_csv`, `*_fit` cuando corresponda
* lineage completo requerido (listas/mapas, serialización según formato)

## 4) Comparación CSV vs FIT (services/comparison)

Para cada par (por patrón de nombre o ventana temporal):

* comparar por timestamp con tolerancia configurable
* calcular:

  * solo CSV
  * solo FIT
  * ambos
  * mismatches por campo
  * métricas (ej. MAE para weight_kg)

El reporte debe incluir por par:

* `csv_file_name`, `fit_file_name`
* `drive_file_ids`
* rango temporal cubierto (min/max timestamps)
* conteos y métricas

Agregar resumen mensual/año.

## 5) Salidas (output)

Generar:

* `output/weight_consolidated.csv`
* `output/weight_consolidated.parquet`
* `output/conflicts.csv` (filas conflictivas + detalle)
* `output/comparison_summary.json`
* `output/ingestion_log.jsonl` (eventos, errores, stats)

Todos deben conservar trazabilidad.

## 6) CLI (Typer recomendado)

Comandos:

* `sync`: lista y descarga desde Drive
* `build`: parse + consolidate (requiere raw descargado)
* `compare`: comparación sin consolidación
* `all`: pipeline completo

Flags:

* `--folder-id` / `--folder-name` (override config)
* `--timezone`
* `--tolerance-seconds`
* `--output-format` (csv/parquet/both)

## 7) Configuración centralizada (utils/parameters.py)

* `config/config.yaml`
* `ParameterLoader` con dataclasses o Pydantic (preferible Pydantic)
* Nada hardcodeado: rutas, TZ, tolerancias, algoritmo hash, scopes, etc.

## 8) Calidad y tooling

* logging estándar (configurable)
* excepciones propias (domain + infra)
* `pyproject.toml` con ruff/flake8 + mypy + pyright config
* `requirements.txt`
* GitHub Actions: lint + typecheck + tests

## 9) Tests (pytest)

Tests unitarios mínimos:

* normalización de columnas CSV (incluye español)
* parsing FIT (fixture mínimo)
* merge/dedupe + comparación
* validaciones de lineage:

  * ningún registro consolidado carece de `source_files`
  * conflictos preservan ambos valores y `conflicting_fields`
  * `drive_file_ids` presentes cuando proviene de Drive

Prohibido `assert`; usar `raise AssertionError`.

---

# Entrega esperada

* Mostrar árbol de archivos del proyecto.
* Proveer contenido completo de archivos principales (no pseudocódigo), incluyendo:

  * `README.md` con:

    * cómo crear credenciales OAuth2 y/o Service Account
    * cómo configurar `config.yaml`
    * cómo ejecutar end-to-end la CLI
* Estructura sugerida:

```
personal-health-ledger/
  src/personal_health_ledger/
    domain/
    infrastructure/
      drive_client/
      parsers/
    services/
    cli/
    utils/
  config/
  data/
    raw/
  output/
  tests/
  pyproject.toml
  requirements.txt
  .github/workflows/ci.yml
  README.md
```

---

## Notas de implementación (importante)

* Evitar members protegidos.
* Usar pandas + pyarrow para Parquet.
* Vectorizar y usar float32 donde sea viable.
* Sin prints: usar logging.
* Docstrings en todos los módulos/clases/funciones.
* Mantener líneas < 100 caracteres.
* Manejo robusto de errores: exceptions propias + logs.