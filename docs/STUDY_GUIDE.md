# Study Guide — banking-engagement-warehouse

Complementa [`RUNBOOK.md`](RUNBOOK.md) (cómo correrlo) y
[`LEARNING_BUILD.md`](LEARNING_BUILD.md) (cómo se construyó). Esta guía es
para entender cada componente, cómo interactúan, qué pasa cuando algo falla
y cómo defender cada número del README en una entrevista técnica.

Todas las rutas son relativas a la raíz del repo. Toda métrica citada está
en la tabla "Measured in this repo" del `README.md`.

---

## 1. Resumen en 3 niveles

**Una frase:** Warehouse dimensional de engagement bancario con historia
SCD Type 2 y gates de calidad que **bloquean** la promoción en vez de solo
alertar.

**30 segundos (EN):** *"A dimensional engagement warehouse — bronze, silver,
gold on plain Parquet — with SCD Type 2 customer history and six data
quality gates that block a month's promotion to silver instead of just
reporting a problem after the fact. A blocked month leaves a real,
intentional gap in the warehouse rather than silently letting bad data
through, and full reprocessing of the same bronze data is byte-identical,
verified by hash comparison."*

**2 minutos (EN):** *"Bank engagement analytics break silently when a
customer's segment changes and the dimension just overwrites history —
last quarter's cohort report becomes unreproducible with nobody alerted. I
built a bronze-to-gold pipeline where six quality gates (required fields,
duplicates, referential integrity, range outliers, cardinality drift,
freshness) run before a month is allowed into silver. A single failing
gate blocks that month's promotion and revokes any stale silver output
left over from a previous successful run — I found that second part by
deliberately corrupting an already-promoted month and re-running: the
block worked, but old silver data for that month was still sitting there
until I added the revocation step. Gold builds dim_customer as true SCD
Type 2 using window functions over the full promoted history, verified
byte-identical on full reprocess. I also built tag-based per-pipeline cost
attribution and SLA breach alerting with a DynamoDB-conditional-write dedup
gate, the same idempotency pattern as the fintech repo's transaction gate,
applied here to alerting instead of money."*

---

## 2. Mapa de componentes

| Componente | Archivo | Responsabilidad | Input → Output | Por qué existe / por qué esa tecnología |
|---|---|---|---|---|
| Generador de eventos | `src/ingestion/data_gen.py` | Sintetiza eventos de engagement (login, card_txn, offer_shown, offer_redeemed) con cambios de segmento a lo largo del tiempo | `--months --customers` → JSONL por mes | Sin cambios de segmento inyectados no hay nada que probar en SCD2 |
| Upload a bronze | `src/ingestion/upload_bronze.py` | Sube cada mes generado a `bank-bronze` | JSONL local → objetos S3 | Bronze es la capa cruda sin transformar, punto de partida de todo reproceso |
| Gates de calidad | `src/models/gates.py` | 6 funciones puras `(spark, df) -> GateResult`: campos requeridos, duplicados, integridad referencial, outliers de rango, deriva de cardinalidad (>30pp), freshness | DataFrame de un mes → lista de resultados pass/fail | Cada gate es una función aislada y testeable — no un script monolítico de validación |
| Orquestador | `src/transformation/pipeline.py` | Corre gates → silver → gold para cada mes en bronze; un mes que falla gates **no** se promueve y revoca silver previo de ese mes | Meses en bronze → `PipelineResult` (promovidos, bloqueados, filas escritas) | Es el único punto que decide qué meses llegan a silver/gold — la garantía de "gates bloquean" vive aquí, no en cada gate individual |
| Silver | `src/transformation/silver.py` | Limpia y tipa un mes de bronze | Bronze JSON de un mes → JSON limpio en `bank-silver/clean/` | Separa "datos que pasaron los gates" de "datos crudos" |
| Gold — dim_customer | `src/transformation/gold.py` | Construye la dimensión SCD Type 2 vía window functions sobre transiciones de segmento | Silver de meses promovidos → Parquet `dim_customer` | Reproceso completo, no incremental — ver ADR 0001 |
| Gold — facts/dim_offer | `src/transformation/facts.py` | Construye `fact_engagement_daily` (grano cliente×día×tipo de evento) y `dim_offer` | Silver de meses promovidos → Parquet | Antes era un CLI standalone que nadie en el run orquestado llamaba — el propio código documenta ese bug: `pipeline.py` producía solo `dim_customer` aunque el README describía 3 tablas gold |
| Catálogo | `src/transformation/catalog.py` | Registra `dim_customer`/`fact_engagement_daily`/`dim_offer` en el Glue Data Catalog | Esquema Spark → tablas en Glue | Demuestra el catálogo real, con el mismo esquema que Spark escribió — no un mock |
| Costo y SLA | `src/orchestration/cost_sla.py` | Atribuye costo por pipeline (tag-based) y detecta breach de SLA con dedup de alertas | Bytes procesados + duración → costo en DynamoDB; breach → alerta SNS deduplicada | El mismo patrón de `PutItem` condicional del gate de fintech, aplicado a "no mandar dos alertas por el mismo breach" |
| Warehouse | `src/utils/warehouse.py` | DuckDB + `httpfs` leyendo Parquet gold directo de S3, stand-in de Redshift | Consulta SQL → filas | Ver ADR 0003: Athena sobre MiniStack devuelve un mock hardcodeado, DuckDB sí ejecuta la consulta real |
| API de serving | `src/serving/api.py` | Flask: retención de cohortes, estado de SLA, costo por pipeline | HTTP GET → JSON | Expone lo que un analista o un dashboard consultaría en producción |
| Terraform Azure | `terraform/azure/main.tf` | Stub de exportación (ADLS + Data Factory), solo `validate`/`plan` | — | Demuestra literacia de IaC y modelado correcto de recursos, no un segundo cloud funcionando — el propio README lo aclara |

---

## 3. Recorrido de un evento

```
data_gen.py → JSONL por mes (logins, card_txn, offer_shown, offer_redeemed,
              con cambios de segmento a lo largo del tiempo)
      |
      v
upload_bronze.py → S3 bank-bronze/month=NN.jsonl
      |
      v
pipeline.py, por cada mes en bronze (en orden):
      |
      +--> gates.py::run_all_gates(mes)
      |         |
      |     +---+---+
      |     v       v
      |   pasan   falla alguno
      |     |       |
      |     |       v
      |     |   publish_gate_failure() -> SNS "quality-alerts"
      |     |   revoke_silver_promotion() -> borra silver/rejects previos de ESE mes
      |     |   mes va a "blocked", NO se promueve
      |     v
      |   silver.py::clean_month() -> S3 bank-silver/clean/month=NN/
      |   mes va a "promoted"
      |
      v (solo sobre los globs de meses PROMOVIDOS, todos juntos)
gold.py::build_dim_customer()       -> S3 bank-gold/dim_customer/     (SCD2)
facts.py::build_fact_engagement_daily() -> S3 bank-gold/fact_engagement_daily/
facts.py::build_dim_offer()         -> S3 bank-gold/dim_offer/
      |
      v
cost_sla.py::check_sla(run_id)  -> si excede SLA_SECONDS=180, alerta SNS deduplicada
cost_sla.py::record_pipeline_run() -> costo en DynamoDB pipeline-cost

api.py :: GET /cohort            lee dim_customer vía DuckDB
api.py :: GET /sla/status        cuenta breaches en sla-alert-dedup
api.py :: GET /cost/by-pipeline  lee pipeline-cost
```

**El punto que hay que poder explicar sin dudar:** `dim_customer` se
reconstruye **desde cero** en cada corrida, usando **todos** los meses
promovidos juntos (no incrementalmente mes a mes) — es la garantía de
"reproceso completo = byte-idéntico" que exige ADR 0001, a costa de que el
compute escale linealmente con los meses.

---

## 4. Interacciones y contratos entre componentes

| Frontera | Garantía | Por qué |
|---|---|---|
| `gates.py` → `pipeline.py` | Un solo gate fallido bloquea la promoción completa del mes | ADR 0002: calidad "en el dashboard" llega tarde — gold ya estaría sucio para cuando alguien lo viera |
| Mes bloqueado → silver | El silver previo de ese mes se **revoca** (se borra), no se deja obsoleto | Encontrado corrompiendo manualmente un mes ya promovido y re-corriendo: el bloqueo funcionaba, pero el silver viejo seguía ahí hasta que se agregó `revoke_silver_promotion()` |
| Silver promovido → gold | `dim_customer` se reconstruye completo sobre todos los meses promovidos, no incrementalmente | Es la garantía de reproceso byte-idéntico — un `MERGE INTO` incremental sería correcto en producción a escala, pero aquí el dataset cabe en `local[2]` y el reproceso total prueba una garantía más fuerte |
| `check_sla` → SNS | Un segundo breach del mismo `pipeline_id#run_date` no re-alerta | `PutItem` condicional sobre `sla-alert-dedup`, mismo patrón que el gate de idempotencia de fintech |
| Catálogo Glue | El esquema se copia a mano desde los builders de Spark, a propósito | Detecta *drift* entre lo que Spark realmente escribe y lo que el catálogo declara — si alguien cambia un builder sin actualizar `catalog.py`, la discrepancia es visible, no silenciosa |

---

## 5. Modos de falla — "¿qué pasa si...?"

| Si esto pasa... | ...entonces | Dónde se ve / se prueba |
|---|---|---|
| Un mes tiene un `customer_id` que no existe en el set conocido | `gate_referential_integrity` falla, el mes no se promueve | `src/models/gates.py::gate_referential_integrity` |
| Un mes tiene `event_id` duplicados | `gate_duplicates` falla | `src/models/gates.py::gate_duplicates` |
| La distribución de segmentos cambia más de 30 puntos porcentuales mes a mes | `gate_cardinality_drift` falla | `src/models/gates.py::gate_cardinality_drift` |
| Un mes ya promovido se re-corre y ahora falla un gate (dato corrompido) | El silver previo de ese mes se borra explícitamente — no queda un mes "fantasma" promovido con datos viejos | `revoke_silver_promotion()`, documentado en el propio docstring como un bug real encontrado y corregido |
| Se re-corre `pipeline.py` sobre el mismo bronze | `dim_customer` debe salir byte-idéntico, sin rangos de validez solapados | `tests/data_quality/test_scd2_backfill.py::test_scd2_backfill_is_byte_identical_on_reprocess` y `test_no_overlapping_validity_ranges_per_customer` |
| Dos breaches de SLA ocurren el mismo día para el mismo pipeline | Solo se envía 1 alerta, la segunda se deduplica | `tests/integration/test_cost_sla.py::test_sla_breach_sends_one_alert_then_dedups` |
| Se intenta correr una consulta real contra Athena sobre MiniStack | `get-query-results` devuelve un mock hardcodeado (`{"result": "mock_value"}`), nunca las filas reales | ADR 0003, README "Emulated vs. real" |
| Ningún mes se promueve en una corrida | `gold_rows_written`, `fact_engagement_daily_rows_written` y `dim_offer_rows_written` quedan en 0 — gold no se toca | `pipeline.py::run_pipeline`, rama `else` |

---

## 6. Conceptos senior — qué son, cómo aparecen aquí, cuándo NO usarlos

### Quality gates que bloquean vs. que solo alertan
**Qué es:** decidir si un chequeo de calidad impide que el dato avance a la
siguiente capa, o solo genera una notificación mientras el dato sigue su
curso.
**Aquí:** los 6 gates de `gates.py` corren **antes** de escribir a silver;
un fallo bloquea la promoción del mes completo.
**Trade-off:** un hueco real e intencional en el warehouse (un mes
bloqueado) a cambio de nunca dejar pasar datos sucios a gold.
**Cuándo NO usarlo:** cuando el grano del dato permite cuarentena parcial
en vez de bloquear todo un período — ADR 0002 señala explícitamente que
fintech cuarentena por evento individual porque su grano es la transacción;
aquí el grano es el mes de carga bancaria, así que bloquear el mes es la
unidad correcta, no una fila.
**Cómo escalaría:** el mismo principio es la base de un "circuit breaker"
de datos en cualquier orquestador de producción (Airflow sensors, dbt
tests que fallan el build).

### SCD Type 2 con reproceso completo (no incremental)
**Qué es:** mantener el historial completo de cambios de un atributo
dimensional (aquí, el segmento del cliente) con `valid_from`/`valid_to`,
reconstruyendo la tabla completa en cada corrida en vez de aplicar un
`MERGE` incremental.
**Aquí:** `gold.py::build_dim_customer`, window functions (`lag`, `lead`)
sobre transiciones de segmento, ordenamiento determinista explícito para
evitar empates inconsistentes entre corridas.
**Trade-off:** costo de compute que escala linealmente con los meses, a
cambio de la garantía más fuerte posible de reproducibilidad — byte-
idéntico, verificado por hash.
**Cuándo NO usarlo:** a escala de producción real (miles de millones de
filas), un `MERGE INTO` incremental es la respuesta correcta — el propio
ADR 0001 lo dice explícitamente, y aclara que el reproceso total aquí es
una elección de este repo a esta escala, no una recomendación universal.
**El detalle que hay que decir sin que lo pregunten:** el ordenamiento
determinista en empates de `valid_from` (`dedup_window`) no es cosmético —
sin él, dos corridas del mismo input podrían asignar `valid_from`/`valid_to`
distintos en un empate, rompiendo la garantía de byte-idéntico.

### Idempotencia aplicada a alertas, no a dinero
**Qué es:** el mismo patrón de `PutItem` condicional usado en fintech para
evitar liquidaciones duplicadas, aplicado aquí a evitar alertas
duplicadas.
**Aquí:** `cost_sla.py::check_sla` — `ConditionExpression:
attribute_not_exists(alert_id)` sobre `sla-alert-dedup`, key
`{pipeline_id}#{run_date}`.
**Por qué importa en entrevista:** mostrar que el mismo patrón de
idempotencia se reconoce y reaplica en un dominio distinto (alerting en vez
de transacciones) es una señal de que se entendió el principio, no solo se
copió el código.

### Atribución de costo por tags, no calculada globalmente
**Qué es:** cada corrida de pipeline reporta su propio uso de recursos
(bytes procesados, segundos de duración) bajo un `pipeline_id`, en vez de
calcular un costo total y repartirlo con una regla arbitraria.
**Aquí:** `cost_sla.py::record_pipeline_run` — costo = bytes procesados ×
$/GB, guardado por `pipeline_id#run_date`.
**Límite honesto:** `COST_PER_GB = 0.023` es un modelo de costo de la era
LocalStack/MiniStack, no un precio real de AWS — el propio comentario en
el código lo dice.
**Cuándo NO usarlo:** si los pipelines comparten recursos de forma que no
se puede atribuir limpiamente el uso a uno solo (ej. un cluster compartido
sin tagging granular), la atribución por tags requiere que cada pipeline
reporte honestamente su propio consumo — si no puede instrumentarse así,
hace falta otro modelo (ej. prorrateo por tiempo de cluster).

### Reproducibilidad verificada por hash, no por inspección
**Qué es:** en vez de mirar el output y decir "se ve bien", comparar un
hash (o un diff exacto) entre dos corridas del mismo pipeline sobre el
mismo input.
**Aquí:** `tests/data_quality/test_scd2_backfill.py::
test_scd2_backfill_is_byte_identical_on_reprocess`.
**Por qué importa:** es la diferencia entre "creo que es determinista" y
"lo probé automáticamente en cada corrida de CI".

---

## 7. ADRs en 3 líneas

- **ADR 0001 — SCD2 full reprocess vs. Type 1 vs. `MERGE INTO`
  incremental:** Type 1 pierde historia (las cohortes del pasado mienten);
  un snapshot diario completo es simple pero pesado en storage y consultas.
  `MERGE INTO` incremental es lo correcto en producción a escala, pero aquí
  el dataset cabe en `local[2]` y el reproceso total da una prueba de
  reproducibilidad más fuerte — el límite está nombrado, no oculto.
- **ADR 0002 — Gates que bloquean vs. que solo alertan vs. cuarentena
  parcial de filas:** solo alertar significa que el analista ve el
  problema cuando el cohort ya salió; cuarentena parcial de filas es más
  fiel al patrón de fintech, pero aquí el grano de negocio es el mes de
  carga, no el evento individual — bloquear el mes completo es la unidad
  correcta.
- **ADR 0003 — DuckDB vs. Athena sobre MiniStack vs. Postgres local:**
  Athena sobre MiniStack devuelve un mock hardcodeado en
  `get-query-results`, así que no se puede afirmar "consulta real"; Postgres
  sería otro motor y otro dialecto, sin demostrar el patrón real de
  Spectrum/`COPY`. DuckDB + `httpfs` sí ejecuta la consulta real sobre el
  mismo Parquet.

---

## 8. Métricas y cómo defenderlas

| Métrica publicada | Cómo se midió | Límite honesto que hay que decir sin que lo pregunten |
|---|---|---|
| Reproceso SCD2 byte-idéntico | `pytest tests/data_quality/test_scd2_backfill.py` | Es una corrida sintética de 6 meses/200 clientes, no un dataset de producción |
| 0 rangos de validez solapados | Mismo test | — |
| 6/6 clases de defecto sembradas capturadas | `pytest tests/unit/test_gates.py` (8/8 tests) | Son defectos inyectados a propósito, diseñados para disparar cada gate — no una muestra representativa de errores reales de producción |
| SLA breach dedup: 2 breaches mismo día → 1 alerta enviada, 1 deduplicada | `pytest tests/integration/test_cost_sla.py` | — |
| Lectura de gold layer en 7 ms (244 filas, DuckDB sobre Parquet S3) | `make bench` | Es un volumen de demo (244 filas); no dice nada sobre latencia a escala de producción con millones de filas y sin MPP real |
| Suite completa 14/14 pasando | `pytest tests/ -v` | — |

---

## 9. Preguntas de entrevista (EN)

1. **"Walk me through what happens when a month's data fails a quality
   gate."** `run_all_gates` runs before that month is promoted to silver.
   Any single failing gate blocks the whole month — it publishes an SNS
   alert, revokes any stale silver output from a prior run of that same
   month, and gold is built only from the months that did pass, so a
   blocked month leaves a real, visible gap rather than silently degrading
   gold with bad data.

2. **"Why revoke silver from a previously-promoted month instead of just
   blocking future promotion?"** I found this gap by deliberately
   corrupting an already-promoted month's bronze data and re-running: the
   new run correctly blocked it, but the *old* silver output from the
   prior successful run was still sitting there, making the month look
   promoted via stale leftover data. Revocation closes that.

3. **"Why full reprocess instead of an incremental MERGE for SCD2?"**
   At this dataset's scale, full reprocess gives the strongest possible
   reproducibility guarantee — byte-identical, verified by hash. At real
   production scale, an incremental `MERGE INTO` is the correct answer,
   and I say that explicitly rather than pretending full reprocess scales
   indefinitely.

4. **"What guarantees byte-identical reprocessing here, specifically?"**
   Deterministic tie-breaking on transitions sharing the same
   `valid_from` — without an explicit ordering window there, two runs
   over identical input could assign different `valid_from`/`valid_to`
   pairs on a tie, breaking reproducibility silently.

5. **"Why does the SLA-breach alert use a DynamoDB conditional write for
   dedup instead of, say, checking if an alert was already sent?"**
   A conditional `PutItem` with `attribute_not_exists` is atomic — a
   check-then-write has the same race window as the equivalent decision in
   the fintech pipeline's idempotency gate. It's the same pattern reused
   for a different domain (alerting, not money).

6. **"Why gates that block instead of gates that just alert?"**
   Alert-only quality means the analyst sees the problem after the
   cohort report already shipped. Blocking at the month grain — not the
   row grain, since the business unit here is a month of loaded bank
   data — keeps bad data out of gold entirely.

7. **"Why block at the month grain instead of quarantining individual
   rows, like the fintech repo does?"** Different repos, different
   business grain: fintech's unit of correctness is a single transaction;
   here it's a month of loaded engagement data. Row-level quarantine
   wouldn't match what "one loaded batch" means in this domain.

8. **"Is your Athena integration real?"**
   No — MiniStack's Athena accepts `start-query-execution` and reports
   `SUCCEEDED`, but `get-query-results` returns a hardcoded mock, never the
   real rows. I use DuckDB with `httpfs` as the actual query layer against
   the same Parquet, and I say plainly Athena isn't the real thing here.

9. **"What's the honest limitation of the cost model?"**
   `COST_PER_GB = 0.023` is a rough LocalStack/MiniStack-era stand-in, not
   a real AWS price — I labeled it as such in the code rather than
   presenting it as a real billing figure.

10. **"Why does `catalog.py` copy schemas by hand from the Spark builders
    instead of introspecting them programmatically?"** It's intentional —
    a manual copy makes schema drift between what Spark actually writes
    and what the catalog declares visible and detectable, instead of
    silently staying in sync (or silently diverging) behind an automatic
    introspection step.

11. **"What would you change for production?"**
    Move `dim_customer` to incremental `MERGE INTO` once volume makes full
    reprocess too slow, and replace the manual schema copy in `catalog.py`
    with a Glue Schema Registry integration once schema drift detection
    needs to be automatic rather than a deliberate manual step.

12. **"How would you know if the pipeline was becoming too slow?"**
    `cost_sla.py::check_sla` compares each run's duration against
    `SLA_SECONDS=180` and fires a deduplicated SNS alert on breach — this
    is wired into the orchestrated `pipeline.py` run, not a separate
    manual check.

---

## 10. Flashcards

<!-- card -->
Q: ¿Qué pasa con un mes de bronze si falla uno solo de los 6 gates de calidad?
A: Se bloquea la promoción completa del mes a silver, se publica una alerta SNS, y se revoca cualquier silver previo de ese mismo mes.

<!-- card -->
Q: ¿Qué bug real se encontró y corrigió respecto a meses ya promovidos que luego fallan un gate?
A: El silver previo de un mes ya promovido no se borraba automáticamente al re-correr y fallar — el mes quedaba "fantasma" promovido con datos viejos hasta que se agregó `revoke_silver_promotion()`.

<!-- card -->
Q: ¿Por qué `dim_customer` se reconstruye completo en cada corrida en vez de aplicar un MERGE incremental?
A: Para garantizar reproceso byte-idéntico, verificado por hash — el ADR 0001 aclara que un MERGE incremental sería lo correcto en producción a escala, pero el reproceso total da una prueba más fuerte a esta escala.

<!-- card -->
Q: ¿Qué evita el ordenamiento determinista en `dedup_window` dentro de `build_dim_customer`?
A: Que dos corridas del mismo input asignen `valid_from`/`valid_to` distintos en un empate de timestamp, lo cual rompería la garantía de byte-idéntico.

<!-- card -->
Q: ¿Qué patrón comparte `cost_sla.py::check_sla` con el gate de idempotencia de fintech-txn-integrity-pipeline?
A: Un `PutItem` condicional (`attribute_not_exists`) para deduplicar — aquí aplicado a evitar alertas duplicadas de SLA, no a evitar liquidaciones duplicadas.

<!-- card -->
Q: ¿Por qué el grano de bloqueo de los gates es el mes y no la fila individual, a diferencia de fintech?
A: El ADR 0002 dice que el grano de negocio aquí es el mes de carga bancaria, no el evento de pago individual — cuarentena parcial de filas sería más fiel al patrón de fintech pero no encaja con esta unidad de negocio.

<!-- card -->
Q: ¿Qué devuelve realmente `get-query-results` de Athena sobre MiniStack, y qué se usa en su lugar?
A: Un mock hardcodeado (`{"result": "mock_value"}`), nunca las filas reales. Se usa DuckDB + `httpfs` como la capa de consulta real sobre el mismo Parquet.

<!-- card -->
Q: ¿Qué bug tenía `pipeline.py` antes de que se corrigiera, respecto a las tres tablas gold?
A: Solo producía `dim_customer` en el run orquestado, aunque el README/arquitectura describían tres tablas gold — `fact_engagement_daily` y `dim_offer` existían solo como CLI standalone que nadie llamaba.

<!-- card -->
Q: ¿Qué constante define el límite de deriva de cardinalidad de segmento entre meses, y qué gate la usa?
A: 30 puntos porcentuales, en `gate_cardinality_drift`.

<!-- card -->
Q: ¿Por qué el esquema en `catalog.py` se copia a mano desde los builders de Spark en vez de introspectarse automáticamente?
A: Para detectar drift a propósito — si alguien cambia un builder de Spark sin actualizar el catálogo, la discrepancia queda visible, no oculta detrás de una introspección automática.

<!-- card -->
Q: ¿Qué tan real es el número `COST_PER_GB = 0.023`?
A: Es un modelo de costo de la era LocalStack/MiniStack, explícitamente no un precio real de AWS — el propio comentario en el código lo aclara.

<!-- card -->
Q: ¿Cuál es el SLA de duración configurado para una corrida del pipeline, y qué pasa si se excede dos veces el mismo día?
A: `SLA_SECONDS = 180`. Un segundo breach el mismo día para el mismo pipeline se deduplica — solo se envía una alerta.

<!-- card -->
Q: ¿Qué demuestra el Terraform de Azure en este repo, y qué NO demuestra?
A: Demuestra literacia de IaC y modelado correcto de recursos (ADLS + Data Factory) vía `validate`/`plan`. NO demuestra un segundo cloud funcionando de verdad — no hay credenciales reales de Azure en el repo.

<!-- card -->
Q: ¿Qué prueba automatizada confirma la reproducibilidad byte-idéntica de `dim_customer`?
A: `tests/data_quality/test_scd2_backfill.py::test_scd2_backfill_is_byte_identical_on_reprocess`, por comparación de hash.

<!-- card -->
Q: ¿Cuántas clases de defecto sembradas capturan los 6 gates, y con qué prueba se verifica?
A: 6 de 6, verificado con `pytest tests/unit/test_gates.py` (8/8 tests pasando).

<!-- card -->
Q: ¿Qué significa que `facts.py` siga existiendo como CLI standalone después de integrarse a `pipeline.py`?
A: Sirve para reconstrucciones ad-hoc de `fact_engagement_daily`/`dim_offer` fuera del run orquestado completo, sin duplicar la lógica de construcción.

<!-- card -->
Q: ¿Qué endpoint de la API Flask expone el conteo de breaches de SLA registrados?
A: `GET /sla/status`, que escanea la tabla `sla-alert-dedup`.

<!-- card -->
Q: ¿Qué alternativa a "gates que bloquean" se descartó explícitamente en el ADR 0002, y por qué?
A: Solo SNS/alert — porque el analista vería el problema hasta que el reporte de cohorte ya hubiera salido, cuando gold ya estaría sucio.

<!-- card -->
Q: ¿Qué le pasa a `gold_rows_written` (y las otras dos cuentas de filas) si ningún mes se promueve en una corrida?
A: Quedan en 0 — gold no se toca en absoluto, no se escribe un Parquet vacío ni se sobreescribe con nada.

---

## 11. Quiz

<!-- quiz -->
Q: Un mes de bronze falla `gate_duplicates`. ¿Qué pasa con ese mes?
- [x] No se promueve a silver, se publica una alerta SNS y se revoca cualquier silver previo de ese mes
- [ ] Se promueve igual, pero con una advertencia en los logs
- [ ] Se descartan solo las filas duplicadas y el resto se promueve
- [ ] Se reintenta automáticamente con un dataset limpio

Why: Un solo gate fallido bloquea la promoción del mes completo — el grano de bloqueo aquí es el mes, no la fila individual, a diferencia del patrón de cuarentena por evento de fintech.

<!-- quiz -->
Q: ¿Por qué existe `revoke_silver_promotion()` como parte del flujo de gates fallidos?
- [ ] Para liberar espacio en S3 automáticamente cada noche
- [x] Porque un mes que falla gates en una corrida podía quedar "fantasma" promovido con silver viejo de una corrida anterior exitosa, hasta que se agregó esta revocación
- [ ] Es un requisito de la política de retención de datos bancarios
- [ ] Para forzar la regeneración del catálogo Glue

Why: Se encontró corrompiendo manualmente un mes ya promovido y re-corriendo el pipeline — el bloqueo nuevo funcionaba, pero el silver viejo seguía sirviéndose hasta que se añadió la revocación explícita.

<!-- quiz -->
Q: ¿Por qué `build_dim_customer` reconstruye la dimensión completa en cada corrida en vez de aplicar los cambios incrementalmente?
- [ ] Porque Spark no soporta actualizaciones incrementales sobre Parquet
- [x] Para garantizar una reproducibilidad byte-idéntica verificada por hash, la garantía más fuerte a la escala de este repo
- [ ] Porque es más rápido que un MERGE incremental
- [ ] Es un descuido que se corregirá en una versión futura

Why: El ADR 0001 nombra explícitamente que un `MERGE INTO` incremental sería lo correcto en producción a escala, pero aquí el reproceso total prueba una garantía de reproducibilidad más fuerte al tamaño de este dataset.

<!-- quiz -->
Q: Dos breaches de SLA ocurren el mismo día para el mismo `pipeline_id`. ¿Cuántas alertas SNS se envían?
- [ ] Dos, una por cada breach detectado
- [x] Una — la segunda se deduplica vía un `PutItem` condicional sobre `sla-alert-dedup`
- [ ] Ninguna, porque el sistema espera confirmación manual
- [ ] Depende de si el pipeline_id cambia entre corridas

Why: `check_sla` usa `ConditionExpression: attribute_not_exists(alert_id)` con key `{pipeline_id}#{run_date}` — el mismo patrón de idempotencia del gate de fintech, aplicado a alertas.

<!-- quiz -->
Q: ¿Qué devuelve realmente una consulta a través de Athena sobre MiniStack en este repo?
- [ ] Las filas reales del Parquet en `bank-gold`
- [x] Un resultado mock hardcodeado, sin relación con los datos reales, aunque `start-query-execution` reporta SUCCEEDED
- [ ] Un error de conexión, porque Athena no está soportado
- [ ] Los mismos resultados que DuckDB, ya que ambos leen el mismo Parquet

Why: El ADR 0003 documenta que `get-query-results` de MiniStack devuelve `{"result": "mock_value"}` sin importar la consulta real — por eso DuckDB es la capa de consulta real en este repo, no Athena.

<!-- quiz -->
Q: ¿Cuál era el bug real en `pipeline.py` respecto a las tres tablas gold, antes de corregirse?
- [x] El run orquestado solo producía `dim_customer`, aunque `fact_engagement_daily` y `dim_offer` existían como CLI standalone que nadie llamaba desde el pipeline principal
- [ ] Las tres tablas se sobreescribían mutuamente por un conflicto de nombres en S3
- [ ] `dim_offer` nunca se implementó
- [ ] Las tablas se escribían en el orden incorrecto, causando referencias rotas

Why: El propio docstring de `pipeline.py` documenta este hallazgo explícitamente — el README/arquitectura describían 3 tablas gold, pero solo una se generaba en la corrida real orquestada.

<!-- quiz -->
Q: ¿Por qué los gates de este repo bloquean a nivel de mes completo y no a nivel de fila individual, como la cuarentena de fintech?
- [x] Porque el grano de negocio aquí es un mes de carga bancaria, no una transacción individual
- [ ] Porque Spark no permite filtrar filas individuales en un DataFrame
- [ ] Porque bloquear filas sería técnicamente imposible con PySpark
- [ ] Es una limitación temporal que se planea cambiar

Why: ADR 0002 lo dice explícitamente — cuarentena parcial de filas sería más fiel al patrón de fintech, pero no encaja con la unidad de negocio de este repo.

<!-- quiz -->
Q: ¿Qué tan confiable es el número `COST_PER_GB = 0.023` como cifra de facturación real?
- [ ] Es el precio oficial de AWS Redshift Serverless por GB procesado
- [x] Es un modelo de costo aproximado de la era LocalStack/MiniStack, explícitamente etiquetado como no-real en el código
- [ ] Es un promedio calculado a partir de facturas reales de AWS de este proyecto
- [ ] No tiene ninguna documentación sobre su origen

Why: El propio comentario en `cost_sla.py` dice que es "not a real AWS price" — es un stand-in para lo que escalaría una factura real de Redshift/EMR, no una cifra medida.

<!-- quiz -->
Q: ¿Qué asegura que `dim_customer` no tenga rangos de validez (`valid_from`/`valid_to`) solapados entre sí para un mismo cliente?
- [x] El uso de window functions (`lag`/`lead`) sobre transiciones ordenadas de forma determinista, verificado por `test_no_overlapping_validity_ranges_per_customer`
- [ ] Una restricción `UNIQUE` a nivel de base de datos en Parquet
- [ ] Un gate de calidad específico que revisa solapamientos después de escribir gold
- [ ] DuckDB rechaza automáticamente rangos solapados al leer

Why: La lógica de construcción en `gold.py` usa `lag`/`lead` sobre una ventana ordenada determinísticamente para calcular `valid_to` como el siguiente `valid_from`, y un test de datos de calidad verifica que nunca se solapen.

<!-- quiz -->
Q: ¿Qué demuestra realmente el `terraform/azure/main.tf` de este repo?
- [x] Modelado correcto de recursos de IaC (ADLS + Data Factory), validado con `terraform validate`/`plan`, sin desplegar nada real
- [ ] Una migración funcional de datos entre AWS y Azure
- [ ] Que el pipeline puede correr indistintamente en AWS o Azure
- [ ] Un benchmark de costo comparado entre ambos clouds

Why: El README es explícito: es un stub ilustrativo de literacia en IaC, no un segundo cloud funcionando — no hay credenciales reales de Azure en el repo.
