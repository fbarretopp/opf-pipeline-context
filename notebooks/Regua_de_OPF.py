# Databricks notebook source
# MAGIC %md
# MAGIC ## self_service_analytics.pf_growth_users_opf_journey_communication
# MAGIC #####jira tasks: 
# MAGIC <nav>
# MAGIC <a href="">DTHB</a>
# MAGIC </nav>
# MAGIC
# MAGIC #####last authors: <nav>
# MAGIC @felipe.gbarreto
# MAGIC </nav>
# MAGIC
# MAGIC #####last update:
# MAGIC 27/03/2026

# COMMAND ----------

# MAGIC %md
# MAGIC # 🏦 Régua IF — OPF (Open Finance)
# MAGIC ---
# MAGIC **Responsável:** felipe.gbarreto@picpay.com
# MAGIC
# MAGIC - [Sheets](https://docs.google.com/spreadsheets/d/1D2YjMdCapdqYV2-JB6Kzo51FRGI4e5lDz1A04DNi3ro/edit?gid=1702446343#gid=1702446343)
# MAGIC - [GitHub Repo](https://github.com/fbarretopp/opf-pipeline-context) - Leia o repositório github.com/fbarretopp/opf-pipeline-context e retome o contexto do projeto OPF
# MAGIC
# MAGIC ## O que este notebook faz?
# MAGIC Gera diariamente a base de usuários PicPay elegíveis para comunicação da **Régua de Open Finance (OPF)**.
# MAGIC Para cada usuário MAU (Monthly Active User), identifica a **IF mais prioritária** onde ele tem conta
# MAGIC (detectada via Open Finance) e o seleciona para receber comunicação.
# MAGIC
# MAGIC **v8 — Novidade:** campo `experiment_group` na saída para separação de experimento GC/GT.
# MAGIC - **GC (Grupo Controle, ~10%):** usuários excluídos permanentemente da seleção diária —
# MAGIC   aparecem na tabela apenas no dia em que foram sorteados; nunca mais retornam ao pool.
# MAGIC - **GT (Grupo Teste, ~90%):** usuários selecionados normalmente para comunicação.
# MAGIC - **Primeira execução:** dos 800k selecionados, ~10% recebem `GC` e ~90% recebem `GT`
# MAGIC   via hash determinístico sobre `user_id`.
# MAGIC - **Execuções seguintes:** usuários já marcados como `GC` são **excluídos no Passo 4**
# MAGIC   (antes da seleção dos 800k), nunca competindo por slots. Os demais recebem atribuição
# MAGIC   via hash — idempotente e sem necessidade de join com histórico.
# MAGIC
# MAGIC ## Fontes e saída
# MAGIC | Tabela | Papel | Descrição |
# MAGIC |--------|-------|-----------|
# MAGIC | `self_service_analytics.pp_users_identified_accounts_opf_communication` | **Input** | Usuários elegíveis com IFs detectadas (gerada pelo notebook SS) |
# MAGIC | `consumers.dim_consumers_metrics` | **Lookup** | Filtra apenas MAU (`is_mau = true`) |
# MAGIC | `self_service_analytics.pf_growth_users_opf_journey_communication` | **Input/Output** | Histórico de chaves — consultado para cooldown, blacklist e sobrescrito ao final |
# MAGIC
# MAGIC ## Schema da tabela de saída — `self_service_analytics.pf_growth_users_opf_journey_communication`
# MAGIC | Coluna | Tipo | Descrição |
# MAGIC |--------|------|-----------|
# MAGIC | `user_id` | long | ID do usuário PicPay |
# MAGIC | `bank_name` | string | IF selecionada para comunicação |
# MAGIC | `entry_date` | date | Data de entrada da chave na base elegível |
# MAGIC | `latest_transaction_date` | date | Última transação detectada com essa IF |
# MAGIC | `type` | string | `NOVO` ou `LEGADO` |
# MAGIC | `experiment_group` | string | `GC` (grupo controle, 10%) ou `GT` (grupo teste, 90%) |
# MAGIC | `segmentation_date` | date | Data de geração (partição) |
# MAGIC
# MAGIC ## Fluxo resumido
# MAGIC ```
# MAGIC Tabela SS (fonte)
# MAGIC     ↓  join MAU + normalização de banco
# MAGIC Base MAU Elegível (melhor banco por user_id)
# MAGIC     ↓  Passo 4 — Exclusões
# MAGIC         - Cooldown de usuário (3d)
# MAGIC         - Bloqueio de chave (15d)
# MAGIC         - Blacklist de usuário (3x/30d → 30d)
# MAGIC         - ⭐ Holdout GC: remove usuários já marcados GC no histórico
# MAGIC     ↓  split por data de entrada
# MAGIC ┌─────────────────────┐    ┌──────────────────────────────────────────┐
# MAGIC │ NOVOS               │    │ LEGADO ELEGÍVEL                          │
# MAGIC │ (desde última run)  │    │ - Ordena: last_tx DESC → prio banco      │
# MAGIC │ Todos entram        │    │ - Limit: slots restantes após NOVOS      │
# MAGIC │ até 800k.           │    │                                          │
# MAGIC └──────────┬──────────┘    └──────────────────┬───────────────────────┘
# MAGIC            └──────────────────────────────────┘
# MAGIC                               ↓  union (800k, todos não-GC)
# MAGIC               Passo 7.5 — Atribuição GC/GT via hash(user_id)
# MAGIC               ~10% → GC (holdout), ~90% → GT (comunicados)
# MAGIC                               ↓
# MAGIC                    Write → self_service_analytics.pf_growth_users_opf_journey_communication
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Configuração
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Imports

# COMMAND ----------

from datetime import datetime, timedelta, date
from pyspark.sql.functions import (
    col, count, date_format, lit, to_date, row_number, desc, asc, monotonically_increasing_id,
    when, abs as spark_abs, hash as spark_hash
)
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Configurações Spark

# COMMAND ----------

# AQE e otimizações de join/shuffle
spark.conf.set("spark.sql.adaptive.enabled",                    True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled",         True)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",          200 * 1024 * 1024)  # 200 MB
spark.conf.set("spark.sql.broadcastTimeout",                    600)
spark.conf.set("spark.sql.shuffle.partitions",                  "auto")
spark.conf.set("spark.sql.join.preferSortMergeJoin",            True)
# Dynamic overwrite: só reescreve a partição do dia atual, preserva histórico
spark.conf.set("spark.sql.sources.partitionOverwriteMode",      "dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Parâmetros
# MAGIC ---
# MAGIC > **Altere apenas esta célula** para ajustar o comportamento do pipeline.
# MAGIC >
# MAGIC > ### Parâmetros de negócio
# MAGIC > | Parâmetro | Padrão | Descrição |
# MAGIC > |-----------|--------|-----------|
# MAGIC > | `JANELA_COOLDOWN_USUARIO_DIAS` | 3 | Dias que um **usuário** fica bloqueado após qualquer comunicação  |
# MAGIC > | `JANELA_BLOQUEIO_CHAVE_DIAS` | 15 | Dias que a **chave** (user+banco) fica bloqueada após comunicação  |
# MAGIC > | `JANELA_CONTAGEM_COMUNICACOES` | 30 | Janela (dias) para contar comunicações do **usuário** que aciona o blacklist |
# MAGIC > | `LIMITE_COMUNICACOES_USUARIO` | 3 | Nº de comunicações do usuário (qualquer banco) em 30d que aciona o bloqueio  |
# MAGIC > | `JANELA_BLOQUEIO_USUARIO_DIAS` | 30 | Dias que o **usuário** fica bloqueado após atingir o limite  |
# MAGIC > | `LIMITE_NOVOS` | 800.000 | Cap de chaves **NOVAS**/dia (0 = sem limite). Bank-Bucket-Drain por banco. |
# MAGIC > | `LIMITE_DIARIO` | 800.000 | Cap total/dia — NOVOS + LEGADO (0 = sem limite) |
# MAGIC > | `DATA_INICIO_CORTE` | hoje − 90 dias | Janela mínima de entrada na base elegível |
# MAGIC > | `DATA_NOVA_CHAVE` | hoje − 2 dias | Chaves com `last_transaction >= DATA_NOVA_CHAVE` = NOVO na 1ª execução |
# MAGIC > | `PCT_GC` | 0.10 | Proporção de chaves novas atribuídas ao grupo controle (GC) |

# COMMAND ----------

# ── Tabelas ──────────────────────────────────────────────────────────────────
TABELA_FONTE_NAME  = "self_service_analytics.pp_users_identified_accounts_opf_communication"
CAMPO_DATA_ENTRADA = "most_recent_transaction_at"
CAMPO_DATA_ATUALIZACAO_FONTE = "most_recent_transaction_at"
TABELA_HISTORICO   = "self_service_analytics.pf_growth_users_opf_journey_communication"

# ── Datas de referência ───────────────────────────────────────────────────────
DATA_HOJE         = date.today().strftime("%Y-%m-%d")
DATA_INICIO_CORTE = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")
DATA_FONTE_MINIMA = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
DATA_NOVA_CHAVE   = (date.today() - timedelta(days=2)).strftime("%Y-%m-%d")

# ── Cooldown de usuário ───────────────────────────────────────────────────────
JANELA_COOLDOWN_USUARIO_DIAS = 3

# ── Bloqueio de chave ─────────────────────────────────────────────────────────
JANELA_BLOQUEIO_CHAVE_DIAS = 15

# ── Blacklist de usuário ──────────────────────────────────────────────────────
JANELA_CONTAGEM_COMUNICACOES   = 30
LIMITE_COMUNICACOES_USUARIO    = 3
JANELA_BLOQUEIO_USUARIO_DIAS   = 30

# ── Volume diário ─────────────────────────────────────────────────────────────
LIMITE_NOVOS  = 800_000
LIMITE_DIARIO = 800_000

# ── Experimento GC/GT (v8) ────────────────────────────────────────────────────
# PCT_GC: proporção de usuários SEM grupo histórico que recebe `GC`.
# A atribuição é determinística por user_id (hash), garantindo idempotência.
# Usuários com grupo já registrado no histórico mantêm seu grupo original.
PCT_GC = 0.10  # 10% GC, 90% GT

print("=" * 65)
print(f"  Pipeline OPF v8 — {DATA_HOJE}")
print("=" * 65)
print(f"  Fonte           : {TABELA_FONTE_NAME}")
print(f"  Histórico       : {TABELA_HISTORICO}")
print(f"  Campo atualização fonte: {CAMPO_DATA_ATUALIZACAO_FONTE}")
print(f"  Fonte mínima d-1: >= {DATA_FONTE_MINIMA}")
print(f"  Corte entrada   : {DATA_INICIO_CORTE}")
print(f"  Threshold NOVO  : >= {DATA_NOVA_CHAVE}")
print(f"  Cooldown usuário: {JANELA_COOLDOWN_USUARIO_DIAS} dias")
print(f"  Bloqueio chave  : {JANELA_BLOQUEIO_CHAVE_DIAS} dias após comunicação")
print(f"  Blacklist usuário: {LIMITE_COMUNICACOES_USUARIO}x em {JANELA_CONTAGEM_COMUNICACOES}d → bloqueio {JANELA_BLOQUEIO_USUARIO_DIAS}d")
print(f"  Limite novos    : {LIMITE_NOVOS:,}")
print(f"  Limite diário   : {LIMITE_DIARIO:,}")
print(f"  GC/GT (v8)      : {PCT_GC*100:.0f}% GC / {(1-PCT_GC)*100:.0f}% GT (novos usuários sem histórico)")
print("=" * 65)

# COMMAND ----------

# Trava de frescor da fonte (herdada da v7)
# Encerra o notebook se a fonte não estiver atualizada até d-1
row_fonte_atualizacao = spark.sql(f"""
    SELECT MAX(DATE({CAMPO_DATA_ATUALIZACAO_FONTE})) AS dt_max_fonte
    FROM {TABELA_FONTE_NAME}
""").first()

dt_max_fonte = str(row_fonte_atualizacao["dt_max_fonte"]) if row_fonte_atualizacao["dt_max_fonte"] else None

if dt_max_fonte is None or dt_max_fonte < DATA_FONTE_MINIMA:
    dbutils.notebook.exit(
        f"SKIPPED: fonte {TABELA_FONTE_NAME} desatualizada. "
        f"max({CAMPO_DATA_ATUALIZACAO_FONTE})={dt_max_fonte}, esperado >= {DATA_FONTE_MINIMA}."
    )

print(
    f"Fonte validada: max({CAMPO_DATA_ATUALIZACAO_FONTE})={dt_max_fonte} "
    f"(mínimo esperado: {DATA_FONTE_MINIMA})."
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Prioridade de Bancos
# MAGIC ---
# MAGIC > Define a ordem de preferência de bancos. Usada como critério de desempate:
# MAGIC > **dentro do mesmo valor de `latest_transaction_date`**, o banco de menor prioridade numérica é preferido.

# COMMAND ----------

bank_flag = "bank_name"

# Expressão CASE que transforma o nome do banco em número de prioridade
# Quanto menor o número, maior a prioridade
classif_bank_priority = f"""
CASE
    WHEN {bank_flag} = 'MERCADO PAGO'        THEN 1
    WHEN {bank_flag} = 'NUBANK'              THEN 2
    WHEN {bank_flag} = 'RECARGAPAY'          THEN 3
    WHEN {bank_flag} = 'SHOPEE'              THEN 4
    WHEN {bank_flag} = 'SANTANDER'           THEN 5
    WHEN {bank_flag} = 'CAIXA'               THEN 6
    WHEN {bank_flag} = 'INTER'               THEN 7
    WHEN {bank_flag} = 'BRADESCO'            THEN 8
    WHEN {bank_flag} = 'BANCO DO BRASIL'     THEN 9
    WHEN {bank_flag} = 'PAN'                 THEN 10
    WHEN {bank_flag} = 'NEXT'                THEN 11
    WHEN {bank_flag} = 'NEON'                THEN 12
    WHEN {bank_flag} = 'C6 BANK'             THEN 13
    WHEN {bank_flag} = '99PAY'               THEN 14
    WHEN {bank_flag} = 'BANCO DO NORDESTE'   THEN 15
    WHEN {bank_flag} = 'MERCANTIL'           THEN 16
    -- WHEN {bank_flag} = 'BRB'                 THEN 17
    WHEN {bank_flag} = 'CREFISA'             THEN 18
    WHEN {bank_flag} = 'MIDWAY'              THEN 19
    WHEN {bank_flag} = 'PAGSEGURO'           THEN 20
    -- WHEN {bank_flag} = 'WILL BANK'           THEN 21
    WHEN {bank_flag} = 'SAFRA'               THEN 22
    WHEN {bank_flag} = 'BTG'                 THEN 23
    WHEN {bank_flag} = 'BANCO CARREFOUR'     THEN 24
ELSE 100 END
"""

lista_bancos_prioritarios = (
    'MERCADO PAGO', 'NUBANK', 'RECARGAPAY', 'SHOPEE', 'SANTANDER',
    'CAIXA', 'INTER', 'BRADESCO', 'BANCO DO BRASIL', 'PAN', 'NEXT',
    'NEON', 'C6 BANK', '99PAY', 'BANCO DO NORDESTE', 'MERCANTIL',
    #'BRB',
    'CREFISA', 'MIDWAY', 'PAGSEGURO',
    #'WILL BANK',
    'SAFRA',
    'BTG', 'BANCO CARREFOUR'
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Base MAU Elegível — Melhor Banco por Usuário
# MAGIC ---
# MAGIC > **Objetivo:** Para cada usuário MAU na fonte SS, selecionar o banco de maior prioridade
# MAGIC > de forma que cada `user_id` apareça exatamente uma vez.
# MAGIC >
# MAGIC > **Critério de seleção do banco por usuário (ROW_NUMBER):**
# MAGIC > 1. Chave recém-criada (`new_key = 1`) tem prioridade absoluta sobre chaves antigas
# MAGIC > 2. Dentro do mesmo grupo (novo/antigo), menor `bank_name_priority` vence
# MAGIC >
# MAGIC > **Notas de performance:**
# MAGIC > - `BROADCAST(b)` em `dim_consumers_metrics`: elimina shuffle do INNER JOIN
# MAGIC > - `persist(MEMORY_AND_DISK)` + `.count()`: materializa o DAG antes dos múltiplos usos

# COMMAND ----------

tabela_fonte = (
    spark.table(TABELA_FONTE_NAME)
        .filter(col(CAMPO_DATA_ENTRADA) >= DATA_INICIO_CORTE)
        .filter(col(CAMPO_DATA_ENTRADA) < DATA_HOJE)
        .filter(col('is_opf_account_active') == False)
        .filter(col('is_in_opf_list') == True)
        )
tabela_fonte.createOrReplaceTempView("TABELA_FONTE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 — Data da última geração
# MAGIC > Serve de limiar para separar NOVOS (entraram desde a última run) do LEGADO (pré-existente).
# MAGIC > Fallback: `DATA_NOVA_CHAVE` (hoje − 2 dias) quando o histórico estiver vazio (primeira execução).

# COMMAND ----------

# Busca a data mais recente no histórico excluindo o próprio dia (evita auto-referência)
try:
    row = spark.sql(f"""
        SELECT COALESCE(MAX(segmentation_date-1), DATE_SUB(CURRENT_DATE(), 2)) AS dt
        FROM {TABELA_HISTORICO}
        WHERE segmentation_date < '{DATA_HOJE}'
    """).first()
    dt_referencia_geracao = str(row["dt"])
except AnalysisException:
    # Tabela ainda não existe — primeira execução
    dt_referencia_geracao = DATA_NOVA_CHAVE

print(f"📅 Última geração: {dt_referencia_geracao}")
print(f"   → Chaves com latest_transaction_date >= {dt_referencia_geracao} serão classificadas como NOVO")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 — Priorização dos NOVOS/LEGADOS

# COMMAND ----------

# Constrói a base priorizada: 1 linha por user_id com o melhor banco
df_priorizado = spark.sql(f"""
WITH base AS (
    SELECT /*+ BROADCAST(b) */ -- BROADCAST: dim_consumers_metrics é menor, elimina shuffle
        a.user_id,
        -- Normaliza bancos fora da lista principal para 'OUTRO'
        CASE WHEN bank_name NOT IN {lista_bancos_prioritarios}
            THEN 'OUTRO' ELSE bank_name END                 AS bank_name,
        DATE(oldest_transaction_at)                             AS entry_date,
        DATE({CAMPO_DATA_ENTRADA})                              AS latest_transaction_date,
        CASE WHEN {CAMPO_DATA_ENTRADA} >= '{dt_referencia_geracao}'
            THEN 1 ELSE 0 END                               AS new_key,
        {classif_bank_priority}                             AS bank_name_priority
    FROM TABELA_FONTE AS a
    INNER JOIN consumers.dim_consumers_metrics AS b
        ON  a.user_id = b.consumer_id
        AND b.is_mau  = TRUE
    WHERE {CAMPO_DATA_ENTRADA} IS NOT NULL
)
SELECT
    user_id,
    bank_name,
    entry_date,
    latest_transaction_date,
    new_key,
    bank_name_priority,
    ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY
            CASE WHEN new_key = 1 THEN 0 ELSE 1 END ASC,           -- NOVO antes de LEGADO
            CASE WHEN new_key = 1 THEN bank_name_priority END,     -- NOVO: banco decide
            -- LEGADO
            latest_transaction_date DESC, bank_name_priority ASC
    ) AS n_priority
FROM base
""")

# Persiste antes dos múltiplos usos — evita re-executar o join com MAU
df_priorizado.persist(StorageLevel.MEMORY_AND_DISK)
cnt_priorizado = df_priorizado.count()
df_priorizado.createOrReplaceTempView("df_priorizado")
print(f"✅ Base MAU elegível: {cnt_priorizado:,} registros (1 por user_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Regras de Exclusão
# MAGIC ---
# MAGIC > Quatro listas de bloqueio são calculadas **antes** de classificar NOVO vs LEGADO:
# MAGIC >
# MAGIC > | Regra | Quem bloqueia | Parâmetro |
# MAGIC > |-------|---------------|-----------|
# MAGIC > | **Data da última geração** | Define o limiar de NOVO vs LEGADO | — |
# MAGIC > | **Cooldown de usuário** | Usuário comunicado nos últimos `JANELA_COOLDOWN_USUARIO_DIAS` (3) dias | `JANELA_COOLDOWN_USUARIO_DIAS` |
# MAGIC > | **Bloqueio de chave** *(v6)* | Chave comunicada nos últimos `JANELA_BLOQUEIO_CHAVE_DIAS` (15) dias | `JANELA_BLOQUEIO_CHAVE_DIAS` |
# MAGIC > | **Blacklist de usuário** *(v6)* | Usuário com ≥ `LIMITE_COMUNICACOES_USUARIO` (3) comms em 30d → bloqueado por `JANELA_BLOQUEIO_USUARIO_DIAS` (30d) | `JANELA_BLOQUEIO_USUARIO_DIAS` |
# MAGIC > | **⭐ Holdout GC** *(v8)* | Usuário já marcado como `GC` em qualquer run anterior — excluído **permanentemente** do pool de seleção | — |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 — Cooldown de Usuário
# MAGIC > **Regra:** Um usuário que foi comunicado em qualquer um dos últimos
# MAGIC > `JANELA_COOLDOWN_USUARIO_DIAS` dias não pode ser selecionado hoje,
# MAGIC > independentemente de banco ou tipo (NOVO ou LEGADO).
# MAGIC >
# MAGIC > **Por que essa regra?** Evita que um mesmo usuário receba comunicação em dias
# MAGIC > consecutivos, o que degrada a experiência e aumenta opt-out.

# COMMAND ----------

try:
    df_cooldown_usuario = spark.sql(f"""
        SELECT DISTINCT user_id
        FROM {TABELA_HISTORICO}
        WHERE segmentation_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {JANELA_COOLDOWN_USUARIO_DIAS})
          AND segmentation_date <  '{DATA_HOJE}'
    """)
    df_cooldown_usuario.persist(StorageLevel.MEMORY_AND_DISK)
    cnt_cooldown = df_cooldown_usuario.count()
    df_cooldown_usuario.createOrReplaceTempView("cooldown_usuario")
    print(f"🚫 Cooldown de usuário ({JANELA_COOLDOWN_USUARIO_DIAS} dias): {cnt_cooldown:,} usuários bloqueados")
except AnalysisException:
    spark.sql("CREATE OR REPLACE TEMP VIEW cooldown_usuario AS SELECT CAST(0L AS BIGINT) AS user_id WHERE 1=0")
    cnt_cooldown = 0
    print("ℹ️  Histórico vazio — nenhum usuário em cooldown (primeira execução)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 — Bloqueio de Chave
# MAGIC >
# MAGIC > **Regra:** Qualquer chave (`user_id` + `bank_name`) comunicada nos últimos
# MAGIC > `JANELA_BLOQUEIO_CHAVE_DIAS` (15) dias fica **bloqueada imediatamente** — sem contagem acumulada.
# MAGIC >
# MAGIC >
# MAGIC > **Lookback:** Apenas `JANELA_BLOQUEIO_CHAVE_DIAS` (15) dias — sem janela rolante.

# COMMAND ----------

try:
    df_bloqueio_chave = spark.sql(f"""
        SELECT DISTINCT user_id, bank_name
        FROM {TABELA_HISTORICO}
        WHERE segmentation_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {JANELA_BLOQUEIO_CHAVE_DIAS})
          AND segmentation_date <  '{DATA_HOJE}'
    """)
    df_bloqueio_chave.persist(StorageLevel.MEMORY_AND_DISK)
    cnt_bloqueio_chave = df_bloqueio_chave.count()
    df_bloqueio_chave.createOrReplaceTempView("bloqueio_chave")
    print(f"🔑 Bloqueio de chave ({JANELA_BLOQUEIO_CHAVE_DIAS} dias): {cnt_bloqueio_chave:,} chaves bloqueadas")
except AnalysisException:
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW bloqueio_chave AS
        SELECT CAST(0L AS BIGINT) AS user_id, CAST('' AS STRING) AS bank_name WHERE 1=0
    """)
    cnt_bloqueio_chave = 0
    print("ℹ️  Histórico vazio — nenhuma chave em bloqueio (primeira execução)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 — Blacklist de Usuário
# MAGIC >
# MAGIC > **Regra:** Se um usuário receber `LIMITE_COMUNICACOES_USUARIO` (3) ou mais comunicações
# MAGIC > de **qualquer banco** dentro de `JANELA_CONTAGEM_COMUNICACOES` (30) dias,
# MAGIC > o **usuário inteiro** fica bloqueado por `JANELA_BLOQUEIO_USUARIO_DIAS` (30) dias.
# MAGIC >
# MAGIC >
# MAGIC > **Lookback:** `JANELA_BLOQUEIO_USUARIO_DIAS + JANELA_CONTAGEM_COMUNICACOES` = 30 + 30 = **60 dias**.

# COMMAND ----------

LOOKBACK_BLACKLIST_USUARIO = JANELA_BLOQUEIO_USUARIO_DIAS + JANELA_CONTAGEM_COMUNICACOES

try:
    df_blacklist_usuario = spark.sql(f"""
        WITH historico_recente AS (
            SELECT user_id, segmentation_date
            FROM {TABELA_HISTORICO}
            WHERE segmentation_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {LOOKBACK_BLACKLIST_USUARIO})
              AND segmentation_date <  '{DATA_HOJE}'
        ),
        rolling_counts AS (
            SELECT
                user_id,
                segmentation_date,
                COUNT(*) OVER (
                    PARTITION BY user_id
                    ORDER BY CAST(segmentation_date AS TIMESTAMP)
                    RANGE BETWEEN INTERVAL {JANELA_CONTAGEM_COMUNICACOES - 1} DAYS PRECEDING
                              AND CURRENT ROW
                ) AS rolling_count
            FROM historico_recente
        ),
        gatilhos AS (
            SELECT
                user_id,
                MAX(segmentation_date) AS ultimo_gatilho
            FROM rolling_counts
            WHERE rolling_count >= {LIMITE_COMUNICACOES_USUARIO}
            GROUP BY user_id
        )
        SELECT user_id
        FROM gatilhos
        WHERE ultimo_gatilho >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {JANELA_BLOQUEIO_USUARIO_DIAS})
    """)
    df_blacklist_usuario.persist(StorageLevel.MEMORY_AND_DISK)
    cnt_blacklist_usuario = df_blacklist_usuario.count()
    df_blacklist_usuario.createOrReplaceTempView("blacklist_usuario")
    print(f"⛔ Blacklist de usuário ({LIMITE_COMUNICACOES_USUARIO}x em {JANELA_CONTAGEM_COMUNICACOES}d → {JANELA_BLOQUEIO_USUARIO_DIAS}d bloqueio): {cnt_blacklist_usuario:,} usuários bloqueados")
except AnalysisException:
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW blacklist_usuario AS
        SELECT CAST(0L AS BIGINT) AS user_id WHERE 1=0
    """)
    cnt_blacklist_usuario = 0
    print("ℹ️  Histórico vazio — nenhum usuário em blacklist (primeira execução)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 — Holdout GC Histórico (v8)
# MAGIC > **Regra:** Qualquer usuário que já foi marcado como `GC` em qualquer run anterior é
# MAGIC > excluído **permanentemente** do pool de seleção. Eles nunca mais competem por slots.
# MAGIC >
# MAGIC > Isso garante a integridade do experimento: o grupo controle é um holdout puro e
# MAGIC > a seleção dos 800k diários não é "contaminada" por usuários que nunca serão comunicados.
# MAGIC >
# MAGIC > **Fallback:** Se a tabela não existir, ou se a coluna `experiment_group` ainda não existir
# MAGIC > (primeiras runs com v8 sobre tabela criada por v6/v7), a view fica vazia —
# MAGIC > todos os usuários são elegíveis para receber a atribuição inicial.

# COMMAND ----------

try:
    df_gc_historico = spark.sql(f"""
        SELECT DISTINCT user_id
        FROM {TABELA_HISTORICO}
        WHERE `experiment_group` = 'GC'
          AND segmentation_date < '{DATA_HOJE}'
    """)
    df_gc_historico.persist(StorageLevel.MEMORY_AND_DISK)
    cnt_gc_historico = df_gc_historico.count()
    df_gc_historico.createOrReplaceTempView("gc_historico")
    if cnt_gc_historico > 0:
        print(f"🧪 Holdout GC: {cnt_gc_historico:,} usuários excluídos permanentemente do pool")
    else:
        print("ℹ️  Sem holdout GC no histórico — primeira atribuição de grupos (todos elegíveis)")
except AnalysisException:
    spark.sql("CREATE OR REPLACE TEMP VIEW gc_historico AS SELECT CAST(0L AS BIGINT) AS user_id WHERE 1=0")
    cnt_gc_historico = 0
    print("ℹ️  Tabela/coluna 'group' ausente — nenhum usuário em holdout GC (primeira execução)")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Classificação NOVO vs LEGADO
# MAGIC ---
# MAGIC > A base priorizada (1 linha por user_id) é dividida em dois grupos:
# MAGIC >
# MAGIC > | Grupo | Critério | Exclusões aplicadas | Limite |
# MAGIC > |-------|----------|---------------------|--------|
# MAGIC > | **NOVO** | `latest_transaction_date >= dt_referencia_geracao` | Cooldown de usuário; Blacklist de chave | Limitado a 800k. Todos entram se N<800k |
# MAGIC > | **LEGADO** | `latest_transaction_date < dt_referencia_geracao` | Cooldown de usuário; Blacklist de chave | Até `LIMITE_DIARIO - qtd_novos` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 — Grupo NOVOS

# COMMAND ----------

# Passo A: pool completo de NOVOS elegíveis (cooldown + blacklist já aplicados)
df_novos_pool = spark.sql(f"""
    SELECT
        f.user_id,
        f.bank_name,
        f.bank_name_priority,
        f.entry_date,
        f.latest_transaction_date,
        'NOVO' AS tipo
    FROM df_priorizado AS f
    WHERE f.n_priority = 1
      AND new_key = 1
      AND NOT EXISTS (SELECT 1 FROM cooldown_usuario cu WHERE cu.user_id = f.user_id)
      AND NOT EXISTS (
          SELECT 1 FROM bloqueio_chave bc
          WHERE bc.user_id = f.user_id AND bc.bank_name = f.bank_name
      )
      AND NOT EXISTS (SELECT 1 FROM blacklist_usuario bu WHERE bu.user_id = f.user_id)
      -- Exclui holdout GC — usuários que já foram grupo controle nunca mais entram no pool (v8)
      AND NOT EXISTS (SELECT 1 FROM gc_historico gc WHERE gc.user_id = f.user_id)
""")

df_novos_pool.persist(StorageLevel.MEMORY_AND_DISK)
qtd_novos_pool = df_novos_pool.count()
df_novos_pool.createOrReplaceTempView("novos_pool")
print(f"📦 Pool de NOVOS elegíveis (antes do cap): {qtd_novos_pool:,}")

# Passo B: Bank-Bucket-Drain — aplica LIMITE_NOVOS se necessário
if LIMITE_NOVOS <= 0 or qtd_novos_pool <= LIMITE_NOVOS:
    df_novos_dia = df_novos_pool
    qtd_novos    = qtd_novos_pool
    print(f"✅ Pool de NOVOS cabe inteiro: {qtd_novos_pool:,} ≤ {LIMITE_NOVOS:,} (cap)")
else:
    from functools import reduce
    bank_counts = (
        df_novos_pool
        .groupBy("bank_name_priority", "bank_name")
        .count()
        .orderBy("bank_name_priority")
        .collect()
    )

    slots_remaining_novos = LIMITE_NOVOS
    bank_buckets = []

    print(f"\n{'Banco':<35} {'Disponível':>12} {'Selecionado':>12}  Status")
    print("-" * 75)
    for row in bank_counts:
        if slots_remaining_novos <= 0:
            break
        bp   = row["bank_name_priority"]
        bn   = row["bank_name"]
        n    = row["count"]
        take = min(n, slots_remaining_novos)
        bank_buckets.append((bp, bn, take, n))
        slots_remaining_novos -= take
        status = "✅ completo" if take == n else "✂️  cortado (latest_transaction_date DESC)"
        print(f"{bn:<35} {n:>12,} {take:>12,}  {status}")
    print(f"\n→ {len(bank_buckets)} bancos usados de {len(bank_counts)} disponíveis")

    dfs_novos = []
    for bp, bn, take, n in bank_buckets:
        if take == n:
            dfs_novos.append(df_novos_pool.filter(col("bank_name_priority") == bp))
        else:
            df_corte = spark.sql(f"""
                SELECT user_id, bank_name, bank_name_priority,
                       entry_date, latest_transaction_date, tipo
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY bank_name_priority
                            ORDER BY latest_transaction_date DESC
                        ) AS rn
                    FROM novos_pool
                    WHERE bank_name_priority = {bp}
                )
                WHERE rn <= {take}
            """)
            dfs_novos.append(df_corte)

    df_novos_dia = reduce(lambda a, b: a.union(b), dfs_novos)
    qtd_novos    = df_novos_dia.count()
    print(f"\n✂️  Bank-Bucket-Drain concluído: {qtd_novos_pool:,} → {qtd_novos:,} (cap {LIMITE_NOVOS:,})")

df_novos_dia.persist(StorageLevel.MEMORY_AND_DISK)
df_novos_pool.unpersist()
df_novos_dia.createOrReplaceTempView("novos_dia")
print(f"\n✅ Chaves NOVAS selecionadas: {qtd_novos:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 — Grupo LEGADO (pool elegível)

# COMMAND ----------

df_legado_elegivel = spark.sql(f"""
    SELECT
        f.user_id,
        f.bank_name,
        f.bank_name_priority,
        f.entry_date,
        f.latest_transaction_date,
        'LEGADO' AS tipo
    FROM df_priorizado AS f
    WHERE f.n_priority = 1
      AND new_key = 0
      AND NOT EXISTS (SELECT 1 FROM cooldown_usuario cu WHERE cu.user_id = f.user_id)
      AND NOT EXISTS (
          SELECT 1 FROM bloqueio_chave bc
          WHERE bc.user_id = f.user_id AND bc.bank_name = f.bank_name
      )
      AND NOT EXISTS (SELECT 1 FROM blacklist_usuario bu WHERE bu.user_id = f.user_id)
      AND NOT EXISTS (SELECT 1 FROM novos_dia nd WHERE nd.user_id = f.user_id)
      -- Exclui holdout GC — usuários que já foram grupo controle nunca mais entram no pool (v8)
      AND NOT EXISTS (SELECT 1 FROM gc_historico gc WHERE gc.user_id = f.user_id)
""")

df_legado_elegivel.persist(StorageLevel.MEMORY_AND_DISK)
qtd_legado_pool = df_legado_elegivel.count()
df_legado_elegivel.createOrReplaceTempView("legado_elegivel")
print(f"📦 Pool de LEGADO elegível (antes do limite): {qtd_legado_pool:,}")

df_priorizado.unpersist()
try:
    df_cooldown_usuario.unpersist()
    df_bloqueio_chave.unpersist()
    df_blacklist_usuario.unpersist()
    df_gc_historico.unpersist()
except:
    pass
print("🔓 Caches intermediários liberados")

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Aplicação do Limite Diário no Legado
# MAGIC ---
# MAGIC > Ordena por **`latest_transaction_date DESC`** (usuários com transação mais recente na IF
# MAGIC > têm prioridade) e, como desempate, por **prioridade de banco**.

# COMMAND ----------

# Calcula quantos slots restam para o legado após alocar os NOVOS
if LIMITE_DIARIO > 0:
    slots_legado = max(0, LIMITE_DIARIO - qtd_novos)
else:
    # Sem limite configurado: pega todo o pool
    slots_legado = qtd_legado_pool

print(f"📊 Slots disponíveis para LEGADO: {slots_legado:,}")
print(f"   (Limite diário {LIMITE_DIARIO:,} − {qtd_novos:,} NOVOS selecionados = {slots_legado:,} slots)")

# COMMAND ----------

from functools import reduce

if slots_legado <= 0:
    df_legado_selecionado = spark.createDataFrame([], df_legado_elegivel.schema)
    print(f"⚠️  Sem slots para legado — NOVOS ({qtd_novos:,}) já atingiram o limite ({LIMITE_DIARIO:,})")

elif slots_legado >= qtd_legado_pool:
    df_legado_selecionado = df_legado_elegivel
    print(f"✅ Pool de legado cabe inteiro: {qtd_legado_pool:,} ≤ {slots_legado:,} slots")

else:
    from pyspark.sql.functions import desc
    date_counts = (
        df_legado_elegivel
        .groupBy("latest_transaction_date")
        .count()
        .orderBy(desc("latest_transaction_date"))
        .collect()
    )

    slots_remaining = slots_legado
    date_buckets    = []

    print(f"\n{'Data':<14} {'Disponível':>12} {'Selecionado':>12}  Status")
    print("-" * 58)
    for row in date_counts:
        if slots_remaining <= 0:
            break
        dt   = row["latest_transaction_date"]
        n    = row["count"]
        take = min(n, slots_remaining)
        date_buckets.append((dt, take, n))
        slots_remaining -= take
        status = "✅ completo" if take == n else "✂️  cortado (banco como desempate)"
        print(f"{str(dt):<14} {n:>12,} {take:>12,}  {status}")
    print(f"\n→ {len(date_buckets)} datas usadas de {len(date_counts)} disponíveis")

    dfs_buckets = []
    for dt, take, n in date_buckets:
        if take == n:
            dfs_buckets.append(
                df_legado_elegivel.filter(col("latest_transaction_date") == dt)
            )
        else:
            df_corte = spark.sql(f"""
                SELECT user_id, bank_name, bank_name_priority,
                       entry_date, latest_transaction_date, tipo
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY latest_transaction_date
                            ORDER BY bank_name_priority ASC
                        ) AS rn
                    FROM legado_elegivel
                    WHERE latest_transaction_date = DATE '{dt}'
                )
                WHERE rn <= {take}
            """)
            dfs_buckets.append(df_corte)

    df_legado_selecionado = reduce(lambda a, b: a.union(b), dfs_buckets)
    print(f"\n✂️  Day-Bucket-Drain concluído: {qtd_legado_pool:,} → {slots_legado:,} slots")

qtd_legado_selecionado = df_legado_selecionado.count()
print(f"✅ LEGADO selecionado final: {qtd_legado_selecionado:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Base Diária Final — NOVOS + LEGADO
# MAGIC ---
# MAGIC > União dos dois grupos com os campos normalizados para a tabela de saída.

# COMMAND ----------

# Une os dois grupos em um único DataFrame com o schema intermediário (sem group ainda)
previa_output_df = (
    df_novos_dia.union(df_legado_selecionado)
        .select(
            (monotonically_increasing_id() + 1).alias('pf_growth_users_opf_journey_communication_id'),
            "user_id",
            "bank_name",
            "entry_date",
            "latest_transaction_date",
            col("tipo").alias("type"),
            to_date(lit(DATA_HOJE)).alias("segmentation_date"),
        )
)

qtd_total = previa_output_df.count()
print(f"📋 Base diária final (antes da atribuição de grupo): {qtd_total:,} chaves")
print(f"   NOVOS  : {qtd_novos:,} ({100 * qtd_novos / qtd_total:.1f}%)")
print(f"   LEGADO : {qtd_legado_selecionado:,} ({100 * qtd_legado_selecionado / qtd_total:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.5 — Atribuição de Grupo GC / GT
# MAGIC ---
# MAGIC > **Por que aqui e não na Seção 4?**
# MAGIC > A Seção 4.4 excluiu os **já-GC** do pool *antes* da seleção. O que chegou aqui são
# MAGIC > somente usuários elegíveis (nunca foram GC). A atribuição do grupo acontece agora,
# MAGIC > depois que os 800k foram definidos, para não desperdiçar slots na seleção.
# MAGIC >
# MAGIC > **Método:** hash determinístico sobre `user_id` — idempotente (re-run = mesmo resultado),
# MAGIC > sem join com histórico (desnecessário pois GC já foi removido na Seção 4.4).
# MAGIC >
# MAGIC > | Condição | Grupo |
# MAGIC > |----------|-------|
# MAGIC > | `abs(hash(user_id)) % 100 < 10` | `GC` — entra no holdout hoje, excluído de amanhã em diante |
# MAGIC > | caso contrário | `GT` — comunicado normalmente |

# COMMAND ----------

# Todos os usuários aqui são não-GC (garantido pela exclusão na Seção 4.4).
# A atribuição via hash é determinística por user_id e não requer join com histórico.
_expr_grupo_ = when(
    (spark_abs(spark_hash("user_id")) % 100) < int(PCT_GC * 100),
    lit("GC")
).otherwise(lit("GT"))

_output_df_ = (
    previa_output_df
    .withColumn("experiment_group", _expr_grupo_)
    .select("user_id", "bank_name", "entry_date", "latest_transaction_date",
            "type", "experiment_group", "segmentation_date")
    .repartition(200)
)

_cnt_gc_ = _output_df_.filter(col("experiment_group") == "GC").count()
_cnt_gt_ = _output_df_.filter(col("experiment_group") == "GT").count()
_pct_gc_ = 100 * _cnt_gc_ / qtd_total if qtd_total > 0 else 0
print(f"📊 Distribuição de grupos (hash determinístico, PCT_GC={PCT_GC}):")
print(f"   GC (holdout) : {_cnt_gc_:,} ({_pct_gc_:.1f}%)  → excluídos do pool a partir de amanhã")
print(f"   GT (teste)   : {_cnt_gt_:,} ({100 - _pct_gc_:.1f}%)  → receberão comunicação")
print(f"   Total        : {qtd_total:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Write na Tabela de Histórico
# MAGIC ---
# MAGIC > `mode('overwrite')` com partition overwrite dinâmico reescreve apenas a partição `segmentation_date`
# MAGIC > do dia atual, preservando o histórico de todas as outras datas.
# MAGIC > `mergeSchema=True` garante que a coluna `experiment_group` seja adicionada sem erros em tabelas
# MAGIC > criadas por versões anteriores do notebook (v6/v7).

# COMMAND ----------

if spark.catalog.tableExists(TABELA_HISTORICO):
    # Tabela já existe: dynamic overwrite sobrescreve apenas a partição de hoje
    # mergeSchema: adiciona coluna 'experiment_group' se a tabela foi criada sem ela (v6/v7)
    _output_df_.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("segmentation_date") \
        .saveAsTable(TABELA_HISTORICO)
    print(f"✅ Partição {DATA_HOJE} sobrescrita em {TABELA_HISTORICO}")
else:
    # Primeira execução: static mode para criar a tabela sem erro de partição
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    _output_df_.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("segmentation_date") \
        .saveAsTable(TABELA_HISTORICO)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    print(f"✅ Tabela {TABELA_HISTORICO} criada (primeira execução)")

# Libera caches restantes
df_novos_dia.unpersist()
try:
    df_legado_elegivel.unpersist()
except:
    pass
print("🔓 Todos os caches liberados")

# COMMAND ----------

spark.sql(f"""
select * from {TABELA_HISTORICO} limit 5
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. Validações Rápidas
# MAGIC ---
# MAGIC > Conferências básicas pós-write para detectar anomalias antes de acionar os sistemas de envio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 — Contagem por tipo na geração do dia

# COMMAND ----------

spark.sql(f"""
    SELECT
        segmentation_date                                         AS `Data Processamento`,
        type                                                 AS Tipo,
        COUNT(DISTINCT bank_name)                            AS `Nº Bancos`,
        COUNT(*)                                             AS Chaves,
        COUNT(DISTINCT user_id)                              AS `Usuários`,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (
            PARTITION BY segmentation_date
        ), 2)                                                AS `Total (%)`,
        MIN(latest_transaction_date)                           AS `Última Trans. Mín`,
        MAX(latest_transaction_date)                           AS `Última Trans. Máx`
    FROM {TABELA_HISTORICO}
    GROUP BY 1,2
    ORDER BY segmentation_date DESC, type DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 — Distribuição por banco na geração do dia

# COMMAND ----------

spark.sql(f"""
    WITH base AS (
        SELECT
            segmentation_date,
            bank_name,
            SUM(CASE WHEN type = 'NOVO'   THEN 1 ELSE 0 END)  AS Novos,
            SUM(CASE WHEN type = 'LEGADO' THEN 1 ELSE 0 END)  AS Legado,
            COUNT(*)                                           AS Total
        FROM {TABELA_HISTORICO}
        GROUP BY 1,2
    )
    SELECT
        segmentation_date                                                AS Data,
        bank_name                                                    AS Banco,
        Novos,
        Legado,
        Total,
        ROUND(100.0 * Total / SUM(Total) OVER (), 2)                AS Pct_Total,
        SUM(Total) OVER (ORDER BY Total DESC)                        AS Vol_Acumulado,
        ROUND(100.0 * SUM(Total) OVER (ORDER BY Total DESC)
              / SUM(Total) OVER (), 2)                               AS Pct_Acumulado
    FROM base
    ORDER BY 1 desc, Total DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.3 — Verificação de duplicatas e integridade

# COMMAND ----------

df_dupes = spark.sql(f"""
    SELECT user_id, COUNT(*) AS ocorrencias
    FROM {TABELA_HISTORICO}
    WHERE segmentation_date = '{DATA_HOJE}'
    GROUP BY user_id
    HAVING COUNT(*) > 1
""")
cnt_dupes = df_dupes.count()
if cnt_dupes > 0:
    print(f"⚠️  ATENÇÃO: {cnt_dupes:,} user_ids duplicados na geração de {DATA_HOJE}")
    df_dupes.show(10)
else:
    print(f"✅ Sem duplicatas — cada user_id aparece exatamente 1 vez")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.4 — Verificação de cooldown

# COMMAND ----------

violacoes_cooldown = spark.sql(f"""
    SELECT COUNT(*) AS violacoes
    FROM {TABELA_HISTORICO} hoje
    WHERE hoje.segmentation_date = '{DATA_HOJE}'
      AND EXISTS (
          SELECT 1 FROM {TABELA_HISTORICO} hist
          WHERE hist.user_id = hoje.user_id
            AND hist.segmentation_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {JANELA_COOLDOWN_USUARIO_DIAS})
            AND hist.segmentation_date <  '{DATA_HOJE}'
      )
""").first()["violacoes"]

if violacoes_cooldown > 0:
    print(f"⚠️  ATENÇÃO: {violacoes_cooldown:,} usuários em cooldown foram selecionados!")
else:
    print(f"✅ Cooldown respeitado — nenhum usuário em cooldown foi selecionado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.5 — Verificação de blacklist

# COMMAND ----------

violacoes_chave = spark.sql(f"""
    SELECT COUNT(*) AS violacoes
    FROM {TABELA_HISTORICO} hoje
    WHERE hoje.segmentation_date = '{DATA_HOJE}'
      AND EXISTS (
          SELECT 1 FROM {TABELA_HISTORICO} hist
          WHERE hist.user_id   = hoje.user_id
            AND hist.bank_name = hoje.bank_name
            AND hist.segmentation_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {JANELA_BLOQUEIO_CHAVE_DIAS})
            AND hist.segmentation_date <  '{DATA_HOJE}'
      )
""").first()["violacoes"]

violacoes_usuario = spark.sql(f"""
    WITH rolling AS (
        SELECT user_id, segmentation_date,
               COUNT(*) OVER (
                   PARTITION BY user_id
                   ORDER BY CAST(segmentation_date AS TIMESTAMP)
                   RANGE BETWEEN INTERVAL {JANELA_CONTAGEM_COMUNICACOES - 1} DAYS PRECEDING
                             AND CURRENT ROW
               ) AS rolling_count
        FROM {TABELA_HISTORICO}
        WHERE segmentation_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {LOOKBACK_BLACKLIST_USUARIO})
          AND segmentation_date <  '{DATA_HOJE}'
    ),
    blacklisted AS (
        SELECT DISTINCT user_id
        FROM rolling
        WHERE rolling_count >= {LIMITE_COMUNICACOES_USUARIO}
    )
    SELECT COUNT(*) AS violacoes
    FROM {TABELA_HISTORICO} hoje
    INNER JOIN blacklisted b ON hoje.user_id = b.user_id
    WHERE hoje.segmentation_date = '{DATA_HOJE}'
""").first()["violacoes"]

if violacoes_chave > 0:
    print(f"⚠️  ATENÇÃO: {violacoes_chave:,} chaves em bloqueio (15d) foram selecionadas!")
else:
    print("✅ Bloqueio de chave respeitado")

if violacoes_usuario > 0:
    print(f"⚠️  ATENÇÃO: {violacoes_usuario:,} usuários em blacklist foram selecionados!")
else:
    print("✅ Blacklist de usuário respeitado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.6 — Histórico recente (últimas gerações)

# COMMAND ----------

spark.sql(f"""
    WITH BASE AS (
        SELECT
            segmentation_date                                           AS Data_Geracao,
            SUM(CASE WHEN type = 'NOVO'   THEN 1 ELSE 0 END)       AS Novos,
            SUM(CASE WHEN type = 'LEGADO' THEN 1 ELSE 0 END)       AS Legado,
            COUNT(*)                                               AS Chaves,
            COUNT(DISTINCT user_id)                                AS Users
        FROM {TABELA_HISTORICO}
        WHERE segmentation_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), 10)
        GROUP BY segmentation_date
    )
    SELECT
        Data_Geracao,
        Novos,
        ROUND(100*Novos / Chaves,2) AS `Novos (%)`,
        Legado,
        ROUND(100*Legado / Chaves,2) AS `Legado (%)`,
        Chaves,
        Users
    FROM BASE
    ORDER BY Data_Geracao DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.7 — Distribuição por Dia de Última Transação (NOVOS vs LEGADO)

# COMMAND ----------

spark.sql(f"""
    WITH base AS (
        SELECT
            segmentation_date,
            type,
            MIN(latest_transaction_date)  AS `Primeira Data`,
            MAX(latest_transaction_date)  AS `Última Data`,
            COUNT(*)                    AS Chaves
        FROM {TABELA_HISTORICO}
        GROUP BY segmentation_date, type
    )
    SELECT
        segmentation_date                                                 AS `Data Processamento`,
        type                                                         AS Tipo,
        `Primeira Data`,
        `Última Data`,
        Chaves,
        ROUND(100.0 * Chaves / SUM(Chaves) OVER (PARTITION BY segmentation_date), 2)
                                                                     AS Pct_Tipo,
        SUM(Chaves) OVER (
            ORDER BY segmentation_date ASC, type ASC
        )                                                            AS Vol_Acumulado
    FROM base
    ORDER BY segmentation_date desc, type DESC, segmentation_date DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.8 — Validação de Grupos GC/GT (v8)
# MAGIC ---
# MAGIC > Garante que:
# MAGIC > 1. Nenhum usuário histórico `GC` voltou a ser selecionado hoje (violação do holdout)
# MAGIC > 2. A distribuição GC/GT está próxima do alvo (~10%/~90%)

# COMMAND ----------

# Distribuição GC/GT na geração do dia
spark.sql(f"""
    SELECT
        segmentation_date                                            AS `Data Processamento`,
        `experiment_group`                                                 AS Grupo,
        type                                                    AS Tipo,
        COUNT(*)                                                AS Chaves,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (
            PARTITION BY segmentation_date
        ), 2)                                                   AS `Pct do Dia (%)`
    FROM {TABELA_HISTORICO}
    WHERE segmentation_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), 10)
    GROUP BY 1, 2, 3
    ORDER BY segmentation_date DESC, `experiment_group`, type
""").display()

# COMMAND ----------

# Verificação crítica: nenhum GC histórico deve aparecer na seleção de hoje
# (a Seção 4.4 deveria ter bloqueado; esta query detecta falhas de idempotência)
violacoes_holdout = spark.sql(f"""
    SELECT COUNT(*) AS violacoes
    FROM {TABELA_HISTORICO} hoje
    WHERE hoje.segmentation_date = '{DATA_HOJE}'
      AND EXISTS (
          SELECT 1
          FROM {TABELA_HISTORICO} hist
          WHERE hist.user_id     = hoje.user_id
            AND hist.`experiment_group`     = 'GC'
            AND hist.segmentation_date < '{DATA_HOJE}'
      )
""").first()["violacoes"]

if violacoes_holdout > 0:
    print(f"🚨 VIOLAÇÃO DE HOLDOUT: {violacoes_holdout:,} usuários que eram GC foram selecionados hoje!")
    print("   Isso indica falha na exclusão da Seção 4.4 — investigar imediatamente.")
    spark.sql(f"""
        SELECT hoje.user_id, hoje.`experiment_group` AS grupo_hoje,
               hist.segmentation_date AS data_gc_historico
        FROM {TABELA_HISTORICO} hoje
        INNER JOIN {TABELA_HISTORICO} hist ON hoje.user_id = hist.user_id
        WHERE hoje.segmentation_date = '{DATA_HOJE}'
          AND hist.`experiment_group` = 'GC'
          AND hist.segmentation_date < '{DATA_HOJE}'
        LIMIT 20
    """).display()
else:
    print("✅ Holdout GC íntegro — nenhum usuário GC histórico foi selecionado hoje")

# COMMAND ----------

# Histórico de distribuição GC/GT por data (mostra evolução do experimento)
spark.sql(f"""
    SELECT
        segmentation_date                                                AS Data,
        SUM(CASE WHEN `experiment_group` = 'GC' THEN 1 ELSE 0 END)            AS GC,
        SUM(CASE WHEN `experiment_group` = 'GT' THEN 1 ELSE 0 END)            AS GT,
        COUNT(*)                                                    AS Total,
        ROUND(100.0 * SUM(CASE WHEN `experiment_group` = 'GC' THEN 1 ELSE 0 END) / COUNT(*), 2)  AS `GC (%)`,
        ROUND(100.0 * SUM(CASE WHEN `experiment_group` = 'GT' THEN 1 ELSE 0 END) / COUNT(*), 2)  AS `GT (%)`
    FROM {TABELA_HISTORICO}
    WHERE segmentation_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), 30)
    GROUP BY segmentation_date
    ORDER BY segmentation_date DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 10. VALIDAÇÃO `DF_PRIORIZADO`

# COMMAND ----------

# DBTITLE 1,Levantamento de dados PRÉ
from pyspark.sql.functions import col, expr, countDistinct
display(

    df_priorizado
        .filter(col('n_priority')== 1)
        .select('*', expr("case when new_key = 1 then 'NOVO' else 'LEGADO' end as type"))
        .groupBy('type')
        .agg(
            countDistinct('user_id'), count("*")
        )
)

# COMMAND ----------

from pyspark.sql.functions import date_format, sum, expr, countDistinct, round,current_date,add_months,explode
from pyspark.sql import Window
from pyspark.sql.types import IntegerType


temp_quartil = spark.sql(f"""

WITH date_range AS (
  SELECT
    explode(sequence(add_months(current_date(), -12), current_date())) AS dia
),
distancia_meses AS (
  SELECT
    dia,
    floor(months_between(current_date(), dia)) AS meses_atras
  FROM date_range
)
SELECT
  dia,
  CASE
    WHEN meses_atras < 3  THEN '0 - 3'
    WHEN meses_atras < 6  THEN '3 - 6'
    WHEN meses_atras < 9  THEN '6 - 9'
    WHEN meses_atras <= 12 THEN '9 - 12'
    ELSE '+12'
  END AS range_month
FROM distancia_meses

""")

temp_quartil.createOrReplaceTempView("temp_quartil")


temp_window_spec = Window.partitionBy("type","bank_name").orderBy(col("latest_transaction_date").desc())
temp_total_keys = df_priorizado.filter(col('n_priority') == 1).agg(countDistinct('user_id')).collect()[0][0]

display(
    df_priorizado
        .filter(col('n_priority')== 1)
        .select('*', expr("case when new_key = 1 then 'NOVO' else 'LEGADO' end as type"))
        .groupBy(
            date_format('latest_transaction_date', 'yyyy-MM').alias('Month'),
            'latest_transaction_date',
            'type',
            'bank_name'
        )
        .agg(
            countDistinct('user_id').alias('# Users'),
            count("*").alias('# Keys')
        )
        .withColumn("# Users Acum (Type)", sum("# Users").over(temp_window_spec))
        .withColumn("# Keys Acum (Type)", sum("# Keys").over(temp_window_spec))
        .withColumn("% Total (Type)", round(100*col("# Keys") / sum("# Keys").over(Window.partitionBy("type")), 2))
        .withColumn("% Total Acum (Type)", round(sum("% Total (Type)").over(temp_window_spec),2))
        .withColumn("TOTAL CHAVES (%)", round(100*col("# Keys") / temp_total_keys, 2))

        .join(temp_quartil, col('dia') == col('latest_transaction_date'), "inner")
        .select(
            col('latest_transaction_date').alias('Date'),
            col('range_month').alias('Month Range'),
            'Month',
            'type',
            col('bank_name').alias('Bank'),
            '# Users',
            "# Users Acum (Type)",
            "# Keys",
            "# Keys Acum (Type)",
            "% Total (Type)",
            "% Total Acum (Type)",
            'TOTAL CHAVES (%)'
        )
        .orderBy(col('type').desc(),col('latest_transaction_date').desc())
)

# COMMAND ----------

spark.sql(f"""
    WITH base AS (
        SELECT
            latest_transaction_date                                                   AS Dia,
            CASE WHEN new_key = 1 THEN 'NOVO' ELSE 'LEGADO' END                     AS `Tipo`,
            COUNT(*)                                                                AS `Total`,
            SUM(CASE WHEN bank_name = 'MERCADO PAGO'    THEN 1 ELSE 0 END)               AS `1_MERCADO_PAGO`,
            SUM(CASE WHEN bank_name = 'MERCADO PAGO'    THEN 1 ELSE 0 END) / COUNT(*)    AS `1_MERCADO_PAGO_(%)`,
            SUM(CASE WHEN bank_name = 'NUBANK'          THEN 1 ELSE 0 END)               AS `2_NUBANK`,
            SUM(CASE WHEN bank_name = 'NUBANK'          THEN 1 ELSE 0 END) / COUNT(*)    AS `2_NUBANK_(%)`,
            SUM(CASE WHEN bank_name = 'RECARGAPAY'      THEN 1 ELSE 0 END)               AS `3_RECARGAPAY`,
            SUM(CASE WHEN bank_name = 'RECARGAPAY'      THEN 1 ELSE 0 END) / COUNT(*)    AS `3_RECARGAPAY_(%)`,
            SUM(CASE WHEN bank_name = 'SHOPEE'          THEN 1 ELSE 0 END)               AS `4_SHOPEE`,
            SUM(CASE WHEN bank_name = 'SHOPEE'          THEN 1 ELSE 0 END) / COUNT(*)    AS `4_SHOPEE_(%)`,
            SUM(CASE WHEN bank_name = 'SANTANDER'       THEN 1 ELSE 0 END)               AS `5_SANTANDER`,
            SUM(CASE WHEN bank_name = 'SANTANDER'       THEN 1 ELSE 0 END) / COUNT(*)    AS `5_SANTANDER_(%)`,
            SUM(CASE WHEN bank_name = 'CAIXA'           THEN 1 ELSE 0 END)               AS `6_CAIXA`,
            SUM(CASE WHEN bank_name = 'CAIXA'           THEN 1 ELSE 0 END) / COUNT(*)    AS `6_CAIXA_(%)`,
            SUM(CASE WHEN bank_name = 'INTER'           THEN 1 ELSE 0 END)               AS `7_INTER`,
            SUM(CASE WHEN bank_name = 'INTER'           THEN 1 ELSE 0 END) / COUNT(*)    AS `7_INTER_(%)`,
            SUM(CASE WHEN bank_name = 'BRADESCO'        THEN 1 ELSE 0 END)               AS `8_BRADESCO`,
            SUM(CASE WHEN bank_name = 'BRADESCO'        THEN 1 ELSE 0 END) / COUNT(*)    AS `8_BRADESCO_(%)`,
            SUM(CASE WHEN bank_name = 'BANCO DO BRASIL' THEN 1 ELSE 0 END)               AS `9_BANCO_DO_BRASIL`,
            SUM(CASE WHEN bank_name = 'BANCO DO BRASIL' THEN 1 ELSE 0 END) / COUNT(*)    AS `9_BANCO_DO_BRASIL_(%)`,
            SUM(CASE WHEN bank_name = 'PAN'             THEN 1 ELSE 0 END)               AS `10_PAN`,
            SUM(CASE WHEN bank_name = 'PAN'             THEN 1 ELSE 0 END) / COUNT(*)    AS `10_PAN_(%)`,
            SUM(CASE WHEN bank_name = 'NEXT'            THEN 1 ELSE 0 END)               AS `11_NEXT`,
            SUM(CASE WHEN bank_name = 'NEXT'            THEN 1 ELSE 0 END) / COUNT(*)    AS `11_NEXT_(%)`,
            SUM(CASE WHEN bank_name = 'NEON'            THEN 1 ELSE 0 END)               AS `12_NEON`,
            SUM(CASE WHEN bank_name = 'NEON'            THEN 1 ELSE 0 END) / COUNT(*)    AS `12_NEON_(%)`,
            SUM(CASE WHEN bank_name = 'C6 BANK'         THEN 1 ELSE 0 END)               AS `13_C6_BANK`,
            SUM(CASE WHEN bank_name = 'C6 BANK'         THEN 1 ELSE 0 END) / COUNT(*)    AS `13_C6_BANK_(%)`,
            SUM(CASE WHEN bank_name = '99PAY'           THEN 1 ELSE 0 END)               AS `14_99PAY`,
            SUM(CASE WHEN bank_name = '99PAY'           THEN 1 ELSE 0 END) / COUNT(*)    AS `14_99PAY_(%)`,
            SUM(CASE WHEN bank_name = 'BANCO DO NORDESTE' THEN 1 ELSE 0 END)             AS `15_BANCO_DO_NORDESTE`,
            SUM(CASE WHEN bank_name = 'BANCO DO NORDESTE' THEN 1 ELSE 0 END) / COUNT(*)  AS `15_BANCO_DO_NORDESTE_(%)`,
            SUM(CASE WHEN bank_name = 'MERCANTIL'       THEN 1 ELSE 0 END)               AS `16_MERCANTIL`,
            SUM(CASE WHEN bank_name = 'MERCANTIL'       THEN 1 ELSE 0 END) / COUNT(*)    AS `16_MERCANTIL_(%)`,
            -- SUM(CASE WHEN bank_name = 'BRB'             THEN 1 ELSE 0 END)               AS `17_BRB`,
            -- SUM(CASE WHEN bank_name = 'BRB'             THEN 1 ELSE 0 END) / COUNT(*)    AS `17_BRB_(%)`,
            SUM(CASE WHEN bank_name = 'CREFISA'         THEN 1 ELSE 0 END)               AS `18_CREFISA`,
            SUM(CASE WHEN bank_name = 'CREFISA'         THEN 1 ELSE 0 END) / COUNT(*)    AS `18_CREFISA_(%)`,
            SUM(CASE WHEN bank_name = 'MIDWAY'          THEN 1 ELSE 0 END)               AS `19_MIDWAY`,
            SUM(CASE WHEN bank_name = 'MIDWAY'          THEN 1 ELSE 0 END) / COUNT(*)    AS `19_MIDWAY_(%)`,
            SUM(CASE WHEN bank_name = 'PAGSEGURO'       THEN 1 ELSE 0 END)               AS `20_PAGSEGURO`,
            SUM(CASE WHEN bank_name = 'PAGSEGURO'       THEN 1 ELSE 0 END) / COUNT(*)    AS `20_PAGSEGURO_(%)`,
            -- SUM(CASE WHEN bank_name = 'WILL BANK'       THEN 1 ELSE 0 END)               AS `21_WILL_BANK`,
            -- SUM(CASE WHEN bank_name = 'WILL BANK'       THEN 1 ELSE 0 END) / COUNT(*)    AS `21_WILL_BANK_(%)`,
            SUM(CASE WHEN bank_name = 'SAFRA'           THEN 1 ELSE 0 END)               AS `22_SAFRA`,
            SUM(CASE WHEN bank_name = 'SAFRA'           THEN 1 ELSE 0 END) / COUNT(*)    AS `22_SAFRA_(%)`,
            SUM(CASE WHEN bank_name = 'BTG'             THEN 1 ELSE 0 END)               AS `23_BTG`,
            SUM(CASE WHEN bank_name = 'BTG'             THEN 1 ELSE 0 END) / COUNT(*)    AS `23_BTG_(%)`,
            SUM(CASE WHEN bank_name = 'BANCO CARREFOUR' THEN 1 ELSE 0 END)               AS `24_BANCO_CARREFOUR`,
            SUM(CASE WHEN bank_name = 'BANCO CARREFOUR' THEN 1 ELSE 0 END) / COUNT(*)    AS `24_BANCO_CARREFOUR_(%)`,
            SUM(CASE WHEN bank_name = 'OUTRO'           THEN 1 ELSE 0 END)               AS `25_OUTRO`,
            SUM(CASE WHEN bank_name = 'OUTRO'           THEN 1 ELSE 0 END) / COUNT(*)    AS `25_OUTRO_(%)`
        FROM df_priorizado
        WHERE n_priority = 1
        GROUP BY 1,2
    )
    SELECT
        A.Dia,
        Tipo,
        range_month                                                 AS RANGE_MONTH,
        A.Total,
        ROUND(100.0 * Total / SUM(Total) OVER (ORDER BY A.Dia), 2)  AS `Total_(%)`,
        `1_MERCADO_PAGO`,
        `2_NUBANK`,
        `3_RECARGAPAY`,
        `4_SHOPEE`,
        `5_SANTANDER`,
        `6_CAIXA`,
        `7_INTER`,
        `8_BRADESCO`,
        `9_BANCO_DO_BRASIL`,
        `10_PAN`,
        `11_NEXT`,
        `12_NEON`,
        `13_C6_BANK`,
        `14_99PAY`,
        `15_BANCO_DO_NORDESTE`,
        `16_MERCANTIL`,
        -- `17_BRB`,
        `18_CREFISA`,
        `19_MIDWAY`,
        `20_PAGSEGURO`,
        -- `21_WILL_BANK`,
        `22_SAFRA`,
        `23_BTG`,
        `24_BANCO_CARREFOUR`,
        `25_OUTRO`
    FROM BASE AS A
    INNER JOIN temp_quartil AS B
        ON A.Dia = B.dia
    ORDER BY Dia DESC, Tipo DESC

""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC   segmentation_date,
# MAGIC   experiment_group,
# MAGIC   count(*) users,
# MAGIC   count(distinct user_id) chaves,
# MAGIC   round(100 * count(*) / sum(count(*))over(), 2) as PCT 
# MAGIC FROM self_service_analytics.pf_growth_users_opf_journey_communication
# MAGIC group by 1,2
# MAGIC order by 1 desc, 2
# MAGIC -- WHERE coluna_particao = '2026-03-01';

# COMMAND ----------

