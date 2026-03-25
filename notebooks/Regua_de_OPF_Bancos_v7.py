# Databricks notebook source
# MAGIC %md
# MAGIC # 🏦 Régua IF — OPF (Open Finance) — v7
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
# MAGIC ## Fontes e saída
# MAGIC | Tabela | Papel | Descrição |
# MAGIC |--------|-------|-----------|
# MAGIC | `validation.pp_users_growth_opf_communication` | **Input** | Usuários elegíveis com IFs detectadas (gerada pelo notebook SS) |
# MAGIC | `consumers.dim_consumers_metrics` | **Lookup** | Filtra apenas MAU (`is_mau = true`) |
# MAGIC | `validation.pp_users_growth_opf` | **Input/Output** | Histórico de chaves — consultado para cooldown, blacklist e sobrescrito ao final |
# MAGIC
# MAGIC ## Schema da tabela de saída — `validation.pp_users_growth_opf`
# MAGIC | Coluna | Tipo | Descrição |
# MAGIC |--------|------|-----------|
# MAGIC | `user_id` | long | ID do usuário PicPay |
# MAGIC | `bank_name` | string | IF selecionada para comunicação |
# MAGIC | `entry_date` | date | Data de entrada da chave na base elegível |
# MAGIC | `last_transaction_date` | date | Última transação detectada com essa IF |
# MAGIC | `type` | string | `NOVO` ou `LEGADO` |
# MAGIC | `updated_date` | date | Data de geração (partição) |
# MAGIC
# MAGIC ## Fluxo resumido
# MAGIC ```
# MAGIC Tabela SS (fonte)
# MAGIC     ↓  join MAU + normalização de banco
# MAGIC Base MAU Elegível (melhor banco por user_id)
# MAGIC     ↓  split por data de entrada
# MAGIC ┌─────────────────────┐    ┌──────────────────────────────────────────┐
# MAGIC │ NOVOS               │    │ LEGADO ELEGÍVEL                          │
# MAGIC │ (desde última run)  │    │ - Remove cooldown de usuário (N dias)    │
# MAGIC │ Todos entram        │    │ - Remove blacklist de chave              │
# MAGIC │ até 800k.           │    │   (≥ X comms em Y dias → bloqueada Z dias)│
# MAGIC └──────────┬──────────┘    │ - Ordena: last_tx DESC → prio banco      │
# MAGIC            │               │ - Limit: slots restantes após NOVOS      │
# MAGIC            │               └──────────────────┬───────────────────────┘
# MAGIC            └──────────────────────────────────┘
# MAGIC                               ↓  union
# MAGIC                    Write → validation.pp_users_growth_opf
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
from pyspark.sql.functions import col, count, date_format, lit, to_date, row_number, desc, asc
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

# COMMAND ----------

# %sql
# drop table validation.pp_users_growth_opf

# COMMAND ----------

# ── Tabelas ──────────────────────────────────────────────────────────────────
# TABELA_FONTE_NAME  = "self_service_analytics.pp_users_identified_accounts_opf_communication"
TABELA_FONTE_NAME  = "validation.pp_users_growth_opf_communication"
# CAMPO_DATA_ENTRADA = "most_recent_transaction_at"
CAMPO_DATA_ENTRADA = "last_transaction"
CAMPO_DATA_ATUALIZACAO_FONTE = "last_transaction"
TABELA_HISTORICO   = "validation.pp_users_growth_opf"

# ── Datas de referência ───────────────────────────────────────────────────────
# DATA_HOJE         = '2026-03-19'
DATA_HOJE         = date.today().strftime("%Y-%m-%d")
DATA_INICIO_CORTE = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")
DATA_FONTE_MINIMA = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
# DATA_NOVA_CHAVE   = '2026-03-17'
DATA_NOVA_CHAVE   = (date.today() - timedelta(days=2)).strftime("%Y-%m-%d")

# ── Cooldown de usuário ───────────────────────────────────────────────────────
# Após ser selecionado em qualquer dia, o usuário não aparece novamente
# até que passem JANELA_COOLDOWN_USUARIO_DIAS dias completos.
JANELA_COOLDOWN_USUARIO_DIAS = 3 

# ── Bloqueio de chave ─────────────────────────────────────────────────────────
# Qualquer chave comunicada fica bloqueada por 15 dias (sem contagem acumulada).
JANELA_BLOQUEIO_CHAVE_DIAS = 15

# ── ALTERAÇÃO v5 → v6 — Blacklist agora é de USUÁRIO ────────────────────────
# O gatilho ainda é 3 comms em 30d, mas bloqueia o USUÁRIO por 30d (qualquer banco).
JANELA_CONTAGEM_COMUNICACOES   = 30
LIMITE_COMUNICACOES_USUARIO    = 3
JANELA_BLOQUEIO_USUARIO_DIAS   = 30

# ── Volume diário ─────────────────────────────────────────────────────────────
LIMITE_NOVOS  = 800_000  # Cap para NOVOS/dia; 0 = sem limite (Bank-Bucket-Drain)
LIMITE_DIARIO = 800_000  # Cap total/dia (NOVOS + LEGADO); 0 = sem limite

print("=" * 65)
print(f"  Pipeline OPF v7 — {DATA_HOJE}")
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
print("=" * 65)

# COMMAND ----------

# Contexto da alteração no GitHub: notebook de produção 2964021027643933
# Link de referência informado pelo usuário:
# https://picpay-principal.cloud.databricks.com/editor/notebooks/2964021027643933?o=3048457690269382#command/6383714641200101

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
# MAGIC > **dentro do mesmo valor de `last_transaction_date`**, o banco de menor prioridade numérica é preferido.

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
        SELECT COALESCE(MAX(updated_date-1), DATE_SUB(CURRENT_DATE(), 2)) AS dt
        FROM {TABELA_HISTORICO}
        WHERE updated_date < '{DATA_HOJE}'
    """).first()
    dt_ultima_geracao = str(row["dt"])
except AnalysisException:
    # Tabela ainda não existe — primeira execução
    dt_ultima_geracao = DATA_NOVA_CHAVE

print(f"📅 Última geração: {dt_ultima_geracao}")
print(f"   → Chaves com last_transaction_date >= {dt_ultima_geracao} serão classificadas como NOVO")

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
        -- DATE(oldest_transaction_at)                             AS entry_date,
        DATE(first_transaction)                             AS entry_date,
        -- DATE(most_recent_transaction_at)                              AS last_transaction_date,
        DATE(last_transaction)                              AS last_transaction_date,
        CASE WHEN {CAMPO_DATA_ENTRADA} >= '{dt_ultima_geracao}' 
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
    last_transaction_date,
    new_key, 
    bank_name_priority,
    ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY
            CASE WHEN new_key = 1 THEN 0 ELSE 1 END ASC,           -- NOVO antes de LEGADO
            CASE WHEN new_key = 1 THEN bank_name_priority END,     -- NOVO: banco decide
            -- LEGADO
            last_transaction_date DESC, bank_name_priority ASC
    ) AS n_priority
FROM base
""")

# Persiste antes dos múltiplos usos — evita re-executar o join com MAU
df_priorizado.persist(StorageLevel.MEMORY_AND_DISK)
cnt_priorizado = df_priorizado.count()
df_priorizado.createOrReplaceTempView("df_priorizado")
print(f"✅ Base MAU elegível: {cnt_priorizado:,} registros (1 por user_id)")


# display(
#     df_priorizado
#         .filter(col('user_id').isin(329906137388766781,141136200300601935,291181991611089103))
#         .orderBy(col('user_id'),col('last_transaction_date').desc(), col('n_priority').asc())
# )

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Regras de Exclusão
# MAGIC ---
# MAGIC > Três listas de bloqueio são calculadas **antes** de classificar NOVO vs LEGADO:
# MAGIC >
# MAGIC > | Regra | Quem bloqueia | Parâmetro |
# MAGIC > |-------|---------------|-----------|
# MAGIC > | **Data da última geração** | Define o limiar de NOVO vs LEGADO | — |
# MAGIC > | **Cooldown de usuário** | Usuário comunicado nos últimos `JANELA_COOLDOWN_USUARIO_DIAS` (3) dias | `JANELA_COOLDOWN_USUARIO_DIAS` |
# MAGIC > | **Bloqueio de chave** *(v6)* | Chave comunicada nos últimos `JANELA_BLOQUEIO_CHAVE_DIAS` (15) dias | `JANELA_BLOQUEIO_CHAVE_DIAS` |
# MAGIC > | **Blacklist de usuário** *(v6)* | Usuário com ≥ `LIMITE_COMUNICACOES_USUARIO` (3) comms em 30d → bloqueado por `JANELA_BLOQUEIO_USUARIO_DIAS` (30d) | `JANELA_BLOQUEIO_USUARIO_DIAS` |

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

# Coleta user_ids que foram comunicados dentro da janela de cooldown
    # Exclui o próprio dia atual para não bloquear reprocessamentos
try:
    df_cooldown_usuario = spark.sql(f"""
        SELECT DISTINCT user_id
        FROM {TABELA_HISTORICO}
        WHERE updated_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {JANELA_COOLDOWN_USUARIO_DIAS})
          AND updated_date <  '{DATA_HOJE}'
    """)
    df_cooldown_usuario.persist(StorageLevel.MEMORY_AND_DISK)
    cnt_cooldown = df_cooldown_usuario.count()
    df_cooldown_usuario.createOrReplaceTempView("cooldown_usuario")
    print(f"🚫 Cooldown de usuário ({JANELA_COOLDOWN_USUARIO_DIAS} dias): {cnt_cooldown:,} usuários bloqueados")
except AnalysisException:
    # Primeira execução: nenhum usuário em cooldown
    spark.sql("CREATE OR REPLACE TEMP VIEW cooldown_usuario AS SELECT CAST(0L AS BIGINT) AS user_id WHERE 1=0")
    cnt_cooldown = 0
    print("ℹ️  Histórico vazio — nenhum usuário em cooldown (primeira execução)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 — Bloqueio de Chave
# MAGIC > *(Seção nova em v6 — substitui a lógica condicional de blacklist de chave da v5)*
# MAGIC >
# MAGIC > **Regra:** Qualquer chave (`user_id` + `bank_name`) comunicada nos últimos
# MAGIC > `JANELA_BLOQUEIO_CHAVE_DIAS` (15) dias fica **bloqueada imediatamente** — sem contagem acumulada.
# MAGIC >
# MAGIC > **Diferença da v5:** Na v5, a chave só era bloqueada após 3 comunicações em 30 dias.
# MAGIC > Agora, uma única comunicação garante 15 dias de descanso para aquela combinação user+banco.
# MAGIC >
# MAGIC > **Lookback:** Apenas `JANELA_BLOQUEIO_CHAVE_DIAS` (15) dias — sem janela rolante.

# COMMAND ----------

# ── 4.2: Bloqueio de Chave ─────────────────────────────────────────────────
# Simples: busca chaves comunicadas nos últimos JANELA_BLOQUEIO_CHAVE_DIAS (15) dias.
# Lookback = 15 dias (sem janela rolante).
try:
    df_bloqueio_chave = spark.sql(f"""
        SELECT DISTINCT user_id, bank_name
        FROM {TABELA_HISTORICO}
        WHERE updated_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {JANELA_BLOQUEIO_CHAVE_DIAS})
          AND updated_date <  '{DATA_HOJE}'
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
# MAGIC > *(Alterado em v6 — na v5 bloqueava a chave; agora bloqueia o usuário inteiro)*
# MAGIC >
# MAGIC > **Regra:** Se um usuário receber `LIMITE_COMUNICACOES_USUARIO` (3) ou mais comunicações
# MAGIC > de **qualquer banco** dentro de `JANELA_CONTAGEM_COMUNICACOES` (30) dias,
# MAGIC > o **usuário inteiro** fica bloqueado por `JANELA_BLOQUEIO_USUARIO_DIAS` (30) dias.
# MAGIC >
# MAGIC > **Diferença da v5:** Na v5, o bloqueio era por chave (user+banco) por 70 dias.
# MAGIC > Na v6, 3 comunicações de qualquer combinação de bancos bloqueiam o usuário completamente.
# MAGIC >
# MAGIC > **Lookback:** `JANELA_BLOQUEIO_USUARIO_DIAS + JANELA_CONTAGEM_COMUNICACOES` = 30 + 30 = **60 dias**.

# COMMAND ----------


# ── 4.3: Blacklist de Usuário ──────────────────────────────────────────────
# Conta comunicações por usuário (qualquer banco) em janela rolante de 30 dias.
# Lookback = JANELA_BLOQUEIO_USUARIO_DIAS + JANELA_CONTAGEM_COMUNICACOES = 30 + 30 = 60 dias.
LOOKBACK_BLACKLIST_USUARIO = JANELA_BLOQUEIO_USUARIO_DIAS + JANELA_CONTAGEM_COMUNICACOES

try:
    df_blacklist_usuario = spark.sql(f"""
        WITH historico_recente AS (
            -- Registros do usuário nos últimos 60 dias (qualquer banco)
            SELECT user_id, updated_date
            FROM {TABELA_HISTORICO}
            WHERE updated_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {LOOKBACK_BLACKLIST_USUARIO})
              AND updated_date <  '{DATA_HOJE}'
        ),
        rolling_counts AS (
            -- Conta comunicações do usuário (qualquer banco) em janela rolante de 30 dias
            SELECT
                user_id,
                updated_date,
                COUNT(*) OVER (
                    PARTITION BY user_id
                    ORDER BY CAST(updated_date AS TIMESTAMP)
                    RANGE BETWEEN INTERVAL {JANELA_CONTAGEM_COMUNICACOES - 1} DAYS PRECEDING
                              AND CURRENT ROW
                ) AS rolling_count
            FROM historico_recente
        ),
        gatilhos AS (
            -- Data mais recente em que o usuário atingiu ou superou o limite
            SELECT
                user_id,
                MAX(updated_date) AS ultimo_gatilho
            FROM rolling_counts
            WHERE rolling_count >= {LIMITE_COMUNICACOES_USUARIO}
            GROUP BY user_id
        )
        -- Usuário bloqueado se o último gatilho está dentro do período de bloqueio
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
# MAGIC # 5. Classificação NOVO vs LEGADO
# MAGIC ---
# MAGIC > A base priorizada (1 linha por user_id) é dividida em dois grupos:
# MAGIC >
# MAGIC > | Grupo | Critério | Exclusões aplicadas | Limite |
# MAGIC > |-------|----------|---------------------|--------|
# MAGIC > | **NOVO** | `last_transaction_date >= dt_ultima_geracao` | Cooldown de usuário; Blacklist de chave | Limitado a 800k. Todos entram se N<800k |
# MAGIC > | **LEGADO** | `last_transaction_date < dt_ultima_geracao` | Cooldown de usuário; Blacklist de chave | Até `LIMITE_DIARIO - qtd_novos` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 — Grupo NOVOS
# MAGIC
# MAGIC - O QUÊ: Adicionado limite de NOVOS via Bank-Bucket-Drain (espelho do Day-Bucket-Drain do LEGADO).
# MAGIC         Pool completo é gerado primeiro; depois aplica o corte se qtd_novos_pool > LIMITE_NOVOS.
# MAGIC
# MAGIC - POR QUÊ: NOVOS podem ultrapassar 800 k em dias de alta captação. Sem limite, todo o LIMITE_DIARIO
# MAGIC          poderia ser consumido por NOVOS, zerando slots do LEGADO. O Bank-Bucket-Drain respeita
# MAGIC          a prioridade de banco (banco 1 esgota antes do banco 2) e usa last_transaction_date DESC
# MAGIC          como desempate dentro de um banco parcialmente cortado.
# MAGIC ---

# COMMAND ----------



# Passo A: pool completo de NOVOS elegíveis (cooldown + blacklist já aplicados)
df_novos_pool = spark.sql(f"""
    SELECT
        f.user_id,
        f.bank_name,
        f.bank_name_priority,
        f.entry_date,
        f.last_transaction_date,
        'NOVO' AS tipo
    FROM df_priorizado AS f
    WHERE f.n_priority = 1
      AND new_key = 1 
      AND NOT EXISTS (SELECT 1 FROM cooldown_usuario cu WHERE cu.user_id = f.user_id)
      -- Exclui chaves em bloqueio (15 dias após comunicação) — v6
      AND NOT EXISTS (
          SELECT 1 FROM bloqueio_chave bc
          WHERE bc.user_id = f.user_id AND bc.bank_name = f.bank_name
      )
      -- Exclui usuários em blacklist de usuário (3x em 30d → 30d) — v6
      AND NOT EXISTS (SELECT 1 FROM blacklist_usuario bu WHERE bu.user_id = f.user_id)
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
    # Conta por banco (pouquíssimos valores — trivial)
    bank_counts = (
        df_novos_pool
        .groupBy("bank_name_priority", "bank_name")
        .count()
        .orderBy("bank_name_priority")  # banco mais prioritário primeiro
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
        status = "✅ completo" if take == n else "✂️  cortado (last_transaction_date DESC)"
        print(f"{bn:<35} {n:>12,} {take:>12,}  {status}")
    print(f"\n→ {len(bank_buckets)} bancos usados de {len(bank_counts)} disponíveis")

    dfs_novos = []
    for bp, bn, take, n in bank_buckets:
        if take == n:
            dfs_novos.append(df_novos_pool.filter(col("bank_name_priority") == bp))
        else:
            # Único banco parcial: sort só neste subconjunto — last_transaction_date DESC decide
            df_corte = spark.sql(f"""
                SELECT user_id, bank_name, bank_name_priority,
                       entry_date, last_transaction_date, tipo
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY bank_name_priority
                            ORDER BY last_transaction_date DESC
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

# Chaves de legado: existiam antes da última geração
# Aplicamos as mesmas exclusões de cooldown e blacklist
# Esta é a base COMPLETA do legado — o limite será aplicado na próxima seção
df_legado_elegivel = spark.sql(f"""
    SELECT
        f.user_id, 
        f.bank_name, 
        f.bank_name_priority,
        f.entry_date, 
        f.last_transaction_date,
        'LEGADO' AS tipo
    FROM df_priorizado AS f
    WHERE f.n_priority = 1 
      AND new_key = 0 
      -- Exclui usuários em cooldown (3 dias)
      AND NOT EXISTS (SELECT 1 FROM cooldown_usuario cu WHERE cu.user_id = f.user_id)
      -- Exclui chaves em bloqueio (15 dias após comunicação)
      AND NOT EXISTS (
          SELECT 1 FROM bloqueio_chave bc
          WHERE bc.user_id = f.user_id AND bc.bank_name = f.bank_name
      )
      -- Exclui usuários em blacklist (3x em 30d → 30d bloqueio)
      AND NOT EXISTS (SELECT 1 FROM blacklist_usuario bu WHERE bu.user_id = f.user_id)
      -- Exclui usuários que já entraram como NOVOS hoje (evita duplicação)
      AND NOT EXISTS (SELECT 1 FROM novos_dia nd WHERE nd.user_id = f.user_id)
""")

df_legado_elegivel.persist(StorageLevel.MEMORY_AND_DISK)
qtd_legado_pool = df_legado_elegivel.count()
df_legado_elegivel.createOrReplaceTempView("legado_elegivel")
print(f"📦 Pool de LEGADO elegível (antes do limite): {qtd_legado_pool:,}")

# Libera o que não é mais necessário
df_priorizado.unpersist()
try:
    df_cooldown_usuario.unpersist()
    df_bloqueio_chave.unpersist()
    df_blacklist_usuario.unpersist()
except:
    pass
print("🔓 Caches intermediários liberados")

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Aplicação do Limite Diário no Legado
# MAGIC ---
# MAGIC > **Nova lógica de priorização (v4→v5):**
# MAGIC > O legado é ordenado por **`last_transaction_date DESC`** (usuários com transação mais recente na IF
# MAGIC > têm prioridade — maior chance de conversão) e, como desempate, por **prioridade de banco**.
# MAGIC >
# MAGIC > Isso substitui o BUCKET-DRAIN da v2 (que esgotava um banco inteiro antes de passar ao próximo).
# MAGIC > Agora a seleção é **transversal a todos os bancos**, priorizando engajamento recente.
# MAGIC >
# MAGIC > **Performance:** `ROW_NUMBER() OVER (ORDER BY ...)` requer um sort global em Spark.
# MAGIC > Para volumes grandes, o AQE coalesce automaticamente as partições após o sort.

# COMMAND ----------

# Calcula quantos slots restam para o legado após alocar os NOVOS
if LIMITE_DIARIO > 0:
    slots_legado = max(0, LIMITE_DIARIO - qtd_novos)
else:
    # Sem limite configurado: pega todo o pool
    slots_legado = qtd_legado_pool

print(f"📊 Slots disponíveis para LEGADO: {slots_legado:,}")
print(f"   (Limite diário {LIMITE_DIARIO:,} − {qtd_novos:,} NOVOS selecionados = {slots_legado:,} slots)")
print(f"   (NOVOS eram {qtd_novos_pool:,} no pool; cap de {LIMITE_NOVOS:,} aplicado)")

# COMMAND ----------

from functools import reduce

if slots_legado <= 0:
    # Todos os slots já foram preenchidos pelos NOVOS
    df_legado_selecionado = spark.createDataFrame([], df_legado_elegivel.schema)
    print(f"⚠️  Sem slots para legado — NOVOS ({qtd_novos:,}) já atingiram o limite ({LIMITE_DIARIO:,})")

elif slots_legado >= qtd_legado_pool:
    # O pool inteiro cabe dentro dos slots disponíveis — sem corte necessário
    df_legado_selecionado = df_legado_elegivel
    print(f"✅ Pool de legado cabe inteiro: {qtd_legado_pool:,} ≤ {slots_legado:,} slots")

else:
    # ── Passo 1: conta por last_transaction_date ────────────────────────────
    # groupBy em ~365 valores distintos — trivial, não há sort global do dataset
    from pyspark.sql.functions import desc
    date_counts = (
        df_legado_elegivel
        .groupBy("last_transaction_date")
        .count()
        .orderBy(desc("last_transaction_date"))   # ordena só as ~365 datas, não o dataset inteiro
        .collect()
    )

    # ── Passo 2: drena dias do mais recente para o mais antigo ──────────────
    # Cada dia é esgotado completamente antes de avançar para o dia anterior.
    # Apenas o último dia pode ser parcial — nele o banco serve de desempate.
    slots_remaining = slots_legado
    date_buckets    = []   # [(last_transaction_date, take, disponivel)]

    print(f"\n{'Data':<14} {'Disponível':>12} {'Selecionado':>12}  Status")
    print("-" * 58)
    for row in date_counts:
        if slots_remaining <= 0:
            break
        dt   = row["last_transaction_date"]
        n    = row["count"]
        take = min(n, slots_remaining)
        date_buckets.append((dt, take, n))
        slots_remaining -= take
        status = "✅ completo" if take == n else "✂️  cortado (banco como desempate)"
        print(f"{str(dt):<14} {n:>12,} {take:>12,}  {status}")
    print(f"\n→ {len(date_buckets)} datas usadas de {len(date_counts)} disponíveis")

    # ── Passo 3: materializa um DF por data e une ───────────────────────────
    # Dias completos  → filter simples (predicate pushdown no cache, zero sort)
    # Dia de corte    → ROW_NUMBER só dentro desse subconjunto, por bank_name_priority
    dfs_buckets = []
    for dt, take, n in date_buckets:
        if take == n:
            # Dia inteiro: zero ordenação necessária
            dfs_buckets.append(
                df_legado_elegivel.filter(col("last_transaction_date") == dt)
            )
        else:
            # Único dia parcial: sort só neste subconjunto pequeno — banco decide quem entra
            df_corte = spark.sql(f"""
                SELECT user_id, bank_name, bank_name_priority,
                       entry_date, last_transaction_date, tipo
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY last_transaction_date
                            ORDER BY bank_name_priority ASC
                        ) AS rn
                    FROM legado_elegivel
                    WHERE last_transaction_date = DATE '{dt}'
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

# Une os dois grupos em um único DataFrame com o schema final
_output_df_ = (
    df_novos_dia.union(df_legado_selecionado)
        .select(
            "user_id",
            "bank_name",
            "entry_date",
            "last_transaction_date",
            col("tipo").alias("type"),
            to_date(lit(DATA_HOJE)).alias("updated_date"),
        )
        .repartition(200)  # Garante arquivos balanceados no write
)

qtd_total = _output_df_.count()
print(f"📋 Base diária final: {qtd_total:,} chaves")
print(f"   NOVOS  : {qtd_novos:,} ({100 * qtd_novos / qtd_total:.1f}%)")
print(f"   LEGADO : {qtd_legado_selecionado:,} ({100 * qtd_legado_selecionado / qtd_total:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Write na Tabela de Histórico
# MAGIC ---
# MAGIC > `mode('overwrite')` com partition overwrite dinâmico reescreve apenas a partição `updated_date`
# MAGIC > do dia atual, preservando o histórico de todas as outras datas.

# COMMAND ----------

if spark.catalog.tableExists(TABELA_HISTORICO):
    # Tabela já existe: dynamic overwrite sobrescreve apenas a partição de hoje
    _output_df_.write \
        .mode("overwrite") \
        .partitionBy("updated_date") \
        .saveAsTable(TABELA_HISTORICO)
    print(f"✅ Partição {DATA_HOJE} sobrescrita em {TABELA_HISTORICO}")
else:
    # Primeira execução: static mode para criar a tabela sem erro de partição
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    _output_df_.write \
        .mode("overwrite") \
        .partitionBy("updated_date") \
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

# Confirma o que foi escrito na tabela de saída
spark.sql(f"""
    SELECT
        updated_date                                         AS `Data Processamento`,
        type                                                 AS Tipo,
        COUNT(DISTINCT bank_name)                            AS `Nº Bancos`,
        COUNT(*)                                             AS Chaves,
        COUNT(DISTINCT user_id)                              AS `Usuários`,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (
            PARTITION BY updated_date
        ), 2)                                                AS `Total (%)`,
        MIN(last_transaction_date)                           AS `Última Trans. Mín`,
        MAX(last_transaction_date)                           AS `Última Trans. Máx`
    FROM {TABELA_HISTORICO}
    -- WHERE updated_date = '{DATA_HOJE}'
    GROUP BY 1,2
    ORDER BY updated_date DESC, type DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 — Distribuição por banco na geração do dia

# COMMAND ----------

# Distribuição por banco com volume acumulado e % acumulado — útil para curva de Pareto
spark.sql(f"""
    WITH base AS (
        SELECT
            updated_date,
            bank_name,
            SUM(CASE WHEN type = 'NOVO'   THEN 1 ELSE 0 END)  AS Novos,
            SUM(CASE WHEN type = 'LEGADO' THEN 1 ELSE 0 END)  AS Legado,
            COUNT(*)                                           AS Total
        FROM {TABELA_HISTORICO}
        -- WHERE updated_date = '{DATA_HOJE}'
        GROUP BY 1,2
    )
    SELECT
        updated_date                                                AS Data,
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

# Verifica se existem user_ids duplicados na geração do dia (deve ser 0)
df_dupes = spark.sql(f"""
    SELECT user_id, COUNT(*) AS ocorrencias
    FROM {TABELA_HISTORICO}
    WHERE updated_date = '{DATA_HOJE}'
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
# MAGIC ## 9.4 — Verificação de cooldown (nenhum usuário em cooldown deve ter sido selecionado)

# COMMAND ----------

# Garante que nenhum usuário em cooldown entrou na base do dia
violacoes_cooldown = spark.sql(f"""
    SELECT COUNT(*) AS violacoes
    FROM {TABELA_HISTORICO} hoje
    WHERE hoje.updated_date = '{DATA_HOJE}'
      AND EXISTS (
          SELECT 1 FROM {TABELA_HISTORICO} hist
          WHERE hist.user_id = hoje.user_id
            AND hist.updated_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {JANELA_COOLDOWN_USUARIO_DIAS})
            AND hist.updated_date <  '{DATA_HOJE}'
      )
""").first()["violacoes"]

if violacoes_cooldown > 0:
    print(f"⚠️  ATENÇÃO: {violacoes_cooldown:,} usuários em cooldown foram selecionados!")
else:
    print(f"✅ Cooldown respeitado — nenhum usuário em cooldown foi selecionado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.5 — Verificação de blacklist (nenhuma chave bloqueada deve ter sido selecionada)

# COMMAND ----------

# ── ALTERAÇÃO v5 → v6 ───────────────────────────────────────────────────────
# Validação atualizada para as duas novas regras de bloqueio:
#   (a) Bloqueio de chave: nenhuma chave comunicada nos últimos 15 dias deve reaparecer
#   (b) Blacklist de usuário: nenhum usuário com 3+ comms em 30d deve aparecer
# ─────────────────────────────────────────────────────────────────────────────

# Valida bloqueio de chave (15 dias)
violacoes_chave = spark.sql(f"""
    SELECT COUNT(*) AS violacoes
    FROM {TABELA_HISTORICO} hoje
    WHERE hoje.updated_date = '{DATA_HOJE}'
      AND EXISTS (
          SELECT 1 FROM {TABELA_HISTORICO} hist
          WHERE hist.user_id   = hoje.user_id
            AND hist.bank_name = hoje.bank_name
            AND hist.updated_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {JANELA_BLOQUEIO_CHAVE_DIAS})
            AND hist.updated_date <  '{DATA_HOJE}'
      )
""").first()["violacoes"]

# Valida blacklist de usuário (3 comms em 30d → 30d bloqueio)
violacoes_usuario = spark.sql(f"""
    WITH rolling AS (
        SELECT user_id, updated_date,
               COUNT(*) OVER (
                   PARTITION BY user_id
                   ORDER BY CAST(updated_date AS TIMESTAMP)
                   RANGE BETWEEN INTERVAL {JANELA_CONTAGEM_COMUNICACOES - 1} DAYS PRECEDING
                             AND CURRENT ROW
               ) AS rolling_count
        FROM {TABELA_HISTORICO}
        WHERE updated_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), {LOOKBACK_BLACKLIST_USUARIO})
          AND updated_date <  '{DATA_HOJE}'
    ),
    blacklisted AS (
        SELECT DISTINCT user_id
        FROM rolling
        WHERE rolling_count >= {LIMITE_COMUNICACOES_USUARIO}
    )
    SELECT COUNT(*) AS violacoes
    FROM {TABELA_HISTORICO} hoje
    INNER JOIN blacklisted b ON hoje.user_id = b.user_id
    WHERE hoje.updated_date = '{DATA_HOJE}'
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
            updated_date                                           AS Data_Geracao,
            SUM(CASE WHEN type = 'NOVO'   THEN 1 ELSE 0 END)       AS Novos,
            SUM(CASE WHEN type = 'LEGADO' THEN 1 ELSE 0 END)       AS Legado,
            COUNT(*)                                               AS Chaves,
            COUNT(DISTINCT user_id)                                AS Users
        FROM {TABELA_HISTORICO}
        WHERE updated_date >= DATE_SUB(CAST('{DATA_HOJE}' AS DATE), 10)
        GROUP BY updated_date
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

# Por data de last_transaction_date — mostra de quão recentes são as chaves selecionadas,
# com % por tipo e volume acumulado (do dia mais recente ao mais antigo)
spark.sql(f"""
    WITH base AS (
        SELECT
            updated_date,
            type,
            MIN(last_transaction_date)  AS `Primeira Data`,
            MAX(last_transaction_date)  AS `Última Data`,
            COUNT(*)                    AS Chaves
        FROM {TABELA_HISTORICO}
        -- WHERE updated_date = '{DATA_HOJE}'
        GROUP BY updated_date, type
    )
    SELECT
        updated_date                                                 AS `Data Processamento`,
        type                                                         AS Tipo,
        `Primeira Data`,
        `Última Data`,
        Chaves,
        ROUND(100.0 * Chaves / SUM(Chaves) OVER (PARTITION BY updated_date), 2)
                                                                     AS Pct_Tipo,
        SUM(Chaves) OVER (
            ORDER BY updated_date ASC, type ASC
        )                                                            AS Vol_Acumulado
    FROM base
    ORDER BY updated_date desc, type DESC, updated_date DESC
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
  -- 1. Geramos uma sequência de datas de 365 dias atrás até hoje
  SELECT 
    explode(sequence(add_months(current_date(), -12), current_date())) AS dia
),
distancia_meses AS (
  -- 2. Calculamos a diferença de meses entre o dia e hoje
  -- O 'floor' garante que o resultado seja um número inteiro arredondado para baixo
  SELECT 
    dia,
    floor(months_between(current_date(), dia)) AS meses_atras
  FROM date_range
)
-- 3. Classificamos nos quartis de 3 em 3 meses
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


temp_window_spec = Window.partitionBy("type","bank_name").orderBy(col("last_transaction_date").desc())
temp_total_keys = df_priorizado.filter(col('n_priority') == 1).agg(countDistinct('user_id')).collect()[0][0]

display(
    df_priorizado
        .filter(col('n_priority')== 1)
        .select('*', expr("case when new_key = 1 then 'NOVO' else 'LEGADO' end as type"))
        .groupBy(
            date_format('last_transaction_date', 'yyyy-MM').alias('Month'),
            'last_transaction_date',
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

        .join(temp_quartil, col('dia') == col('last_transaction_date'), "inner")
        .select(
            col('last_transaction_date').alias('Date'),
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
        .orderBy(col('type').desc(),col('last_transaction_date').desc())
)

# COMMAND ----------

spark.sql(f"""
    WITH base AS (
        SELECT
            last_transaction_date                                                   AS Dia,
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
        WHERE n_priority = 1   -- 1 linha por usuário (melhor banco já selecionado)
        GROUP BY 1,2
        
    )
    SELECT
        A.Dia, 
        Tipo,
        range_month                                                 AS RANGE_MONTH,
        A.Total,
        ROUND(100.0 * Total / SUM(Total) OVER (ORDER BY A.Dia), 2)  AS `Total_(%)`,
        `1_MERCADO_PAGO`,       -- ROUND(`1_MERCADO_PAGO_(%)`,2)       AS `1_MERCADO_PAGO_(%)`,
        `2_NUBANK`,             -- ROUND(`2_NUBANK_(%)`,2)             AS `2_NUBANK_(%)`,
        `3_RECARGAPAY`,         -- ROUND(`3_RECARGAPAY_(%)`,2)         AS `3_RECARGAPAY_(%)`,
        `4_SHOPEE`,             -- ROUND(`4_SHOPEE_(%)`,2)             AS `4_SHOPEE_(%)`,
        `5_SANTANDER`,          -- ROUND(`5_SANTANDER_(%)`,2)          AS `5_SANTANDER_(%)`,
        `6_CAIXA`,              -- ROUND(`6_CAIXA_(%)`,2)              AS `6_CAIXA_(%)`,
        `7_INTER`,              -- ROUND(`7_INTER_(%)`,2)              AS `7_INTER_(%)`,
        `8_BRADESCO`,           -- ROUND(`8_BRADESCO_(%)`,2)           AS `8_BRADESCO_(%)`,
        `9_BANCO_DO_BRASIL`,    -- ROUND(`9_BANCO_DO_BRASIL_(%)`,2)    AS `9_BANCO_DO_BRASIL_(%)`,
        `10_PAN`,               -- ROUND(`10_PAN_(%)`,2)               AS `10_PAN_(%)`,
        `11_NEXT`,              -- ROUND(`11_NEXT_(%)`,2)              AS `11_NEXT_(%)`,
        `12_NEON`,              -- ROUND(`12_NEON_(%)`,2)              AS `12_NEON_(%)`,
        `13_C6_BANK`,           -- ROUND(`13_C6_BANK_(%)`,2)           AS `13_C6_BANK_(%)`,
        `14_99PAY`,             -- ROUND(`14_99PAY_(%)`,2)             AS `14_99PAY_(%)`,
        `15_BANCO_DO_NORDESTE`, -- ROUND(`15_BANCO_DO_NORDESTE_(%)`,2) AS `15_BANCO_DO_NORDESTE_(%)`,
        `16_MERCANTIL`,         -- ROUND(`16_MERCANTIL_(%)`,2)         AS `16_MERCANTIL_(%)`,
        -- `17_BRB`,               -- ROUND(`17_BRB_(%)`,2)               AS `17_BRB_(%)`,
        `18_CREFISA`,           -- ROUND(`18_CREFISA_(%)`,2)           AS `18_CREFISA_(%)`,
        `19_MIDWAY`,            -- ROUND(`19_MIDWAY_(%)`,2)            AS `19_MIDWAY_(%)`,
        `20_PAGSEGURO`,         -- ROUND(`20_PAGSEGURO_(%)`,2)         AS `20_PAGSEGURO_(%)`,
        -- `21_WILL_BANK`,         -- ROUND(`21_WILL_BANK_(%)`,2)         AS `21_WILL_BANK_(%)`,
        `22_SAFRA`,             -- ROUND(`22_SAFRA_(%)`,2)             AS `22_SAFRA_(%)`,
        `23_BTG`,               -- ROUND(`23_BTG_(%)`,2)               AS `23_BTG_(%)`,
        `24_BANCO_CARREFOUR`,   -- ROUND(`24_BANCO_CARREFOUR_(%)`,2)   AS `24_BANCO_CARREFOUR_(%)`,
        `25_OUTRO`             -- ROUND(`25_OUTRO_(%)`,2)             AS `25_OUTRO_(%)`
    FROM BASE AS A
    INNER JOIN temp_quartil AS B
        ON A.Dia = B.dia
    ORDER BY Dia DESC, Tipo DESC

""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dashboard
# MAGIC ---