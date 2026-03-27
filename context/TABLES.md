# Tabelas Spark/Delta — OPF Pipeline

## Tabelas principais

| Tabela | Tipo | Descrição |
|--------|------|-----------|
| `self_service_analytics.pf_growth_users_opf_journey_communication` | Delta (output) | Histórico de comunicações geradas, particionado por `segmentation_date` |
| `self_service_analytics.pp_users_identified_accounts_opf_communication` | Delta (source) | Fonte: usuários com contas detectadas via OPF (SS notebook) |
| `consumers.dim_consumers_metrics` | Dimensão | Métricas de consumidores — filtro `is_mau=true` |

---

## Schema da tabela de saída — `self_service_analytics.pf_growth_users_opf_journey_communication`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `user_id` | long | ID do usuário PicPay |
| `bank_name` | string | IF selecionada para comunicação |
| `entry_date` | date | Data de entrada da chave na base elegível |
| `latest_transaction_date` | date | Última transação detectada com essa IF |
| `type` | string | `NOVO` ou `LEGADO` |
| `experiment_group` | string | `GC` (grupo controle, ~10%) ou `GT` (grupo teste, ~90%) |
| `segmentation_date` | date | Data de geração (partição) |

---

## Schema da tabela fonte — `self_service_analytics.pp_users_identified_accounts_opf_communication`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `user_id` | long | ID do usuário PicPay |
| `bank_name` | string | Nome do banco detectado |
| `most_recent_transaction_at` | date | Data da última transação (campo de data de entrada e atualização) |
| `oldest_transaction_at` | date | Data da transação mais antiga |
| `is_opf_account_active` | boolean | Se a conta OPF está ativa (filtro: `false`) |
| `is_in_opf_list` | boolean | Se está na lista OPF (filtro: `true`) |

---

## Join MAU

```sql
SELECT a.*
FROM self_service_analytics.pp_users_identified_accounts_opf_communication a
INNER JOIN consumers.dim_consumers_metrics b
  ON a.user_id = b.consumer_id
WHERE b.is_mau = true
```

---

## Queries de bloqueio

```sql
-- Cooldown de usuário (3 dias)
SELECT DISTINCT user_id
FROM self_service_analytics.pf_growth_users_opf_journey_communication
WHERE segmentation_date >= DATE_SUB(CURRENT_DATE(), 3)
  AND segmentation_date <  CURRENT_DATE()

-- Bloqueio de chave (15 dias)
SELECT DISTINCT user_id, bank_name
FROM self_service_analytics.pf_growth_users_opf_journey_communication
WHERE segmentation_date >= DATE_SUB(CURRENT_DATE(), 15)
  AND segmentation_date <  CURRENT_DATE()

-- Blacklist de usuário (3x em 30d → bloqueio 30d, lookback 60d)
-- Ver notebook para query completa com rolling window

-- Holdout GC (permanente)
SELECT DISTINCT user_id
FROM self_service_analytics.pf_growth_users_opf_journey_communication
WHERE experiment_group = 'GC'
  AND segmentation_date < CURRENT_DATE()
```

---

## Tabelas históricas (não mais usadas)

| Tabela | Status | Descrição |
|--------|--------|-----------|
| `validation.pp_users_growth_opf` | Descontinuada | Output das versões v5–v7 |
| `validation.pp_users_growth_opf_communication` | Descontinuada | Fonte das versões v5–v7 |
