# Tabelas Spark/Delta — OPF Pipeline

## Tabelas principais

| Tabela | Tipo | Descrição |
|--------|------|-----------|
| `validation.pp_users_growth_opf` | Delta (output) | Histórico de comunicações geradas, particionado por `updated_date` |
| `validation.pp_users_growth_opf_communication` | Delta (source) | Fonte: usuários com contas detectadas via OPF (SS notebook) |
| `validation.pp_users_growth_opf_v2` | Delta | Versão alternativa da base (regras atualizadas) |
| `consumers.dim_consumers_metrics` | Dimensão | Métricas de consumidores — filtro `is_mau=true` |

---

## Schema relevante — `validation.pp_users_growth_opf`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `user_id` | long | ID do usuário PicPay |
| `bank_name` | string | Nome do banco detectado |
| `type` | string | `NOVO` ou `LEGADO` |
| `last_transaction_date` | date | Data da última transação detectada |
| `updated_date` | date | Data de geração (partição) |

---

## Schema relevante — `validation.pp_users_growth_opf_communication`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `user_id` | long | ID do usuário PicPay |
| `bank_name` | string | Nome do banco detectado |
| `last_transaction` | date | Data da última transação (campo de entrada) |

---

## Join MAU

```sql
SELECT a.*
FROM validation.pp_users_growth_opf_communication a
INNER JOIN consumers.dim_consumers_metrics b
  ON a.user_id = b.consumer_id
WHERE b.is_mau = true
```

---

## Queries de bloqueio (v6)

```sql
-- Bloqueio de chave (15 dias)
SELECT DISTINCT user_id, bank_name
FROM validation.pp_users_growth_opf
WHERE updated_date >= DATE_SUB(CURRENT_DATE, 15)
  AND updated_date < CURRENT_DATE

-- Blacklist de usuário (3 comms em 30 dias → bloqueia 30 dias)
SELECT user_id FROM (
  SELECT user_id,
    COUNT(*) OVER (
      PARTITION BY user_id
      ORDER BY CAST(updated_date AS TIMESTAMP)
      RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW
    ) AS rc
  FROM validation.pp_users_growth_opf
  WHERE updated_date >= DATE_SUB(CURRENT_DATE, 60)
    AND updated_date < CURRENT_DATE
) WHERE rc >= 3
```
