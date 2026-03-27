# Notebooks Databricks — OPF Pipeline

**Workspace:** `https://picpay-principal.cloud.databricks.com`
**Profile local:** `picpay-datalake` (em `~/.databrickscfg`)
**Pasta:** `/Users/felipe.gbarreto@picpay.com/regua_if_opf/`

---

## Notebook de Produção

| Nome | Object ID | Link | Status |
|------|-----------|------|--------|
| Régua de OPF | `1328412056932520` | [abrir](https://picpay-principal.cloud.databricks.com/editor/notebooks/1328412056932520) | **Produção** |

---

## O que o notebook faz

Pipeline principal de geração diária da base de comunicação OPF.

1. Valida frescor da fonte (encerra se `most_recent_transaction_at < d-1`)
2. Filtra usuários MAU com contas detectadas via Open Finance (`is_opf_account_active = false`, `is_in_opf_list = true`)
3. Seleciona o melhor banco por user_id (ROW_NUMBER por prioridade)
4. Aplica filtros: cooldown (3d), bloqueio de chave (15d), blacklist de usuário (3x/30d → 30d), holdout GC
5. Prioriza NOVOS (cap 800k via Bank-Bucket-Drain), completa com LEGADO (Day-Bucket-Drain)
6. Atribui `experiment_group` via hash determinístico (GC ~10% / GT ~90%)
7. Escreve em `self_service_analytics.pf_growth_users_opf_journey_communication` (partição dinâmica por `segmentation_date`)
8. Executa validações pós-write (duplicatas, cooldown, blacklist, holdout GC)

---

## Notebooks históricos (referência)

| Nome | Object ID | Descrição |
|------|-----------|-----------|
| Régua de OPF Bancos v6 | `2964021027643933` | Produção anterior (sem experiment_group) |
| Régua de OPF Bancos v5 | `2964021027635636` | Base histórica |
| Acompanhamento v1 | `2964021027667167` | Monitoramento |
| Validação OPF | `1500094211290451` | Referência |

---

## Outros notebooks na pasta

| Nome | Object ID | Descrição |
|------|-----------|-----------|
| ajuste_ss_users_opf | `665942654981934` | Ajustes na tabela fonte SS |

---

## Deploy

O notebook roda via **Airflow** no repo `picpay-ss-airflow`.
- Schedule: `0 14 * * *` (diário, 14h UTC)
- YAML de metadados: `artifacts/self_service_analytics/pf_growth_users_opf_journey_communication/`
