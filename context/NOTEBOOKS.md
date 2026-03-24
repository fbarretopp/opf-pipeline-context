# Notebooks Databricks — OPF Pipeline

**Workspace:** `https://picpay-principal.cloud.databricks.com`  
**Profile local:** `picpay-datalake` (em `~/.databrickscfg`)  
**Pasta:** `/Users/felipe.gbarreto@picpay.com/regua_if_opf/`

---

## Notebooks Ativos

| Nome | Object ID | Link | Status |
|------|-----------|------|--------|
| Régua de OPF Bancos v5 | `2964021027635636` | [abrir](https://picpay-principal.cloud.databricks.com/editor/notebooks/2964021027635636) | Base histórica |
| Régua de OPF Bancos v6 | `2964021027643933` | [abrir](https://picpay-principal.cloud.databricks.com/editor/notebooks/2964021027643933) | ✅ Produção |
| Régua de OPF Bancos v7 | `1328412056909775` | [abrir](https://picpay-principal.cloud.databricks.com/editor/notebooks/1328412056909775) | ✅ Publicado |
| Acompanhamento v1 | `2964021027667167` | [abrir](https://picpay-principal.cloud.databricks.com/editor/notebooks/2964021027667167) | ✅ Monitoramento |
| Validação OPF (referência) | `1500094211290451` | [abrir](https://picpay-principal.cloud.databricks.com/editor/notebooks/1500094211290451) | Referência |

---

## O que cada notebook faz

### v6 — Régua de OPF Bancos v6
Pipeline principal de geração diária da base de comunicação.
- Aplica filtros MAU, cooldown, bloqueio de chave e blacklist de usuário
- Gera a tabela `validation.pp_users_growth_opf` (particionada por `updated_date`)
- Prioriza NOVOS (cap 800k), depois completa com LEGADO até 800k total

### v7 — Régua de OPF Bancos v7
Evolução operacional da régua principal.
- Mantém a lógica de seleção da v6
- Adiciona trava de frescor da fonte `validation.pp_users_growth_opf_communication`
- Só permite gravação quando `max(last_transaction) >= d-1`
- Aborta a execução sem gravar partição quando a fonte estiver vazia ou desatualizada

### Acompanhamento v1
Notebook interativo de monitoramento com 8 seções e widgets:
- **Widgets:** `data_fim`, `n_dias`, `data_hoje`, `tipo` (TODOS/NOVO/LEGADO), `banco_filter`, `top_n_bancos`
- **Seção 1:** Funil de criação (volume por filtro)
- **Seção 2:** Histórico recente NOVOS vs LEGADO
- **Seção 3:** Distribuição por banco (ranking, Pareto, composição %)
- **Seção 4:** Recência das chaves por faixa de meses
- **Seção 5:** Monitoramento de bloqueios (cooldown + chaves bloqueadas)
- **Seção 6:** Pivot Dia × Banco (heatmap)
- **Seção 7:** Tendências semanais WoW
- **Seção 8:** KPIs de qualidade (duplicatas, cobertura)

---

## Autenticação Databricks

```bash
# Verificar token
databricks auth token --profile picpay-datalake

# Reautenticar (token expira ~1h)
databricks auth login --profile picpay-datalake

# Upload de notebook
curl -sf -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  https://picpay-principal.cloud.databricks.com/api/2.0/workspace/import \
  -d '{"path":"...","format":"SOURCE","language":"PYTHON","content":"<base64>","overwrite":true}'
```

> ⚠️ A chave no JSON de retorno é `access_token` (não `token_value`)
