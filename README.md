# OPF Pipeline Context — PicPay

Repositório de contexto do pipeline **OPF (Open Finance)** para incentivo ao consentimento da conta.

> **Como usar com AI assistants (HubAI Nitro, Copilot, etc.):**
> No início de cada sessão diga:
> _"Leia o repositório github.com/fbarretopp/opf-pipeline-context e retome o contexto do projeto OPF"_

---

## Estrutura

| Arquivo | Conteúdo |
|---------|----------|
| `README.md` | Visão geral e links |
| `context/BUSINESS_RULES.md` | Regras de negócio do pipeline |
| `context/NOTEBOOKS.md` | Links e IDs dos notebooks Databricks |
| `context/TABLES.md` | Tabelas Spark/Delta usadas |
| `context/PALETTE.md` | Paleta de cores PicPay aprovada |
| `context/TECH.md` | Configurações técnicas e autenticação |
| `notebooks/Regua_de_OPF.py` | Notebook de produção em formato SOURCE |

---

## Objetivo do Pipeline

Selecionar diariamente usuários MAU do PicPay que possuem contas detectadas em outros bancos (via Open Finance) para comunicação incentivando portabilidade de saldo/salário.

**Notebook de produção:** `Régua de OPF` (Databricks object ID `1328412056932520`)

**Fluxo resumido:**
```
Fonte SS (pp_users_identified_accounts_opf_communication)
  → Join MAU (is_mau = true)
  → Melhor banco por user_id (ROW_NUMBER por prioridade)
  → Remove cooldown de usuário (3d)
  → Remove bloqueio de chave user+banco (15d)
  → Remove blacklist de usuário (3x em 30d → 30d bloqueio)
  → Remove holdout GC (usuários já marcados como grupo controle)
  → Prioriza NOVOS (cap 800k, Bank-Bucket-Drain)
  → Completa com LEGADO (slots restantes até 800k total)
  → Atribui experiment_group (GC ~10% / GT ~90% via hash determinístico)
  → Write em self_service_analytics.pf_growth_users_opf_journey_communication
```

## Experimento GC/GT

- **GC (Grupo Controle, ~10%):** usuários excluídos permanentemente da seleção diária — aparecem na tabela apenas no dia em que foram sorteados; nunca mais retornam ao pool.
- **GT (Grupo Teste, ~90%):** usuários selecionados normalmente para comunicação.
- A atribuição é **determinística** via `hash(user_id)` — idempotente entre re-runs.

## Histórico de versões

| Versão | Mudança principal |
|--------|-------------------|
| v5 | Cooldown 5d, bloqueio condicional de chave |
| v6 | Cooldown 3d, bloqueio fixo 15d por chave, blacklist por usuário (não mais por chave) |
| v7 | Trava de frescor da fonte (encerra se dados < d-1) |
| v8 (atual) | Campo `experiment_group` (GC/GT), nova tabela fonte SS, holdout permanente de GC |

## Links úteis

- [Sheets de acompanhamento](https://docs.google.com/spreadsheets/d/1D2YjMdCapdqYV2-JB6Kzo51FRGI4e5lDz1A04DNi3ro/edit?gid=1702446343#gid=1702446343)
- [Notebook no Databricks](https://picpay-principal.cloud.databricks.com/editor/notebooks/1328412056932520)
