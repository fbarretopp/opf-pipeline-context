# OPF Pipeline Context — PicPay

Repositório de contexto do pipeline **OPF (Open Finance)** para incentivo ao consentimento da conta.

> 💡 **Como usar com o GitHub Copilot CLI:**
> No início de cada sessão diga:
> _"Leia o Copilot Space ou repositório fbarretopp/opf-pipeline-context para entender o contexto do projeto OPF"_

---

## 📁 Estrutura

| Arquivo | Conteúdo |
|---------|----------|
| `README.md` | Visão geral e links |
| `context/NOTEBOOKS.md` | Links e IDs dos notebooks Databricks |
| `context/BUSINESS_RULES.md` | Regras de negócio do pipeline v6 |
| `context/TABLES.md` | Tabelas Spark/Delta usadas |
| `context/PALETTE.md` | Paleta de cores PicPay aprovada |
| `context/TECH.md` | Configurações técnicas e autenticação |
| `notebooks/Regua_de_OPF_Bancos_v7.py` | Versão v7 em SOURCE, derivada do notebook de produção v6, com trava de frescor da fonte |

---

## 🎯 Objetivo do Pipeline

Selecionar usuários MAU do PicPay que possuem contas detectadas em outros bancos (via Open Finance) para comunicação incentivando portabilidade de saldo/salário.

**Versão versionada no GitHub:** `Regua_de_OPF_Bancos_v7.py` — derivada do notebook de produção `2964021027643933` e ajustada para encerrar a execução com `dbutils.notebook.exit(...)` quando `validation.pp_users_growth_opf_communication` não estiver atualizada até `d-1`.

**Fluxo resumido:**
```
Fonte OPF → Filtro MAU → Remove cooldown → Remove bloqueio de chave → Remove blacklist → Prioriza NOVOS (cap 800k) → Seleciona LEGADO (completa até 800k)
```


Referência de contexto usada para esta versão: `https://picpay-principal.cloud.databricks.com/editor/notebooks/2964021027643933?o=3048457690269382#command/6383714641200101`
