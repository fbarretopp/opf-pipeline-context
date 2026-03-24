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
| `notebooks/Regua_de_OPF_Bancos_v7.ipynb` | Export do notebook v7 com trava de frescor da fonte |

---

## 🎯 Objetivo do Pipeline

Selecionar usuários MAU do PicPay que possuem contas detectadas em outros bancos (via Open Finance) para comunicação incentivando portabilidade de saldo/salário.

**Última versão publicada:** `Régua de OPF Bancos v7` — adiciona uma trava operacional para não gravar partições em `validation.pp_users_growth_opf` quando a fonte `validation.pp_users_growth_opf_communication` não estiver atualizada até `d-1`.

**Fluxo resumido:**
```
Fonte OPF → Filtro MAU → Remove cooldown → Remove bloqueio de chave → Remove blacklist → Prioriza NOVOS (cap 800k) → Seleciona LEGADO (completa até 800k)
```
