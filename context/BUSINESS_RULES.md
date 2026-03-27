# Regras de Negócio — Pipeline OPF

## Parâmetros principais

| Parâmetro | Valor | Descrição |
|-----------|-------|-----------|
| `JANELA_COOLDOWN_USUARIO_DIAS` | `3` | Dias de bloqueio do **usuário** após qualquer comunicação |
| `JANELA_BLOQUEIO_CHAVE_DIAS` | `15` | Dias de bloqueio da **chave** (user+banco) após comunicação |
| `LIMITE_COMUNICACOES_USUARIO` | `3` | Máximo de comunicações em 30 dias antes do blacklist |
| `JANELA_CONTAGEM_COMUNICACOES` | `30` | Janela de contagem para o limite acima |
| `JANELA_BLOQUEIO_USUARIO_DIAS` | `30` | Dias de blacklist do usuário após atingir o limite |
| `LOOKBACK_BLACKLIST_USUARIO` | `60` | Lookback total para blacklist (30+30) |
| `LIMITE_NOVOS` | `800.000` | Cap diário de chaves NOVAS |
| `LIMITE_DIARIO` | `800.000` | Cap diário total |
| `PCT_GC` | `0.10` | Proporção de usuários atribuídos ao grupo controle (GC) |

---

## Definições de NOVO vs LEGADO

| Tipo | Critério |
|------|----------|
| **NOVO** | Usuário cuja `latest_transaction_date >= dt_referencia_geracao` (data da última run) |
| **LEGADO** | Usuário cuja `latest_transaction_date < dt_referencia_geracao` |

---

## Experimento GC/GT (v8)

| Grupo | Proporção | Comportamento |
|-------|-----------|---------------|
| **GC** (Grupo Controle) | ~10% | Holdout permanente — entra na tabela no dia do sorteio, depois é excluído de todas as runs futuras |
| **GT** (Grupo Teste) | ~90% | Selecionado normalmente para comunicação |

**Mecanismo de atribuição:**
- `abs(hash(user_id)) % 100 < 10` → GC, caso contrário → GT
- Determinístico por `user_id` — re-runs produzem o mesmo resultado
- Usuários já marcados como GC são excluídos do pool na Seção 4.4 (antes da seleção dos 800k)

---

## Filtros aplicados em sequência

```
1. Fonte bruta (últimos 90 dias de most_recent_transaction_at)
   ↓ filtro: is_opf_account_active = false AND is_in_opf_list = true
   ↓ join MAU (consumers.dim_consumers_metrics, is_mau=true)
2. Usuários MAU com conta detectada em outro banco
   ↓ remove cooldown de usuário (3 dias)
3. Sem cooldown de usuário
   ↓ remove bloqueio de chave (15 dias)
4. Sem bloqueio de chave (user+banco)
   ↓ remove blacklist de usuário (3 comms em 30 dias → bloqueio 30 dias)
5. Sem blacklist
   ↓ remove holdout GC (usuários já marcados GC no histórico)
6. Pool elegível final
   ↓ prioriza NOVOS até 800k (Bank-Bucket-Drain)
   ↓ completa com LEGADO até 800k total (Day-Bucket-Drain)
7. Base de 800k
   ↓ atribui experiment_group via hash(user_id): ~10% GC, ~90% GT
8. Write com partição dinâmica por segmentation_date
```

---

## Banco de prioridade (ordem da lista)

```
1. MERCADO PAGO    6. CAIXA           11. NEXT          16. MERCANTIL     22. SAFRA
2. NUBANK          7. INTER           12. NEON           18. CREFISA       23. BTG
3. RECARGAPAY      8. BRADESCO        13. C6 BANK        19. MIDWAY        24. BANCO CARREFOUR
4. SHOPEE          9. BANCO DO BRASIL 14. 99PAY          20. PAGSEGURO
5. SANTANDER      10. PAN             15. BANCO DO NORDESTE
```

> BRB e WILL BANK estão comentados no código (volume zero). Posições 17 e 21 estão vazias.

---

## Trava de frescor da fonte (v7+)

O notebook encerra a execução com `dbutils.notebook.exit(...)` se a fonte (`pp_users_identified_accounts_opf_communication`) não tiver dados com `most_recent_transaction_at >= d-1`. Isso evita gerar bases desatualizadas quando a fonte upstream falha.
