# Regras de Negócio — Pipeline OPF v6

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

---

## Definições de NOVO vs LEGADO

| Tipo | Critério |
|------|----------|
| **NOVO** | Usuário nunca foi comunicado antes no pipeline OPF |
| **LEGADO** | Usuário já foi comunicado ao menos uma vez |

---

## Filtros aplicados em sequência

```
1. Fonte bruta (último 1 ano de last_transaction_date)
   ↓ filtro MAU (join consumers.dim_consumers_metrics, is_mau=true)
2. Usuários MAU com conta detectada em outro banco
   ↓ remove cooldown de usuário (3 dias)
3. Sem cooldown de usuário
   ↓ remove bloqueio de chave (15 dias)
4. Sem bloqueio de chave (user+banco)
   ↓ remove blacklist de usuário (3 comms em 30 dias → bloqueio 30 dias)
5. Pool elegível final
   ↓ prioriza NOVOS até 800k
   ↓ completa com LEGADO até 800k total
6. Base diária para comunicação
```

---

## Diferenças v5 → v6

| Aspecto | v5 | v6 |
|---------|----|----|
| Cooldown usuário | 5 dias | **3 dias** |
| Bloqueio de chave | Condicional (3x → 70d na chave) | **15 dias fixo após qualquer comunicação** |
| Blacklist | Por chave (user+banco), 70 dias | **Por usuário, 30 dias** |
| Views criadas | `blacklist_chave` | `bloqueio_chave` + `blacklist_usuario` |

---

## Banco de prioridade (ordem da lista)

```
1. MERCADO PAGO    6. CAIXA           11. NEXT          16. MERCANTIL     21. WILL BANK
2. NUBANK          7. INTER           12. NEON           17. BRB           22. SAFRA
3. RECARGAPAY      8. BRADESCO        13. C6 BANK        18. CREFISA       23. BTG
4. SHOPEE          9. BANCO DO BRASIL 14. 99PAY          19. MIDWAY        24. BANCO CARREFOUR
5. SANTANDER      10. PAN             15. BANCO DO NORDESTE 20. PAGSEGURO  25. OUTRO
```
> BTG, MERCANTIL e BANCO CARREFOUR têm volume zero nos dados históricos.
