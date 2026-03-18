# Paleta de Cores PicPay — OPF Pipeline

## Regras obrigatórias
- ✅ Usar **verde** como cor primária (escala G)
- ✅ Usar **amarelo** para LEGADO e alertas secundários
- ✅ Usar **cinza** para elementos neutros
- ❌ **Roxo proibido**
- ❌ **Azul proibido**

---

## Variáveis Python (copiar direto no notebook)

```python
# Verde (primário)
G800 = "#123F29"   # verde escuro — títulos, destaques
G700 = "#116C40"   # verde médio-escuro
G600 = "#238662"   # verde médio
G400 = "#11C76F"   # verde principal — NOVOS, positivo
G200 = "#9ADBC2"   # verde claro
G100 = "#E1FAF0"   # verde muito claro — fundo, heatmap zero

# Amarelo (secundário)
Y600 = "#F37422"   # laranja/alerta — cap, blacklist, variação negativa
Y400 = "#FAC942"   # amarelo — LEGADO
Y100 = "#FEF8E3"   # amarelo claro — fundo de destaque

# Cinza (neutro)
GR800 = "#333333"  # texto principal
GR600 = "#717171"  # texto secundário
GR400 = "#898989"  # cinza médio — cooldown, neutro
GR200 = "#EBEBEB"  # grid lines, bordas
WHITE = "#FFFFFF"  # fundo

# Aliases semânticos
NOVO_COL   = G400    # "#11C76F"
LEGADO_COL = Y400    # "#FAC942"
ALERT_COL  = Y600    # "#F37422"
GRAY_COL   = GR400   # "#898989"
```

---

## Escala para heatmaps (Plotly colorscale)

```python
COLORSCALE_GREEN = [
    [0,    "#E1FAF0"],   # G100
    [0.25, "#9ADBC2"],   # G200
    [0.5,  "#11C76F"],   # G400
    [0.75, "#238662"],   # G600
    [1,    "#123F29"],   # G800
]
```

---

## Mapeamento semântico

| Elemento | Cor | Hex |
|----------|-----|-----|
| NOVOS | G400 | `#11C76F` |
| LEGADO | Y400 | `#FAC942` |
| Cap / Alerta | Y600 | `#F37422` |
| Cooldown / Neutro | GR400 | `#898989` |
| Título / Destaque | G800 | `#123F29` |
| Grid lines | GR200 | `#EBEBEB` |
| Fundo | WHITE | `#FFFFFF` |

---

## Paleta completa aprovada (ordem de prioridade)

```
#05271B · #13563E · #238662 · #9ADBC2 · #E1FAF0
#123F29 · #116C40 · #11C76F · #88E3B7 · #E7F9F1
#333333 · #717171 · #898989 · #EBEBEB · #F5F5F5 · #FFFFFF
#BDEBDF · #58D289 · #11C76F · #00984B · #008441
#FEF8E3 · #FCDF8B · #FAC942 · #F8B330 · #F37422
#FFE6EB · #FF96AE · #FF5075 · #FF2048 · #FF0828  ← usar com moderação
```
