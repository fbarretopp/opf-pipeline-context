# Configurações Técnicas — OPF Pipeline

## Databricks

| Item | Valor |
|------|-------|
| Host | `https://picpay-principal.cloud.databricks.com` |
| Profile local | `picpay-datalake` (em `~/.databrickscfg`) |
| Workspace OPF | `/Users/felipe.gbarreto@picpay.com/regua_if_opf/` |
| Token expira | ~1 hora |

### Reautenticar
```bash
databricks auth login --profile picpay-datalake
```

### Obter token atual
```bash
databricks auth token --profile picpay-datalake
# retorna JSON com campo "access_token" (não "token_value")
```

---

## Plotly em Databricks — boas práticas

### Exibir gráfico inline
```python
displayHTML(fig.to_html(include_plotlyjs="cdn", full_html=False))
```

### Evitar "multiple values for keyword argument"
```python
# LAY_BASE deve conter APENAS chaves que nunca são passadas como kwargs diretos
LAY_BASE = dict(
    paper_bgcolor="#FFFFFF",
    plot_bgcolor="#FFFFFF",
    font=dict(family="Segoe UI, sans-serif", color="#333333", size=12),
)

def apply_layout(fig, h=420, **kwargs):
    kwargs.setdefault("margin", dict(t=60, r=20, b=55, l=65))
    kwargs.setdefault("legend", dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)))
    fig.update_layout(**LAY_BASE, height=h, **kwargs)
    fig.update_xaxes(gridcolor="#EBEBEB", zerolinecolor="#EBEBEB")
    fig.update_yaxes(gridcolor="#EBEBEB", zerolinecolor="#EBEBEB")
    displayHTML(fig.to_html(include_plotlyjs="cdn", full_html=False))
```

> ⚠️ `go.Waterfall` não suporta `marker=` em versões antigas do Plotly no Databricks.
> Use `go.Bar` com `base=` como alternativa.

### Widgets interativos
```python
dbutils.widgets.removeAll()
dbutils.widgets.text("data_fim", "2025-01-01", "Data Fim")
dbutils.widgets.dropdown("tipo", "TODOS", ["TODOS","NOVO","LEGADO"], "Tipo")

valor = dbutils.widgets.get("data_fim")
```

---

## Pandas / Spark

```python
# Converter coluna de data para string (evita erros de tipo)
df["data"] = df["data"].astype(str)

# Números brasileiros no CSV (ponto como separador de milhar)
df[col] = df[col].astype(str).str.replace(".", "", regex=False).astype(float)
```

---

## Upload de notebook via API

```python
import base64, json, subprocess

with open("notebook.py", "rb") as f:
    b64 = base64.b64encode(f.read()).decode()

tr = subprocess.run(
    ["databricks", "auth", "token", "--profile", "picpay-datalake"],
    capture_output=True, text=True)
token = json.loads(tr.stdout)["access_token"]

payload = {
    "path":      "/Users/felipe.gbarreto@picpay.com/regua_if_opf/Nome_Notebook",
    "format":    "SOURCE",
    "language":  "PYTHON",
    "content":   b64,
    "overwrite": True   # False para criar novo sem sobrescrever
}

subprocess.run([
    "curl", "-sf", "-X", "POST",
    "-H", "Content-Type: application/json",
    "-H", f"Authorization: Bearer {token}",
    "https://picpay-principal.cloud.databricks.com/api/2.0/workspace/import",
    "-d", json.dumps(payload)
])
```

> ⚠️ Caminhos com caracteres UTF-8 (é, ã) devem ser unicode-escaped no JSON:
> `"R\u00e9gua"` em vez de `"Régua"`
