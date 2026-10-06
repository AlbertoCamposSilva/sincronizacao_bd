# Recepção da nuvem: contrato entre o Job de exportação e o puxador

Sentido único: **nuvem → local**. Nada é enviado à nuvem. O puxador é o **PC do CNPq** (decisão D2, 06/10/2026).
Código local: `sincronizador/nuvem.py`. Testes: `test/test_nuvem.py` (Job simulado neste formato).

## 1. Lado da nuvem (a implementar no repositório do VAR)

Cloud Run Job `var-sync-exportar` (região `southamerica-east1`, projeto `var-cnpq-2026`), mesma imagem do backend,
módulo novo `backend/jobs/exportar_usuarios.py`.

**Argumentos** (o puxador dispara o Job pela API REST com `overrides.containerOverrides[0].args`):

```
--prefixo nuvem_para_local/<execucao>/        pasta de saída no bucket (termina com /)
--marca <tabela>=<N> [--marca ...]            só para tabelas "incremental": exporta apenas id > N
```

**Conexão:** papel `var_sync_leitura` (só `SELECT` nas 11 tabelas), transação `REPEATABLE READ READ ONLY`, para que
todas as tabelas venham do mesmo instante.

**Saída** em `gs://var-cnpq-2026-db-sync/<prefixo>`:

| Arquivo | Conteúdo |
|---|---|
| `<tabela>.csv.gz` | `COPY (SELECT <colunas> ...) TO STDOUT WITH (FORMAT csv, HEADER true, NULL '\N')`, em gzip, UTF-8 |
| `manifesto.json` | **gravado por último**; sem ele a execução não está pronta |

`NULL '\N'` é obrigatório (distingue NULL de texto vazio). O Job não deve exportar `rag_usuarios.senha_temporaria` nem
`expiracao_senha` (o importador também as descarta: decisão D4).

**`manifesto.json`:**

```json
{
  "versao": 1,
  "gerado_em": "2026-10-06T19:00:00Z",
  "execucao": "nuvem_para_local/20261006T190000Z-ab12cd/",
  "tabelas": {
    "rag_usuarios": {
      "modo": "retrato", "arquivo": "rag_usuarios.csv.gz", "sha256": "<hex>", "linhas": 16,
      "colunas": ["id", "email", "..."], "chave": "id", "orfaos": 0
    },
    "llm_registros_custos": {
      "modo": "incremental", "arquivo": "llm_registros_custos.csv.gz", "sha256": "<hex>", "linhas": 120,
      "colunas": ["id", "data_hora", "..."], "chave": "id", "max_id": 5400, "linhas_total": 5400
    }
  }
}
```

- **`retrato`** (9 tabelas mutáveis): todas as linhas, só as que têm "pai" (`FILTROS_SINC` de `db_diagnostico.py`);
  os órfãos entram só como contagem em `orfaos`.
- **`incremental`** (`llm_registros_custos`, `rag_auditoria_acesso`): só `id > marca` recebida. **Sem `--marca` para a
  tabela, não exportar linhas**: `arquivo` fica `null` e o manifesto traz apenas `max_id` e `linhas_total` (o puxador usa
  isso para pedir ao usuário que defina a marca inicial; evita duplicar o que já foi copiado antes).
- `colunas`: as colunas do CSV, na ordem. Colunas que não existem no banco local são ignoradas (e registradas no log).

**Bucket:** regra de ciclo de vida de 3 dias em `nuvem_para_local/`. O puxador apaga a pasta após importar.

## 2. Lado local (feito)

A cada `nuvem_intervalo_min` (30) dentro do `ciclo`, só se `puxar_nuvem = true`:

1. dispara o Job com as marcas de `sincronizacao.nuvem_marcas` e espera terminar (até `nuvem_espera_max_s`);
2. baixa o manifesto e os arquivos; confere o `sha256`; qualquer divergência cancela tudo;
3. numa **única transação**, sem origem de replicação (o publicador captura e o outro PC recebe pelo fluxo normal):
   - `retrato`: `INSERT ... ON CONFLICT (id) DO UPDATE ... WHERE linha IS DISTINCT FROM` (só escreve o que mudou);
     exclui **somente** linhas registradas em `sincronizacao.nuvem_ids` que sumiram do retrato;
   - `incremental`: insere com id local novo, grava `id_nuvem → id_local` e avança a marca;
4. regra 9 do VAR: se `alberto.silva@cnpq.br` não tiver exatamente `['administrador','CNPq']`, **alerta** (não corrige);
5. apaga a pasta do bucket.

**Proteções:** primeira importação não apaga nada e gera o relatório de linhas só-locais
(`%LOCALAPPDATA%\SincronizacaoBD\logs\nuvem_somente_local_*.json`); exclusão em massa (>20 linhas e >50% do que veio da
nuvem) trava com erro; reimportar nunca duplica; erro de FK ou de unicidade desfaz tudo e avisa.

## 3. Como ligar (nesta ordem)

1. **Nos dois PCs:** `git pull` e `python -m sincronizador inicializar`. Cria `sincronizacao.nuvem_ids` e
   `nuvem_marcas` e as inclui na publicação. **A casa precisa ter feito isto antes de o CNPq ligar a puxada**, senão ela
   recebe mudanças de tabelas que ainda não tem e o fluxo para.
2. No CNPq, `config.local.toml`: `puxar_nuvem = true`. Na casa, não ligar.
3. Primeira execução (`python -m sincronizador puxar-nuvem`): importa os retratos. Para `llm_registros_custos` e
   `rag_auditoria_acesso` ela só informa o `max_id` da nuvem. Defina a marca com o maior id da nuvem que o banco local **já
   tem**: `python -m sincronizador nuvem-marca llm_registros_custos <N>`. Depois disso, o ciclo importa só o que for novo.
4. Confira o relatório de linhas só-locais antes de qualquer limpeza.
