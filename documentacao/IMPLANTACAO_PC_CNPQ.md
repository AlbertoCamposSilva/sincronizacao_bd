# Tarefa futura: Implantação da Sincronização no PC do CNPq

> **Criado em:** 06/10/2026 · **Revisado em:** 06/10/2026 (o CNPq receberá uma **cópia integral e limpa** do banco da casa)
> **Status:** pendente. Executar **depois** da Fase 1 do plano (código pronto e testado na casa).
> **Plano geral:** `PLANO_SINCRONIZACAO_BIDIRECIONAL.md`.

Restrições do PC do CNPq: ligado 24/7, **sem administrador**, firewall só de saída (HTTPS liberado), a rede **não**
alcança o Cloud SQL.

## Ideia central

Toda mudança de estrutura é feita **só na casa**. O CNPq recebe uma cópia física do banco da casa, tirada **depois** de a
sincronização estar instalada. A cópia já leva:
- as tabelas legadas removidas e o catálogo renomeado;
- as chaves primárias, inclusive os `id` artificiais, com os **mesmos valores** nos dois PCs;
- a configuração (`wal_level` etc.), o schema `sincronizacao`, a publicação, os gatilhos de evento, as origens e o
  **slot**.

O slot copiado começa exatamente no ponto da cópia. É o ponto de partida perfeito: os dois bancos são idênticos ali, e
tudo o que cada um escrever depois é capturado. A casa pode voltar a ser usada logo após a cópia. O que for escrito nela
enquanto o CNPq é restaurado fica guardado em lotes no Drive e é aplicado no CNPq quando ele subir.

Já feito na casa em 06/10/2026:
- removidas `lattes_xml`, `lattes_vinculos`, `lattes_publicacoes`, `lattes_palavras_chave` e `projetos_cannabis_cnpq`;
- renomeado o catálogo para `cnpq_areas_conhecimento`;
- criadas as chaves primárias (§ Anexo A).

---

## Situação em 06/10/2026: a cópia JÁ foi tirada (antes de instalar a sincronização)

O banco da casa foi copiado para `I:\PostgreSQL` (pendrive) com o PostgreSQL desligado de forma limpa:
- `pg_controldata` da cópia: estado `shut down`, último checkpoint em **`1C9/A9CAB70`** (06/10/2026 10:43:41);
- o banco da casa ficou desligado até as 12:24 e, em seguida, só havia gravado o `CHECKPOINT_SHUTDOWN` e um `RUNNING_XACTS`
  (176 bytes de WAL, verificado com `pg_waldump`): **nenhuma escrita de dados desde a cópia**. A cópia é idêntica ao banco da casa;
- a pasta `data` tem os mesmos 2.019 arquivos nos dois lados (as 2 diferenças são efeito do reinício posterior).
  Os 902 erros do robocopy eram da `data_backup`, apagada durante a cópia, e não afetam o banco.

**Consequência: não é preciso copiar de novo**, desde que a casa **não receba nenhuma escrita de dados** entre a cópia e a
criação do slot (Etapa 4, passo 3). O slot da casa é criado depois da cópia, mas o ponto de partida é o mesmo porque nada mudou.
A cópia não tem o schema `sincronizacao`, a publicação, os gatilhos, a origem nem o slot. Por isso o CNPq roda o próprio
`inicializar` depois de restaurar (Etapa 5), em vez de herdar esses objetos. O `inicializar` é idempotente e cria tudo igual.

**Verificação obrigatória antes de criar o slot na casa** (prova que nada foi gravado desde a cópia):
```powershell
& "E:\Programas\postgres18\bin\pg_waldump.exe" -p "E:\PostgreSQL\data\pg_wal" -s 1C9/A9CAB70
```
Só podem aparecer registros dos tipos `XLOG` (checkpoint/switch), `Standby` (RUNNING_XACTS) e `Transaction` de transações vazias.
Qualquer `Heap`/`Heap2` com `INSERT`, `UPDATE`, `DELETE` ou `MULTI_INSERT` (fora de manutenção) significa que o banco mudou:
nesse caso, ou se recopia, ou se reaplica à mão no CNPq o que mudou.

---

## Etapa 1: Verificações no CNPq (nada é alterado)

| # | O que verificar | Como | Esperado / o que fazer |
|---|---|---|---|
| 1.1 | **O que o banco atual do CNPq tem que a casa não tem** | Rotinas que rodaram lá desde o último clone (Importações Diárias, mensais, `extratorlattes`, scrapers) | O banco do CNPq será **substituído**. Tudo o que só existe lá se perde, salvo se a rotina for rodada de novo depois. Listar e decidir. |
| 1.2 | **Como o PostgreSQL roda e se dá para pará-lo e iniciá-lo sem administrador** | `Get-Service postgres*`; `Get-CimInstance Win32_Process -Filter "name='postgres.exe'" \| select ProcessId, CommandLine -First 1`; `(Get-CimInstance Win32_Service -Filter "name like 'postgres%'").StartName` | Se for processo do seu usuário (`pg_ctl`), ok. Se for serviço de outra conta, testar `Stop-Service`/`Start-Service` fora do horário de uso. Se for negado, pedir à TI para parar e iniciar o serviço na Etapa 5 (única dependência de TI). Conferir também se a conta do serviço consegue ler a pasta de dados restaurada. |
| 1.3 | Python e `uv` | `uv --version` | Instalar no nível do usuário (`winget install astral-sh.uv`), se faltar. |
| 1.4 | Google Drive | Letra, conta logada e espaço livre (local e na nuvem) | **Mesma conta Google** da casa. A cópia ocupa ~120 GB compactados no Drive (Etapa 4). |
| 1.5 | Espaço em disco | `Get-PSDrive` | ~160 GB para o banco restaurado, mais o banco antigo (mantido até validar), mais folga para WAL. |
| 1.6 | Acesso à nuvem (HTTPS) | `Test-NetConnection storage.googleapis.com -Port 443`; idem `run.googleapis.com`, `github.com` | Se bloqueado, o CNPq não puxa a nuvem (decisão D2): a casa assume. |
| 1.7 | Credenciais Google (ADC) | `gcloud auth application-default print-access-token > $null; $?` | `True`. Se não, `gcloud auth application-default login`. |
| 1.8 | Rotinas automáticas | `Get-ScheduledTask \| ? TaskName -match 'CNPq\|Lattes\|Import\|CDC'` | Listar. Decidir o PC dono de cada uma (decisão D7). As tarefas `CNPq_CDC_*` do pipeline antigo serão removidas. |

---

## Etapa 2: Preparar o ambiente do CNPq (sem tocar no banco)

1. Clonar o repositório da sincronização em caminho **sem** `&`, espaços nem acento, fora do OneDrive e do Drive.
   Exemplo: `C:\Projetos\CNPq\importantes\sincronizacao_bd` (decisão D8).
2. `uv sync`.
3. Conferir o cofre `acs-toolbox-secrets`: credenciais do banco e chave dos lotes (`SYNC_BD_CHAVE`).
4. Marcar a pasta `Meu Drive\SincronizacaoBD` como **"Disponível off-line"**, quando a casa criá-la (Etapa 4).

---

## Etapa 3: Atualizar o `extratorlattes` (nos dois PCs, antes de religar rotinas)

As tabelas legadas não existem mais no banco. A biblioteca atualizada continua sabendo gravá-las, mas **não grava por
padrão**. Se uma opção legada for ligada, ela lança `RuntimeError`. Instalar a nova versão em **todos os ambientes que usam
o `extratorlattes`**: Importações Diárias, Planilhas Auxílio Julgamento, PreSelecaoCNPq e scripts avulsos.

Conferência: `python -c "from extratorlattes.carga import Carga; c = Carga(); print(c.importar_vinculos, c.importar_publicações, c.importar_palavras_chave)"`
deve imprimir `False False False`.

---

## Etapa 4: Na casa, instalar a sincronização e gerar a cópia (janela curta, só na casa)

1. **Congelar a casa:** parar rotinas automáticas e o VAR local. Nada pode escrever no banco até o fim do passo 4.
2. Em `postgresql.conf`:
   ```ini
   wal_level = logical
   track_commit_timestamp = on
   max_replication_slots = 10
   max_wal_senders = 10
   max_slot_wal_keep_size = 50GB
   ```
   Reiniciar o PostgreSQL.
3. `python -m sincronizador preflight` (somente leitura) e depois `python -m sincronizador inicializar` (nó definido em `config.local.toml`), que aplica o `instalar.sql`:
   - schema `sincronizacao`;
   - gatilhos de evento;
   - publicação `sinc_pub`;
   - as origens `de_casa` e `de_cnpq`;
   - slot `sinc_slot`;
   - pasta e marcador no Drive.

   Também remove o legado: `DROP TABLE IF EXISTS public._cdc_applied_batches;` e qualquer slot antigo.
4. **Cópia física:** já feita em 06/10/2026 (ver acima). Só gere outra se a verificação do `pg_waldump` mostrar escritas de dados.
   Para refazer: pare o PostgreSQL (desligamento limpo) e copie `E:\PostgreSQL` (`robocopy /E /COPY:DAT /DCOPY:DAT /J`).
   - Decisão D6: as partes contêm dados pessoais. Gerar criptografado, ou apagá-las do Drive logo após a restauração.
   - Apagar também as 122 partes antigas em `G:\Meu Drive\PostgreSQL`.
5. Religar o PostgreSQL da casa. **A casa já pode ser usada:** o slot guarda tudo a partir do ponto da cópia.
6. `python -m sincronizador instalar-tarefas` (com `no = "casa"` no `config.local.toml`). A casa passa a publicar lotes para o CNPq. Eles ficam no Drive até o CNPq
   aplicar.

---

## Etapa 5: No CNPq, restaurar a cópia

1. Desabilitar as rotinas automáticas do CNPq e remover as tarefas do pipeline antigo
   (`Unregister-ScheduledTask CNPq_CDC_Publisher, CNPq_CDC_Subscriber, CNPq_CDC_Cleaner`, as que existirem).
2. Parar o PostgreSQL do CNPq (item 1.2) e **renomear** a pasta de dados atual (ex.: `E:\PostgreSQL\data_antigo_AAAAMMDD`).
   Não apagar até a Etapa 7.
3. Restaurar: copiar `I:\PostgreSQL` (pendrive) para o lugar do banco do CNPq (ex.: `robocopy "I:\PostgreSQL" "E:\PostgreSQL" /E /COPY:DAT /DCOPY:DAT /J`).
   A cópia leva a pasta `data` completa. Conferir se a conta que roda o PostgreSQL consegue ler a pasta (item 1.2).
4. No `postgresql.conf` **do CNPq**, aplicar o mesmo bloco da Etapa 4 (passo 2): `wal_level = logical`, `track_commit_timestamp = on`,
   `max_replication_slots = 10`, `max_wal_senders = 10`, `max_slot_wal_keep_size = 50GB`. Iniciar o PostgreSQL.
5. No CNPq, com o código do sincronizador atualizado, `config.local.toml` com `no = "cnpq"` e o segredo `SYNC_BD_CHAVE`
   **igual ao da casa**: `python -m sincronizador preflight` e depois `python -m sincronizador inicializar`. Ele cria, só
   no CNPq, o schema `sincronizacao`, os gatilhos, a publicação, as origens e o **slot do CNPq**. O ponto de partida é este
   momento: os dois bancos são idênticos e nenhum deles foi alterado desde a cópia (ver verificação no início).
6. Conferir:
   ```sql
   SELECT current_setting('wal_level');                              -- logical
   SELECT slot_name, plugin, confirmed_flush_lsn FROM pg_replication_slots;  -- sinc_slot, pgoutput
   SELECT count(*) FROM pg_publication WHERE pubname = 'sinc_pub';   -- 1
   SELECT to_regclass('public.cnpq_areas_conhecimento'), to_regclass('public.lattes_vinculos');  -- existe, NULL
   ```

---

## Etapa 6: Ativar no CNPq

1. `python -m sincronizador instalar-tarefas` (com `no = "cnpq"` no `config.local.toml` do CNPq). Cria `SincBD_Ciclo` (a cada 10 min e no logon) e `SincBD_Comparar`
   (semanal), com `pythonw.exe` (sem janela), sem administrador. A sessão do Windows precisa ficar **logada** (pode estar
   bloqueada).
2. Esperar o CNPq aplicar os lotes acumulados da casa. `sincronizador status` nos dois deve mostrar atraso zero.
3. `python -m sincronizador comparar` nos dois PCs. Deve dar tudo igual.
4. Teste de aceite com a tabela `_sinc_teste`:
   - INSERT, UPDATE e DELETE de cada lado;
   - um conflito proposital;
   - um `ALTER TABLE ... ADD COLUMN`.

   Conferir `sincronizacao.conflitos`. Depois, `DROP TABLE _sinc_teste` (o DROP também replica).

---

## Etapa 7: Religar rotinas e limpar

1. Instalar o `extratorlattes` atualizado (Etapa 3) e reabilitar cada rotina **somente no PC dono** (decisão D7),
   desabilitando-a de vez no outro. Rodar de novo as rotinas cujos dados só existiam no banco antigo do CNPq (item 1.1).
2. Acompanhar uma semana, incluindo uma carga mensal (`painel_vigentes`).
3. Apagar a pasta `data_antigo_*` do CNPq e as partes da cópia no Drive (decisão D6).

---

## Etapa 8: Recepção da nuvem (se o CNPq for o puxador)

Pré-requisito: Fase 4 do plano concluída no lado da nuvem (Job `var-sync-exportar`, papel `var_sync_leitura`, bucket). Ligar
`puxar_nuvem = true` no `config.toml` do CNPq e `false` na casa. A primeira importação gera o relatório de linhas que só
existem no local. Decidir antes de qualquer exclusão.

---

## Como desfazer

| Situação | Ação |
|---|---|
| Restauração no CNPq falhou | Parar o PostgreSQL, voltar a pasta `data_antigo_*` para o nome original e iniciar. |
| Sincronização causando problema | `sincronizador pausar` nos dois (os ciclos passam a não fazer nada; o slot continua guardando as mudanças). |
| Desistir de vez | `sincronizador desinstalar`: remove tarefas, slot, publicação, gatilhos de evento e o schema `sincronizacao`. |
| Precisar das tabelas Lattes removidas | Recriar com `extratorlattes.schema` e reprocessar a partir do `lattes_json` (opções `lattes_vinculos=True` etc. no `atualiza()`). A biblioteca nunca recria as tabelas sozinha. |

---

## Checklist (preencher na execução)

| Item | Resultado | Data |
|---|---|---|
| 1.1 Dados só do CNPq levantados | | |
| 1.2 Parar/iniciar PostgreSQL sem administrador | | |
| 1.3 `uv` instalado | | |
| 1.4 Drive (letra / conta / espaço) | | |
| 1.5 Espaço em disco | | |
| 1.6 Acesso nuvem e GitHub | | |
| 1.7 ADC válido | | |
| 1.8 Rotinas e PC dono de cada uma | | |
| 3 `extratorlattes` atualizado (os dois PCs) | | |
| 4 Casa: sincronização instalada e cópia gerada | | |
| 5 CNPq: cópia restaurada e conferida | | |
| 6 Atraso zero, comparação igual, teste de aceite | | |
| 7 Rotinas só no dono; limpeza feita | | |
| 8 Nuvem | | |

---

## Anexo A: chaves primárias criadas na casa em 06/10/2026

| Tabela | Chave | Observação |
|---|---|---|
| `cnpq_areas_conhecimento` | `codigo_formatado` | catálogo (renomeado de `lattes_areas_conhecimento`) |
| `lattes_lista_indicadores` | `id` | |
| `painel_demanda_atendimento` | `processo` | |
| `alberto_acordos_faps` | `"Processo"` | |
| `painel_mapa_fomento` | `id` (artificial, `GENERATED BY DEFAULT AS IDENTITY`) | `id_ordem` se repete; numeração determinística por todas as colunas |
| `dados_abertos_pagamentos` | `id` (artificial, `GENERATED BY DEFAULT AS IDENTITY`) | 2,9% de linhas idênticas legítimas, sobretudo bolsas de menores com processo, nome e CPF mascarados na fonte (2023–2025). Numeração determinística. |

Índices criados na mesma data: `dados_abertos_pagamentos (processo)`, `painel_mapa_fomento (data_extracao)` e
`painel_demanda_atendimento (sgl_chamada)`. Correção de dado: o pagamento `id = 1` (processo `470276/2006-1`) tinha
`ano_referencia = 206` e passou a `2006`.

Os carregadores foram ajustados para não recriar essas tabelas (`if_exists='replace'` apagaria a PK). Eles esvaziam e
recarregam numa única transação:
- `CNPq - Acordo de Cooperação com as FAPs - Atualiza BD e pega csv para BI.ipynb`;
- `CNPq - Importando os Pagamentos de Relatórios Dados Abertos.ipynb`;
- `CNPq - Códigos diversos.ipynb` (catálogo de áreas);
- `extratorlattes/atualizar_mapa_fomento.py`.
