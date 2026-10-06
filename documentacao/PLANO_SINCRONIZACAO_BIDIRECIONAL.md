# Plano: Sincronização Bidirecional dos Bancos (Casa ⇄ CNPq) + Recepção da Nuvem (VAR CNPq)

> **Data:** 05/10/2026
> **Status:** proposta, aguardando aprovação. Nada foi alterado em banco, configuração ou agendador.
> **Substitui:** o pipeline unidirecional via OneDrive descrito em `ARQUITETURA_E_GUIA.md` (será reescrito na Fase 5).

---

## 1. Objetivo e regras

1. **Casa ⇄ CNPq (bidirecional):** toda alteração de dados (INSERT, UPDATE, DELETE, TRUNCATE) e de estrutura (DDL) feita em
   um dos bancos chega ao outro, de forma intervalada, usando **somente uma pasta do Google Drive** como meio de transporte.
   Não há conexão direta entre os bancos.
2. **Nuvem → local (somente recepção):** as tabelas de usuários do VAR CNPq (Cloud SQL) são **copiadas** para os bancos
   locais. **Nada é enviado à nuvem**, nunca. As tabelas do RAG (acervo) não vêm da nuvem: a verdade delas é o banco local.
3. **Conflitos:** raros, porque só uma pessoa usa os bancos e não nos dois ao mesmo tempo. Não há faixas de ids.
   Regra: **a última escrita vence** (pela hora do commit original), e **todo conflito é registrado** em tabela, com os
   dados que perderam, para poder recuperar.
4. **Sem janelas piscando:** nada de prompt do cmd aparecendo. Tudo roda com `pythonw.exe` pelo Agendador de Tarefas,
   no nível do usuário comum (sem administrador no PC do CNPq).
5. **Princípio "sem dor de cabeça":** falhar alto e cedo, nunca divergir em silêncio. Qualquer situação que o sistema não
   saiba resolver **para o fluxo** e avisa, em vez de pular dados.

### Nomes usados neste documento
| Nome | Máquina | Observação |
|---|---|---|
| **casa** | este PC (dados em `C:\Disco_E`, montado como `E:`) | tem administrador; não fica ligado 24 h |
| **cnpq** | PC do CNPq | 24/7, sem administrador, firewall só de saída; a rede do CNPq **não** alcança o Cloud SQL |
| **nuvem** | Cloud SQL `var-cnpq-db` (projeto `var-cnpq-2026`, São Paulo) | só lida, nunca escrita |

---

## 2. Diagnóstico atual (levantado em 05/10/2026 no banco da casa)

| Item | Situação | Consequência |
|---|---|---|
| PostgreSQL | 18.0, serviço Windows `postgresql-18`, banco `cnpq` com **160 GB** e **61 tabelas** | — |
| `wal_level` | `replica` | precisa ser `logical` (exige reinício) |
| Slots e publicações | nenhum | criar no ponto certo (Fase 2) |
| Plugins | `pgoutput.dll` e `test_decoding.dll` presentes | **usaremos `pgoutput`** (nativo, tipado) |
| `pg_hba.conf` | já permite `replication` em `127.0.0.1` (md5) | conexão de replicação local funciona |
| Usuário do banco | superusuário com `REPLICATION` | pode criar slot, origem e gatilhos de evento |
| **Tabelas sem chave primária** | 10: `lattes_publicacoes`, `lattes_palavras_chave`, `lattes_vinculos`, `lattes_areas_conhecimento`, `dados_abertos_pagamentos`, `painel_demanda_atendimento`, `painel_mapa_fomento`, `projetos_cannabis_cnpq`, `alberto_acordos_faps`, `lattes_lista_indicadores` | **crítico, ver §6.1** |
| Tabelas Lattes legadas | `lattes_xml` (vazia), `lattes_vinculos`, `lattes_publicacoes` e `lattes_palavras_chave` cobrem no máximo 2% dos 9,9 milhões de currículos, não são atualizadas e nenhum programa as lê (tudo sai do `lattes_json`) | **serão removidas** (decidido em 06/10/2026, ver §6.1) |
| Colunas geradas | `rag_chunks.tsv`, `rag_documentos.ano` | o aplicador precisa ignorá-las |
| Gatilhos | `trg_rag_documentos_origem`, `trg_rag_chunks_origem` | não podem disparar ao aplicar dados replicados |
| Sequences | 14 (`llm_registros_custos`, `painel_vigentes`, `chamadas_*`, etc.) | **não são replicadas** pelo PostgreSQL: ajustar após cada lote |
| Tabelas de usuários do VAR | pequenas: 16 usuários, 75 sessões, 242 mensagens; chaves **UUID** (exceto `llm_registros_custos` e `rag_auditoria_acesso`, `bigint` serial) | cópia da nuvem é barata |
| Pipeline antigo | `public._cdc_applied_batches` com 39 lotes aplicados; scripts `dist/cdc_*.py` apontando para OneDrive | será aposentado |
| Clone completo | 122 partes **não criptografadas** em `G:\Meu Drive\PostgreSQL` | ver decisão D6 (LGPD) |
| Disco | ~1,3 TB livres em `C:` | permite folga grande de WAL |

Escrita observada desde o último início do banco: moderada (centenas a dezenas de milhares de linhas por tabela). As cargas
mensais (ex.: `painel_vigentes` apaga e recarrega o mês, ~110 mil linhas) geram os maiores lotes, na faixa de dezenas de MB
compactados. É volume tranquilo para o Google Drive.

---

## 3. Arquitetura

```
                         NUVEM (Cloud SQL, só leitura)
                                   │
            Cloud Run Job "var-sync-exportar" (papel SÓ-SELECT nas 11 tabelas de usuários)
                                   │  CSV.gz + manifesto
                                   ▼
                gs://var-cnpq-2026-db-sync/nuvem_para_local/   (Brasil, expira em 3 dias)
                                   │  baixado SOMENTE pelo "puxador" (1 PC)
                                   ▼
┌──────────────────────── CNPq (24/7) ─────────────────────────┐      ┌────────────────────────── CASA ──────────────────────────┐
│ PostgreSQL ─ slot lógico (pgoutput, origin=none)             │      │ PostgreSQL ─ slot lógico (pgoutput, origin=none)         │
│     │                                                        │      │     │                                                    │
│ sincronizador ciclo (a cada 10 min, pythonw, sem janela)     │      │ sincronizador ciclo (a cada 10 min + ao fazer logon)     │
│  1. PUBLICA: lê o slot → lote criptografado → saída própria  │      │  1. PUBLICA                                              │
│  2. APLICA: lotes da casa, em ordem, com origem de replicação│      │  2. APLICA: lotes do CNPq                                │
│  3. NUVEM: dispara o Job, baixa e mescla (só no puxador)     │      │  3. (nuvem desligada aqui)                               │
│  4. ESTADO: grava ack + saúde; poda o que o par já aplicou   │      │  4. ESTADO                                               │
└───────────────┬──────────────────────────────────────────────┘      └──────────────────────────────┬───────────────────────────┘
                │                     Google Drive  (G:\Meu Drive\SincronizacaoBD\)                    │
                └──────────► cnpq\saida\*.lote  ──────────────── lidos pela casa ───────────────────────┘
                ◄─────────── casa\saida\*.lote  ──────────────── lidos pelo CNPq ◄──────────────────────
                             cnpq\estado.json  /  casa\estado.json   (ack, saúde, alertas)
```

### 3.1 Por que estas escolhas

| Decisão | Escolha | Motivo |
|---|---|---|
| Captura de mudanças | **slot lógico + `pgoutput`** lido pelo protocolo de replicação (streaming) | Nativo do PostgreSQL 18, sem DLL externa. Entrega tipos, nomes de colunas, chave da tabela e TOAST não alterado. Respeita a publicação e tem o filtro `origin = none`, que evita o eco. Em streaming, transações enormes não estouram a memória (o `pg_logical_slot_get_changes` atual carrega tudo na RAM). `test_decoding` é texto para depuração e de parse frágil (aspas, arrays, jsonb, vetores). |
| Evitar eco (A→B→A) | **origens de replicação** (`pg_replication_origin_*`) | O aplicador marca as escritas dele com a origem do par. O publicador (`origin = none`) ignora o que veio de fora. Bônus: o PostgreSQL grava atomicamente, junto com cada transação, até qual LSN do par foi aplicado. Reaplicar um lote é inofensivo (idempotência real, não por nome de arquivo). |
| Detecção de conflito | **`track_commit_timestamp = on`** + `pg_xact_commit_timestamp_origin(xmin)` | Para cada linha, o banco sabe quando ela foi alterada pela última vez e se a alteração foi local ou replicada. É o mesmo mecanismo do PostgreSQL 18 nativo. Dispensa tabelas de rastreamento. |
| DDL | **gatilho de evento** grava o comando em `sincronizacao.ddl_log` (tabela replicada) | O comando viaja **na mesma transação e na mesma ordem** dos dados. O par o executa no ponto exato do fluxo. |
| Transporte | **pasta do Google Drive**, um escritor por arquivo | Cada PC só escreve na própria pasta `saida\` e no próprio `estado.json`. Nunca há dois escritores no mesmo arquivo, então não aparecem as "cópias em conflito" do Drive. O consumidor não move nem apaga arquivos do outro: só registra o ack, e o dono poda. |
| Integridade e LGPD no Drive | **lotes criptografados** (AES-GCM, chave no cofre `acs-toolbox`) | Os lotes contêm dados pessoais (Lattes, SEI, conversas do VAR) e o Drive é pessoal. A criptografia autenticada também detecta arquivo truncado ou corrompido pelo Drive. |
| Nuvem → local | **Job de exportação só-leitura + bucket no Brasil**, puxado por um único PC | A rede do CNPq não alcança o Cloud SQL, mas alcança HTTPS. Não exige slot nem gatilho no banco de produção (um slot parado lá poderia encher o disco do Cloud SQL). Um papel só-SELECT garante fisicamente o "nunca mandar". |
| Execução | **Agendador de Tarefas + `pythonw.exe`**, ciclos curtos | Sem janela. Sem administrador. Uma falha só afeta um ciclo, e o próximo retoma. Sem vazamento de memória de processo eterno. `ExecutionTimeLimit` mata um ciclo travado. |

---

## 4. Componente PC ⇄ PC

### 4.1 Pasta no Google Drive
```
G:\Meu Drive\SincronizacaoBD\              (marcada "Disponível off-line" nos dois PCs)
├── .sincronizacao_bd.json                 marcador: versão do protocolo, ids dos nós
├── cnpq\
│   ├── saida\cnpq_000000001.lote ...      escritos só pelo CNPq
│   └── estado.json                        ack (o que o CNPq já aplicou da casa) + saúde + alertas
└── casa\
    ├── saida\casa_000000001.lote ...
    └── estado.json
```
- A pasta é localizada pelo **marcador**, não pela letra da unidade. Aqui há `G:` e `H:`, e no CNPq pode ser outra letra.
  Os dois PCs precisam estar logados na **mesma conta Google**.
- **Disponível off-line:** no modo streaming do Drive, os arquivos do par só baixam quando são lidos. Com a pasta off-line,
  o Drive baixa assim que chegam e grava localmente mesmo sem rede.

### 4.2 Formato do lote
- Nome: `{no}_{sequencial:09d}.lote` (sequencial contínuo por nó, guardado no banco do produtor).
- Conteúdo: JSON Lines compactado com gzip e criptografado em blocos (AES-GCM, índice do bloco autenticado, marca de fim).
  Para evitar leitura pela metade, o arquivo é gravado como `.tmp` e renomeado só depois de completo.
- Cabeçalho: versão do protocolo, nó, sequencial, LSN inicial e final, nº de transações e mudanças, e "visto do par até"
  (commit do par mais recente já aplicado aqui, usado na detecção de conflito).
- Registros: `B` (início: xid, LSN e **hora do commit**), `R` (metadados da tabela: colunas, tipos, colunas-chave),
  `I`/`U`/`D` (linhas; no UPDATE, as colunas TOAST não alteradas vêm marcadas e **não** entram no SET), `T` (TRUNCATE) e
  `C` (commit). Rodapé com contagens.
- Rotação: um novo arquivo é aberto ao passar de 64 MB, sempre em fronteira de transação. Uma transação gigante vira um
  arquivo gigante, gravado em streaming.

### 4.3 Publicador (em cada PC)
1. Conecta pelo protocolo de replicação (`psycopg2` `LogicalReplicationConnection`) e inicia o slot `sinc_slot` com
   `proto_version '4'`, `publication_names 'sinc_pub'`, `origin 'none'` e `streaming 'off'`.
2. Lê mensagens até alcançar o `pg_current_wal_lsn()` do início do ciclo, gravando o lote em streaming.
3. Só depois de **fechar, sincronizar em disco e renomear** o arquivo, confirma o LSN ao slot (`send_feedback(flush_lsn)`).
   Se cair no meio, o slot não avança e o próximo ciclo regrava o mesmo conteúdo. Isso corrige a perda de dados do
   publicador atual, que consumia o slot antes de gravar.
4. Publicação: `CREATE PUBLICATION sinc_pub FOR TABLES IN SCHEMA public, TABLE sincronizacao.ddl_log, sincronizacao.nuvem_ids,
   sincronizacao.nuvem_marcas;`. Tabelas novas em `public` entram sozinhas. As de controle por nó (`sincronizacao.*`
   restantes) ficam de fora.

### 4.4 Aplicador (em cada PC)
1. Lê o `estado.json` do par e aplica os lotes **em ordem estrita de sequencial**. Se faltar um número (o Drive ainda
   sincronizando), espera. Se a falta passar de 6 h, alerta.
2. Sessão com:
   - `session_replication_role = replica`, que não dispara gatilhos de usuário nem checa FK, como a replicação nativa;
   - `synchronous_commit = off`;
   - `pg_replication_origin_session_setup('de_<par>')`.
3. Pula transações com commit ≤ `pg_replication_origin_progress('de_<par>')`, o que torna reaplicar idempotente.
   Agrupa várias transações do par por commit local e chama `pg_replication_origin_xact_setup(lsn, hora_commit)`.
4. Regras por operação (com a chave primária **real**, lida do catálogo, ou todas as colunas nas tabelas FULL):

   | Chega | Situação no destino | Ação | Registro |
   |---|---|---|---|
   | INSERT | linha não existe | insere | — |
   | INSERT | já existe | vira UPDATE (última escrita vence, ver passo 5) | `conflitos` se o conteúdo diferir |
   | UPDATE | existe | atualiza só as colunas enviadas (sem as TOAST não alteradas) | ver passo 5 |
   | UPDATE | não existe | recria com a linha nova completa. Se faltarem colunas TOAST, não dá para recriar: registra e segue | `conflitos` |
   | DELETE | existe | apaga (nas tabelas sem PK, **uma** linha idêntica, como o PostgreSQL nativo) | ver passo 5 |
   | DELETE | não existe | nada | `conflitos` (tipo "ausente") |
   | TRUNCATE | — | executa | — |
   | INSERT em `sincronizacao.ddl_log` | — | executa o DDL (fora de transação se for `CONCURRENTLY`) | falha **para o fluxo** e alerta |

5. **Última escrita vence:** há conflito real quando a linha local foi alterada **localmente** (origem 0, via
   `pg_xact_commit_timestamp_origin(xmin)`) depois do "visto do par até" do cabeçalho. Nesse caso, vence o commit mais
   recente. Se o local vencer, a mudança recebida é descartada, mas fica gravada em `sincronizacao.conflitos`. A mudança
   local, mais nova, chega ao par e vence lá também, e os dois bancos convergem. Linhas antigas, cujo carimbo já foi
   descartado pelo banco, contam como "sem alteração local recente".
6. **Tabelas só de inserção** (`llm_registros_custos`, `rag_auditoria_acesso`, `log_importacoes_diarias`,
   `pre_selecao_auditoria`): se o id recebido já existir **com outro conteúdo** (dois PCs gravando log ao mesmo tempo), a
   linha é inserida com um **id novo** do destino, em vez de sobrescrever. Os ids podem diferir entre os PCs, mas nenhum
   registro de custo ou de log se perde. Nada referencia esses ids.
7. **Sequences:** ao final de cada lote, para cada tabela tocada, `setval(seq, max(coluna))` se o máximo passar do valor
   atual. É isso que permite inserir no outro PC depois sem colidir, **sem faixas de ids**.
8. Ao terminar: grava `sincronizacao.lotes_aplicados` (auditoria) e atualiza o ack no próprio `estado.json`.
9. Qualquer erro inesperado faz rollback do grupo, **para o fluxo** (não pula para o próximo lote) e alerta.

### 4.5 Tabela de conflitos (`sincronizacao.conflitos`, local em cada nó)
`id, detectado_em, no_origem, lote, lsn, tabela, chave (jsonb), operacao, tipo ('concorrente' | 'ausente' | 'divergente'),
vencedor ('recebido' | 'local'), hora_commit_recebido, hora_commit_local, dados_recebidos (jsonb), dados_locais (jsonb)`.
Para recuperar um valor perdido, basta consultar e reaplicar à mão. O `estado.json` e o aviso diário mostram quantos
conflitos novos surgiram.

---

## 5. Componente Nuvem → local (somente recepção)

### 5.1 O que vem
As 11 tabelas da lista oficial do VAR (`backend/jobs/db_diagnostico.py`, `TABELAS_USUARIOS`): `rag_usuarios`,
`rag_sessoes_chat`, `rag_mensagens_chat`, `rag_sessoes_documentos`, `rag_gems_usuarios`, `rag_gem_documentos_usuarios`,
`rag_feedbacks` (likes e dislikes), `llm_registros_custos`, `tarefas_apresentacao`, `rag_deep_research_tarefas` e
`rag_auditoria_acesso`. **Não vêm:** `rag_documentos`, `rag_versoes`, `rag_chunks`, `chamadas_*` e `rag_cache_consultas`.
Assim, se um usuário na nuvem faz uma consulta, cria ou renomeia uma conversa, atualiza o cadastro, dá like ou dislike,
ou cria um GEM, a mudança aparece aqui em até um intervalo de puxada mais um ciclo PC⇄PC no outro PC.

Colunas excluídas: `rag_usuarios.senha_temporaria` e `expiracao_senha`. São o código de acesso de uso único, um segredo
pela regra 11 do VAR, e não têm uso local (decisão D4). Só são copiadas as colunas comuns aos dois lados. Colunas legadas
da nuvem são ignoradas e registradas uma vez no log.

### 5.2 Lado da nuvem (no repositório do VAR)
- **Job `var-sync-exportar`** (Cloud Run Job, mesma imagem do backend, novo módulo `backend/jobs/exportar_usuarios.py`):
  1. conecta com o **papel `var_sync_leitura`** (só `SELECT` nas 11 tabelas, sem acesso ao acervo), em transação
     `REPEATABLE READ READ ONLY`, para que todas as tabelas venham do mesmo instante;
  2. tabelas mutáveis: exporta o **retrato completo** de cada uma (`COPY ... TO STDOUT CSV`, gzip). Hoje cabe em poucos MB.
     Exporta só linhas com "pai", como os `FILTROS_SINC` existentes, porque a nuvem guarda mensagens de conversas apagadas;
     os órfãos entram como contagem no manifesto;
  3. tabelas só de inserção (`llm_registros_custos`, `rag_auditoria_acesso`): exporta só `id > marca` (marca recebida
     como argumento do PC);
  4. grava `manifesto.json` **por último** (colunas, contagens, sha256 de cada arquivo, órfãos). Sem manifesto, o lote não
     está pronto.
- **Bucket** `var-cnpq-2026-db-sync` (São Paulo, já previsto no plano 2.0.0), prefixo `nuvem_para_local/`, com regra de
  ciclo de vida de **3 dias**. O PC também apaga cada pasta depois de importar.
- **Disparo:** o próprio PC puxador dispara o Job pela API do Cloud Run (`google-cloud-run`, credenciais ADC do usuário,
  as mesmas do `acs-toolbox`), espera terminar e baixa. Não é preciso ativar o Cloud Scheduler, e o Job só roda quando o
  puxador está ligado. Custo estimado: centavos por mês.
- Governança do VAR:
  - o módulo novo segue o `versoes/PROCEDIMENTO_VERSIONAMENTO.md` (AGENTS.md §7);
  - criar o papel `var_sync_leitura` (`CREATE ROLE` + `GRANT SELECT`) é alteração no Cloud SQL e segue o **rito da
    Diretriz 10**, com "autorizado" textual;
  - nenhuma linha de dados da nuvem é alterada.

### 5.3 Lado local (no puxador)
1. A cada 30 min: dispara o Job, baixa e confere o sha256 contra o manifesto.
2. Carrega cada CSV numa tabela temporária e mescla **por conjunto**, na ordem das FKs:
   - **upsert** `INSERT ... ON CONFLICT (id) DO UPDATE ... WHERE linha IS DISTINCT FROM excluded`. Só as linhas que
     mudaram geram escrita, e portanto só elas viajam para o outro PC;
   - **exclusão** só de linhas que **vieram da nuvem** (registradas em `sincronizacao.nuvem_ids`) e sumiram do retrato.
     Linhas criadas localmente (ex.: testes do VAR rodando aqui) **nunca** são apagadas pela nuvem. Excluir uma sessão
     apaga em cascata as mensagens e documentos dela, como na nuvem;
   - **só de inserção:** insere com id local novo, guarda `id_nuvem → id_local` e avança `sincronizacao.nuvem_marcas`.
3. Essa importação roda **sem** origem de replicação, de propósito: o publicador do puxador a captura e ela chega ao outro
   PC pelo fluxo normal. Há um caminho único, `nuvem → puxador → par`, sem duplicar nem divergir ids.
   `nuvem_ids` e `nuvem_marcas` são replicadas. Se o papel de puxador mudar de PC, o estado vai junto.
4. Verificação da regra 9 do VAR após cada importação: se `alberto.silva@cnpq.br` não tiver exatamente
   `['administrador','CNPq']`, **alerta** e não corrige nada.
5. Na **primeira** importação, `nuvem_ids` está vazia: nada é apagado. Um relatório lista as linhas locais que não existem
   na nuvem (cópia da Fase 2 do VAR já apagada lá, ou testes locais) para você decidir.

**Puxador:** o **CNPq** (24/7), se a rede de lá alcançar `run.googleapis.com` e `storage.googleapis.com` (verificar na
Fase 0). Caso contrário, a casa (só puxa quando ligada). É configuração, sem mudar código.

---

## 6. Pontos que dariam dor de cabeça, e como ficam resolvidos

### 6.1 Tabelas sem chave primária quebrariam as importações (crítico)
Com uma publicação que publica UPDATE e DELETE, o PostgreSQL **recusa** UPDATE e DELETE em tabela sem chave de réplica
(`cannot delete from table ... because it does not have a replica identity`). Sem tratamento, o `extratorlattes`
(`DELETE FROM lattes_palavras_chave WHERE id_lattes = ...`) e as cargas dos painéis falhariam nos dois PCs no dia seguinte.
**Resolvido na casa em 06/10/2026.** O CNPq recebe uma cópia nova e limpa, que já leva tudo isto:
- **Removidas** as tabelas legadas `lattes_xml`, `lattes_vinculos`, `lattes_publicacoes` e `lattes_palavras_chave`
  (~3,1 GB) e a tabela temporária `projetos_cannabis_cnpq`. O `extratorlattes` continua sabendo gravar as legadas, mas
  com as opções **desligadas por padrão**. Ligadas sem a tabela, lançam `RuntimeError`.
- **Renomeado** o catálogo de áreas para `cnpq_areas_conhecimento`.
- **Chave primária em todas as tabelas**:
  - naturais: `cnpq_areas_conhecimento (codigo_formatado)`, `lattes_lista_indicadores (id)`,
    `painel_demanda_atendimento (processo)` e `alberto_acordos_faps ("Processo")`;
  - artificiais (`id`, identidade): `painel_mapa_fomento` (o `id_ordem` se repete) e `dados_abertos_pagamentos` (2,9% de
    linhas idênticas legítimas, sobretudo bolsas de menores com processo, nome e CPF mascarados na fonte). Numeração determinística, ordenando por todas as colunas.
  - Os carregadores que usavam `to_sql(if_exists='replace')` foram ajustados para esvaziar e recarregar numa transação,
    sem recriar a tabela.
- Com isso, **nenhuma tabela precisa de `REPLICA IDENTITY FULL`** e não é preciso índice em `id_lattes`.
- Roteiro exato: `IMPLANTACAO_PC_CNPQ.md` (Etapas 4 e 5, Anexo A).
- **Gatilho de evento de proteção:** toda tabela criada no futuro em `public` sem PK recebe `REPLICA IDENTITY FULL`
  automaticamente (ex.: `pandas.to_sql`). O sistema também emite um aviso sugerindo criar a PK.

### 6.2 DDL (CREATE, ALTER, DROP)
- O gatilho de evento `ddl_command_end`/`sql_drop` grava o comando em `sincronizacao.ddl_log`, na mesma transação. O par
  executa no mesmo ponto do fluxo. Migrações do VAR (`backend/migracoes/*`) e `to_sql(if_exists='replace')` passam a
  replicar sozinhas.
- Replicação automática **só** quando o texto capturado é **um único comando**. Se for um script com vários comandos
  misturando DDL e DML, reexecutar duplicaria os dados. Nesse caso o fluxo **para** e mostra o comando, e você resolve com
  `sincronizador ddl "<comando>"`, que roda aqui e enfileira para o par.
- Antes de aplicar cada lote, o aplicador compara as colunas de cada tabela do lote com as locais. Se houver divergência
  não explicada por DDL do fluxo, para e alerta.

### 6.3 Sequences
Resolvido pelo `setval` após cada lote (§4.4, passo 7) e pela renumeração nas tabelas só de inserção (§4.4, passo 6).

### 6.4 Programas automáticos rodando nos dois PCs
Se a rotina diária, a mensal ou o `extratorlattes` rodarem **nos dois PCs**, cada um apaga e recarrega os mesmos dados e
o fluxo traz as duas versões, gerando conflitos em massa e duplicidade em tabelas sem PK. **Regra:** cada rotina
automática tem **um único PC dono** (decisão D7). O sincronizador avisa se detectar a mesma carga mensal vinda dos dois
lados.

### 6.5 Slot acumulando WAL
O slot só retém WAL se o publicador parar, porque ele consome o slot a cada ciclo, com ou sem o par ligado.
`max_slot_wal_keep_size = 50GB` dá folga para semanas de pane, já que há ~1,3 TB livres. Há alerta a partir de 5 GB
retidos. Se o limite estourar, o slot é invalidado e é preciso reclonar, por isso o alerta vem bem antes.

### 6.6 PC desligado por muito tempo
Nada se perde: os lotes ficam no Drive até o ack do par. Na volta, o aplicador processa o atraso em ordem. Lotes já
confirmados são podados pelo dono após 7 dias. Os não confirmados **nunca** são podados.

### 6.7 Divergência silenciosa
- `sincronizador comparar`: quando os dois lados estiverem sem atraso, cada nó publica no Drive uma impressão por tabela.
  Tabelas até 1 milhão de linhas recebem `md5` do conteúdo ordenado pela PK; as maiores, contagem e `max(pk)`. O outro nó
  compara e alerta. Roda semanalmente e sob demanda.
- `sincronizador reparar-tabela <t>`: no produtor, trava a tabela, anota o LSN e faz `pg_dump` dela (criptografado, no
  Drive). No par, faz `TRUNCATE` + restauração e ignora no fluxo as mudanças daquela tabela até o LSN anotado. Conserta
  uma tabela sem reclonar 160 GB.

### 6.8 Operações em massa
Reescrever uma tabela de dezenas de GB (ex.: recalcular `lattes_indicadores`, 26 GB) gera um lote proporcional. Até
alguns GB, o fluxo normal dá conta. Acima disso, o caminho é `reparar-tabela`, ou fazer a operação nos dois PCs com a
tabela pausada no fluxo (`sincronizador pausar-tabela`).

### 6.9 Pausa geral
Um arquivo `PAUSAR` na pasta do Drive (ou `sincronizador pausar`) faz os ciclos não fazerem nada. Serve para manutenção,
reclonagem ou atualização do código.

### 6.10 Versão do código diferente nos dois PCs
O cabeçalho do lote leva a versão do protocolo. Um aplicador mais antigo **recusa** lote de protocolo mais novo e alerta:
"atualize o código".

### 6.11 Caminho com `&`, espaços e acento
O nome desta pasta (`Integração BD Local  & Remoto`) tem `&`, que no `cmd` separa comandos, além de dois espaços e acento.
Isso é fonte clássica de falhas no Agendador. Recomendo usar `C:\Projetos\CNPq\importantes\sincronizacao_bd` (decisão D8)
e não colocar o código nem o `.venv` dentro do OneDrive ou do Drive.

### 6.12 Replicação não é backup
Um `DELETE` errado replica para o outro PC. Os lotes guardados por 7 dias servem de histórico, mas não de desfazer
completo. Recomendo um `pg_dump` semanal das tabelas pequenas e valiosas (usuários do VAR, `fomento_*`, `chamadas_*`)
numa pasta local, fora da sincronização.

---

## 7. Execução sem janelas e sem administrador

- **Ambiente:** projeto `uv` com `.venv` próprio em cada PC. As tarefas chamam `.venv\Scripts\pythonw.exe -m sincronizador
  ciclo`. `pythonw` não abre console. Não se usa `uv run` nem `.bat`, que piscariam janela. Chamadas a programas externos,
  se houver, usam `CREATE_NO_WINDOW`. A nuvem é acessada por bibliotecas Python, sem `gcloud.cmd`.
- **Tarefas** (criadas por `sincronizador instalar-tarefas`, via `Register-ScheduledTask` do próprio usuário, sem elevação):

  | Tarefa | Gatilho | Configuração |
  |---|---|---|
  | `SincBD_Ciclo` | a cada 10 min, indefinidamente, e ao fazer logon | `MultipleInstances IgnoreNew`, `StartWhenAvailable`, `ExecutionTimeLimit 2h`, aceita bateria, oculta |
  | `SincBD_Comparar` | semanal (domingo 04:00) | idem |

  O ciclo já inclui a puxada da nuvem a cada 30 min no puxador e a poda dos lotes. Não há tarefa separada para isso.
- **No CNPq, sem administrador:** a tarefa roda com "somente quando o usuário estiver conectado". A sessão precisa ficar
  logada, mas pode estar bloqueada. "Executar mesmo deslogado" exigiria guardar senha ou direito de "logon em lote",
  normalmente indisponível sem administrador.
- **Avisos sem janela:** notificação do Windows (toast, pacote `windows-toasts`) só para problemas:
  - fluxo parado;
  - erro em 2 ciclos seguidos;
  - par em silêncio há mais de 48 h (o CNPq deveria estar sempre ligado);
  - WAL retido acima de 5 GB;
  - nuvem falhando há mais de 6 h;
  - Drive ausente ou fora de linha;
  - conflitos novos (resumo diário).

  Logs rotativos ficam em `%LOCALAPPDATA%\SincronizacaoBD\logs`, fora do Drive. `sincronizador status` mostra os dois
  nós a partir dos `estado.json`.

---

## 8. Estrutura do código (este projeto)

```
sincronizacao_bd/
├── pyproject.toml              (uv; psycopg2, cryptography, google-cloud-storage, google-cloud-run, windows-toasts, acs-toolbox)
├── config.toml                 políticas por tabela (bidirecional | só-inserção | pausada), intervalos, puxador
├── sincronizador/
│   ├── __main__.py             CLI: ciclo, status, inicializar, instalar-tarefas, ddl, comparar, reparar-tabela, pausar
│   ├── configuracao.py         nó, pasta do Drive (pelo marcador), segredos via acs-toolbox
│   ├── pgoutput.py             decodificador do protocolo pgoutput (Begin, Relation, Insert, Update, Delete, Truncate, Commit)
│   ├── publicador.py           slot → lote (streaming) → feedback
│   ├── aplicador.py            lote → banco (origem, LWW, conflitos, sequences, DDL)
│   ├── lote.py                 formato, criptografia em blocos, gravação atômica
│   ├── nuvem.py                dispara o Job, baixa, mescla
│   ├── estado.py               estado.json, ack, poda, saúde
│   ├── avisos.py               toast + log
│   └── sql/                    instalar.sql (schema sincronizacao, gatilhos de evento, publicação), desinstalar.sql
└── test/                       testes com 2 PostgreSQL temporários (initdb em portas 55432/55433) e nuvem simulada
```
Distribuição para o CNPq: repositório **git** privado (`git pull` para atualizar). Os scripts `dist/cdc_*.py`,
`agendar_publisher.ps1` e a tabela `public._cdc_applied_batches` são aposentados na Fase 5.

---

## 9. Fases

Os comandos que alteram banco ou configuração só rodam com o seu "pode fazer" no momento.

### Fase 0: Verificações e decisões (sem alterar nada)
No **CNPq**:
1. Versão do PostgreSQL e `wal_level`.
2. Se ainda existe o slot antigo `cnpq_cdc_slot`. Um slot parado retém WAL e precisa ser removido.
3. **Como o PostgreSQL roda:** serviço do Windows ou `pg_ctl`? **Você consegue reiniciá-lo sem administrador?** Se for
   serviço e não conseguir, essa é a única dependência de TI do plano: **um único reinício** para ativar
   `wal_level = logical`.
4. Python e `uv` disponíveis.
5. Letra do Drive, mesma conta Google, pasta off-line.
6. Acesso a `run.googleapis.com`, `storage.googleapis.com` e GitHub.
7. Credenciais ADC válidas.

Nos dois PCs:
8. `comparar` provisório para confirmar que os bancos estão **idênticos** após o clone (contagens e `max(pk)` em todas as
   tabelas; `md5` nas pequenas).

Responder às decisões D1 a D8 (§10).

### Fase 1: Código e testes (só na casa, sem tocar no banco real) — **CONCLUÍDA em 06/10/2026**
Projeto em `C:\Projetos\CNPq\importantes\sincronizacao_bd` (ver README). 42 testes passam em dois PostgreSQL temporários; o
pré-voo no banco real (`python -m sincronizador preflight`, somente leitura) não achou bloqueios. Fica para depois a
recepção da nuvem (Fase 4) e `reparar-tabela`/`pausar-tabela`. Descrição original da fase:
Implementar §4 a §8. Os testes rodam com dois PostgreSQL 18 temporários e cobrem:
- INSERT, UPDATE e DELETE nos dois sentidos;
- tabelas sem PK e TOAST;
- TRUNCATE e DDL;
- conflito concorrente (os dois lados), linha ausente e renumeração em tabela só de inserção;
- `setval`;
- lote truncado ou corrompido;
- falta de sequencial, reaplicação idempotente e queda no meio da gravação;
- nuvem simulada: upsert, exclusão só do que veio da nuvem e órfãos.

### Fase 2: Instalação na casa e cópia nova para o CNPq (janela curta, **só na casa**)
O CNPq ainda não recebeu o banco atual da casa. Por isso, a sincronização é instalada **na casa** e o CNPq recebe uma
**cópia física integral e limpa**, tirada depois. Roteiro completo: `IMPLANTACAO_PC_CNPQ.md`, Etapas 4 a 6.
1. Na casa, parar rotinas e o VAR local. Em `postgresql.conf`:
   ```ini
   wal_level = logical
   track_commit_timestamp = on
   max_replication_slots = 10
   max_wal_senders = 10
   max_slot_wal_keep_size = 50GB
   ```
   Reiniciar o PostgreSQL.
2. `sincronizador inicializar` aplica o `instalar.sql`:
   - schema `sincronizacao`;
   - gatilhos de evento;
   - publicação `sinc_pub`;
   - as origens `de_casa` e `de_cnpq`;
   - slot `sinc_slot`.

   Também remove `_cdc_applied_batches` e qualquer slot antigo.
3. Parar o PostgreSQL e gerar a cópia física. Ela leva estrutura, configuração e **o slot**. Os dois bancos partem do
   mesmo ponto, com os mesmos `id`. Não há comparação nem reaplicação de DDL a fazer no CNPq.
4. Religar a casa. Ela já pode ser usada: o que for escrito enquanto o CNPq é restaurado vira lote no Drive.
5. No CNPq: renomear a pasta de dados antiga (manter até validar), restaurar a cópia e iniciar.

### Fase 3: Ativação Casa ⇄ CNPq
1. `instalar-tarefas` nos dois PCs. O CNPq aplica os lotes acumulados da casa até zerar o atraso; depois,
   `comparar --completo`.
2. Teste de aceite com a tabela `_sinc_teste`, que também testa o DDL:
   - INSERT, UPDATE e DELETE de cada lado;
   - um conflito proposital;
   - um `ALTER TABLE ADD COLUMN`;
   - confirmar os dois lados e a tabela `conflitos`.
3. Religar as rotinas automáticas **só no PC dono** de cada uma (D7). Acompanhar uma semana, incluindo uma carga mensal.

### Fase 4: Recepção da nuvem
1. VAR: módulo `exportar_usuarios.py` + Job `var-sync-exportar`, pelo procedimento de versionamento (§7 do AGENTS.md).
2. Rito da Diretriz 10: papel `var_sync_leitura` só-SELECT nas 11 tabelas e regra de ciclo de vida do bucket.
3. Ligar a puxada no puxador. Primeira importação com o relatório de linhas só-locais (§5.3, item 5).
4. Aceite: dar um like, criar uma conversa e editar um cadastro **na nuvem**, e ver chegar aos dois PCs.

### Fase 5: Documentação e limpeza
Reescrever `README.md` e `ARQUITETURA_E_GUIA.md`, aposentar `dist/` e `_cdc_applied_batches`, registrar a exceção no
AGENTS.md do VAR (D5) e tratar as partes do clone no Drive (D6).

---

## 10. Decisões (respondidas em 06/10/2026)

| # | Decisão | Resposta |
|---|---|---|
| D1 | Criptografar os lotes no Google Drive | **Sim.** AES-GCM, chave no cofre `acs-toolbox` (`SYNC_BD_CHAVE`). |
| D2 | Quem puxa a nuvem | **Em aberto, fora deste projeto.** Será tratada no projeto "Importações Diárias". O código aceita qualquer PC por configuração (`puxar_nuvem`). |
| D3 | Intervalos | Ciclo PC⇄PC a cada **10 min**; puxada da nuvem a cada **30 min**, **desde que totalmente invisível**: `pythonw.exe`, sem janela, sem foco, sem som. Notificação só em problema real. |
| D4 | Excluir `senha_temporaria`/`expiracao_senha` da cópia da nuvem | **Sim, excluir.** |
| D5 | Registrar a exceção no AGENTS.md do VAR | **Feito:** §9-A de `VAR CNPq - RAG/AGENTS.md`. |
| D6 | Apagar as 122 partes do clone antigo em `G:\Meu Drive\PostgreSQL` | **Feito em 06/10/2026** (foram para a lixeira do Google Drive). A cópia nova será gerada criptografada. |
| D7 | PC dono de cada rotina automática | **Em aberto, fora deste projeto** (Importações Diárias). |
| D8 | Projeto em `C:\Projetos\CNPq\importantes\sincronizacao_bd`, em git privado | **Sim.** |

---

## 11. Riscos residuais (aceitos)
- **Conflito concorrente perde uma das versões no banco**, mas ela fica em `sincronizacao.conflitos`. É o que foi
  combinado.
- **UPDATE de uma linha que o par já apagou** recria a linha. É raro e fica registrado.
- **Relógios dos dois PCs:** a "última escrita vence" usa a hora do commit de cada PC. O Windows sincroniza por NTP, e
  diferenças de segundos só importam em edições quase simultâneas, que não ocorrem no seu uso.
- **Atraso:** com os dois ligados, uma mudança chega ao outro PC em 10 a 20 min, mais o tempo de sincronização do Drive.
  Da nuvem até o PC não-puxador: até ~1 h.
- **Dependência de um reinício do PostgreSQL no CNPq** (Fase 0, item 3).
