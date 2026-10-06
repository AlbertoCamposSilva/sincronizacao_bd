# sincronizador

Sincronização **bidirecional e assíncrona** dos bancos PostgreSQL **casa ⇄ CNPq**, sem conexão direta entre eles:
cada PC publica as próprias mudanças em lotes criptografados numa pasta do Google Drive (`G:\Meu Drive\SincronizacaoBD`)
e aplica os lotes do outro. Roda pelo Agendador de Tarefas com `pythonw.exe` (**nenhuma janela**, sem administrador).

Plano, decisões e roteiro de implantação: `C:\Projetos\CNPq\importantes\Integração BD Local  & Remoto\documentacao\`
(`PLANO_SINCRONIZACAO_BIDIRECIONAL.md` e `IMPLANTACAO_PC_CNPQ.md`).

## Como funciona
- **Captura**: slot lógico + publicação `pgoutput` (nativo do PostgreSQL). O DDL (`CREATE/ALTER/DROP`) é capturado por
  gatilhos de evento e viaja na mesma transação, na mesma ordem dos dados.
- **Sem eco**: o aplicador grava com uma *origem de replicação* (`de_casa` / `de_cnpq`); o publicador ignora o que tem origem.
- **Idempotente**: cada transação do par só é aplicada se o LSN dela passa do progresso da origem. Reaplicar é inofensivo.
- **Nada se perde**: lote gravado → sincronizado em disco → só então o slot é confirmado.
- **Conflitos** (mesma linha alterada nos dois lados): vence a última escrita (hora do commit); tudo vai para
  `sincronizacao.conflitos`, com os dados dos dois lados. Tabelas só de inserção (logs/custos) renumeram o `id`.
- **Sequences** são ajustadas depois de cada lote (o PostgreSQL não as replica).
- **Segurança**: lotes em AES-256-GCM (chave `SYNC_BD_CHAVE` no cofre `acs-toolbox`), gravação atômica e detecção de
  arquivo truncado/corrompido. Erro inesperado **para o fluxo** (nunca pula transação) e avisa.

## Comandos (`python -m sincronizador ...`)
| Comando | O que faz |
|---|---|
| `preflight` | verifica o banco (somente leitura): o que atrapalharia a instalação |
| `gerar-chave` | gera a chave dos lotes (guardar no cofre como `SYNC_BD_CHAVE`, igual nos dois PCs) |
| `inicializar` | cria a pasta/marcador no Drive e instala schema, gatilhos, publicação, slot e origens |
| `ciclo` | publica + aplica + estado + poda + avisos (é o que a tarefa agendada roda) |
| `status` | estado dos dois nós |
| `comparar` | impressão digital das tabelas e conferência com o par |
| `pausar` / `retomar` | pausa/retoma nos dois PCs (arquivo `PAUSAR` no Drive) |
| `ddl "<comando>"` | executa um DDL aqui e o enfileira para o par |
| `ddl-resolvido <id>` | marca um DDL "manual" como já aplicado à mão neste banco (retoma o fluxo) |
| `instalar-tarefas [--dry-run]` | cria as tarefas `SincBD_Ciclo` (10 min + logon) e `SincBD_Comparar` (semanal) |
| `remover-tarefas` | remove as tarefas |

O nó (`casa` ou `cnpq`) vem de `config.local.toml` (`no = "casa"`) ou da variável `SINC_NO`. Veja `config.example.toml`.

## Pré-requisitos do banco (Fase 2 do plano)
`wal_level = logical`, `track_commit_timestamp = on`, `max_replication_slots >= 2`, `max_wal_senders >= 2`
(`max_slot_wal_keep_size = 50GB` recomendado) e usuário superusuário. O `inicializar` confere e recusa se faltar algo.

## DDL que o fluxo não replica sozinho
`CREATE TABLE AS`/`SELECT INTO` (carregam dados), DDL dentro de função/`DO`/script com vários comandos e `CONCURRENTLY`.
O fluxo **para** no ponto, com a mensagem e o `id` do DDL. Aplique o comando à mão no outro PC e rode
`ddl-resolvido <id>` (ou, na origem, use `sincronizador ddl "<comando>"`, que já enfileira o comando certo). Ainda
sobre tabelas novas: se não tiverem chave primária, recebem `REPLICA IDENTITY FULL` automaticamente (crie uma PK).

## Testes
```
uv sync
uv run pytest                      # sobe 2 PostgreSQL temporários (portas 55432/55433); nunca toca no banco real
SINC_BENCH_N=50000 uv run pytest test/test_desempenho.py -s     # medição de desempenho
```
Os testes cobrem: formato do lote, ida e volta, sem eco, reaplicação, DDL, conflito, renumeração, lacuna, lote corrompido,
tipos (uuid, jsonb, arrays, bytea, vector, TOAST...), colunas geradas, triggers, FK em cascata, mudança de PK, lotes grandes
e as tarefas agendadas (invisíveis).

## Desempenho medido (PC da casa, bancos temporários)
INSERT ≈ 15–24 mil linhas/s; UPDATE ≈ 600–900 linhas/s (linha a linha). Cargas típicas levam segundos.

## Ainda não implementado (ver o plano)
- Recepção da nuvem (Fase 4): Job de exportação + puxada + mescla (decisão D2 em aberto, projeto Importações Diárias).
- `reparar-tabela` e `pausar-tabela` (conserto de uma tabela sem reclonar o banco).
- Aplicação em lote de UPDATE/DELETE (otimização).
- `windows-toasts` é opcional (`uv sync --extra avisos`); sem ele os avisos vão só para o log.

## Documentação
- `documentacao/PLANO_SINCRONIZACAO_BIDIRECIONAL.md`: arquitetura, regras de conflito, riscos e decisões.
- `documentacao/IMPLANTACAO_PC_CNPQ.md`: roteiro passo a passo da implantação (casa e CNPq) e checklist.
- `documentacao/legado/`: documentos do pipeline antigo (OneDrive), só como histórico.

A cópia editada dos documentos é a deste repositório; a pasta `Integração BD Local  & Remoto\documentacao` (fora do git) guarda
a versão anterior.
