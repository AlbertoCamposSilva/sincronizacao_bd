# Guia Completo de Arquitetura e Operação: Integração BD Local & Remoto

> **Data da Última Atualização:** 11/09/2026  
> **Status:** 100% Concluído, Operacional e Validado em Ambos os Nós (CNPq 24/7 e PC Remoto/Casa).

---

## 1. Visão Geral da Arquitetura

O objetivo deste projeto é manter uma réplica contínua e assíncrona do Banco de Dados PostgreSQL do CNPq (que roda 24/7 na origem) em um computador pessoal/remoto (Casa), contornando as restrições reais de infraestrutura:
- **Firewall Unidirecional:** A rede do CNPq bloqueia qualquer conexão de entrada (*inbound*). Apenas conexões de saída (HTTPS / Nuvem) são permitidas.
- **PC Remoto Offline:** O computador de destino (casa) não opera 24 horas por dia.
- **Sem Privilégios de Administrador:** Toda a automação roda no nível do usuário comum (`Windows Task Scheduler`).

```
+-------------------------------------------------------------+
| FASE 1: CARGA INICIAL (CONCLUÍDA E VALIDADA)                |
| 1. Backup Físico a Frio de E:\PostgreSQL e E:\Programas     |
| 2. Compactação em 122 volumes de 1 GB (121.3 GB total)      |
| 3. Upload sincronizado no Google Drive:                     |
|    "N:\Meu Drive\PostgreSQL" (ou "J:\Meu Drive\PostgreSQL") |
| 4. Restauração em E:\ no PC Remoto (via Unidade Virtual)    |
| 5. PostgreSQL 18 iniciado com todas as 53 tabelas (155 GB)  |
+-------------------------------------------------------------+
                               |
                               v
+-------------------------------------------------------------+
| FASE 2: CDC CONTÍNUO VIA ONEDRIVE (ATIVO E HOMOLOGADO)      |
| [PostgreSQL CNPq 24/7]                                      |
|      │ (localhost:5432)                                     |
|      ▼                                                      |
| [cdc_publisher.py] (Agendado a cada 1 hora)                 |
|      │                                                      |
|      ▼                                                      |
| /pendentes/*.jsonl.gz (Sincronizado pelo OneDrive do CNPq)  |
|      │                                                      |
|      ▼                                                      |
| [cdc_subscriber.py] (No PC Remoto: Logon + a cada 15 min)  |
|      │                                                      |
|      ├──► Decodifica test_decoding (INSERT/UPDATE/DELETE)   |
|      ├──► Aplica mutações DML com isolamento via SAVEPOINT  |
|      ├──► Grava auditoria em _cdc_applied_batches           |
|      └──► Move lote para /processados/                      |
|                                                             |
| [cdc_cleaner.py] (No PC Remoto: Diariamente às 03:00)       |
|      └──► Expurga lotes em /processados/ com mais de 7 dias |
+-------------------------------------------------------------+
```

---

## 2. Histórico de Implementação e Lições Aprendidas (11/09/2026)

Durante a implantação inicial, foram superados desafios práticos essenciais que agora estão incorporados ao código:

1. **Paridade de Letras de Unidade (`E:\`) sem segundo HD físico:**
   - No PC Remoto, o disco `C:` possuía >500 GB livres, mas o drive `E:` era apenas um cartão MicroSD pequeno.
   - **Solução:** O MicroSD foi realocado e a letra `E:` foi mapeada diretamente para `C:\Disco_E` via comando nativo do Windows `subst E: C:\Disco_E` e persistida no Registro do Windows (`HKLM\...\DOS Devices`).
   - O arquivo `.env` foi vinculado com paridade total via **HardLink** (`New-Item -ItemType HardLink -Path "E:\Python\.env" -Target "C:\Python\.env"`), o que dispensa privilégios de Administrador por estarem no mesmo volume físico `C:`.

2. **Diretórios Estruturais Vazios do PostgreSQL (`FATAL: pg_notify`):**
   - Ao descompactar o backup original a frio, o PostgreSQL falhou na primeira inicialização acusando ausência do diretório `pg_notify`.
   - **Causa Raiz:** O script de backup anterior compactou apenas arquivos existentes. Pastas do PostgreSQL que ficam vazias quando desligado (`pg_notify`, `pg_tblspc`, `pg_commit_ts`, etc.) foram ignoradas no `.tar.gz`.
   - **Solução Aplicada:** 
     - [dist/gerar_backup_particionado.py](file:///c:/Users/silva/OneDrive%20-%20CNPq/Python/Projetos/Integra%C3%A7%C3%A3o%20BD%20Local%20%20&%20Remoto/dist/gerar_backup_particionado.py) foi atualizado para empacotar diretórios vazios como entradas de pasta e garantir a estrutura antes do backup.
     - [dist/restaurar_backup_particionado.py](file:///c:/Users/silva/OneDrive%20-%20CNPq/Python/Projetos/Integra%C3%A7%C3%A3o%20BD%20Local%20%20&%20Remoto/dist/restaurar_backup_particionado.py) recebeu salvaguarda pós-extração para recriar automaticamente todas as 12 pastas obrigatórias (`pg_commit_ts`, `pg_dynshmem`, `pg_logical/mappings`, `pg_logical/snapshots`, `pg_notify`, `pg_replslot`, `pg_serial`, `pg_snapshots`, `pg_stat_tmp`, `pg_tblspc`, `pg_twophase`, `pg_wal/archive_status`).

3. **Compatibilidade com o Plugin Nativo `test_decoding`:**
   - No CNPq, o slot lógico foi criado com `test_decoding` (formato texto padrão do PostgreSQL para Windows sem necessidade de DLL externa `wal2json`).
   - O [dist/cdc_subscriber.py](file:///c:/Users/silva/OneDrive%20-%20CNPq/Python/Projetos/Integra%C3%A7%C3%A3o%20BD%20Local%20%20&%20Remoto/dist/cdc_subscriber.py) foi equipado com um parser de expressões regulares para `INSERT`, `UPDATE` e `DELETE` em `test_decoding`.
   - Cada instrução foi encapsulada em `SAVEPOINT cdc_op` para garantir que falhas em linhas individuais não abortem o restante do lote.

4. **Validação Definitiva de Ponta a Ponta:**
   - Inserido registro na origem: `'Teste definitivo CDC CNPq -> Remoto via OneDrive'` (LSN: `194/912AD6D8`).
   - Consumido pelo `cdc_publisher.py` no CNPq -> Transmitido pelo OneDrive -> Processado pelo `cdc_subscriber.py` no PC Remoto.
   - Refletido e auditado na tabela `_cdc_applied_batches` com status `SUCCESS`.

---

## 3. Guia de Operação: Como Replicar do Zero

### A) No PC do CNPq (Origem - 24/7)

Se precisar reconfigurar o servidor de origem:

1. **Configurar o `postgresql.conf`:**
   - Local: `E:\PostgreSQL\data\postgresql.conf`
   - Adicione as linhas:
     ```ini
     wal_level = logical
     max_replication_slots = 5
     max_wal_senders = 5
     max_slot_wal_keep_size = 5120MB
     ```
   - Reinicie o serviço PostgreSQL.

2. **Criar a Publicação e o Slot de Replicação:**
   - Conecte no banco `cnpq` e execute (ou rode `dist/ativar_cdc_slot.py`):
     ```sql
     CREATE PUBLICATION cnpq_cdc_pub FOR ALL TABLES;
     SELECT pg_create_logical_replication_slot('cnpq_cdc_slot', 'test_decoding');
     ```

3. **Agendar o Extrator no Windows (`CNPq_CDC_Publisher`):**
   - Execute no PowerShell (ou via `dist/agendar_publisher.ps1`):
     ```powershell
     $action = New-ScheduledTaskAction -Execute 'C:\Python\.venvs\Codigos\.venv\Scripts\python.exe' -Argument '"D:\alberto.silva\OneDrive - CNPq\Python\Projetos\Integração BD Local  & Remoto\dist\cdc_publisher.py"'
     $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Hours 1)
     Register-ScheduledTask -TaskName 'CNPq_CDC_Publisher' -Action $action -Trigger $trigger -Description 'Extrator CDC do CNPq para OneDrive' -Force
     ```

---

### B) No PC Remoto (Casa - Destino)

Se precisar formatar ou configurar um novo computador pessoal:

1. **Garantir a Paridade de Unidade (`E:\`):**
   - Crie a pasta no disco principal `C:` e monte a unidade virtual:
     ```powershell
     New-Item -ItemType Directory -Force -Path "C:\Disco_E"
     subst E: C:\Disco_E
     ```
   - Para fixar a unidade permanentemente no boot do Windows (PowerShell como Administrador):
     ```powershell
     Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\DOS Devices" -Name "E:" -Value "\??\C:\Disco_E"
     ```

2. **Paridade do Arquivo `.env` (Sem precisar de Admin):**
   ```powershell
   New-Item -ItemType Directory -Force -Path "E:\Python"
   New-Item -ItemType HardLink -Path "E:\Python\.env" -Target "C:\Python\.env"
   ```

3. **Restaurar os Arquivos (Full Backup Particionado):**
   - O script detecta automaticamente o Google Drive em `N:\Meu Drive\PostgreSQL` ou `J:\Meu Drive\PostgreSQL` e checa espaço livre:
     ```powershell
     & "C:\Python\.venvs\Codigos\.venv\Scripts\python.exe" "dist\restaurar_backup_particionado.py"
     ```

4. **Scripts Auxiliares de Controle do PostgreSQL:**
   Para operar o banco manualmente ou verificar status, foram criados atalhos em `E:\Programas\postgres18\`:
   - **`activate.bat`**: Inicia o PostgreSQL 18 e exibe mensagem de confirmação.
   - **`deactivate.bat`**: Para o serviço de forma rápida e segura (`pg_ctl -m fast stop`).
   - **`status.bat`**: Verifica se o processo está ativo e o PID correspondente.

5. **Configurar as 3 Tarefas Agendadas no Windows:**
   Execute no PowerShell do PC Remoto:
   ```powershell
   # 1. Aplicador CDC (Logon + a cada 15 min)
   $actionSub = New-ScheduledTaskAction -Execute 'C:\Python\.venvs\Codigos\.venv\Scripts\python.exe' -Argument '"C:\Users\silva\OneDrive - CNPq\Python\Projetos\Integração BD Local  & Remoto\dist\cdc_subscriber.py"'
   $trigLogon = New-ScheduledTaskTrigger -AtLogOn
   $trigRep15 = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 15)
   Register-ScheduledTask -TaskName 'CNPq_CDC_Subscriber' -Action $actionSub -Trigger @($trigLogon, $trigRep15) -Description 'Aplicador CDC no PC Remoto' -Force

   # 2. Expurgo de lotes antigos (> 7 dias, Diariamente às 03:00)
   $actionCln = New-ScheduledTaskAction -Execute 'C:\Python\.venvs\Codigos\.venv\Scripts\python.exe' -Argument '"C:\Users\silva\OneDrive - CNPq\Python\Projetos\Integração BD Local  & Remoto\dist\cdc_cleaner.py"'
   $trigClean = New-ScheduledTaskTrigger -Daily -At 03:00
   Register-ScheduledTask -TaskName 'CNPq_CDC_Cleaner' -Action $actionCln -Trigger $trigClean -Description 'Expurgo de lotes CDC processados (> 7 dias)' -Force

   # 3. Inicialização automática do PostgreSQL no Logon
   $actionPG = New-ScheduledTaskAction -Execute 'E:\Programas\postgres18\bin\pg_ctl.exe' -Argument '-D "E:\PostgreSQL\data" -l "E:\PostgreSQL\logs\postgres.log" start'
   Register-ScheduledTask -TaskName 'CNPq_PostgreSQL_AutoStart' -Action $actionPG -Trigger $trigLogon -Description 'Inicia o PostgreSQL 18 no Logon' -Force
   ```

---

## 4. Regra de Ouro da Replicação: DML vs DDL

| Categoria | Comandos | Comportamento na Replicação |
| :--- | :--- | :--- |
| **DML (Dados)** | `INSERT`, `UPDATE`, `DELETE` | **100% Automático via CDC/OneDrive.** Lotes capturados no CNPq a cada 1 hora e aplicados no PC Remoto com idempotência. |
| **DDL (Estrutura)** | `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `CREATE INDEX` | **Manual / Por Design.** O motor PostgreSQL Logical Decoding não captura DDL. Caso uma nova tabela seja criada no CNPq, basta executar o mesmo `CREATE TABLE` no banco remoto para que as inserções subsequentes sejam aplicadas. |

---

## 5. Rotina de Teste Rápido (Checkup Periódico)

Se desejar testar a saúde do pipeline no futuro:

1. **No CNPq:** Crie e insira na tabela de teste:
   ```sql
   CREATE TABLE IF NOT EXISTS public._cdc_sync_test (id SERIAL PRIMARY KEY, mensagem TEXT);
   INSERT INTO public._cdc_sync_test (mensagem) VALUES ('Teste Checkup');
   ```
2. **Dispare o Extrator no CNPq:**
   ```powershell
   & "C:\Python\.venvs\Codigos\.venv\Scripts\python.exe" "dist\cdc_publisher.py"
   ```
3. **No PC Remoto:** Garanta a tabela no destino (`CREATE TABLE IF NOT EXISTS public._cdc_sync_test (id SERIAL PRIMARY KEY, mensagem TEXT);`) e execute o `cdc_subscriber.py`.
4. **Valide:** Consulte a linha no destino (`SELECT * FROM public._cdc_sync_test;`), confira a tabela `_cdc_applied_batches` e delete a tabela de teste em ambos com `DROP TABLE IF EXISTS public._cdc_sync_test;`.

---

## 6. Mapa dos Arquivos do Projeto

| Caminho | Descrição |
| :--- | :--- |
| `dist/gerar_backup_particionado.py` | Gera o backup inicial físico particionado em volumes de 1 GB (preservando diretórios vazios) |
| `dist/restaurar_backup_particionado.py` | Restaura os 122 volumes particionados de volta em `E:\` com validação de pastas estruturais |
| `dist/ativar_cdc_slot.py` | Script seguro de criação da publicação `cnpq_cdc_pub` e do slot `cnpq_cdc_slot` |
| `dist/agendar_publisher.ps1` | Script de agendamento da tarefa `CNPq_CDC_Publisher` (1 em 1 hora) no CNPq |
| `dist/cdc_publisher.py` | Extrator periódico que lê o slot do PostgreSQL e salva em `pendentes/` |
| `dist/cdc_subscriber.py` | Ingestor que consome os lotes do OneDrive e aplica no PostgreSQL remoto (com suporte a `test_decoding`) |
| `dist/cdc_cleaner.py` | Expurgo automático de lotes em `processados/` após 7 dias |
| `documentacao/SETUP_POSTGRESQL.sql` | Scripts DDL para publicação, slot e tabelas de controle |
| `documentacao/ARQUITETURA_E_GUIA.md` | Guia completo de arquitetura, lições aprendidas, histórico e replicação do zero |
| `pendentes/` | Pasta sincronizada onde ficam os lotes `.jsonl.gz` aguardando ingestão |
| `processados/` | Histórico dos lotes já aplicados no destino |
| `test/test_cdc_pipeline.py` | Testes automatizados do pipeline |
| `E:\Programas\postgres18\activate.bat` | Script batch para iniciar o PostgreSQL local com feedback visual |
| `E:\Programas\postgres18\deactivate.bat` | Script batch para parar o PostgreSQL local com segurança |
| `E:\Programas\postgres18\status.bat` | Script batch para checar o status do processo PostgreSQL local |
