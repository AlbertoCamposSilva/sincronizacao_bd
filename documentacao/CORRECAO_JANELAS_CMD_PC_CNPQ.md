# Correção das janelas de CMD piscando: aplicar no PC do CNPq

Data: 13/10/2026. Vale para o PC do CNPq (e para qualquer PC que ainda tenha código anterior ao commit `39cb8b0`).

## O problema
A cada ciclo (10 min), o sincronizador lia o cofre pelo `google.auth`, que rodava `cmd /c gcloud.cmd config get project` e um Python do
Cloud SDK. Como o ciclo roda sem console (`pythonw`), o Windows abria uma janela de CMD que fechava logo depois.

## A correção (já está no GitHub)
O módulo `sincronizador/invisivel.py` (carregado pelo pacote) neutraliza a chamada ao `gcloud` e força `CREATE_NO_WINDOW` em qualquer
subprocesso. **Não é preciso reinstalar as tarefas agendadas**: o próximo ciclo já usa o código novo.

## Passo a passo no PC do CNPq (PowerShell, usuário comum, sem administrador)

1. Atualizar o código:
   ```powershell
   git -C "C:\Projetos\CNPq\importantes\sincronizacao_bd" pull origin main
   ```
   Se o `git pull` reclamar de alterações locais em arquivos versionados, **não** descarte nada: guarde com
   `git -C "C:\Projetos\CNPq\importantes\sincronizacao_bd" stash`, rode o pull e depois `stash pop`. O `config.local.toml` não é versionado.

2. Sincronizar as dependências:
   ```powershell
   uv sync --directory "C:\Projetos\CNPq\importantes\sincronizacao_bd"
   ```

3. Conferir que o código novo está presente:
   ```powershell
   Test-Path "C:\Projetos\CNPq\importantes\sincronizacao_bd\sincronizador\invisivel.py"
   ```
   Deve responder `True`. Se responder `False`, o passo 1 não funcionou: não siga.

4. Teste de invisibilidade (obrigatório). Dispara o ciclo e observa durante 30 s se o `pythonw` cria algum processo filho:
   ```powershell
   Start-ScheduledTask -TaskName SincBD_Ciclo
   $seen=@{}; $visto=$false; $fim=(Get-Date).AddSeconds(30)
   while((Get-Date) -lt $fim){
     $t=Get-CimInstance Win32_Process
     $raiz=@($t | ? { $_.Name -eq 'pythonw.exe' -and $_.CommandLine -match 'sincronizador' } | % ProcessId)
     if($raiz.Count){
       $visto=$true; $conj=@{}; foreach($r in $raiz){$conj[[int]$r]=1}; $mudou=$true
       while($mudou){ $mudou=$false
         foreach($p in $t){ if($conj[[int]$p.ParentProcessId] -and -not $conj[[int]$p.ProcessId]){
           $conj[[int]$p.ProcessId]=1; $mudou=$true
           if($p.Name -notmatch '^pythonw'){ $seen["$($p.Name) $($p.CommandLine)"]=1 } } } } }
     Start-Sleep -Milliseconds 80 }
   "pythonw do ciclo visto: $visto"; "processos filhos criados: $($seen.Count)"; $seen.Keys
   "resultado: " + (Get-ScheduledTaskInfo SincBD_Ciclo).LastTaskResult
   ```
   Esperado: `pythonw do ciclo visto: True`, `processos filhos criados: 0` e `resultado: 0`.
   Se aparecer qualquer `cmd.exe`, `gcloud` ou `powershell.exe`, ainda há janela piscando: anotar a linha e avisar.

5. Se o CNPq ainda **não** tem as tarefas instaladas (Etapa 6 de `IMPLANTACAO_PC_CNPQ.md`), siga o roteiro normal. O passo 4 deste arquivo
   já faz parte dele.

## Se ainda aparecer janela depois disso
Nem toda janela de CMD vem do sincronizador. Outras fontes comuns neste ambiente:
- Antigravity IDE (`cmd /c npx ...`, servidores MCP) e a extensão do Chrome/Claude (`chrome-native-host`).
- A tarefa "Importacoes Diarias CNPq" (projeto Importações Diárias), que roda `uv.exe run` visível, à meia-noite.

Para achar a origem, rodar um observador por alguns minutos e conferir a cadeia de processos pais de cada `cmd.exe` novo
(`Get-CimInstance Win32_Process`, campo `ParentProcessId`). Só é do sincronizador se o ancestral for `pythonw.exe ... -m sincronizador`.
