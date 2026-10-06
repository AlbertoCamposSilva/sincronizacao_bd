"""Instala as tarefas no Agendador do Windows, no nível do usuário comum (sem administrador) e sem nenhuma janela.

O ciclo roda com pythonw.exe (subsistema "windows": nunca abre console) e a tarefa é marcada como oculta.
"""
import pathlib
import subprocess
import sys

RAIZ = pathlib.Path(__file__).resolve().parent.parent


def _pythonw() -> pathlib.Path:
    p = pathlib.Path(sys.executable)
    pw = p.with_name("pythonw.exe")
    return pw if pw.exists() else p


def script_powershell(minutos: int = 10) -> str:
    pw, raiz = _pythonw(), RAIZ
    return f"""$ErrorActionPreference = 'Stop'
$usuario = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$cfg = New-ScheduledTaskSettingsSet -Hidden -MultipleInstances IgnoreNew -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 2)
$princ = New-ScheduledTaskPrincipal -UserId $usuario -LogonType Interactive -RunLevel Limited

# Ciclo: a cada {minutos} min (indefinidamente) e ao fazer logon
$acao = New-ScheduledTaskAction -Execute '{pw}' -Argument '-m sincronizador ciclo' -WorkingDirectory '{raiz}'
$repete = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes {minutos}) -RepetitionDuration (New-TimeSpan -Days 9999)
$logon = New-ScheduledTaskTrigger -AtLogOn -User $usuario
try {{
    Register-ScheduledTask -TaskName 'SincBD_Ciclo' -Action $acao -Trigger @($repete, $logon) -Settings $cfg -Principal $princ -Force | Out-Null
}} catch {{
    # sem permissão para o gatilho de logon: fica só a repetição
    Register-ScheduledTask -TaskName 'SincBD_Ciclo' -Action $acao -Trigger $repete -Settings $cfg -Principal $princ -Force | Out-Null
}}

# Comparação semanal dos bancos
$acao2 = New-ScheduledTaskAction -Execute '{pw}' -Argument '-m sincronizador comparar' -WorkingDirectory '{raiz}'
$sem = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 04:00
Register-ScheduledTask -TaskName 'SincBD_Comparar' -Action $acao2 -Trigger $sem -Settings $cfg -Principal $princ -Force | Out-Null
'Tarefas SincBD_Ciclo e SincBD_Comparar instaladas.'
"""


def script_remocao() -> str:
    return ("Unregister-ScheduledTask -TaskName 'SincBD_Ciclo' -Confirm:$false -ErrorAction SilentlyContinue\n"
            "Unregister-ScheduledTask -TaskName 'SincBD_Comparar' -Confirm:$false -ErrorAction SilentlyContinue\n"
            "'Tarefas removidas.'\n")


def executar_powershell(script: str) -> str:
    r = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script],
                       capture_output=True, text=True, creationflags=0x08000000 if sys.platform == "win32" else 0)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or r.stdout.strip())
    return r.stdout.strip()
