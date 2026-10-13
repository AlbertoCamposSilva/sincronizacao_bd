"""Garante que nada do sincronizador (nem das bibliotecas que ele usa) abra janela de console no Windows.

Sob pythonw não há console; qualquer subprocesso (cmd.exe, gcloud.cmd, powershell...) criado sem CREATE_NO_WINDOW faz o
Windows abrir uma janela que pisca. Duas camadas:
  1. GOOGLE_CLOUD_PROJECT definido: o google.auth não chama mais "gcloud.cmd config get project".
  2. subprocess.Popen passa a usar CREATE_NO_WINDOW + STARTF_USESHOWWINDOW/SW_HIDE por padrão (cobre acs_toolbox com
     shell=True, bibliotecas de terceiros e qualquer código futuro). Quem já passou creationflags/startupinfo é respeitado.
"""
import os
import subprocess
import sys

CREATE_NO_WINDOW = 0x08000000
_PROJETO_PADRAO = "var-cnpq-2026"
_instalado = False


def aplicar() -> None:
    """Idempotente. Chamada ao importar o pacote."""
    global _instalado
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT",
                          os.environ.get("ACS_TOOLBOX_GCP_PROJECT") or _PROJETO_PADRAO)
    # google.auth._default chama _cloud_sdk.get_project_id() ao carregar o ADC, e isso roda "gcloud.cmd config get project"
    # (um cmd + um Python do Cloud SDK, a cada leitura do cofre), mesmo com GOOGLE_CLOUD_PROJECT definida. Não precisamos disso.
    try:
        from google.auth import _cloud_sdk
        _cloud_sdk.get_project_id = lambda: os.environ.get("GOOGLE_CLOUD_PROJECT")
    except Exception:       # google-auth ausente: nada a neutralizar
        pass
    if sys.platform != "win32" or _instalado:
        return
    _instalado = True
    original = subprocess.Popen.__init__

    def init(self, *args, **kwargs):
        kwargs["creationflags"] = kwargs.get("creationflags", 0) | CREATE_NO_WINDOW
        if kwargs.get("startupinfo") is None:
            si = subprocess.STARTUPINFO()
            si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            si.wShowWindow = subprocess.SW_HIDE
            kwargs["startupinfo"] = si
        original(self, *args, **kwargs)

    subprocess.Popen.__init__ = init


aplicar()
