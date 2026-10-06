"""Avisos ao usuário: só para problemas reais, sem janela de console e sem repetir o mesmo aviso toda hora.

Tudo vai para o log. A notificação do Windows (toast) é opcional: usa a biblioteca windows-toasts se instalada
(pip install windows-toasts); não abre nenhuma janela e não exige administrador.
"""
import datetime
import json
import logging
import pathlib

log = logging.getLogger("sincronizador.avisos")
REPETIR_APOS_H = 6


def avisar(cfg, chave: str, titulo: str, mensagem: str) -> bool:
    """Devolve True se uma notificação foi emitida (False se repetida dentro da janela de silêncio)."""
    log.warning("AVISO [%s] %s - %s", chave, titulo, mensagem)
    registro = pathlib.Path(cfg.pasta_logs) / "avisos_enviados.json"
    try:
        enviados = json.loads(registro.read_text(encoding="utf-8")) if registro.exists() else {}
    except (OSError, ValueError):
        enviados = {}
    agora = datetime.datetime.now()
    ultimo = enviados.get(chave)
    if ultimo and agora - datetime.datetime.fromisoformat(ultimo) < datetime.timedelta(hours=REPETIR_APOS_H):
        return False
    enviados[chave] = agora.isoformat()
    try:
        registro.parent.mkdir(parents=True, exist_ok=True)
        registro.write_text(json.dumps(enviados), encoding="utf-8")
    except OSError:
        pass
    try:
        from windows_toasts import Toast, WindowsToaster
        t = Toast([titulo, mensagem[:240]])
        WindowsToaster("Sincronização BD").show_toast(t)
    except Exception:           # biblioteca ausente ou sistema sem toast: o log já tem o aviso
        pass
    return True


def limpar(cfg, chave: str):
    """Problema resolvido: o aviso pode voltar a ser emitido se reaparecer."""
    registro = pathlib.Path(cfg.pasta_logs) / "avisos_enviados.json"
    try:
        enviados = json.loads(registro.read_text(encoding="utf-8"))
        if enviados.pop(chave, None) is not None:
            registro.write_text(json.dumps(enviados), encoding="utf-8")
    except (OSError, ValueError):
        pass
