"""Estado de cada nó (estado.json na pasta do Drive): cada nó escreve SÓ o próprio arquivo e lê o do par."""
import datetime
import json
import logging
import os
import pathlib

log = logging.getLogger("sincronizador.estado")


def agora_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def ler(caminho: pathlib.Path) -> dict:
    try:
        return json.loads(pathlib.Path(caminho).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def gravar(caminho: pathlib.Path, dados: dict) -> bool:
    """Gravação atômica. O Drive pode estar ocupado: falha vira aviso no log, nunca derruba o ciclo."""
    caminho = pathlib.Path(caminho)
    try:
        caminho.parent.mkdir(parents=True, exist_ok=True)
        tmp = caminho.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(dados, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp, caminho)
        return True
    except OSError as e:
        log.warning("não consegui gravar %s: %s", caminho, e)
        return False


def desde(anterior: dict, campo: str, ativo, chave=None):
    """Mantém a data de início de uma condição (parado, lacuna) enquanto ela persiste."""
    if not ativo:
        return None
    antigo = anterior.get(campo)
    if antigo and (chave is None or antigo.get("chave") == chave):
        return antigo
    return {"desde": agora_iso(), "chave": chave}


def podar_saida(cfg, aplicou_o_par: int) -> int:
    """Apaga lotes próprios que o par já confirmou aplicar E que têm mais de retencao_dias. Nunca apaga o não confirmado."""
    n = 0
    limite = datetime.datetime.now().timestamp() - cfg.retencao_dias * 86400
    if not cfg.saida.exists():
        return 0
    for p in cfg.saida.glob(f"{cfg.no}_*.lote"):
        try:
            seq = int(p.stem.split("_")[1])
            if seq <= aplicou_o_par and p.stat().st_mtime < limite:
                p.unlink()
                n += 1
        except (IndexError, ValueError, OSError):
            continue
    # sobras de gravações interrompidas
    for p in cfg.saida.glob("*.tmp"):
        try:
            if p.stat().st_mtime < datetime.datetime.now().timestamp() - 86400:
                p.unlink()
        except OSError:
            pass
    return n
