"""Um ciclo de sincronização: publica, aplica, grava o estado, poda lotes antigos e emite avisos de problemas reais.

Roda pelo Agendador de Tarefas com pythonw.exe (sem janela). Nunca levanta exceção para fora: tudo vai para o log e para
o estado.json, para que o próximo ciclo tente de novo.
"""
import datetime
import json
import logging
import logging.handlers
import os
import pathlib
import sys
import time

import psycopg2

from . import aplicador, avisos, estado, lote, nuvem, publicador
from .config import MARCADOR

log = logging.getLogger("sincronizador")
VERSAO = "0.1.0"
WAL_ALERTA_BYTES = 5 * 1024 ** 3
PAR_SILENCIOSO_H = 48


def configurar_log(cfg):
    pathlib.Path(cfg.pasta_logs).mkdir(parents=True, exist_ok=True)
    raiz = logging.getLogger("sincronizador")
    if any(getattr(h, "_sinc", False) for h in raiz.handlers):
        return
    raiz.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    h = logging.handlers.RotatingFileHandler(pathlib.Path(cfg.pasta_logs) / f"{cfg.no}.log", maxBytes=5_000_000,
                                             backupCount=5, encoding="utf-8")
    h.setFormatter(fmt)
    h._sinc = True
    raiz.addHandler(h)
    if sys.stdout is not None and sys.stdout.isatty():     # no pythonw não há console
        s = logging.StreamHandler(sys.stdout)
        s.setFormatter(fmt)
        s._sinc = True
        raiz.addHandler(s)


class Trava:
    """Impede dois ciclos ao mesmo tempo no mesmo PC (o Agendador já usa IgnoreNew; isto é a segunda proteção)."""

    def __init__(self, cfg, validade_s=3 * 3600):
        self.caminho = pathlib.Path(cfg.pasta_logs) / f"{cfg.no}.lock"
        self.validade = validade_s
        self.ok = False

    def __enter__(self):
        self.caminho.parent.mkdir(parents=True, exist_ok=True)
        if self.caminho.exists() and time.time() - self.caminho.stat().st_mtime > self.validade:
            self.caminho.unlink(missing_ok=True)       # trava órfã (processo morto)
        try:
            fd = os.open(self.caminho, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            self.ok = True
        except FileExistsError:
            self.ok = False
        return self

    def __exit__(self, *a):
        if self.ok:
            self.caminho.unlink(missing_ok=True)


def pausado(cfg) -> bool:
    return (pathlib.Path(cfg.pasta) / "PAUSAR").exists() or (pathlib.Path(cfg.pasta_logs) / "PAUSAR").exists()


def _wal_retido(cfg) -> int | None:
    try:
        c = psycopg2.connect(**cfg.dsn)
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("select pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) from pg_replication_slots where slot_name = %s",
                        (cfg.slot,))
            r = cur.fetchone()
        c.close()
        return int(r[0]) if r and r[0] is not None else None
    except Exception:
        return None


def _conflitos_total(cfg) -> int | None:
    try:
        c = psycopg2.connect(**cfg.dsn)
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("select count(*) from sincronizacao.conflitos")
            r = cur.fetchone()[0]
        c.close()
        return r
    except Exception:
        return None


def _ultimos_seq(cfg) -> tuple[int, int]:
    """(publicado, aplicado do par) segundo o banco."""
    c = psycopg2.connect(**cfg.dsn)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute("select coalesce(max(ultimo_seq), 0) from sincronizacao.publicador where no = %s", (cfg.no,))
        pub = cur.fetchone()[0]
        cur.execute("select coalesce(max(seq), 0) from sincronizacao.lotes_aplicados where no_origem = %s", (cfg.par,))
        apl = cur.fetchone()[0]
    c.close()
    return pub, apl


def executar_ciclo(cfg) -> dict:
    configurar_log(cfg)
    saida = {"no": cfg.no, "publicado": None, "aplicado": None, "erro": None, "parado": None, "pausado": False}
    with Trava(cfg) as trava:
        if not trava.ok:
            log.info("outro ciclo em andamento; encerrando")
            saida["erro"] = "ocupado"
            return saida
        anterior = estado.ler(cfg.estado_proprio)
        if pausado(cfg):
            saida["pausado"] = True
            log.info("sincronização PAUSADA (arquivo PAUSAR presente)")
            estado.gravar(cfg.estado_proprio, {**anterior, "no": cfg.no, "atualizado_em": estado.agora_iso(), "pausado": True})
            return saida
        if not (pathlib.Path(cfg.pasta) / MARCADOR).exists():
            msg = f"Pasta do Drive não encontrada ou sem marcador: {cfg.pasta}. O Google Drive está montado e a pasta foi criada (inicializar)?"
            avisos.avisar(cfg, "drive", "Sincronização BD: Drive ausente", msg)
            saida["erro"] = "drive"
            return saida
        avisos.limpar(cfg, "drive")

        erro = None
        try:
            saida["publicado"] = publicador.publicar(cfg)
        except Exception as e:
            erro = f"publicar: {type(e).__name__}: {e}"
            log.exception("falha ao publicar")
        aguardando = None
        parado = None
        try:
            ap = aplicador.aplicar(cfg)
            saida["aplicado"] = ap
            aguardando = ap.get("aguardando")
        except aplicador.FluxoParado as e:
            parado = str(e)
            log.error("FLUXO PARADO: %s", e)
        except Exception as e:
            erro = (erro + " | " if erro else "") + f"aplicar: {type(e).__name__}: {e}"
            log.exception("falha ao aplicar")
        saida["erro"], saida["parado"] = erro, parado

        # --- nuvem -> local (só no puxador; com o fluxo PC<->PC parado não importa nada novo)
        if not parado:
            try:
                saida["nuvem"] = nuvem.puxar(cfg)
            except Exception:
                log.exception("falha inesperada na puxada da nuvem")

        # --- estado e poda
        try:
            pub, apl = _ultimos_seq(cfg)
        except Exception:
            pub, apl = anterior.get("publicado_seq", 0), anterior.get("aplicou_do_par", 0)
        do_par = estado.ler(cfg.estado_do_par)
        wal = _wal_retido(cfg)
        confl = _conflitos_total(cfg)
        erros_seguidos = (anterior.get("erros_seguidos", 0) + 1) if erro else 0
        novo = {
            "no": cfg.no, "versao": VERSAO, "atualizado_em": estado.agora_iso(), "pausado": False,
            "publicado_seq": pub, "aplicou_do_par": apl,
            "parado": estado.desde(anterior, "parado", parado, parado),
            "mensagem_parado": parado,
            "lacuna": estado.desde(anterior, "lacuna", aguardando, aguardando),
            "ultimo_erro": erro, "erros_seguidos": erros_seguidos,
            "wal_retido_bytes": wal, "conflitos_total": confl,
            "conflitos_avisados": anterior.get("conflitos_avisados", 0),
        }
        if cfg.puxar_nuvem:
            ne = nuvem.ler_estado(cfg)
            novo["nuvem"] = {k: ne.get(k) for k in ("ultima_ok", "falhando_desde", "ultimo_erro", "marcas_pendentes")}
        try:
            estado.podar_saida(cfg, int(do_par.get("aplicou_do_par", 0)))
        except Exception:
            log.exception("falha ao podar lotes antigos")

        # --- avisos (só problemas reais)
        if parado:
            avisos.avisar(cfg, "parado", "Sincronização BD PARADA", parado[:400])
        else:
            avisos.limpar(cfg, "parado")
        if erros_seguidos >= 2:
            avisos.avisar(cfg, "erro", "Sincronização BD com erro repetido", erro or "")
        elif not erro:
            avisos.limpar(cfg, "erro")
        if novo["lacuna"]:
            desde = datetime.datetime.fromisoformat(novo["lacuna"]["desde"])
            if datetime.datetime.now(datetime.timezone.utc) - desde > datetime.timedelta(hours=cfg.espera_lote_faltando_h):
                avisos.avisar(cfg, "lacuna", "Sincronização BD: lote faltando",
                              f"O lote {aguardando} do {cfg.par} não chegou ao Drive há mais de {cfg.espera_lote_faltando_h:g} h.")
        if wal is not None and wal > WAL_ALERTA_BYTES:
            avisos.avisar(cfg, "wal", "Sincronização BD: WAL acumulando",
                          f"O slot retém {wal / 1024 ** 3:.1f} GB de WAL. Verifique se o publicador está rodando.")
        if do_par.get("atualizado_em"):
            idade = datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(do_par["atualizado_em"])
            if idade > datetime.timedelta(hours=PAR_SILENCIOSO_H):
                avisos.avisar(cfg, "par", "Sincronização BD: o outro PC está em silêncio",
                              f"O {cfg.par} não atualiza o estado há {idade.total_seconds() / 3600:.0f} h.")
            else:
                avisos.limpar(cfg, "par")
        if confl is not None and confl > novo["conflitos_avisados"]:
            hoje = datetime.date.today().isoformat()
            if avisos.avisar(cfg, f"conflitos-{hoje}", "Sincronização BD: conflitos registrados",
                             f"{confl - novo['conflitos_avisados']} conflito(s) novo(s) em sincronizacao.conflitos."):
                novo["conflitos_avisados"] = confl
        estado.gravar(cfg.estado_proprio, novo)
        return saida
