"""Publicador: lê as mudanças do slot lógico (pgoutput) e grava lotes criptografados na pasta de saída do nó.

Ordem que garante que nada se perde:  1) lote gravado, sincronizado em disco e renomeado  ->  2) sequencial gravado no
banco  ->  3) só então o LSN é confirmado ao slot (send_feedback). Se algo falhar antes do passo 3, o slot não avança e
o próximo ciclo regrava as mesmas transações; o aplicador do outro lado ignora o que já aplicou (progresso por LSN).
"""
import datetime
import logging
import select
import time

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

from . import lote, pgoutput

log = logging.getLogger("sincronizador.publicador")


def proximo_seq(cfg, cur) -> int:
    cur.execute("select coalesce(ultimo_seq, 0) from sincronizacao.publicador where no = %s", (cfg.no,))
    r = cur.fetchone()
    no_banco = r[0] if r else 0
    no_disco = 0
    if cfg.saida.exists():
        for p in cfg.saida.glob(f"{cfg.no}_*.lote"):
            try:
                no_disco = max(no_disco, int(p.stem.split("_")[1]))
            except (IndexError, ValueError):
                continue
    return max(no_banco, no_disco) + 1


def _peer_applied_ts(cfg, cur):
    cur.execute("select commit_ts from sincronizacao.progresso where origem = %s", (cfg.origem_do_par,))
    r = cur.fetchone()
    return r[0].isoformat() if r and r[0] else None


class _Saida:
    """Mantém o arquivo de lote aberto, reabre outro quando passa do tamanho (sempre em fronteira de transação)."""

    def __init__(self, cfg, seq_inicial, cabecalho_extra):
        self.cfg, self.seq, self.extra = cfg, seq_inicial, cabecalho_extra
        self.esc = None
        self.tam = 0
        self.arquivos = []
        self.relacoes = {}
        self.lsn_ini = None
        self.lsn_fim = None
        self.transacoes = 0
        self.mudancas = 0
        self.tot_t = 0   # totais do ciclo (os de cima valem só para o arquivo aberto)
        self.tot_m = 0

    def abrir(self):
        self.esc = lote.EscritorLote(self.cfg.saida, self.cfg.no, self.seq, self.cfg.chave)
        self.tam = 0
        self.lsn_ini = None
        self.lsn_fim = None
        self.transacoes = 0
        self.mudancas = 0
        self.esc.escrever({"t": "H", "protocolo": lote.VERSAO_PROTOCOLO, "no": self.cfg.no, "seq": self.seq,
                           "criado_em": datetime.datetime.now(datetime.timezone.utc).isoformat(), **self.extra})
        for r in self.relacoes.values():  # o lote precisa ser autossuficiente: repete as relações já conhecidas
            self.esc.escrever(r)

    def escrever(self, reg):
        if self.esc is None:
            self.abrir()
        self.esc.escrever(reg)
        self.tam += 200 + sum(len(str(v)) for v in (reg.get("novo") or []) + (reg.get("velho") or []))

    def fechar(self):
        if self.esc is None:
            return
        caminho = self.esc.fechar({"lsn_inicio": self.lsn_ini, "lsn_fim": self.lsn_fim,
                                   "transacoes": self.transacoes, "mudancas": self.mudancas})
        self.arquivos.append(caminho)
        self.esc = None
        self.seq += 1


def publicar(cfg) -> dict:
    cfg.saida.mkdir(parents=True, exist_ok=True)
    sql = psycopg2.connect(**cfg.dsn)
    sql.autocommit = True
    cur_sql = sql.cursor()
    seq = proximo_seq(cfg, cur_sql)
    cur_sql.execute("select pg_current_wal_flush_lsn()")
    alvo = pgoutput.texto_para_lsn(cur_sql.fetchone()[0])
    saida = _Saida(cfg, seq, {"peer_applied_ts": _peer_applied_ts(cfg, cur_sql)})

    rep = psycopg2.connect(connection_factory=LogicalReplicationConnection, **cfg.dsn)
    rcur = rep.cursor()
    resumo = {"arquivos": [], "transacoes": 0, "mudancas": 0, "confirmado": None}
    try:
        rcur.start_replication(slot_name=cfg.slot, decode=False, options={
            "proto_version": "1", "publication_names": cfg.publicacao, "origin": "none"})
        em_tx = False
        ultimo_fim = None
        ultima_msg = time.monotonic()
        ultimo_feedback = time.monotonic()
        while True:
            msg = rcur.read_message()
            if msg is None:
                ocioso = time.monotonic() - ultima_msg
                if not em_tx and ocioso >= cfg.ocioso_s and (getattr(rcur, "wal_end", 0) >= alvo or ocioso >= 60):
                    break
                if time.monotonic() - ultimo_feedback > 10:  # mantém a conexão viva durante esperas longas
                    rcur.send_feedback(reply=False)
                    ultimo_feedback = time.monotonic()
                select.select([rcur], [], [], 0.5)
                continue
            ultima_msg = time.monotonic()
            m = pgoutput.decodificar(bytes(msg.payload))
            t = m["t"]
            if t == "B":
                em_tx = True
                saida.escrever({**m, "lsn_final": pgoutput.lsn_para_texto(m["lsn_final"])})
            elif t == "R":
                saida.relacoes[m["relid"]] = m
                saida.escrever(m)
            elif t in ("I", "U", "D", "T"):
                saida.escrever(m)
                saida.mudancas += 1
                saida.tot_m += 1
            elif t == "C":
                m = {**m, "lsn_commit": pgoutput.lsn_para_texto(m["lsn_commit"]), "lsn_fim": pgoutput.lsn_para_texto(m["lsn_fim"])}
                saida.escrever(m)
                saida.transacoes += 1
                saida.tot_t += 1
                em_tx = False
                ultimo_fim = m["lsn_fim"]
                saida.lsn_ini = saida.lsn_ini or m["lsn_commit"]
                saida.lsn_fim = m["lsn_fim"]
                if saida.tam >= cfg.tam_max_lote:
                    saida.fechar()
            # 'O' (origem), 'Y' (tipo) e 'M' não são necessários ao aplicador

        # Fim: confirma ate onde o slot foi esvaziado (alvo, se o servidor ja chegou la; senao ate o ultimo commit)
        if em_tx:
            raise RuntimeError("o slot parou no meio de uma transação (sem COMMIT); o ciclo será repetido")
        saida.fechar()
        resumo["transacoes"] = saida.tot_t
        resumo["mudancas"] = saida.tot_m
        if saida.arquivos:
            cur_sql.execute("insert into sincronizacao.publicador (no, ultimo_seq) values (%s, %s) "
                            "on conflict (no) do update set ultimo_seq = excluded.ultimo_seq", (cfg.no, saida.seq - 1))
        confirmar = None
        if not em_tx:
            if getattr(rcur, "wal_end", 0) >= alvo:
                confirmar = alvo if ultimo_fim is None else max(alvo, pgoutput.texto_para_lsn(ultimo_fim))
            elif ultimo_fim is not None:
                confirmar = pgoutput.texto_para_lsn(ultimo_fim)
        if confirmar:
            rcur.send_feedback(flush_lsn=confirmar, apply_lsn=confirmar, write_lsn=confirmar, force=True)
            time.sleep(0.3)  # deixa o servidor processar o feedback antes de fechar a conexão
            resumo["confirmado"] = pgoutput.lsn_para_texto(confirmar)
    except BaseException:
        if saida.esc is not None:
            saida.esc.abortar()
        raise
    finally:
        rep.close()
        sql.close()
    resumo["arquivos"] = [str(p) for p in saida.arquivos]
    return resumo
