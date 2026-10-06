"""Aplicador: lê os lotes do par (em ordem de sequencial) e os aplica no banco local.

Garantias:
  * idempotência: cada transação do par só é aplicada se o LSN dela for maior que o progresso gravado na origem de
    replicação (pg_replication_origin); reaplicar um lote é inofensivo;
  * sem eco: tudo é gravado com a origem "de_<par>", e o publicador local ignora o que tem origem (origin = none);
  * ordem: lotes estritamente em sequencial; faltando um número, espera;
  * qualquer erro inesperado PARA o fluxo (rollback da transação, nada é pulado) e é relatado;
  * conflitos (mesma linha alterada nos dois lados): última escrita vence, e tudo vai para sincronizacao.conflitos.
"""
import datetime
import json
import logging
import re

import psycopg2
from psycopg2.extensions import quote_ident

from . import lote

log = logging.getLogger("sincronizador.aplicador")

DDL_TABELA = ("sincronizacao", "ddl_log")


class FluxoParado(Exception):
    """Algo que o sincronizador não sabe resolver sozinho: o fluxo para e o problema é relatado."""


def _agora():
    return datetime.datetime.now(datetime.timezone.utc)


def _dt(txt):
    return datetime.datetime.fromisoformat(txt) if txt else None


class Aplicador:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = psycopg2.connect(**cfg.dsn)
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        self.tipos = {}          # (esquema, tabela) -> {coluna: tipo local}
        self.pk_local = {}
        cur = self.cur
        cur.execute("SET session_replication_role = replica")
        cur.execute("SELECT set_config('sincronizacao.aplicando', 'on', false)")
        cur.execute("SELECT pg_replication_origin_session_setup(%s)", (cfg.origem_do_par,))
        self.conn.commit()

    def fechar(self):
        try:
            self.conn.close()
        except Exception:
            pass

    # ------------------------------------------------------------------ utilidades
    def _qi(self, nome):
        return quote_ident(nome, self.cur)

    def _tabela(self, esq, tab):
        return f"{self._qi(esq)}.{self._qi(tab)}"

    def _tipos(self, esq, tab):
        k = (esq, tab)
        if k not in self.tipos:
            self.cur.execute("""select a.attname, format_type(a.atttypid, a.atttypmod) from pg_attribute a
                                where a.attrelid = to_regclass(%s) and a.attnum > 0 and not a.attisdropped""",
                             (self._tabela(esq, tab),))
            tp = dict(self.cur.fetchall())
            if not tp:
                raise FluxoParado(f"a tabela {esq}.{tab} não existe neste banco (falta aplicar o DDL?)")
            self.tipos[k] = tp
        return self.tipos[k]

    # ------------------------------------------------------------------ lotes
    def seq_aplicado(self):
        self.cur.execute("select coalesce(max(seq), 0) from sincronizacao.lotes_aplicados where no_origem = %s",
                         (self.cfg.par,))
        r = self.cur.fetchone()[0]
        self.conn.commit()
        return r

    def aplicar_pendentes(self) -> dict:
        cfg = self.cfg
        res = {"lotes": 0, "transacoes": 0, "mudancas": 0, "conflitos": 0, "aguardando": None, "arquivos": []}
        feitos = self.seq_aplicado()
        disponiveis = {}
        if cfg.saida_do_par.exists():
            for p in cfg.saida_do_par.glob(f"{cfg.par}_*.lote"):
                try:
                    disponiveis[int(p.stem.split("_")[1])] = p
                except (IndexError, ValueError):
                    continue
        while True:
            prox = feitos + 1
            if prox in disponiveis:
                st = self.aplicar_lote(disponiveis[prox], prox)
                for k in ("transacoes", "mudancas", "conflitos"):
                    res[k] += st[k]
                res["lotes"] += 1
                res["arquivos"].append(disponiveis[prox].name)
                feitos = prox
            else:
                if any(s > prox for s in disponiveis):
                    res["aguardando"] = prox    # lacuna: o Drive ainda não entregou este número
                break
        return res

    def aplicar_lote(self, caminho, seq) -> dict:
        cfg = self.cfg
        try:
            lote.verificar_lote(caminho, cfg.chave)     # integridade ANTES de tocar no banco
        except lote.LoteInvalido as e:
            raise FluxoParado(f"lote {caminho.name} inválido ({e}); aguardando o Drive terminar de sincronizar ou reenviar") from e
        st = {"transacoes": 0, "mudancas": 0, "conflitos": 0}
        ctx = {"relacoes": {}, "peer_visto": None, "tabelas_tocadas": set(), "lote": caminho.name,
               "seq": seq, "txn": None}
        self.cur.execute("select pg_replication_origin_progress(%s, true)", (cfg.origem_do_par,))
        progresso = self.cur.fetchone()[0]
        self.conn.commit()
        progresso_int = _lsn_int(progresso) if progresso else 0
        pular = False
        for reg in lote.ler_lote(caminho, cfg.chave):
            if "__rodape__" in reg:
                break
            t = reg["t"]
            try:
                if t == "H":
                    if reg.get("protocolo", 1) > lote.VERSAO_PROTOCOLO:
                        raise FluxoParado(f"lote de protocolo {reg['protocolo']}: atualize o código do sincronizador")
                    ctx["peer_visto"] = _dt(reg.get("peer_applied_ts"))
                elif t == "R":
                    ctx["relacoes"][reg["relid"]] = reg
                elif t == "B":
                    pular = _lsn_int(reg["lsn_final"]) < progresso_int
                    ctx["txn"] = {"ts": _dt(reg["ts"]), "lsn": reg["lsn_final"], "mudancas": 0, "conflitos": 0}
                elif t == "C":
                    if not pular:
                        self._descarregar_inserts(ctx)
                        self._fechar_transacao(reg, ctx, st)
                    pular = False
                    ctx["txn"] = None
                elif not pular:
                    self._mudanca(reg, ctx)
                    ctx["txn"]["mudancas"] += 1
            except FluxoParado:
                self.conn.rollback()
                raise
            except Exception as e:
                self.conn.rollback()
                raise FluxoParado(f"erro ao aplicar {caminho.name} (transação {ctx['txn'] and ctx['txn']['lsn']}): "
                                  f"{type(e).__name__}: {e}") from e
        self._acertar_sequences(ctx["tabelas_tocadas"])
        self.cur.execute("""insert into sincronizacao.lotes_aplicados (no_origem, seq, arquivo, transacoes, mudancas, conflitos)
                            values (%s, %s, %s, %s, %s, %s) on conflict do nothing""",
                         (cfg.par, seq, caminho.name, st["transacoes"], st["mudancas"], st["conflitos"]))
        self.conn.commit()
        log.info("lote %s aplicado: %s transações, %s mudanças, %s conflitos", caminho.name, st["transacoes"],
                 st["mudancas"], st["conflitos"])
        return st

    def _fechar_transacao(self, c, ctx, st):
        tx = ctx["txn"]
        lsn_fim = c["lsn_fim"]
        ts = c["ts"]
        self.cur.execute("select pg_replication_origin_xact_setup(%s::pg_lsn, %s::timestamptz)", (lsn_fim, ts))
        self.cur.execute("""insert into sincronizacao.progresso (origem, lsn, commit_ts) values (%s, %s::pg_lsn, %s)
                            on conflict (origem) do update set lsn = excluded.lsn, commit_ts = excluded.commit_ts""",
                         (self.cfg.origem_do_par, lsn_fim, ts))
        self.conn.commit()
        st["transacoes"] += 1
        st["mudancas"] += tx["mudancas"]
        st["conflitos"] += tx["conflitos"]

    # ------------------------------------------------------------------ mudanças
    def _mudanca(self, m, ctx):
        t = m["t"]
        if t == "T":
            self._descarregar_inserts(ctx)
            nomes = [self._tabela(ctx["relacoes"][r]["esquema"], ctx["relacoes"][r]["tabela"]) for r in m["relids"]]
            sql = "TRUNCATE " + ", ".join(nomes) + (" RESTART IDENTITY" if m["restart"] else "") + (" CASCADE" if m["cascade"] else "")
            self.cur.execute(sql)
            return
        rel = ctx["relacoes"].get(m["relid"])
        if rel is None:
            raise FluxoParado(f"mudança para relação desconhecida (relid {m['relid']}) no lote {ctx['lote']}")
        esq, tab = rel["esquema"], rel["tabela"]
        if (esq, tab) == DDL_TABELA and m["t"] == "I":
            self._descarregar_inserts(ctx)
            return self._ddl(rel, m)
        cols = [c["nome"] for c in rel["cols"]]
        chaves = [c["nome"] for c in rel["cols"] if c["chave"]]
        tp = self._tipos(esq, tab)
        faltando = [c for c in cols if c not in tp]
        if faltando:
            raise FluxoParado(f"{esq}.{tab}: colunas {faltando} existem no par mas não aqui (falta aplicar o DDL?)")
        ctx["tabelas_tocadas"].add((esq, tab, tuple(chaves)))
        novo = dict(zip(cols, m["novo"])) if m.get("novo") is not None else None
        velho = dict(zip(cols, m["velho"])) if m.get("velho") is not None else None
        if m["t"] == "I":
            pend = ctx.get("ins")
            if pend is not None and pend["chave"] != (esq, tab, tuple(cols)):
                self._descarregar_inserts(ctx)
                pend = None
            if pend is None:
                pend = ctx["ins"] = {"chave": (esq, tab, tuple(cols)), "chaves": chaves, "tp": tp, "linhas": []}
            pend["linhas"].append(novo)
            if len(pend["linhas"]) >= 500:
                self._descarregar_inserts(ctx)
            return
        self._descarregar_inserts(ctx)
        if m["t"] == "U":
            self._atualizar(esq, tab, cols, chaves, tp, novo, velho, ctx)
        elif m["t"] == "D":
            self._apagar(esq, tab, cols, chaves, tp, velho, ctx)

    def _where(self, cols, tp, valores, alias=""):
        """Condição por igualdade (usa índice). NULL vira IS NULL. 'IS NOT DISTINCT FROM' não usa índice: evitado de propósito."""
        partes, params = [], []
        for c in cols:
            v = valores[c]
            if v is None:
                partes.append(f"{alias}{self._qi(c)} IS NULL")
            else:
                partes.append(f"{alias}{self._qi(c)} = %s::{tp[c]}")
                params.append(_val(v))
        return " AND ".join(partes), params

    def _local(self, esq, tab, chaves, tp, valores):
        """(existe, ts_commit_local, origem_local, linha_json) da linha com esta chave."""
        w, p = self._where(chaves, tp, valores, "t.")
        self.cur.execute(f"""select (pg_xact_commit_timestamp_origin(t.xmin)).timestamp,
                                    (pg_xact_commit_timestamp_origin(t.xmin)).roident, to_jsonb(t)
                             from {self._tabela(esq, tab)} t where {w} limit 1""", p)
        r = self.cur.fetchone()
        return (False, None, None, None) if r is None else (True, r[0], r[1], r[2])

    def _concorrente(self, ts_local, origem_local, ctx):
        """Linha alterada LOCALMENTE depois do que o par já tinha visto de nós."""
        if ts_local is None or origem_local not in (0, None):
            return False
        visto = ctx["peer_visto"]
        return visto is None or ts_local > visto

    def _conflito(self, ctx, esq, tab, chave, op, tipo, vencedor, ts_loc, recebido, local):
        self.cur.execute("""insert into sincronizacao.conflitos (no_origem, lote, lsn, tabela, chave, operacao, tipo, vencedor,
                                hora_commit_recebido, hora_commit_local, dados_recebidos, dados_locais)
                            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                         (self.cfg.par, ctx["lote"], ctx["txn"]["lsn"], f"{esq}.{tab}", json.dumps(chave), op, tipo,
                          vencedor, ctx["txn"]["ts"], ts_loc, json.dumps(recebido, default=str),
                          json.dumps(local, default=str) if local is not None else None))
        ctx["txn"]["conflitos"] += 1

    def _inserir_linha(self, esq, tab, tp, valores, omitir=()):
        cols = [c for c in valores if c not in omitir and not isinstance(valores[c], dict)]
        marcas = ", ".join(f"%s::{tp[c]}" for c in cols)
        self.cur.execute(f"INSERT INTO {self._tabela(esq, tab)} ({', '.join(self._qi(c) for c in cols)}) VALUES ({marcas})",
                         [_val(valores[c]) for c in cols])

    def _inserir(self, esq, tab, cols, chaves, tp, novo, ctx):
        cfg = self.cfg
        marcas = ", ".join(f"%s::{tp[c]}" for c in cols)
        self.cur.execute(f"INSERT INTO {self._tabela(esq, tab)} ({', '.join(self._qi(c) for c in cols)}) VALUES ({marcas}) "
                         f"ON CONFLICT DO NOTHING RETURNING 1", [_val(novo[c]) for c in cols])
        if self.cur.fetchone():
            return
        # já existe uma linha com esta chave
        existe, ts_loc, orig_loc, json_loc = self._local(esq, tab, chaves, tp, novo)
        if not existe:
            raise FluxoParado(f"{esq}.{tab}: INSERT recusado por restrição de unicidade que não é a chave primária ({_chave(chaves, novo)})")
        w, p = self._where(cols, tp, novo)
        self.cur.execute(f"select 1 from {self._tabela(esq, tab)} where {w} limit 1", p)
        if self.cur.fetchone():
            return                                       # idêntica: reaplicação, nada a fazer
        if tab in cfg.somente_insercao:
            self._inserir_linha(esq, tab, tp, novo, omitir=chaves)    # id novo gerado pelo banco; nada se perde
            self._conflito(ctx, esq, tab, _chave(chaves, novo), "INSERT", "renumerado", "recebido", ts_loc, novo, json_loc)
            return
        self._atualizar_existente(esq, tab, cols, chaves, tp, novo, None, ctx, "INSERT", (existe, ts_loc, orig_loc, json_loc), "divergente")

    def _descarregar_inserts(self, ctx):
        """Grava de uma vez os INSERTs consecutivos da mesma tabela. Se algum já existir (unicidade), refaz um a um
        pelo caminho completo, que trata conflito, reaplicação e renumeração."""
        pend = ctx.get("ins")
        if not pend:
            return
        ctx["ins"] = None
        esq, tab, cols = pend["chave"]
        cols = list(cols)
        tp, chaves, linhas = pend["tp"], pend["chaves"], pend["linhas"]
        if tab in self.cfg.somente_insercao:
            for n in linhas:                       # tabelas só de inserção: sempre pelo caminho completo (renumeração)
                self._inserir(esq, tab, cols, chaves, tp, n, ctx)
            return
        modelo = "(" + ", ".join(f"%s::{tp[c]}" for c in cols) + ")"
        valores = ", ".join(self.cur.mogrify(modelo, [_val(n[c]) for c in cols]).decode("utf-8") for n in linhas)
        self.cur.execute("SAVEPOINT lote_ins")
        try:
            self.cur.execute(f"INSERT INTO {self._tabela(esq, tab)} ({', '.join(self._qi(c) for c in cols)}) VALUES {valores}")
            self.cur.execute("RELEASE SAVEPOINT lote_ins")
        except psycopg2.errors.UniqueViolation:
            self.cur.execute("ROLLBACK TO SAVEPOINT lote_ins")
            self.cur.execute("RELEASE SAVEPOINT lote_ins")
            for n in linhas:
                self._inserir(esq, tab, cols, chaves, tp, n, ctx)

    def _atualizar(self, esq, tab, cols, chaves, tp, novo, velho, ctx):
        chave_nova = {c: novo[c] for c in chaves}
        chave_busca = {c: velho[c] for c in chaves} if velho else chave_nova
        local = self._local(esq, tab, chaves, tp, chave_busca)
        if not local[0]:
            completo = all(not isinstance(v, dict) for v in novo.values())
            if completo:
                self._inserir_linha(esq, tab, tp, novo)
            self._conflito(ctx, esq, tab, _chave(chaves, chave_busca), "UPDATE", "ausente",
                           "recebido" if completo else "local", None, novo, None)
            return
        self._atualizar_existente(esq, tab, cols, chaves, tp, novo, chave_busca, ctx, "UPDATE", local, "concorrente")

    def _atualizar_existente(self, esq, tab, cols, chaves, tp, novo, chave_busca, ctx, op, local, tipo_div):
        existe, ts_loc, orig_loc, json_loc = local
        chave_busca = chave_busca or {c: novo[c] for c in chaves}
        conc = self._concorrente(ts_loc, orig_loc, ctx)
        if conc and ctx["txn"]["ts"] < ts_loc:
            self._conflito(ctx, esq, tab, _chave(chaves, chave_busca), op, "concorrente", "local", ts_loc, novo, json_loc)
            return                                       # a versão local é mais nova: vence
        alvo = [c for c in cols if not isinstance(novo[c], dict)]      # sem colunas TOAST não alteradas
        sets = ", ".join(f"{self._qi(c)} = %s::{tp[c]}" for c in alvo)
        dif = " OR ".join(f"{self._qi(c)} IS DISTINCT FROM %s::{tp[c]}" for c in alvo)
        vals = [_val(novo[c]) for c in alvo]
        w, pw = self._where(chaves, tp, chave_busca)
        self.cur.execute(f"UPDATE {self._tabela(esq, tab)} SET {sets} WHERE {w} AND ({dif})", vals + pw + vals)
        if self.cur.rowcount and (conc or tipo_div == "divergente"):
            self._conflito(ctx, esq, tab, _chave(chaves, chave_busca), op, "concorrente" if conc else tipo_div,
                           "recebido", ts_loc, novo, json_loc)

    def _apagar(self, esq, tab, cols, chaves, tp, velho, ctx):
        chave = {c: velho[c] for c in chaves}
        existe, ts_loc, orig_loc, json_loc = self._local(esq, tab, chaves, tp, chave)
        if not existe:
            self._conflito(ctx, esq, tab, _chave(chaves, chave), "DELETE", "ausente", "recebido", None, velho, None)
            return
        if self._concorrente(ts_loc, orig_loc, ctx) and ctx["txn"]["ts"] < ts_loc:
            self._conflito(ctx, esq, tab, _chave(chaves, chave), "DELETE", "concorrente", "local", ts_loc, velho, json_loc)
            return
        w, p = self._where(chaves, tp, chave)
        # ctid: com chave = todas as colunas (tabela sem PK) apaga UMA linha, como a replicação nativa
        self.cur.execute(f"DELETE FROM {self._tabela(esq, tab)} WHERE ctid = (SELECT ctid FROM {self._tabela(esq, tab)} WHERE {w} LIMIT 1)", p)
        if self._concorrente(ts_loc, orig_loc, ctx):
            self._conflito(ctx, esq, tab, _chave(chaves, chave), "DELETE", "concorrente", "recebido", ts_loc, velho, json_loc)

    # ------------------------------------------------------------------ DDL e sequences
    def _ddl(self, rel, m):
        reg = dict(zip([c["nome"] for c in rel["cols"]], m["novo"]))
        self.cur.execute("select 1 from sincronizacao.ddl_resolvidos where id = %s::uuid", (reg["id"],))
        if self.cur.fetchone():
            log.info("DDL %s já resolvido manualmente: ignorado", reg["id"])
            return
        if reg.get("manual") in ("t", "true", True):
            raise FluxoParado(f"DDL que precisa ser aplicado manualmente (depois rode: sincronizador ddl-resolvido {reg['id']}): "
                              f"{reg.get('motivo')} | {reg.get('query')}")
        query = reg["query"]
        try:
            if re.search(r"\bCONCURRENTLY\b", query, re.I):
                raise FluxoParado(f"DDL com CONCURRENTLY não roda dentro do fluxo: aplique manualmente e rode: sincronizador ddl-resolvido {reg['id']} | " + query)
            self.cur.execute(query)
        except FluxoParado:
            raise
        except Exception as e:
            raise FluxoParado(f"falha ao aplicar DDL do par ({type(e).__name__}: {e}): {query}") from e
        self.tipos.clear()
        self.cur.execute("""insert into sincronizacao.ddl_log (id, criado_em, txid, tag, query, manual, motivo)
                            values (%s::uuid, %s, %s, %s, %s, false, null) on conflict do nothing""",
                         (reg["id"], reg.get("criado_em"), reg.get("txid"), reg.get("tag"), query))

    def _acertar_sequences(self, tocadas):
        """Sequences não são replicadas: depois de cada lote, leva cada uma ao maior valor da coluna."""
        for esq, tab, chaves in tocadas:
            for c in chaves:
                self.cur.execute("select pg_get_serial_sequence(%s, %s)", (self._tabela(esq, tab), c))
                seq = self.cur.fetchone()[0]
                if not seq:
                    continue
                self.cur.execute(f"select max({self._qi(c)}) from {self._tabela(esq, tab)}")
                mx = self.cur.fetchone()[0]
                if mx is None:
                    continue
                self.cur.execute("select last_value, is_called from " + seq)
                ultimo, chamado = self.cur.fetchone()
                if (not chamado) or mx > ultimo:
                    self.cur.execute("select setval(%s, %s, true)", (seq, mx))
        self.conn.commit()


def _val(v):
    """Valor de coluna vindo do lote: str ou None (a marca TOAST não alterado nunca chega aqui)."""
    return v


def _chave(chaves, valores):
    return {c: valores[c] for c in chaves}


def _lsn_int(txt) -> int:
    alto, baixo = str(txt).split("/")
    return (int(alto, 16) << 32) | int(baixo, 16)


def aplicar(cfg) -> dict:
    ap = Aplicador(cfg)
    try:
        return ap.aplicar_pendentes()
    finally:
        ap.fechar()
