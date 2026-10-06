"""Comparação dos dois bancos: impressão digital por tabela, publicada no Drive e conferida contra a do par.

Tabelas pequenas/médias: contagem + md5 do conteúdo ordenado pela chave. Tabelas grandes: contagem + maior chave.
Só é conclusiva quando os dois lados estão em dia (cada um já aplicou tudo o que o outro publicou); senão avisa que há atraso.
"""
import datetime
import logging

import psycopg2
from psycopg2.extensions import quote_ident

from . import avisos, estado

log = logging.getLogger("sincronizador.comparar")
LIMITE_LINHAS = 2_000_000
LIMITE_BYTES = 2 * 1024 ** 3


def impressao(cfg) -> dict:
    conn = psycopg2.connect(**cfg.dsn)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""select c.oid, c.relname, c.reltuples::bigint, pg_total_relation_size(c.oid),
                          (select array_agg(a.attname order by array_position(i.indkey::int2[], a.attnum))
                             from pg_index i join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
                            where i.indrelid = c.oid and i.indisprimary)
                   from pg_class c join pg_namespace n on n.oid = c.relnamespace
                   where n.nspname = 'public' and c.relkind = 'r' order by c.relname""")
    out = {}
    for oid, nome, linhas, tam, pk in cur.fetchall():
        q = f"public.{quote_ident(nome, cur)}"
        if linhas <= LIMITE_LINHAS and tam <= LIMITE_BYTES:
            ordem = ", ".join(quote_ident(c, cur) for c in pk) if pk else "md5(t::text)"
            cur.execute(f"select count(*), md5(coalesce(string_agg(md5(t::text), '' order by {ordem}), '')) from {q} t")
            n, h = cur.fetchone()
            out[nome] = {"n": n, "md5": h}
        else:
            if pk:
                col = quote_ident(pk[0], cur)
                cur.execute(f"select count(*), max({col})::text from {q}")
                n, mx = cur.fetchone()
                out[nome] = {"n": n, "max": mx}
            else:
                cur.execute(f"select count(*) from {q}")
                out[nome] = {"n": cur.fetchone()[0]}
    conn.close()
    return out


def gerar(cfg) -> dict:
    atual = estado.ler(cfg.estado_proprio)
    dados = {"no": cfg.no, "gerado_em": estado.agora_iso(), "publicado_seq": atual.get("publicado_seq", 0),
             "aplicou_do_par": atual.get("aplicou_do_par", 0), "tabelas": impressao(cfg)}
    estado.gravar(cfg.pasta / cfg.no / "comparacao.json", dados)
    return dados


def conferir(cfg, minha: dict) -> dict:
    do_par = estado.ler(cfg.pasta / cfg.par / "comparacao.json")
    if not do_par:
        return {"conclusivo": False, "motivo": "o par ainda não publicou a impressão digital", "diferencas": []}
    # estão em dia se cada lado já aplicou tudo o que o outro tinha publicado quando tirou a impressão
    em_dia = (minha["aplicou_do_par"] == do_par["publicado_seq"] and do_par["aplicou_do_par"] == minha["publicado_seq"])
    difs = []
    for t in sorted(set(minha["tabelas"]) | set(do_par["tabelas"])):
        a, b = minha["tabelas"].get(t), do_par["tabelas"].get(t)
        if a is None or b is None:
            difs.append({"tabela": t, "problema": "existe só de um lado"})
        elif a != b:
            difs.append({"tabela": t, "problema": "conteúdo diferente", cfg.no: a, cfg.par: b})
    if not em_dia:
        return {"conclusivo": False, "motivo": "há lotes ainda não aplicados; tente de novo quando o atraso zerar", "diferencas": difs}
    return {"conclusivo": True, "diferencas": difs}


def executar(cfg) -> dict:
    minha = gerar(cfg)
    r = conferir(cfg, minha)
    anterior = estado.ler(cfg.estado_proprio)
    seguidas = anterior.get("divergencias_seguidas", 0)
    if r["conclusivo"] and r["diferencas"]:
        seguidas += 1
    elif r["conclusivo"]:
        seguidas = 0
    anterior.update({"divergencias_seguidas": seguidas,
                     "ultima_comparacao": {"em": estado.agora_iso(), "conclusivo": r["conclusivo"], "diferencas": len(r["diferencas"])}})
    estado.gravar(cfg.estado_proprio, anterior)
    if seguidas >= 2:
        avisos.avisar(cfg, "divergencia", "Sincronização BD: bancos DIVERGENTES",
                      "Tabelas diferentes: " + ", ".join(d["tabela"] for d in r["diferencas"][:8]))
    return r
