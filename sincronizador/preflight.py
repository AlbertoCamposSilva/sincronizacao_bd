"""Pré-voo SOMENTE LEITURA: o que, no banco real, atrapalharia a instalação da sincronização. Não altera nada."""
import pathlib

import psycopg2

from .config import MARCADOR


def auditar(cfg) -> list:
    """Devolve lista de (nivel, mensagem): 'ok', 'info', 'aviso' ou 'erro'."""
    r = []
    c = psycopg2.connect(**cfg.dsn)
    c.set_session(readonly=True, autocommit=True)
    cur = c.cursor()

    def q(sql, params=None):
        cur.execute(sql, params)
        return cur.fetchall()

    ver = int(q("show server_version_num")[0][0])
    r.append(("ok" if ver >= 160000 else "erro", f"PostgreSQL {ver // 10000}.{ver % 10000 // 100:02d}"
              + ("" if ver >= 160000 else " (precisa ser 16 ou mais: filtro de origem do pgoutput)")))
    wal, ts, slots, senders, su = q("select current_setting('wal_level'), current_setting('track_commit_timestamp'), "
                                    "current_setting('max_replication_slots')::int, current_setting('max_wal_senders')::int, "
                                    "(select rolsuper from pg_roles where rolname = current_user)")[0]
    r.append(("ok" if wal == "logical" else "aviso", f"wal_level = {wal}" + ("" if wal == "logical" else " (será ajustado na Fase 2; exige reinício)")))
    r.append(("ok" if ts == "on" else "aviso", f"track_commit_timestamp = {ts}" + ("" if ts == "on" else " (será ajustado na Fase 2; exige reinício)")))
    r.append(("ok" if slots >= 2 and senders >= 2 else "aviso", f"max_replication_slots = {slots}, max_wal_senders = {senders}"))
    r.append(("ok" if su else "erro", "usuário do banco é superusuário" if su else "usuário do banco NÃO é superusuário (necessário)"))

    sem_pk = [x[0] for x in q("""select c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public' and c.relkind = 'r' and not exists (select 1 from pg_index i where i.indrelid = c.oid and i.indisprimary)""")]
    r.append(("ok", "todas as tabelas de public têm chave primária") if not sem_pk else
             ("aviso", f"tabelas sem chave primária (usarão REPLICA IDENTITY FULL): {', '.join(sem_pk)}"))
    ident = q("""select c.relname, c.relreplident from pg_class c join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public' and c.relkind = 'r' and c.relreplident in ('n', 'i')""")
    if ident:
        r.append(("erro", "tabelas com REPLICA IDENTITY NOTHING/INDEX (incompatível): " + ", ".join(f"{a}({b})" for a, b in ident)))
    outros = q("""select n.nspname, count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace
        where c.relkind in ('r', 'p') and n.nspname not in ('public', 'pg_catalog', 'information_schema', 'sincronizacao', 'pg_toast')
        group by 1""")
    if outros:
        r.append(("aviso", "tabelas fora do schema public NÃO serão replicadas: " + ", ".join(f"{a} ({b})" for a, b in outros)))
    unl = [x[0] for x in q("select relname from pg_class where relpersistence = 'u' and relkind = 'r' and relnamespace = 'public'::regnamespace")]
    if unl:
        r.append(("aviso", "tabelas UNLOGGED não vão para o WAL, logo NÃO são replicadas: " + ", ".join(unl)))
    part = [x[0] for x in q("select relname from pg_class where relkind = 'p' and relnamespace = 'public'::regnamespace")]
    if part:
        r.append(("aviso", "tabelas particionadas (verificar caso a caso): " + ", ".join(part)))
    ger = q("""select c.relname, a.attname, a.attgenerated from pg_attribute a join pg_class c on c.oid = a.attrelid
        where c.relnamespace = 'public'::regnamespace and c.relkind = 'r' and a.attgenerated <> '' and not a.attisdropped""")
    if ger:
        r.append(("info", "colunas geradas (recalculadas no destino, não replicadas): " + ", ".join(f"{a}.{b}" for a, b, _ in ger)))
    tipos = q("""select format_type(a.atttypid, null), count(*) from pg_attribute a join pg_class c on c.oid = a.attrelid
        where c.relnamespace = 'public'::regnamespace and c.relkind = 'r' and a.attnum > 0 and not a.attisdropped group by 1 order by 2 desc""")
    r.append(("info", "tipos em uso: " + ", ".join(f"{t} ({n})" for t, n in tipos)))
    exoticos = [t for t, _ in tipos if t.split("(")[0].split("[")[0] in ("xml", "money", "point", "polygon", "path", "circle", "box", "line", "lseg")]
    if exoticos:
        r.append(("aviso", "tipos pouco comuns (testar a ida e volta): " + ", ".join(exoticos)))
    lo = q("select count(*) from pg_largeobject_metadata")[0][0]
    if lo:
        r.append(("erro", f"{lo} large object(s): a replicação lógica NÃO os replica"))
    old = q("select slot_name, plugin, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) from pg_replication_slots")
    for nome, plugin, ativo, ret in old:
        r.append(("aviso", f"slot existente '{nome}' ({plugin}, {'ativo' if ativo else 'inativo'}) retém {ret} de WAL"
                  + (" (antigo do CDC: remover na Fase 2)" if nome == "cnpq_cdc_slot" else "")))
    pubs = q("select pubname from pg_publication")
    if pubs:
        r.append(("info", "publicações existentes: " + ", ".join(p[0] for p in pubs)))
    ev = q("select evtname from pg_event_trigger")
    if ev:
        r.append(("info", "gatilhos de evento existentes: " + ", ".join(e[0] for e in ev)))
    ext = q("select extname, extversion from pg_extension order by 1")
    r.append(("info", "extensões: " + ", ".join(f"{a} {b}" for a, b in ext)))
    antigo = q("select to_regclass('public._cdc_applied_batches')")[0][0]
    if antigo:
        r.append(("info", "tabela do pipeline antigo public._cdc_applied_batches (será removida na Fase 2)"))
    tam = q("select pg_size_pretty(pg_database_size(current_database()))")[0][0]
    r.append(("info", f"tamanho do banco: {tam}"))
    c.close()

    pasta = pathlib.Path(cfg.pasta)
    r.append(("ok" if (pasta / MARCADOR).exists() else "aviso",
              f"pasta do Drive: {pasta}" + ("" if (pasta / MARCADOR).exists() else " (ainda sem marcador: criada por 'inicializar')")))
    return r
