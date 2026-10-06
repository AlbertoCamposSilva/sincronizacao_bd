"""Instala a sincronização em um banco: schema de controle, gatilhos de evento, publicação, slot e origens."""
import pathlib

import psycopg2

SQL = pathlib.Path(__file__).parent / "sql" / "instalar.sql"


class PreRequisito(Exception):
    pass


def conectar(dsn: dict, autocommit=True):
    c = psycopg2.connect(**dsn)
    c.autocommit = autocommit
    return c


def verificar_prerequisitos(cur):
    cur.execute("select current_setting('wal_level'), current_setting('track_commit_timestamp'), "
                "current_setting('max_replication_slots')::int, current_setting('max_wal_senders')::int, "
                "(select rolsuper from pg_roles where rolname = current_user)")
    wal, ts, slots, senders, su = cur.fetchone()
    erros = []
    if wal != "logical":
        erros.append(f"wal_level = {wal} (precisa ser logical; ajuste o postgresql.conf e reinicie)")
    if ts != "on":
        erros.append("track_commit_timestamp = off (precisa ser on; ajuste o postgresql.conf e reinicie)")
    if slots < 2 or senders < 2:
        erros.append("max_replication_slots e max_wal_senders precisam ser pelo menos 2")
    if not su:
        erros.append("o usuário do banco precisa ser superusuário")
    if erros:
        raise PreRequisito("; ".join(erros))


def instalar(cfg) -> dict:
    """Idempotente. Devolve um resumo do que foi feito."""
    resumo = {"tabelas_sem_pk": [], "slot_criado": False, "publicacao_criada": False, "origens_criadas": []}
    conn = conectar(cfg.dsn)
    try:
        with conn.cursor() as cur:
            verificar_prerequisitos(cur)
            # proteção ANTES da publicação: tabela sem chave primária não aceita UPDATE/DELETE publicado
            cur.execute("""select c.oid::regclass::text from pg_class c join pg_namespace n on n.oid = c.relnamespace
                           where n.nspname = 'public' and c.relkind = 'r' and c.relreplident = 'd'
                             and not exists (select 1 from pg_index i where i.indrelid = c.oid and i.indisprimary)""")
            for (t,) in cur.fetchall():
                cur.execute(f"ALTER TABLE {t} REPLICA IDENTITY FULL")
                resumo["tabelas_sem_pk"].append(t)
            cur.execute(SQL.read_text(encoding="utf-8"))
            cur.execute("select 1 from pg_publication where pubname = %s", (cfg.publicacao,))
            if not cur.fetchone():
                cur.execute(f"CREATE PUBLICATION {cfg.publicacao} FOR TABLES IN SCHEMA public, TABLE sincronizacao.ddl_log, "
                            "sincronizacao.nuvem_ids, sincronizacao.nuvem_marcas")
                resumo["publicacao_criada"] = True
            for t in ("nuvem_ids", "nuvem_marcas"):        # publicações criadas antes da recepção da nuvem não as têm
                cur.execute("select 1 from pg_publication_tables where pubname = %s and schemaname = 'sincronizacao' and tablename = %s",
                            (cfg.publicacao, t))
                if not cur.fetchone():
                    cur.execute(f"ALTER PUBLICATION {cfg.publicacao} ADD TABLE sincronizacao.{t}")
            for o in cfg.origens:
                cur.execute("select 1 from pg_replication_origin where roname = %s", (o,))
                if not cur.fetchone():
                    cur.execute("select pg_replication_origin_create(%s)", (o,))
                    resumo["origens_criadas"].append(o)
            cur.execute("select 1 from pg_replication_slots where slot_name = %s", (cfg.slot,))
            if not cur.fetchone():
                cur.execute("select pg_create_logical_replication_slot(%s, 'pgoutput')", (cfg.slot,))
                resumo["slot_criado"] = True
            cur.execute("insert into sincronizacao.publicador (no) values (%s) on conflict do nothing", (cfg.no,))
    finally:
        conn.close()
    return resumo
