"""Sobe clusters PostgreSQL temporários (initdb) para os testes. Nunca toca no banco real (porta 5432)."""
import os
import pathlib
import shutil
import subprocess
import time

import psycopg2

PG_BIN = pathlib.Path(os.environ.get("SINC_PG_BIN", r"E:\Programas\postgres18\bin"))
CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0


class ClusterTemp:
    def __init__(self, base: pathlib.Path, nome: str, porta: int):
        self.nome, self.porta = nome, porta
        self.dir = base / f"pg_{nome}"
        self.dsn = dict(host="localhost", port=porta, dbname="cnpq", user="postgres")
        self.log = base / f"pg_{nome}.log"

    def criar(self):
        if self.dir.exists():
            shutil.rmtree(self.dir, ignore_errors=True)
        subprocess.run([str(PG_BIN / "initdb.exe"), "-D", str(self.dir), "-U", "postgres", "--auth=trust",
                        "-E", "UTF8", "--locale=C"], check=True, capture_output=True, creationflags=CREATE_NO_WINDOW)
        with open(self.dir / "postgresql.conf", "a", encoding="utf-8") as f:
            f.write(f"""
port = {self.porta}
listen_addresses = 'localhost'
wal_level = logical
track_commit_timestamp = on
max_replication_slots = 10
max_wal_senders = 10
max_connections = 50
fsync = off
synchronous_commit = off
full_page_writes = off
shared_buffers = 64MB
""")
        self.iniciar()
        self.executar("postgres", "CREATE DATABASE cnpq")

    def iniciar(self):
        # stdout/stderr para DEVNULL: com pipe, o servidor filho herda o cano e o subprocess.run nunca retorna
        subprocess.run([str(PG_BIN / "pg_ctl.exe"), "-D", str(self.dir), "-l", str(self.log), "-w", "start"],
                       check=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       creationflags=CREATE_NO_WINDOW)

    def parar(self):
        subprocess.run([str(PG_BIN / "pg_ctl.exe"), "-D", str(self.dir), "-m", "fast", "-w", "stop"],
                       stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       creationflags=CREATE_NO_WINDOW)

    def destruir(self):
        self.parar()
        shutil.rmtree(self.dir, ignore_errors=True)

    def conectar(self, dbname="cnpq", autocommit=True):
        c = psycopg2.connect(**{**self.dsn, "dbname": dbname})
        c.autocommit = autocommit
        return c

    def executar(self, dbname, sql, params=None):
        c = self.conectar(dbname)
        try:
            with c.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall() if cur.description else None
        finally:
            c.close()
