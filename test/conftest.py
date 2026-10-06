"""Fixtures: dois clusters PostgreSQL temporários (portas 55432/55433), um banco novo por teste, nós 'casa' e 'cnpq'."""
import itertools
import os
import pathlib
import sys
import tempfile

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from pg_temp import ClusterTemp  # noqa: E402
from sincronizador.config import Config  # noqa: E402
from sincronizador import lote, instalar  # noqa: E402

CONTADOR = itertools.count(1)

ESQUEMA = """
CREATE TABLE public.pessoa (
    id     serial PRIMARY KEY,
    nome   text,
    idade  integer,
    dados  jsonb,
    tags   text[],
    obs    text
);
CREATE TABLE public.sem_pk (a integer, b text);
CREATE TABLE public.log_custos (id bigserial PRIMARY KEY, valor numeric, quem text);
"""


@pytest.fixture(scope="session")
def clusters():
    base = pathlib.Path(tempfile.mkdtemp(prefix="sinc_test_"))
    a, b = ClusterTemp(base, "casa", 55432), ClusterTemp(base, "cnpq", 55433)
    try:
        a.criar()
        b.criar()
        yield a, b
    finally:
        a.destruir()
        b.destruir()


class Par:
    """Os dois nós de um teste: bancos, configs e atalhos."""

    def __init__(self, a, b, cfg_a, cfg_b, n):
        self.cluster = {"casa": a, "cnpq": b}
        self.cfg = {"casa": cfg_a, "cnpq": cfg_b}
        self.n = n

    def conectar(self, no):
        c = self.cluster[no].conectar(dbname=self.cfg[no].dsn["dbname"])
        return c

    def sql(self, no, comando, params=None):
        c = self.conectar(no)
        try:
            with c.cursor() as cur:
                cur.execute(comando, params)
                return cur.fetchall() if cur.description else None
        finally:
            c.close()


@pytest.fixture()
def par(clusters, tmp_path):
    a, b = clusters
    n = next(CONTADOR)
    chave = os.urandom(32)
    pasta = tmp_path / "SincronizacaoBD"
    cfgs = {}
    for no, cl in (("casa", a), ("cnpq", b)):
        dbname = f"t{n}"
        cl.executar("postgres", f"CREATE DATABASE {dbname}")
        cl.executar(dbname, ESQUEMA)
        dsn = {**cl.dsn, "dbname": dbname}
        cfgs[no] = Config(no=no, dsn=dsn, pasta=pasta, chave=chave, slot=f"sinc_slot_t{n}", publicacao="sinc_pub",
                          prefixo_origem=f"t{n}_de_", ocioso_s=1.0, somente_insercao=("log_custos",),
                          pasta_logs=tmp_path / "logs")
    for no in ("casa", "cnpq"):
        instalar.instalar(cfgs[no])
    yield Par(a, b, cfgs["casa"], cfgs["cnpq"], n)
    for no, cl in (("casa", a), ("cnpq", b)):
        cfg = cfgs[no]
        try:
            cl.executar("postgres", "select pg_drop_replication_slot(%s)", (cfg.slot,))
        except Exception:
            pass
        for o in cfg.origens:
            try:
                cl.executar("postgres", "select pg_replication_origin_drop(%s)", (o,))
            except Exception:
                pass
        try:
            cl.executar("postgres", f"DROP DATABASE IF EXISTS t{n} WITH (FORCE)")
        except Exception:
            pass
