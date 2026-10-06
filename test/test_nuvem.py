"""Recepção da nuvem: Job simulado (CSV.gz + manifesto no formato do contrato) -> mescla -> par.

O Job real vive no repositório do VAR; aqui `NuvemSimulada.produtor` faz o papel dele a partir de dicionários em memória.
"""
import csv
import dataclasses
import gzip
import hashlib
import io
import json

import pytest

from sincronizador import ciclo, nuvem
from sincronizador.nuvem import TransporteLocal
from test_ciclo import preparar_drive

ESQUEMA_VAR = """
select set_config('sincronizacao.aplicando', 'on', false);
CREATE TABLE public.rag_usuarios (id serial PRIMARY KEY, email text UNIQUE NOT NULL, nome text, cargos text[],
                                  senha_temporaria text);
CREATE TABLE public.rag_sessoes_chat (id integer PRIMARY KEY, usuario_email text NOT NULL REFERENCES rag_usuarios(email)
                                      ON DELETE CASCADE, titulo text);
CREATE TABLE public.llm_registros_custos (id bigserial PRIMARY KEY, modelo text, custo numeric);
"""
ADMIN = "alberto.silva@cnpq.br"


def csv_gz(colunas, linhas) -> bytes:
    buf = io.StringIO(newline="")
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(colunas)
    for l in linhas:
        w.writerow([nuvem.NULO if v is None else v for v in l])
    return gzip.compress(buf.getvalue().encode("utf-8"))


class NuvemSimulada:
    """Estado da 'nuvem' + o que o Job faria. `corromper` troca o conteúdo depois do sha256 (download corrompido)."""

    def __init__(self):
        self.usuarios = [(1, ADMIN, "Alberto", "{administrador,CNPq}"), (2, "ana@cnpq.br", "Ana", "{CNPq}")]
        self.sessoes = [(10, ADMIN, "conversa 1"), (11, "ana@cnpq.br", "conversa 2")]
        self.custos = [(1, "m1", "0.5"), (2, "m1", "0.7"), (3, "m2", "1.1")]
        self.corromper = False
        self.execucoes = []

    def produtor(self, prefixo, marcas, pasta):
        self.execucoes.append(dict(marcas))
        tabs = {}

        def grava(nome, colunas, linhas, modo, **extra):
            dados = csv_gz(colunas, linhas)
            sha = hashlib.sha256(dados).hexdigest()
            if self.corromper:
                dados = dados[:-5] + b"xxxxx"
            (pasta / f"{nome}.csv.gz").write_bytes(dados)
            tabs[nome] = {"arquivo": f"{nome}.csv.gz", "sha256": sha, "linhas": len(linhas), "colunas": colunas, "modo": modo,
                          "chave": "id", **extra}

        # senha_temporaria vem de propósito no CSV: o importador tem que ignorá-la (D4)
        grava("rag_usuarios", ["id", "email", "nome", "cargos", "senha_temporaria"],
              [(i, e, n, c, "SEGREDO") for i, e, n, c in self.usuarios], "retrato")
        grava("rag_sessoes_chat", ["id", "usuario_email", "titulo"], self.sessoes, "retrato", orfaos=3)
        if "llm_registros_custos" in marcas:
            novos = [c for c in self.custos if c[0] > marcas["llm_registros_custos"]]
            grava("llm_registros_custos", ["id", "modelo", "custo"], novos, "incremental", max_id=max(c[0] for c in self.custos),
                  linhas_total=len(self.custos))
        else:
            tabs["llm_registros_custos"] = {"arquivo": None, "modo": "incremental", "max_id": max(c[0] for c in self.custos),
                                            "linhas_total": len(self.custos)}
        manifesto = {"versao": nuvem.VERSAO_CONTRATO, "gerado_em": "2026-10-06T00:00:00Z", "execucao": prefixo, "tabelas": tabs}
        (pasta / "manifesto.json").write_text(json.dumps(manifesto), encoding="utf-8")


@pytest.fixture()
def nv(par, tmp_path):
    """Par de nós com as tabelas do VAR nos dois lados; o CNPq é o puxador."""
    for no in ("casa", "cnpq"):
        par.sql(no, ESQUEMA_VAR)
    par.cfg["cnpq"] = dataclasses.replace(par.cfg["cnpq"], puxar_nuvem=True, nuvem_intervalo_min=30)
    sim = NuvemSimulada()
    transp = TransporteLocal(tmp_path / "bucket", sim.produtor)
    return par, sim, transp, par.cfg["cnpq"]


def local(par, no, sql):
    return par.sql(no, sql)


def test_primeira_importacao_e_idempotencia(nv):
    par, sim, transp, cfg = nv
    par.sql("cnpq", "insert into rag_usuarios (id, email, nome, cargos) values (99, 'so_local@cnpq.br', 'Local', '{CNPq}')")
    r = nuvem.puxar(cfg, transp)
    assert "erro" not in r, r
    # retratos: tudo veio; a linha só-local fica (nada é apagado na primeira importação) e é reportada
    assert [x[0] for x in local(par, "cnpq", "select email from rag_usuarios order by id")] == [ADMIN, "ana@cnpq.br", "so_local@cnpq.br"]
    assert r["relatorio_primeira_importacao"]["rag_usuarios"]["linhas_so_locais"] == 1
    # D4: o código de acesso nunca entra
    assert local(par, "cnpq", "select count(*) from rag_usuarios where senha_temporaria is not null") == [(0,)]
    # tabela só de inserção sem marca: não importa nada e pede a marca
    assert local(par, "cnpq", "select count(*) from llm_registros_custos") == [(0,)]
    assert "llm_registros_custos" in r["marcas_pendentes"] and r["marcas_pendentes"]["llm_registros_custos"]["max_id_nuvem"] == 3
    assert r["alerta_admin"] is None
    # reimportar o mesmo retrato não escreve nada
    r2 = nuvem.puxar(cfg, transp, forcar=True)
    assert r2["tabelas"]["rag_usuarios"]["escritas"] == 0 and r2["tabelas"]["rag_sessoes_chat"]["escritas"] == 0
    assert sim.execucoes[0] == {}


def test_atualiza_so_o_que_mudou_e_exclui_so_o_que_veio_da_nuvem(nv):
    par, sim, transp, cfg = nv
    nuvem.puxar(cfg, transp)
    par.sql("cnpq", "insert into rag_usuarios (id, email, nome, cargos) values (99, 'so_local@cnpq.br', 'Local', '{CNPq}')")
    sim.usuarios[1] = (2, "ana@cnpq.br", "Ana Maria", "{CNPq}")             # mudou
    sim.sessoes = [s for s in sim.sessoes if s[0] != 11]                    # sumiu na nuvem
    sim.usuarios.append((3, "beto@cnpq.br", "Beto", "{publico}"))           # novo
    r = nuvem.puxar(cfg, transp, forcar=True)
    assert r["tabelas"]["rag_usuarios"]["escritas"] == 2                    # Ana (update) + Beto (insert)
    assert r["tabelas"]["rag_sessoes_chat"]["apagadas"] == 1
    assert local(par, "cnpq", "select nome from rag_usuarios where id = 2") == [("Ana Maria",)]
    assert local(par, "cnpq", "select count(*) from rag_sessoes_chat") == [(1,)]
    assert local(par, "cnpq", "select count(*) from rag_usuarios where id = 99") == [(1,)]     # só-local sobrevive


def test_so_insercao_com_marca_renumera_e_nao_duplica(nv):
    par, sim, transp, cfg = nv
    par.sql("cnpq", "insert into llm_registros_custos (modelo, custo) values ('local', 9)")    # id local 1
    nuvem.definir_marca(cfg, "llm_registros_custos", 1)                                         # a nuvem já tinha o id 1
    r = nuvem.puxar(cfg, transp)
    assert sim.execucoes[0] == {"llm_registros_custos": 1}
    assert r["tabelas"]["llm_registros_custos"]["escritas"] == 2                                # ids 2 e 3 da nuvem
    linhas = local(par, "cnpq", "select modelo from llm_registros_custos order by id")
    assert linhas == [("local",), ("m1",), ("m2",)]
    assert local(par, "cnpq", "select marca from sincronizacao.nuvem_marcas") == [(3,)]
    nuvem.puxar(cfg, transp, forcar=True)                                                       # nada novo: não duplica
    assert local(par, "cnpq", "select count(*) from llm_registros_custos") == [(3,)]
    sim.custos.append((4, "m3", "2"))
    nuvem.puxar(cfg, transp, forcar=True)
    assert local(par, "cnpq", "select count(*) from llm_registros_custos") == [(4,)]


def test_sha256_errado_nao_importa_nada(nv):
    par, sim, transp, cfg = nv
    sim.corromper = True
    r = nuvem.puxar(cfg, transp)
    assert "sha256" in r["erro"]
    assert local(par, "cnpq", "select count(*) from rag_usuarios") == [(0,)]
    assert nuvem.ler_estado(cfg)["falhando_desde"]


def test_freio_de_exclusao_em_massa(nv):
    par, sim, transp, cfg = nv
    sim.sessoes = [(1000 + i, ADMIN, f"s{i}") for i in range(30)]
    nuvem.puxar(cfg, transp)
    sim.sessoes = []                                                        # retrato vazio por bug no Job
    r = nuvem.puxar(cfg, transp, forcar=True)
    assert "travado por segurança" in r["erro"]
    assert local(par, "cnpq", "select count(*) from rag_sessoes_chat") == [(30,)]


def test_alerta_do_administrador_sem_corrigir(nv):
    par, sim, transp, cfg = nv
    sim.usuarios[0] = (1, ADMIN, "Alberto", "{administrador,CNPq,extra}")
    r = nuvem.puxar(cfg, transp)
    assert "cargos" in r["alerta_admin"]
    assert local(par, "cnpq", "select cargos from rag_usuarios where id = 1")[0][0] == ["administrador", "CNPq", "extra"]


def test_intervalo_e_desligada(nv):
    par, sim, transp, cfg = nv
    assert nuvem.puxar(dataclasses.replace(cfg, puxar_nuvem=False), transp) is None
    assert nuvem.puxar(cfg, transp) is not None
    assert nuvem.puxar(cfg, transp) is None                                 # dentro dos 30 min
    assert len(sim.execucoes) == 1


def test_nuvem_para_puxador_para_par_pelo_ciclo(nv, monkeypatch):
    """Caminho único: a escrita da importação (sem origem) é capturada pelo publicador e chega à casa."""
    par, sim, transp, cfg = nv
    preparar_drive(par)
    monkeypatch.setattr(nuvem, "TransporteGCP", lambda c: transp)
    ciclo.executar_ciclo(cfg)                     # CNPq: puxa a nuvem
    ciclo.executar_ciclo(cfg)                     # CNPq: publica o que a importação gravou
    r = ciclo.executar_ciclo(par.cfg["casa"])
    assert r["erro"] is None and r["parado"] is None, r
    assert [x[0] for x in par.sql("casa", "select email from rag_usuarios order by id")] == [ADMIN, "ana@cnpq.br"]
    assert par.sql("casa", "select count(*) from sincronizacao.nuvem_ids")[0][0] >= 4     # o estado da nuvem também viaja
    assert par.sql("casa", "select count(*) from rag_usuarios where senha_temporaria is not null") == [(0,)]
