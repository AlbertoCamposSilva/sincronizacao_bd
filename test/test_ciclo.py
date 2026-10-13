import json
import os
import time

from sincronizador import avisos, ciclo, comparar, estado, publicador
from sincronizador.config import MARCADOR


def preparar_drive(par):
    pasta = par.cfg["casa"].pasta
    pasta.mkdir(parents=True, exist_ok=True)
    (pasta / MARCADOR).write_text("{}", encoding="utf-8")


def test_ciclo_completo_nos_dois_sentidos(par):
    preparar_drive(par)
    par.sql("casa", "insert into pessoa (nome) values ('Ana')")
    ciclo.executar_ciclo(par.cfg["casa"])
    r = ciclo.executar_ciclo(par.cfg["cnpq"])
    assert r["erro"] is None and r["parado"] is None and r["aplicado"]["transacoes"] == 1
    assert par.sql("cnpq", "select nome from pessoa") == [("Ana",)]
    par.sql("cnpq", "insert into pessoa (nome) values ('Beto')")
    ciclo.executar_ciclo(par.cfg["cnpq"])
    ciclo.executar_ciclo(par.cfg["casa"])
    assert sorted(x[0] for x in par.sql("casa", "select nome from pessoa")) == ["Ana", "Beto"]
    # estados publicados no Drive, cada nó escreve só o próprio
    e_casa = estado.ler(par.cfg["casa"].estado_proprio)
    e_cnpq = estado.ler(par.cfg["cnpq"].estado_proprio)
    assert e_casa["publicado_seq"] == 1 and e_casa["aplicou_do_par"] == 1
    assert e_cnpq["publicado_seq"] == 1 and e_cnpq["aplicou_do_par"] == 1
    assert e_casa["parado"] is None


def test_pausa_nao_faz_nada(par):
    preparar_drive(par)
    (par.cfg["casa"].pasta / "PAUSAR").write_text("x")
    par.sql("casa", "insert into pessoa (nome) values ('Ana')")
    r = ciclo.executar_ciclo(par.cfg["casa"])
    assert r["pausado"] and r["publicado"] is None
    (par.cfg["casa"].pasta / "PAUSAR").unlink()
    assert ciclo.executar_ciclo(par.cfg["casa"])["publicado"]["transacoes"] == 1   # nada se perdeu durante a pausa


def test_drive_ausente_nao_consome_o_slot(par):
    # sem o marcador na pasta, o ciclo não publica (não pode confirmar o slot sem ter onde gravar)
    par.sql("casa", "insert into pessoa (nome) values ('Ana')")
    r = ciclo.executar_ciclo(par.cfg["casa"])
    assert r["erro"] == "drive"
    preparar_drive(par)
    assert ciclo.executar_ciclo(par.cfg["casa"])["publicado"]["transacoes"] == 1


def test_fluxo_parado_e_estado(par):
    preparar_drive(par)
    par.sql("casa", "create table t_ctas as select 1 as a")
    ciclo.executar_ciclo(par.cfg["casa"])
    r = ciclo.executar_ciclo(par.cfg["cnpq"])
    assert r["parado"] and "manualmente" in r["parado"]
    e = estado.ler(par.cfg["cnpq"].estado_proprio)
    assert e["parado"]["desde"] and e["mensagem_parado"]
    desde1 = e["parado"]["desde"]
    ciclo.executar_ciclo(par.cfg["cnpq"])
    assert estado.ler(par.cfg["cnpq"].estado_proprio)["parado"]["desde"] == desde1   # mantém o início da condição


def test_poda_so_o_que_o_par_confirmou(par):
    preparar_drive(par)
    cfg = par.cfg["casa"]
    cfg.retencao_dias = 0
    par.sql("casa", "insert into pessoa (nome) values ('a')")
    ciclo.executar_ciclo(par.cfg["casa"])
    par.sql("casa", "insert into pessoa (nome) values ('b')")
    ciclo.executar_ciclo(par.cfg["casa"])
    antes = sorted(p.name for p in cfg.saida.glob("*.lote"))
    assert len(antes) == 2
    ciclo.executar_ciclo(par.cfg["casa"])                       # o cnpq ainda não confirmou nada: nada é podado
    assert len(list(cfg.saida.glob("*.lote"))) == 2
    ciclo.executar_ciclo(par.cfg["cnpq"])                       # cnpq aplica os 2 e registra no estado dele
    time.sleep(0.2)
    os.utime(cfg.saida / antes[0], (1, 1))
    os.utime(cfg.saida / antes[1], (1, 1))
    ciclo.executar_ciclo(par.cfg["casa"])
    assert list(cfg.saida.glob("*.lote")) == []


def test_comparar_bancos(par):
    preparar_drive(par)
    par.sql("casa", "insert into pessoa (nome) values ('Ana')")
    ciclo.executar_ciclo(par.cfg["casa"])
    ciclo.executar_ciclo(par.cfg["cnpq"])
    ciclo.executar_ciclo(par.cfg["casa"])
    comparar.executar(par.cfg["casa"])
    r = comparar.executar(par.cfg["cnpq"])
    assert r["conclusivo"] and r["diferencas"] == []
    # divergência provocada diretamente no banco (fora do fluxo): precisa ser vista
    par.sql("cnpq", "update pessoa set nome = 'Ana2'")
    comparar.executar(par.cfg["casa"])
    r = comparar.executar(par.cfg["cnpq"])
    assert r["conclusivo"] and [d["tabela"] for d in r["diferencas"]] == ["pessoa"]


def test_aviso_nao_repete_dentro_da_janela(par):
    cfg = par.cfg["casa"]
    assert avisos.avisar(cfg, "x", "t", "m") is True
    assert avisos.avisar(cfg, "x", "t", "m") is False
    avisos.limpar(cfg, "x")
    assert avisos.avisar(cfg, "x", "t", "m") is True


def test_script_das_tarefas_e_invisivel():
    from sincronizador import tarefas
    s = tarefas.script_powershell(10)
    assert "-Hidden" in s and "pythonw" in s.lower() and "-RunLevel Limited" in s and "IgnoreNew" in s
    assert "-m sincronizador ciclo" in s and "RepetitionInterval (New-TimeSpan -Minutes 10)" in s


def test_nenhum_subprocesso_abre_janela(monkeypatch):
    """Sob pythonw, subprocesso sem CREATE_NO_WINDOW (ex.: gcloud.cmd via google.auth/acs_toolbox, shell=True) pisca um CMD."""
    import subprocess
    import sys
    from sincronizador import invisivel
    assert os.environ.get("GOOGLE_CLOUD_PROJECT")          # google.auth não precisa perguntar ao gcloud
    if sys.platform != "win32":
        return
    vistos = []
    cria = subprocess._winapi.CreateProcess

    def espia(*a):
        vistos.append((a[5], a[8]))                      # (creationflags, startupinfo)
        return cria(*a)
    monkeypatch.setattr(subprocess._winapi, "CreateProcess", espia)
    subprocess.Popen("exit 0", shell=True).wait()
    subprocess.Popen(["cmd", "/c", "exit 0"], creationflags=0x00000200).wait()   # flags do chamador são somados
    assert all(fl & invisivel.CREATE_NO_WINDOW for fl, _ in vistos) and vistos[1][0] & 0x200
    assert all(si.wShowWindow == subprocess.SW_HIDE for _, si in vistos)


def test_google_auth_nao_roda_gcloud_para_descobrir_o_projeto():
    """google.auth._default chamava 'gcloud.cmd config get project' a cada leitura do cofre (cmd + Python do SDK, a cada ciclo)."""
    import sincronizador  # noqa: F401
    from google.auth import _cloud_sdk
    os.environ["GOOGLE_CLOUD_PROJECT"] = os.environ.get("GOOGLE_CLOUD_PROJECT") or "projeto-teste"
    assert _cloud_sdk.get_project_id() == os.environ["GOOGLE_CLOUD_PROJECT"]
