import os
import pytest
from sincronizador import lote

CH = os.urandom(32)


def _gravar(pasta, n=1000, seq=1, no="casa"):
    e = lote.EscritorLote(pasta, no, seq, CH)
    for i in range(n):
        e.escrever({"i": i, "txt": "acao e coracao " * 20})
    return e.fechar({"cabecalho": True})


def test_ida_e_volta(tmp_path):
    p = _gravar(tmp_path, 5000)
    itens = list(lote.ler_lote(p, CH))
    assert [x["i"] for x in itens[:-1]] == list(range(5000))
    assert itens[-1]["__rodape__"]["linhas"] == 5000 and itens[-1]["__seq__"] == 1
    assert lote.verificar_lote(p, CH)["__no__"] == "casa"
    assert not list(tmp_path.glob("*.tmp"))


def test_varios_blocos(tmp_path, monkeypatch):
    monkeypatch.setattr(lote, "TAM_BLOCO", 2000)
    p = _gravar(tmp_path, 300)
    assert lote.verificar_lote(p, CH)["__rodape__"]["linhas"] == 300


def test_truncado_e_detectado(tmp_path):
    p = _gravar(tmp_path, 500)
    dados = p.read_bytes()
    for corte in (10, len(dados) // 2, len(dados) - 3):
        p.write_bytes(dados[:corte])
        with pytest.raises(lote.LoteInvalido):
            lote.verificar_lote(p, CH)


def test_corrompido_ou_chave_errada(tmp_path):
    p = _gravar(tmp_path, 500)
    dados = bytearray(p.read_bytes())
    dados[len(dados) // 2] ^= 0xFF
    p.write_bytes(bytes(dados))
    with pytest.raises(lote.LoteInvalido):
        lote.verificar_lote(p, CH)
    p2 = _gravar(tmp_path, 10, seq=2)
    with pytest.raises(lote.LoteInvalido):
        lote.verificar_lote(p2, os.urandom(32))


def test_seq_interno_e_autenticado(tmp_path):
    p = _gravar(tmp_path, 10, seq=1)
    q = tmp_path / "casa_000000002.lote"
    p.rename(q)
    assert lote.verificar_lote(q, CH)["__seq__"] == 1  # o nome mente, o cabecalho autenticado nao
