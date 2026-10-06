import pytest

from sincronizador import aplicador, publicador, lote


def ciclo(par, de, para):
    """de publica, para aplica."""
    r = publicador.publicar(par.cfg[de])
    a = aplicador.aplicar(par.cfg[para])
    return r, a


def ids(par, no, tabela="pessoa", col="nome"):
    return sorted(x[0] for x in par.sql(no, f"select {col} from {tabela}") if x[0] is not None)


def test_ida_e_volta_sem_eco(par):
    par.sql("casa", "insert into pessoa (nome, idade, dados, tags) values ('Ana', 30, '{\"a\": [1]}', '{x,y}'), ('Beto', null, null, null)")
    par.sql("casa", "update pessoa set idade = 31 where nome = 'Ana'")
    r, a = ciclo(par, "casa", "cnpq")
    assert a["lotes"] == 1 and a["transacoes"] == 2 and a["conflitos"] == 0
    assert par.sql("cnpq", "select nome, idade, dados, tags from pessoa order by id") == [
        ("Ana", 31, {"a": [1]}, ["x", "y"]), ("Beto", None, None, None)]
    # o que o cnpq aplicou NÃO volta para a casa (sem eco)
    r2 = publicador.publicar(par.cfg["cnpq"])
    assert r2["transacoes"] == 0 and r2["arquivos"] == []
    # e o cnpq pode inserir depois, sem colidir ids (sequence acertada)
    par.sql("cnpq", "insert into pessoa (nome) values ('Cris')")
    assert par.sql("cnpq", "select max(id) from pessoa")[0][0] == 3
    r3, a3 = ciclo(par, "cnpq", "casa")
    assert r3["transacoes"] == 1 and a3["transacoes"] == 1
    assert ids(par, "casa") == ["Ana", "Beto", "Cris"]


def test_reaplicar_e_inofensivo(par):
    par.sql("casa", "insert into pessoa (nome) values ('Ana')")
    publicador.publicar(par.cfg["casa"])
    assert aplicador.aplicar(par.cfg["cnpq"])["lotes"] == 1
    assert aplicador.aplicar(par.cfg["cnpq"])["lotes"] == 0
    # apaga o registro de lote aplicado: o lote é relido, mas as transações já aplicadas são ignoradas pelo LSN
    par.sql("cnpq", "delete from sincronizacao.lotes_aplicados")
    a = aplicador.aplicar(par.cfg["cnpq"])
    assert a["lotes"] == 1 and a["transacoes"] == 0
    assert ids(par, "cnpq") == ["Ana"]


def test_delete_truncate_e_tabela_sem_pk(par):
    par.sql("casa", "insert into sem_pk values (1,'x'), (1,'x'), (2,'y')")
    par.sql("casa", "insert into pessoa (nome) values ('a'), ('b')")
    ciclo(par, "casa", "cnpq")
    par.sql("casa", "delete from sem_pk where a = 1 and ctid = (select min(ctid) from sem_pk where a = 1)")
    par.sql("casa", "delete from pessoa where nome = 'a'")
    ciclo(par, "casa", "cnpq")
    assert par.sql("cnpq", "select a, count(*) from sem_pk group by a order by a") == [(1, 1), (2, 1)]
    assert ids(par, "cnpq") == ["b"]
    par.sql("casa", "truncate pessoa")
    ciclo(par, "casa", "cnpq")
    assert ids(par, "cnpq") == []


def test_ddl_replica_na_ordem(par):
    par.sql("casa", "alter table pessoa add column extra integer default 7")
    par.sql("casa", "create table nova (id integer primary key, v text)")
    par.sql("casa", "insert into nova values (1, 'um')")
    par.sql("casa", "insert into pessoa (nome, extra) values ('Ana', 9)")
    par.sql("casa", "create table sem_chave_nova (x integer)")
    ciclo(par, "casa", "cnpq")
    assert par.sql("cnpq", "select nome, extra from pessoa") == [("Ana", 9)]
    assert par.sql("cnpq", "select * from nova") == [(1, "um")]
    # tabela nova sem PK ganhou REPLICA IDENTITY FULL nos dois lados
    for no in ("casa", "cnpq"):
        assert par.sql(no, "select relreplident from pg_class where oid = 'public.sem_chave_nova'::regclass")[0][0] == "f"
    par.sql("casa", "drop table nova")
    ciclo(par, "casa", "cnpq")
    assert par.sql("cnpq", "select to_regclass('public.nova')")[0][0] is None


def test_ddl_nao_replicavel_para_o_fluxo(par):
    par.sql("casa", "create table t_ctas as select 1 as a")
    r = publicador.publicar(par.cfg["casa"])
    with pytest.raises(aplicador.FluxoParado, match="manualmente"):
        aplicador.aplicar(par.cfg["cnpq"])
    # nada foi pulado: continua parado no mesmo ponto
    with pytest.raises(aplicador.FluxoParado):
        aplicador.aplicar(par.cfg["cnpq"])
    assert par.sql("cnpq", "select to_regclass('public.t_ctas')")[0][0] is None


def test_conflito_concorrente_ultima_escrita_vence(par):
    par.sql("casa", "insert into pessoa (nome, idade) values ('Ana', 30)")
    ciclo(par, "casa", "cnpq")
    par.sql("casa", "update pessoa set idade = 40 where nome = 'Ana'")
    par.sql("cnpq", "update pessoa set idade = 50 where nome = 'Ana'")      # depois: deve vencer
    publicador.publicar(par.cfg["casa"])
    publicador.publicar(par.cfg["cnpq"])
    a_cnpq = aplicador.aplicar(par.cfg["cnpq"])
    a_casa = aplicador.aplicar(par.cfg["casa"])
    for no in ("casa", "cnpq"):
        assert par.sql(no, "select idade from pessoa where nome = 'Ana'")[0][0] == 50
    assert a_cnpq["conflitos"] == 1 and a_casa["conflitos"] == 1
    c_cnpq = par.sql("cnpq", "select tipo, vencedor from sincronizacao.conflitos")
    c_casa = par.sql("casa", "select tipo, vencedor from sincronizacao.conflitos")
    assert c_cnpq == [("concorrente", "local")] and c_casa == [("concorrente", "recebido")]
    # o valor que perdeu fica registrado
    perdido = par.sql("cnpq", "select dados_recebidos->>'idade' from sincronizacao.conflitos")[0][0]
    assert perdido == "40"


def test_edicao_em_sequencia_nao_gera_conflito(par):
    par.sql("casa", "insert into pessoa (nome, idade) values ('Ana', 30)")
    ciclo(par, "casa", "cnpq")
    par.sql("cnpq", "update pessoa set idade = 41 where nome = 'Ana'")
    ciclo(par, "cnpq", "casa")
    par.sql("casa", "update pessoa set idade = 42 where nome = 'Ana'")
    ciclo(par, "casa", "cnpq")
    for no in ("casa", "cnpq"):
        assert par.sql(no, "select idade from pessoa")[0][0] == 42
        assert par.sql(no, "select count(*) from sincronizacao.conflitos")[0][0] == 0


def test_update_de_linha_ausente_recria_e_registra(par):
    par.sql("casa", "insert into pessoa (nome, idade) values ('Ana', 30)")
    ciclo(par, "casa", "cnpq")
    par.sql("cnpq", "delete from pessoa")                    # o cnpq apagou, a casa ainda não sabe
    par.sql("casa", "update pessoa set idade = 31 where nome = 'Ana'")
    ciclo(par, "casa", "cnpq")
    assert par.sql("cnpq", "select nome, idade from pessoa") == [("Ana", 31)]
    assert par.sql("cnpq", "select tipo from sincronizacao.conflitos") == [("ausente",)]


def test_tabela_so_insercao_renumera(par):
    par.sql("casa", "insert into log_custos (valor, quem) values (1.5, 'casa')")
    par.sql("cnpq", "insert into log_custos (valor, quem) values (2.5, 'cnpq')")
    publicador.publicar(par.cfg["casa"])
    publicador.publicar(par.cfg["cnpq"])
    aplicador.aplicar(par.cfg["cnpq"])
    aplicador.aplicar(par.cfg["casa"])
    for no in ("casa", "cnpq"):
        assert par.sql(no, "select quem from log_custos order by quem") == [("casa",), ("cnpq",)]     # nada se perdeu
    assert par.sql("casa", "select tipo from sincronizacao.conflitos") == [("renumerado",)]


def test_lacuna_aguarda_e_depois_aplica(par):
    par.sql("casa", "insert into pessoa (nome) values ('a')")
    r1 = publicador.publicar(par.cfg["casa"])
    par.sql("casa", "insert into pessoa (nome) values ('b')")
    r2 = publicador.publicar(par.cfg["casa"])
    primeiro = r1["arquivos"][0]
    import pathlib, shutil
    guardado = pathlib.Path(primeiro).with_suffix(".guardado")
    shutil.move(primeiro, guardado)
    a = aplicador.aplicar(par.cfg["cnpq"])
    assert a["lotes"] == 0 and a["aguardando"] == 1
    shutil.move(guardado, primeiro)
    a = aplicador.aplicar(par.cfg["cnpq"])
    assert a["lotes"] == 2 and ids(par, "cnpq") == ["a", "b"]


def test_lote_corrompido_nao_aplica_nada(par):
    par.sql("casa", "insert into pessoa (nome) values ('a')")
    r = publicador.publicar(par.cfg["casa"])
    import pathlib
    p = pathlib.Path(r["arquivos"][0])
    dados = bytearray(p.read_bytes())
    dados[len(dados) // 2] ^= 0xFF
    p.write_bytes(bytes(dados))
    with pytest.raises(aplicador.FluxoParado, match="inválido"):
        aplicador.aplicar(par.cfg["cnpq"])
    assert ids(par, "cnpq") == []


def test_transacao_com_varias_tabelas_e_atomica(par):
    c = par.conectar("casa")
    c.autocommit = False
    cur = c.cursor()
    cur.execute("insert into pessoa (nome) values ('a')")
    cur.execute("insert into log_custos (valor, quem) values (1, 'x')")
    c.rollback()
    c.close()
    par.sql("casa", "begin; insert into pessoa (nome) values ('b'); insert into log_custos (valor, quem) values (2, 'y'); commit")
    r, a = ciclo(par, "casa", "cnpq")
    assert a["transacoes"] == 1 and a["mudancas"] == 2
    assert ids(par, "cnpq") == ["b"] and ids(par, "cnpq", "log_custos", "quem") == ["y"]
