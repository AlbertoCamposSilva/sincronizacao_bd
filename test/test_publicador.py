from sincronizador import lote, publicador


def _itens(cfg, caminho):
    return [x for x in lote.ler_lote(caminho, cfg.chave)]


def test_instalacao_protege_tabela_sem_pk(par):
    r = par.sql("casa", "select relreplident from pg_class where oid = 'public.sem_pk'::regclass")
    assert r[0][0] == "f"          # FULL
    r = par.sql("casa", "select relreplident from pg_class where oid = 'public.pessoa'::regclass")
    assert r[0][0] == "d"          # com PK, padrão


def test_publica_e_nao_repete(par):
    cfg = par.cfg["casa"]
    par.sql("casa", "insert into pessoa (nome, idade, dados, tags) values ('Ana', 30, '{\"a\": [1, 2]}', '{x,y}'), ('Beto', null, null, null)")
    par.sql("casa", "update pessoa set idade = 31 where nome = 'Ana'")
    par.sql("casa", "delete from pessoa where nome = 'Beto'")
    r = publicador.publicar(cfg)
    assert r["transacoes"] == 3 and r["mudancas"] == 4 and len(r["arquivos"]) == 1
    itens = _itens(cfg, r["arquivos"][0])
    tipos = [x.get("t") for x in itens if "__rodape__" not in x]
    assert tipos[0] == "H"
    assert tipos.count("B") == tipos.count("C") == 3
    ins = [x for x in itens if x.get("t") == "I"]
    assert ins[0]["novo"][1] == "Ana" and ins[0]["novo"][3] == '{"a": [1, 2]}' and ins[0]["novo"][4] == "{x,y}"
    assert ins[1]["novo"][2] is None                     # NULL preservado
    # segundo ciclo: nada novo (o slot foi confirmado)
    r2 = publicador.publicar(cfg)
    assert r2["arquivos"] == [] and r2["transacoes"] == 0


def test_numeracao_continua(par):
    cfg = par.cfg["casa"]
    par.sql("casa", "insert into pessoa (nome) values ('a')")
    a = publicador.publicar(cfg)["arquivos"]
    par.sql("casa", "insert into pessoa (nome) values ('b')")
    b = publicador.publicar(cfg)["arquivos"]
    assert a[0].endswith("casa_000000001.lote") and b[0].endswith("casa_000000002.lote")


def test_ddl_vira_registro(par):
    cfg = par.cfg["casa"]
    par.sql("casa", "alter table pessoa add column extra integer")
    r = publicador.publicar(cfg)
    itens = _itens(cfg, r["arquivos"][0])
    regs = [x for x in itens if x.get("t") == "I"]
    assert any("alter table pessoa add column extra" in str(x["novo"]) for x in regs)
