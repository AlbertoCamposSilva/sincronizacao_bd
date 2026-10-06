"""Fidelidade dos dados: tipos, TOAST, colunas geradas, triggers, FK em cascata, mudança de chave, lotes grandes."""
import pytest

from sincronizador import aplicador, publicador


def ciclo(par, de="casa", para="cnpq"):
    publicador.publicar(par.cfg[de])
    return aplicador.aplicar(par.cfg[para])


def igual(par, consulta):
    a = par.sql("casa", consulta)
    b = par.sql("cnpq", consulta)
    assert a == b, (a, b)
    return a


TIPOS = """
create table tipos (
    id uuid primary key default gen_random_uuid(),
    t timestamptz, ts timestamp, d date, iv interval, n numeric(12,4), f double precision, r real,
    b bytea, bo boolean, j jsonb, ja json, ai integer[], at text[], tx text, v varchar(20), c char(3), i2 smallint, i8 bigint
);
"""


def test_tipos_variados_ida_e_volta(par):
    par.sql("casa", TIPOS)
    par.sql("casa", r"""insert into tipos (id, t, ts, d, iv, n, f, r, b, bo, j, ja, ai, at, tx, v, c, i2, i8) values
        ('11111111-1111-1111-1111-111111111111', '2026-10-06 08:15:30.123456-03', '2026-10-06 08:15:30', '2026-10-06',
         '3 days 04:05:06', 12345.6789, 3.141592653589793, 2.5, '\xdeadbeef', true, '{"a": {"b": [1, 2.5, null]}}',
         '{"x":  1}', '{1,2,NULL,4}', '{"a b","c,d","e\"f", NULL}', E'linha1\nlinha2\ttab \\ barra ''aspas'' "duplas"',
         'varchar', 'abc', -32768, 9223372036854775807),
        ('22222222-2222-2222-2222-222222222222', null, null, null, null, null, 'NaN', 'Infinity', '', false, null, null, null, null,
         '', null, null, null, null),
        ('33333333-3333-3333-3333-333333333333', now(), now(), current_date, '1 year 2 mons', -0.0001, -1e308, 1e-30, '\x00ff00',
         null, '[]', 'null', '{}', '{}', 'ação ç ã é ü 日本語 🙂 é', null, null, 0, 0)""")
    ciclo(par)
    igual(par, "select id, t, ts, d, iv::text, n, f::text, r::text, encode(b,'hex'), bo, j, ja::text, ai, at, tx, v, c, i2, i8 from tipos order by id")


def test_texto_grande_toast_e_update_sem_tocar(par):
    grande = "x" * 200_000
    par.sql("casa", "insert into pessoa (nome, obs) values ('Ana', %s)", (grande,))
    ciclo(par)
    assert igual(par, "select length(obs) from pessoa")[0][0] == 200_000
    # UPDATE que não mexe na coluna TOAST: a coluna não viaja (marca 'u') e não pode ser apagada no destino
    par.sql("casa", "update pessoa set idade = 33 where nome = 'Ana'")
    ciclo(par)
    r = igual(par, "select idade, length(obs) from pessoa")
    assert r == [(33, 200_000)]


def test_coluna_gerada_e_calculada_no_destino(par):
    par.sql("casa", "create table g (id integer primary key, a integer, dobro integer generated always as (a * 2) stored, "
                    "tsv tsvector generated always as (to_tsvector('portuguese', coalesce(txt, ''))) stored, txt text)")
    par.sql("casa", "insert into g (id, a, txt) values (1, 21, 'ação rápida de compras'), (2, 5, null)")
    par.sql("casa", "update g set a = 50 where id = 2")
    ciclo(par)
    igual(par, "select id, a, dobro, tsv::text from g order by id")


def test_trigger_nao_dispara_no_destino(par):
    par.sql("casa", "create table com_trigger (id integer primary key, v text, origem text)")
    # função com ';' dentro do corpo $$...$$ continua sendo UM comando e replica sozinha
    par.sql("casa", """create function marca() returns trigger language plpgsql as $$
                       begin new.origem := 'trigger'; return new; end $$""")
    par.sql("casa", "create trigger t_marca before insert or update on com_trigger for each row execute function marca()")
    par.sql("casa", "insert into com_trigger (id, v) values (1, 'x')")
    assert par.sql("casa", "select origem from com_trigger")[0][0] == "trigger"
    ciclo(par)
    assert par.sql("cnpq", "select count(*) from pg_trigger where tgname = 't_marca'")[0][0] == 1   # o trigger também foi replicado
    igual(par, "select id, v, origem from com_trigger")          # valor que a casa calculou; o trigger não rodou de novo
    par.sql("cnpq", "insert into com_trigger (id, v) values (2, 'y')")      # mas funciona normalmente para escritas locais
    assert par.sql("cnpq", "select origem from com_trigger where id = 2")[0][0] == "trigger"


def test_fk_com_cascata(par):
    par.sql("casa", "create table pai (id integer primary key, nome text)")
    par.sql("casa", "create table filho (id integer primary key, pai_id integer references pai(id) on delete cascade, v text)")
    par.sql("casa", "insert into pai values (1,'p1'), (2,'p2')")
    par.sql("casa", "insert into filho values (10,1,'a'), (11,1,'b'), (12,2,'c')")
    ciclo(par)
    par.sql("casa", "delete from pai where id = 1")               # apaga os filhos em cascata
    ciclo(par)
    assert igual(par, "select id from filho order by id") == [(12,)]
    assert igual(par, "select id from pai order by id") == [(2,)]


def test_mudanca_de_chave_primaria(par):
    par.sql("casa", "insert into pessoa (id, nome) values (100, 'Ana'), (101, 'Beto')")
    ciclo(par)
    par.sql("casa", "update pessoa set id = 500 where id = 100")
    ciclo(par)
    assert igual(par, "select id, nome from pessoa order by id") == [(101, "Beto"), (500, "Ana")]


def test_update_em_tabela_sem_pk(par):
    par.sql("casa", "insert into sem_pk values (1, 'a'), (2, 'b'), (2, 'b')")
    ciclo(par)
    par.sql("casa", "update sem_pk set b = 'novo' where a = 1")
    ciclo(par)
    assert igual(par, "select a, b from sem_pk order by a, b") == [(1, "novo"), (2, "b"), (2, "b")]


def test_lote_grande_com_rotacao(par):
    par.cfg["casa"].tam_max_lote = 200_000       # força vários arquivos
    par.sql("casa", "insert into pessoa (nome, obs) select 'n' || g, repeat('z', 200) from generate_series(1, 5000) g")
    par.sql("casa", "begin; update pessoa set idade = id % 90; delete from pessoa where id % 7 = 0; commit")
    r = publicador.publicar(par.cfg["casa"])
    assert len(r["arquivos"]) >= 2
    a = aplicador.aplicar(par.cfg["cnpq"])
    assert a["lotes"] == len(r["arquivos"]) and a["conflitos"] == 0
    igual(par, "select count(*), sum(idade), md5(string_agg(nome, ',' order by id)) from pessoa")


def test_transacao_unica_gigante_nao_e_dividida(par):
    par.cfg["casa"].tam_max_lote = 50_000
    par.sql("casa", "insert into pessoa (nome, obs) select 'n' || g, repeat('z', 300) from generate_series(1, 3000) g")
    r = publicador.publicar(par.cfg["casa"])
    assert len(r["arquivos"]) == 1 and r["transacoes"] == 1      # rotação só em fronteira de transação
    assert aplicador.aplicar(par.cfg["cnpq"])["mudancas"] == 3000


def test_vazamento_zero_entre_ciclos_rapidos(par):
    for i in range(5):
        par.sql("casa", "insert into pessoa (nome) values (%s)", (f"p{i}",))
        ciclo(par)
    assert igual(par, "select count(*) from pessoa") == [(5,)]


def test_pgvector_se_disponivel(par):
    disp = par.sql("casa", "select 1 from pg_available_extensions where name = 'vector'")
    if not disp:
        pytest.skip("extensão vector não disponível neste PostgreSQL")
    par.sql("casa", "create extension if not exists vector")
    par.sql("casa", "create table emb (id integer primary key, e vector(3))")
    par.sql("casa", "insert into emb values (1, '[1,2,3]'), (2, '[0.5,-1.25,1e-3]')")
    ciclo(par)
    igual(par, "select id, e::text from emb order by id")


def test_eh_multi_ignora_ponto_e_virgula_em_corpo_e_literal(par):
    f = lambda q: par.sql("casa", "select sincronizacao.eh_multi(%s)", (q,))[0][0]
    assert f("create table a (x int)") is False
    assert f("create table a (x int);") is False
    assert f("comment on table a is 'a; b'") is False
    assert f("create function f() returns int language sql as $$ select 1; $$") is False
    assert f("create function f() returns int language sql as $body$ select 1; select 2; $body$") is False
    assert f("create table a (x int); create table b (y int)") is True
    assert f("drop table a; drop table b;") is True


def test_ddl_manual_para_e_ddl_resolvido_retoma(par):
    par.sql("casa", "create table t_ctas as select 1 as a")
    par.sql("casa", "insert into pessoa (nome) values ('depois')")
    publicador.publicar(par.cfg["casa"])
    with pytest.raises(aplicador.FluxoParado) as e:
        aplicador.aplicar(par.cfg["cnpq"])
    import re
    ident = re.search(r"ddl-resolvido ([0-9a-f-]{36})", str(e.value)).group(1)
    # o usuário cria a tabela à mão no destino (e carrega o que precisa) e marca o DDL como resolvido
    par.sql("cnpq", "create table t_ctas as select 1 as a")
    par.sql("cnpq", "insert into sincronizacao.ddl_resolvidos (id) values (%s)", (ident,))
    # os INSERTs do CTAS que vieram junto agora entram na tabela criada à mão: aqui já existe a linha, então vira "divergente"/idempotente
    a = aplicador.aplicar(par.cfg["cnpq"])
    assert par.sql("cnpq", "select nome from pessoa") == [("depois",)]
