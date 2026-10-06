-- Instalacao da sincronizacao em UM banco (idempotente). Executar como superusuario.
-- Os nomes dos nos (casa/cnpq) NAO ficam gravados no banco: o banco e copiado fisicamente de um PC para o outro.

CREATE SCHEMA IF NOT EXISTS sincronizacao;

-- DDL capturado (replicado): o comando viaja na mesma transacao e na mesma ordem dos dados.
CREATE TABLE IF NOT EXISTS sincronizacao.ddl_log (
    id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    criado_em timestamptz NOT NULL DEFAULT now(),
    txid      bigint,
    tag       text,
    query     text NOT NULL,
    manual    boolean NOT NULL DEFAULT false,   -- true: o comando NAO pode ser reexecutado sozinho (script, funcao, CTAS...)
    motivo    text
);

-- Controle LOCAL (nao publicado)
CREATE TABLE IF NOT EXISTS sincronizacao.publicador (
    no         text PRIMARY KEY,
    ultimo_seq bigint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sincronizacao.lotes_aplicados (
    no_origem   text   NOT NULL,
    seq         bigint NOT NULL,
    arquivo     text   NOT NULL,
    aplicado_em timestamptz NOT NULL DEFAULT now(),
    transacoes  integer NOT NULL DEFAULT 0,
    mudancas    integer NOT NULL DEFAULT 0,
    conflitos   integer NOT NULL DEFAULT 0,
    PRIMARY KEY (no_origem, seq)
);

-- Ate que ponto das transacoes do par ja foi aplicado (e a hora de commit dele): usado na deteccao de conflito
CREATE TABLE IF NOT EXISTS sincronizacao.progresso (
    origem    text PRIMARY KEY,
    lsn       pg_lsn,
    commit_ts timestamptz
);

CREATE TABLE IF NOT EXISTS sincronizacao.conflitos (
    id                    bigserial PRIMARY KEY,
    detectado_em          timestamptz NOT NULL DEFAULT now(),
    no_origem             text,
    lote                  text,
    lsn                   text,
    tabela                text,
    chave                 jsonb,
    operacao              text,
    tipo                  text,    -- concorrente | ausente | divergente
    vencedor              text,    -- recebido | local
    hora_commit_recebido  timestamptz,
    hora_commit_local     timestamptz,
    dados_recebidos       jsonb,
    dados_locais          jsonb
);

-- Recepcao da nuvem (replicadas: o estado vai junto se o papel de "puxador" mudar de PC)
-- nuvem_ids: o que veio da nuvem (so isso pode ser apagado quando some de la). id_local difere de id_nuvem so nas tabelas so de insercao.
CREATE TABLE IF NOT EXISTS sincronizacao.nuvem_ids (
    tabela   text NOT NULL,
    id_nuvem text NOT NULL,
    id_local text NOT NULL,
    PRIMARY KEY (tabela, id_nuvem)
);
-- nuvem_marcas: ate qual id da nuvem ja foi importado (tabelas so de insercao)
CREATE TABLE IF NOT EXISTS sincronizacao.nuvem_marcas (
    tabela        text PRIMARY KEY,
    marca         bigint NOT NULL DEFAULT 0,
    atualizado_em timestamptz NOT NULL DEFAULT now()
);

-- Mais de um comando no mesmo envio? Ignora ';' dentro de corpos $$...$$ e de literais 'texto'.
CREATE OR REPLACE FUNCTION sincronizacao.eh_multi(q text) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $f$
DECLARE s text := q;
BEGIN
    s := regexp_replace(s, '\$([A-Za-z_0-9]*)\$.*?\$\1\$', '', 'g');
    s := regexp_replace(s, '''([^'']|'''')*''', '', 'g');
    s := regexp_replace(s, '--[^\n]*', '', 'g');
    RETURN position(';' IN rtrim(s, E'; \n\t\r')) > 0;
END $f$;

-- DDL que o usuario resolveu manualmente no par: o aplicador pula (ver: sincronizador ddl-resolvido)
CREATE TABLE IF NOT EXISTS sincronizacao.ddl_resolvidos (
    id           uuid PRIMARY KEY,
    resolvido_em timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------------------------
-- Captura de DDL
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION sincronizacao.registrar_ddl(p_tag text, p_query text, p_manual boolean, p_motivo text)
RETURNS void LANGUAGE plpgsql AS $f$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sincronizacao.ddl_log
                   WHERE txid = txid_current() AND md5(query) = md5(p_query)) THEN
        INSERT INTO sincronizacao.ddl_log (txid, tag, query, manual, motivo)
        VALUES (txid_current(), p_tag, p_query, p_manual, p_motivo);
    END IF;
END $f$;

CREATE OR REPLACE FUNCTION sincronizacao.capturar_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $f$
DECLARE
    r        record;
    q        text := current_query();
    tag      text;
    ignorar  boolean := false;
    manual   boolean := false;
    motivo   text;
    resto    text;
BEGIN
    IF coalesce(current_setting('sincronizacao.aplicando', true), '') = 'on'
       OR coalesce(current_setting('sincronizacao.interno', true), '') = 'on' THEN
        RETURN;
    END IF;
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        tag := r.command_tag;
        IF r.in_extension OR coalesce(r.schema_name, '') IN ('sincronizacao', 'pg_catalog', 'information_schema')
           OR coalesce(r.schema_name, '') LIKE 'pg_temp%' OR coalesce(r.schema_name, '') LIKE 'pg_toast%'
           OR r.command_tag IN ('CREATE EVENT TRIGGER', 'ALTER EVENT TRIGGER', 'CREATE PUBLICATION', 'ALTER PUBLICATION',
                                'CREATE SUBSCRIPTION', 'ALTER SUBSCRIPTION', 'CREATE SCHEMA') THEN
            ignorar := true;
        END IF;
    END LOOP;
    IF tag IS NULL OR ignorar THEN
        RETURN;
    END IF;

    -- so da para reexecutar no par um comando DDL simples e direto
    resto := regexp_replace(q, '^\s+', '');
    IF tag IN ('CREATE TABLE AS', 'SELECT INTO') THEN
        manual := true; motivo := 'CREATE TABLE AS / SELECT INTO carrega dados: crie a tabela nos dois lados e use a carga normal';
    ELSIF resto !~* '^(CREATE|ALTER|DROP|COMMENT)\s' THEN
        manual := true; motivo := 'DDL executado dentro de funcao, DO, script ou outro comando';
    ELSIF sincronizacao.eh_multi(q) THEN
        manual := true; motivo := 'varios comandos no mesmo envio (ou ponto e virgula dentro do texto)';
    END IF;
    PERFORM sincronizacao.registrar_ddl(tag, q, manual, motivo);

    -- protecao: tabela nova/alterada em public sem chave primaria replica a linha inteira (senao UPDATE/DELETE falham)
    PERFORM set_config('sincronizacao.interno', 'on', true);
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands()
             WHERE object_type = 'table' AND schema_name = 'public' LOOP
        IF EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = r.objid AND c.relkind = 'r' AND c.relreplident = 'd')
           AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = r.objid AND i.indisprimary) THEN
            EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', r.object_identity);
            PERFORM sincronizacao.registrar_ddl('ALTER TABLE',
                format('ALTER TABLE %s REPLICA IDENTITY FULL', r.object_identity), false, NULL);
            RAISE NOTICE 'sincronizacao: % nao tem chave primaria; usando REPLICA IDENTITY FULL. Crie uma chave primaria.', r.object_identity;
        END IF;
    END LOOP;
    PERFORM set_config('sincronizacao.interno', 'off', true);
END $f$;

CREATE OR REPLACE FUNCTION sincronizacao.capturar_drop() RETURNS event_trigger LANGUAGE plpgsql AS $f$
DECLARE
    r       record;
    q       text := current_query();
    achou   boolean := false;
    manual  boolean := false;
    motivo  text;
BEGIN
    IF coalesce(current_setting('sincronizacao.aplicando', true), '') = 'on'
       OR coalesce(current_setting('sincronizacao.interno', true), '') = 'on' THEN
        RETURN;
    END IF;
    FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
        IF r.original AND NOT coalesce(r.is_temporary, false)
           AND coalesce(r.schema_name, 'public') NOT IN ('sincronizacao', 'pg_catalog', 'information_schema')
           AND coalesce(r.schema_name, '') NOT LIKE 'pg_temp%' AND coalesce(r.schema_name, '') NOT LIKE 'pg_toast%'
           AND r.object_type NOT IN ('event trigger', 'publication', 'schema') THEN
            achou := true;
        END IF;
    END LOOP;
    IF NOT achou THEN
        RETURN;
    END IF;
    IF regexp_replace(q, '^\s+', '') !~* '^DROP\s' THEN
        manual := true; motivo := 'DROP executado dentro de funcao, DO, script ou outro comando';
    ELSIF sincronizacao.eh_multi(q) THEN
        manual := true; motivo := 'varios comandos no mesmo envio (ou ponto e virgula dentro do texto)';
    END IF;
    PERFORM sincronizacao.registrar_ddl('DROP', q, manual, motivo);
END $f$;

-- CREATE TABLE AS / SELECT INTO carregam dados DENTRO do comando: o aviso de "manual" precisa chegar ANTES dos dados,
-- por isso e registrado no INICIO do comando (o gatilho de fim chegaria depois dos INSERTs).
CREATE OR REPLACE FUNCTION sincronizacao.capturar_inicio() RETURNS event_trigger LANGUAGE plpgsql AS $f$
BEGIN
    IF coalesce(current_setting('sincronizacao.aplicando', true), '') = 'on'
       OR coalesce(current_setting('sincronizacao.interno', true), '') = 'on' THEN
        RETURN;
    END IF;
    IF current_query() ~* '^\s*create\s+((global|local)\s+)?(temp|temporary|unlogged)\s' THEN
        RETURN;
    END IF;
    PERFORM sincronizacao.registrar_ddl(tg_tag, current_query(), true,
        'CREATE TABLE AS / SELECT INTO carrega dados: crie a tabela nos dois lados e use a carga normal');
END $f$;

DROP EVENT TRIGGER IF EXISTS sincronizacao_ini;
CREATE EVENT TRIGGER sincronizacao_ini ON ddl_command_start WHEN TAG IN ('CREATE TABLE AS', 'SELECT INTO')
    EXECUTE FUNCTION sincronizacao.capturar_inicio();

DROP EVENT TRIGGER IF EXISTS sincronizacao_ddl;
CREATE EVENT TRIGGER sincronizacao_ddl ON ddl_command_end EXECUTE FUNCTION sincronizacao.capturar_ddl();
DROP EVENT TRIGGER IF EXISTS sincronizacao_drop;
CREATE EVENT TRIGGER sincronizacao_drop ON sql_drop EXECUTE FUNCTION sincronizacao.capturar_drop();
