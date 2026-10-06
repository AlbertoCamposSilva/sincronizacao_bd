-- ==============================================================================
-- SETUP POSTGRESQL PARA REPLICAÇÃO CDC ASSÍNCRONA VIA ONEDRIVE
-- Projeto: Integração BD Local & Remoto
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. NO BANCO DE DADOS DE ORIGEM (PC DO CNPQ - 24/7)
-- ------------------------------------------------------------------------------

/*
IMPORTANTE: No arquivo 'postgresql.conf' do servidor PostgreSQL de origem,
certifique-se de que os seguintes parâmetros estejam configurados:

    wal_level = logical
    max_replication_slots = 5
    max_wal_senders = 5
    max_slot_wal_keep_size = 5120MB   -- Limite de segurança de 5GB para não lotar o disco

Após alterar o 'postgresql.conf', é necessário reiniciar o serviço do PostgreSQL.
*/

-- A) Criação da Publicação para TODAS as tabelas do banco
-- (Permite que o slot capture alterações de todas as tabelas atuais e futuras)
DO 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'cnpq_cdc_pub') THEN
        CREATE PUBLICATION cnpq_cdc_pub FOR ALL TABLES;
        RAISE NOTICE 'Publicação cnpq_cdc_pub criada com sucesso.';
    ELSE
        RAISE NOTICE 'Publicação cnpq_cdc_pub já existe.';
    END IF;
END ;

-- B) Criação do Slot de Replicação Lógica
-- Se você tiver a extensão/plugin 'wal2json' instalada na pasta /lib:
-- SELECT pg_create_logical_replication_slot('cnpq_cdc_slot', 'wal2json');

-- Se for utilizar o plugin nativo do PostgreSQL (test_decoding):
-- SELECT pg_create_logical_replication_slot('cnpq_cdc_slot', 'test_decoding');

-- Consulta para verificar o status e o consumo do slot:
SELECT 
    slot_name,
    plugin,
    slot_type,
    active,
    active_pid,
    restart_lsn,
    confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = 'cnpq_cdc_slot';


-- ------------------------------------------------------------------------------
-- 2. NO BANCO DE DADOS DE DESTINO (PC REMOTO / PESSOAL)
-- ------------------------------------------------------------------------------

-- Tabela de auditoria e controle de lotes processados (Idempotência)
CREATE TABLE IF NOT EXISTS _cdc_applied_batches (
    id SERIAL PRIMARY KEY,
    batch_file VARCHAR(255) NOT NULL UNIQUE,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    total_records INTEGER NOT NULL,
    min_lsn VARCHAR(64),
    max_lsn VARCHAR(64),
    status VARCHAR(32) DEFAULT 'SUCCESS'
);

-- Índice para busca rápida de lotes já aplicados
CREATE INDEX IF NOT EXISTS idx_cdc_applied_batches_file 
ON _cdc_applied_batches (batch_file);
