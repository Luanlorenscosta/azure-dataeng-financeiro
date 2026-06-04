-- ================================================================
-- 01_create_tables.sql
-- Banco fonte: financeiro_source (PostgreSQL)
-- Simula o banco transacional de uma fintech
-- ================================================================

-- Remove tabelas se existirem (para recriar)
DROP TABLE IF EXISTS transacoes CASCADE;
DROP TABLE IF EXISTS contas CASCADE;
DROP TABLE IF EXISTS clientes CASCADE;
DROP TABLE IF EXISTS categorias_transacao CASCADE;
DROP TABLE IF EXISTS comerciantes CASCADE;

-- ----------------------------------------------------------------
-- Clientes
-- ----------------------------------------------------------------
CREATE TABLE clientes (
    id_cliente      SERIAL PRIMARY KEY,
    nome            VARCHAR(150) NOT NULL,
    cpf             VARCHAR(14)  NOT NULL UNIQUE,
    email           VARCHAR(150) NOT NULL UNIQUE,
    data_nascimento DATE         NOT NULL,
    genero          CHAR(1)      CHECK (genero IN ('M','F','O')),
    cidade          VARCHAR(100),
    estado          CHAR(2),
    score_credito   INTEGER      CHECK (score_credito BETWEEN 0 AND 1000),
    data_cadastro   TIMESTAMP    DEFAULT NOW(),
    ativo           BOOLEAN      DEFAULT TRUE
);

-- ----------------------------------------------------------------
-- Contas
-- ----------------------------------------------------------------
CREATE TABLE contas (
    id_conta        SERIAL PRIMARY KEY,
    id_cliente      INTEGER      NOT NULL REFERENCES clientes(id_cliente),
    numero_conta    VARCHAR(20)  NOT NULL UNIQUE,
    tipo_conta      VARCHAR(20)  NOT NULL CHECK (tipo_conta IN ('corrente','poupanca','investimento','cartao')),
    saldo           NUMERIC(15,2) DEFAULT 0.00,
    limite          NUMERIC(15,2) DEFAULT 0.00,
    data_abertura   TIMESTAMP    DEFAULT NOW(),
    ativa           BOOLEAN      DEFAULT TRUE
);

-- ----------------------------------------------------------------
-- Categorias de transação
-- ----------------------------------------------------------------
CREATE TABLE categorias_transacao (
    id_categoria    SERIAL PRIMARY KEY,
    nome            VARCHAR(50)  NOT NULL UNIQUE,
    tipo            VARCHAR(20)  NOT NULL CHECK (tipo IN ('debito','credito'))
);

INSERT INTO categorias_transacao (nome, tipo) VALUES
    ('alimentacao',     'debito'),
    ('transporte',      'debito'),
    ('saude',           'debito'),
    ('educacao',        'debito'),
    ('lazer',           'debito'),
    ('compras',         'debito'),
    ('servicos',        'debito'),
    ('transferencia',   'debito'),
    ('saque',           'debito'),
    ('salario',         'credito'),
    ('pix_recebido',    'credito'),
    ('estorno',         'credito'),
    ('investimento',    'credito'),
    ('outros',          'debito');

-- ----------------------------------------------------------------
-- Comerciantes
-- ----------------------------------------------------------------
CREATE TABLE comerciantes (
    id_comerciante  SERIAL PRIMARY KEY,
    nome            VARCHAR(150) NOT NULL,
    categoria       VARCHAR(50),
    cidade          VARCHAR(100),
    estado          CHAR(2),
    cnpj            VARCHAR(18)  UNIQUE
);

-- ----------------------------------------------------------------
-- Transações (tabela principal — alta volumetria)
-- ----------------------------------------------------------------
CREATE TABLE transacoes (
    id_transacao        BIGSERIAL    PRIMARY KEY,
    id_conta            INTEGER      NOT NULL REFERENCES contas(id_conta),
    id_categoria        INTEGER      REFERENCES categorias_transacao(id_categoria),
    id_comerciante      INTEGER      REFERENCES comerciantes(id_comerciante),
    valor               NUMERIC(12,2) NOT NULL,
    tipo                VARCHAR(20)  NOT NULL CHECK (tipo IN ('debito','credito','pix','ted','doc','boleto')),
    status              VARCHAR(20)  NOT NULL CHECK (status IN ('aprovada','negada','cancelada','pendente','suspeita')),
    canal               VARCHAR(20)  CHECK (canal IN ('app','web','pos','atm','agencia')),
    descricao           VARCHAR(255),
    data_transacao      TIMESTAMP    NOT NULL DEFAULT NOW(),
    data_processamento  TIMESTAMP,
    is_fraude           BOOLEAN      DEFAULT FALSE,
    score_fraude        NUMERIC(5,4) CHECK (score_fraude BETWEEN 0 AND 1),
    ip_origem           VARCHAR(45),
    device_id           VARCHAR(100)
);

-- ----------------------------------------------------------------
-- Índices para performance (demonstra conhecimento de DBA!)
-- ----------------------------------------------------------------
CREATE INDEX idx_transacoes_conta      ON transacoes(id_conta);
CREATE INDEX idx_transacoes_data       ON transacoes(data_transacao);
CREATE INDEX idx_transacoes_status     ON transacoes(status);
CREATE INDEX idx_transacoes_fraude     ON transacoes(is_fraude) WHERE is_fraude = TRUE;
CREATE INDEX idx_contas_cliente        ON contas(id_cliente);

-- ----------------------------------------------------------------
-- View auxiliar (simula uma camada de acesso)
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW vw_transacoes_completa AS
SELECT
    t.id_transacao,
    t.data_transacao,
    t.valor,
    t.tipo,
    t.status,
    t.canal,
    t.is_fraude,
    t.score_fraude,
    c.numero_conta,
    c.tipo_conta,
    cl.nome          AS nome_cliente,
    cl.cidade        AS cidade_cliente,
    cl.estado        AS estado_cliente,
    cl.score_credito,
    cat.nome         AS categoria,
    com.nome         AS comerciante
FROM transacoes t
JOIN contas              c   ON t.id_conta       = c.id_conta
JOIN clientes            cl  ON c.id_cliente     = cl.id_cliente
LEFT JOIN categorias_transacao cat ON t.id_categoria  = cat.id_categoria
LEFT JOIN comerciantes   com ON t.id_comerciante = com.id_comerciante;
