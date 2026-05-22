-- ============================================================
-- ShopStream DataWarehouse Schema
-- PostgreSQL 15 — Star Schema para analítica de comportamiento
-- ============================================================

CREATE SCHEMA IF NOT EXISTS shopstream;

-- Tabla de dimensión: páginas
CREATE TABLE IF NOT EXISTS shopstream.dim_pages (
    page_id      SERIAL PRIMARY KEY,
    page_url     TEXT NOT NULL UNIQUE,
    page_type    VARCHAR(50)
);

-- Tabla de dimensión: dispositivos y países
CREATE TABLE IF NOT EXISTS shopstream.dim_device_country (
    dc_id        SERIAL PRIMARY KEY,
    device_type  VARCHAR(20) NOT NULL,
    country      CHAR(2) NOT NULL,
    UNIQUE (device_type, country)
);

-- Tabla de dimensión: productos
CREATE TABLE IF NOT EXISTS shopstream.dim_products (
    product_id   TEXT PRIMARY KEY,
    category     VARCHAR(100),
    price        NUMERIC(10, 2)
);

-- ============================================================
-- Tablas de hechos (fact tables) — métricas calculadas
-- ============================================================

-- Fact 1: Métricas de páginas por día
CREATE TABLE IF NOT EXISTS shopstream.fact_page_metrics (
    id               SERIAL PRIMARY KEY,
    date             DATE NOT NULL,
    page_url         TEXT NOT NULL,
    avg_time_seconds NUMERIC(10, 2),
    total_views      BIGINT,
    bounce_rate      NUMERIC(5, 2),
    page_type        VARCHAR(50),
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact 2: Resumen de sesiones por dispositivo/país/día
CREATE TABLE IF NOT EXISTS shopstream.fact_session_summary (
    id               SERIAL PRIMARY KEY,
    date             DATE NOT NULL,
    device_type      VARCHAR(20),
    country          CHAR(2),
    avg_time_seconds NUMERIC(10, 2),
    total_views      BIGINT,
    stddev_time      NUMERIC(10, 2),
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact 3: Embudo de conversión por día
CREATE TABLE IF NOT EXISTS shopstream.fact_conversion_funnel (
    id          SERIAL PRIMARY KEY,
    date        DATE NOT NULL,
    funnel_step VARCHAR(50),
    user_count  BIGINT,
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact 4: Productos vistos vs carrito
CREATE TABLE IF NOT EXISTS shopstream.fact_product_performance (
    id                 SERIAL PRIMARY KEY,
    date               DATE NOT NULL,
    product_id         TEXT NOT NULL,
    category           VARCHAR(100),
    views              BIGINT,
    cart_adds          BIGINT,
    view_to_cart_ratio NUMERIC(8, 4),
    high_view_low_cart BOOLEAN,
    loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact 5: Rutas de navegación top 10
CREATE TABLE IF NOT EXISTS shopstream.fact_navigation_paths (
    id        SERIAL PRIMARY KEY,
    date      DATE NOT NULL,
    path      TEXT NOT NULL,
    frequency BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact 6: Anomalías detectadas
CREATE TABLE IF NOT EXISTS shopstream.fact_anomalies (
    id           SERIAL PRIMARY KEY,
    date         DATE NOT NULL,
    session_id   TEXT NOT NULL,
    user_id      TEXT,
    total_time   NUMERIC(10, 2),
    event_count  BIGINT,
    zscore_time  NUMERIC(8, 4),
    anomaly_type VARCHAR(50),
    loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para performance de la API
CREATE INDEX IF NOT EXISTS idx_page_metrics_date       ON shopstream.fact_page_metrics(date);
CREATE INDEX IF NOT EXISTS idx_session_summary_date    ON shopstream.fact_session_summary(date);
CREATE INDEX IF NOT EXISTS idx_anomalies_date          ON shopstream.fact_anomalies(date);
CREATE INDEX IF NOT EXISTS idx_page_metrics_page_url   ON shopstream.fact_page_metrics(page_url);
CREATE INDEX IF NOT EXISTS idx_session_summary_device  ON shopstream.fact_session_summary(device_type, country);