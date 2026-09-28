-- Database and schemas
CREATE DATABASE IF NOT EXISTS training;

CREATE SCHEMA IF NOT EXISTS training.raw;
CREATE SCHEMA IF NOT EXISTS training.clean;
CREATE SCHEMA IF NOT EXISTS training.gold;

-- Small virtual warehouse with auto-suspend/auto-resume
CREATE WAREHOUSE IF NOT EXISTS training_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60          -- suspend after 60 seconds idle
  AUTO_RESUME = TRUE         -- automatically wake up on next query
  INITIALLY_SUSPENDED = TRUE;

SHOW DATABASES LIKE 'training';
SHOW SCHEMAS IN DATABASE training;
SHOW WAREHOUSES LIKE 'training_wh';


SHOW WAREHOUSES LIKE 'training_wh';

USE WAREHOUSE training_wh;
USE DATABASE training;
USE SCHEMA raw;

CREATE TABLE IF NOT EXISTS orders (
    order_id     VARCHAR,
    order_ts     VARCHAR,   
    customer_id  VARCHAR,
    product_id   VARCHAR,
    store_id     VARCHAR,
    quantity     INTEGER,
    unit_price   NUMBER(12,2),
    updated_ts   VARCHAR    
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id    VARCHAR,
    customer_name  VARCHAR,
    segment        VARCHAR,
    city           VARCHAR,
    created_date   VARCHAR
);

CREATE TABLE IF NOT EXISTS products (
    product_id    VARCHAR,
    product_name  VARCHAR,
    category      VARCHAR,
    unit_price    NUMBER(12,2),
    active_flag   VARCHAR
);

CREATE TABLE IF NOT EXISTS stores (
    store_id    VARCHAR,
    store_name  VARCHAR,
    region      VARCHAR
);

SHOW TABLES IN SCHEMA training.raw;
DESCRIBE TABLE training.raw.orders

