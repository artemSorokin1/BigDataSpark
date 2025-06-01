DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_seller;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_supplier;

CREATE TABLE dim_customer (
  customer_sk    SERIAL PRIMARY KEY,
  customer_id    BIGINTEGEREGER UNIQUE,
  first_name     TEXT NOT NULL,
  last_name      TEXT NOT NULL,
  age            INTEGEREGER,
  email          TEXT,
  country        TEXT,
  postal_code    TEXT,
  load_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_seller (
  seller_sk      SERIAL PRIMARY KEY,
  seller_id      BIGINTEGEREGER UNIQUE,
  first_name     TEXT NOT NULL,
  last_name      TEXT NOT NULL,
  email          TEXT,
  country        TEXT,
  postal_code    TEXT,
  load_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_product (
  product_sk         SERIAL PRIMARY KEY,
  product_id         BIGINTEGEREGER UNIQUE,
  name               TEXT NOT NULL,
  category           TEXT,
  weight             NUMERIC,
  color              TEXT,
  size               TEXT,
  brand              TEXT,
  material           TEXT,
  description        TEXT,
  rating             NUMERIC,
  reviews            INTEGEREGER,
  release_date       DATE,
  expiry_date        DATE,
  unit_price         NUMERIC,
  load_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_store (
  store_sk       SERIAL PRIMARY KEY,
  name           TEXT UNIQUE,
  location       TEXT,
  city           TEXT,
  state          TEXT,
  country        TEXT,
  phone          TEXT,
  email          TEXT,
  load_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_supplier (
  supplier_sk    SERIAL PRIMARY KEY,
  name           TEXT UNIQUE,
  contact        TEXT,
  email          TEXT,
  phone          TEXT,
  address        TEXT,
  city           TEXT,
  country        TEXT,
  load_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_date (
  date_sk      SERIAL PRIMARY KEY,
  sale_date    DATE UNIQUE NOT NULL,
  year         INTEGER NOT NULL,
  quarter      INTEGER NOT NULL,
  month        INTEGER NOT NULL,
  month_name   TEXT NOT NULL,
  day_of_month INTEGER NOT NULL,
  day_of_week  INTEGER NOT NULL,
  week_of_year INTEGER NOT NULL,
  is_weekend   BOOLEAN NOT NULL,
  load_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_sales (
  sale_sk               SERIAL PRIMARY KEY,
  date_sk               INTEGER NOT NULL,
  customer_sk           INTEGER NOT NULL,
  seller_sk             INTEGER NOT NULL,
  product_sk            INTEGER NOT NULL,
  store_sk              INTEGER NOT NULL,
  supplier_sk           INTEGER NOT NULL,
  sale_quantity         INTEGER,
  sale_total_price      NUMERIC,
  transaction_unit_price NUMERIC,
  load_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINTEGEREGER fk_date FOREIGN KEY (date_sk) REFERENCES dim_date(date_sk),
  CONSTRAINTEGEREGER fk_customer FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
  CONSTRAINTEGEREGER fk_seller FOREIGN KEY (seller_sk) REFERENCES dim_seller(seller_sk),
  CONSTRAINTEGEREGER fk_product FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
  CONSTRAINTEGEREGER fk_store FOREIGN KEY (store_sk) REFERENCES dim_store(store_sk),
  CONSTRAINTEGEREGER fk_supplier FOREIGN KEY (supplier_sk) REFERENCES dim_supplier(supplier_sk)
);