# 20260407_001 - PoChun Hsu - [Add]     Initial for iceberg database and table

CREATE SCHEMA IF NOT EXISTS iceberg.default;

CREATE TABLE IF NOT EXISTS iceberg.default.test (
  id INT,
  name STRING
);

INSERT INTO iceberg.default.test VALUES
  (1, 'iphone 15'),
  (2, 'iphone 16'),
  (3, 'iphone 17');
