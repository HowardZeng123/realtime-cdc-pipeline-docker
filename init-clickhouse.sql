-- Tạo Database
CREATE DATABASE IF NOT EXISTS cdc_data;

/* Tạo 4 bảng 'mart' tương ứng với 4 bảng nguồn.
  Chúng ta dùng ReplacingMergeTree để xử lý các bản ghi UPDATE/DELETE (CDC).
  - 'ts_ms': Là timestamp (phiên bản) của event từ Debezium.
  - 'sign': Là 1 (insert/update) hoặc -1 (delete).
*/

-- Bảng 1: Customers
CREATE TABLE IF NOT EXISTS cdc_data.customers (
    id Int64,
    name String,
    email String,
    address String,
    ts_ms UInt64,  -- Timestamp (phiên bản) từ Debezium payload
    sign Int8      -- Dấu hiệu (1 = insert/update, -1 = delete)
) ENGINE = ReplacingMergeTree(ts_ms, sign)
ORDER BY id;

-- Bảng 2: Products
CREATE TABLE IF NOT EXISTS cdc_data.products (
    id Int64,
    name String,
    category String,
    price Nullable(Decimal(10, 2)),
    ts_ms UInt64,
    sign Int8
) ENGINE = ReplacingMergeTree(ts_ms, sign)
ORDER BY id;

-- Bảng 3: Orders
CREATE TABLE IF NOT EXISTS cdc_data.orders (
    id Int64,
    customer_id Int64,
    order_date DateTime,
    status String,
    total_amount Nullable(Decimal(10, 2)),
    ts_ms UInt64,
    sign Int8
) ENGINE = ReplacingMergeTree(ts_ms, sign)
ORDER BY id;

-- Bảng 4: Order Items
CREATE TABLE IF NOT EXISTS cdc_data.order_items (
    id Int64,
    order_id Int64,
    product_id Int64,
    quantity Int32,
    ts_ms UInt64,
    sign Int8
) ENGINE = ReplacingMergeTree(ts_ms, sign)
ORDER BY id;