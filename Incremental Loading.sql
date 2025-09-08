-- Databricks notebook source
-- MAGIC %md
-- MAGIC Incremental Loading

-- COMMAND ----------

create database sales;


-- COMMAND ----------

CREATE TABLE sales.orders (
    OrderID INT PRIMARY KEY,
    OrderDate DATE NOT NULL,
    CustomerID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    TotalAmount DECIMAL(12,2) NOT NULL,
    LastUpdated TIMESTAMP NOT NULL
);

-- Initial Load (batch 1)
INSERT INTO sales.orders (OrderID, OrderDate, CustomerID, ProductID, Quantity, UnitPrice, TotalAmount, LastUpdated) VALUES
(1001, '2025-01-05', 1, 101, 2, 50.00, 100.00, '2025-01-05 10:15:00'),
(1002, '2025-01-06', 2, 102, 1, 75.00, 75.00, '2025-01-06 14:20:00'),
(1003, '2025-01-07', 1, 103, 5, 20.00, 100.00, '2025-01-07 09:45:00'),
(1004, '2025-01-08', 3, 104, 3, 40.00, 120.00, '2025-01-08 11:30:00');

-- COMMAND ----------

INSERT INTO sales.orders (OrderID, OrderDate, CustomerID, ProductID, Quantity, UnitPrice, TotalAmount, LastUpdated) VALUES
(1007, '2025-01-11', 5, 107, 8, 25.00, 200.00, '2025-01-11 09:20:00'),
(1008, '2025-01-12', 3, 108, 2, 120.00, 240.00, '2025-01-12 14:05:00'),
(1009, '2025-01-13', 6, 109, 6, 30.00, 180.00, '2025-01-13 11:50:00'),
(1010, '2025-01-14', 2, 110, 1, 500.00, 500.00, '2025-01-14 16:35:00'),
(1011, '2025-01-15', 4, 111, 4, 75.00, 300.00, '2025-01-15 10:10:00');

-- COMMAND ----------

select * from sales.orders

-- COMMAND ----------

-- MAGIC %md DATA WAREHOUSING

-- COMMAND ----------

create database sales_datawarehouse;

-- COMMAND ----------

-- MAGIC %md STAGING LAYER

-- COMMAND ----------

-- initial load

create or replace table sales_datawarehouse.stg_sales as select * from sales.orders where OrderDate > '2025-01-08'

-- COMMAND ----------

-- MAGIC %md Transformation

-- COMMAND ----------


create view sales_datawarehouse.stg_sales_view as select OrderID,OrderDate,CustomerID,ProductID,Quantity*10 as Quantity,UnitPrice,TotalAmount,LastUpdated from sales_datawarehouse.stg_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Core Layer

-- COMMAND ----------

CREATE TABLE sales_datawarehouse.core_sale (
    OrderID INT PRIMARY KEY,
    OrderDate DATE NOT NULL,
    CustomerID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    TotalAmount DECIMAL(12,2) NOT NULL,
    LastUpdated TIMESTAMP NOT NULL
);

-- COMMAND ----------

insert into sales_datawarehouse.core_sale
select * from sales_datawarehouse.stg_sales_view

-- COMMAND ----------

select * from sales_datawarehouse.core_sale

-- COMMAND ----------

