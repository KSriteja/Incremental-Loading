-- Databricks notebook source
create database sales_SCD;

-- COMMAND ----------

CREATE TABLE sales_scd.orders (
    OrderID INT PRIMARY KEY,
    OrderDate DATE,
    CustomerID INT,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    ProductID INT,
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(50),
    RegionID INT,
    RegionName VARCHAR(50),
    Country VARCHAR(50),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalAmount DECIMAL(12,2)
);

-- COMMAND ----------

INSERT INTO sales_scd.orders
(OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount)
VALUES
(3001, '2024-02-01', 101, 'Alice Johnson', 'alice@example.com', 201, 'Laptop', 'Electronics', 1, 'North America', 'USA', 1, 1200.00, 1200.00),
(3002, '2024-02-02', 102, 'Bob Smith', 'bob@example.com', 202, 'Smartphone', 'Electronics', 2, 'Europe', 'Germany', 2, 800.00, 1600.00),
(3003, '2024-02-03', 103, 'Charlie Brown', 'charlie@example.com', 203, 'Tablet', 'Electronics', 3, 'Asia', 'India', 1, 500.00, 500.00),
(3004, '2024-02-04', 101, 'Alice Johnson', 'alice@example.com', 204, 'Headphones', 'Accessories', 1, 'North America', 'USA', 3, 60.00, 180.00),
(3005, '2024-02-05', 104, 'David Lee', 'david@example.com', 205, 'Gaming Console', 'Electronics', 3, 'Asia', 'Japan', 1, 400.00, 400.00),
(3006, '2024-02-06', 102, 'Bob Smith', 'bob@example.com', 206, 'Smartwatch', 'Electronics', 2, 'Europe', 'Germany', 1, 250.00, 250.00),
(3007, '2024-02-07', 105, 'Eve Adams', 'eve@example.com', 207, 'Laptop', 'Electronics', 4, 'South America', 'Brazil', 1, 1100.00, 1100.00),
(3008, '2024-02-08', 106, 'Frank Miller', 'frank@example.com', 208, 'Monitor', 'Accessories', 2, 'Europe', 'UK', 2, 200.00, 400.00),
(3009, '2024-02-09', 107, 'Grace White', 'grace@example.com', 209, 'Keyboard', 'Accessories', 1, 'North America', 'Canada', 1, 80.00, 80.00),
(3010, '2024-02-10', 103, 'Charlie Brown', 'charlie@example.com', 210, 'Office Chair', 'Furniture', 3, 'Asia', 'India', 1, 300.00, 300.00);


-- COMMAND ----------

select * from sales_scd.orders

-- COMMAND ----------

select distinct(ProductID) as ProductID,ProductName,ProductCategory from sales_scd.orders

-- COMMAND ----------

create or replace table sales_scd.DimProduct(
  ProductID int,
  ProductName string,
  ProductCategory string
)

-- COMMAND ----------



-- COMMAND ----------

create or replace view sales_scd.dimproduct_view as select distinct(ProductID) as ProductID,ProductName,ProductCategory from sales_scd.orders where OrderDate>'2024-02-10'

-- COMMAND ----------

select * from sales_scd.dimproduct_view

-- COMMAND ----------

insert into sales_scd.dimproduct

select * from sales_scd.dimproduct_view

-- COMMAND ----------

select * from sales_scd.DimProduct

-- COMMAND ----------

INSERT INTO sales_scd.orders
(OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount)
VALUES
(3001, '2024-02-11', 101, 'Alice Johnson', 'alice@example.com', 201, 'Gaming Laptop', 'Electronics', 1, 'North America', 'USA', 1, 1200.00, 1200.00),
(3002, '2024-02-12', 102, 'Bob Smith', 'bob@example.com', 230, 'Airpods', 'Electronics', 2, 'Europe', 'Germany', 2, 800.00, 1600.00)

-- COMMAND ----------

merge into sales_scd.DimProduct as target
using sales_scd.dimproduct_view as source
on target.ProductID = source.ProductID when matched then update set *
when not matched then insert *


-- COMMAND ----------

select * from sales_scd.dimproduct

-- COMMAND ----------

