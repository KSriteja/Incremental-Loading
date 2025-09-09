-- Databricks notebook source
create database sales_new;

-- COMMAND ----------

CREATE TABLE sales_new.orders (
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

INSERT INTO sales_new.orders
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

select * from sales_new.orders

-- COMMAND ----------

create database orders_datawarehouse;

-- COMMAND ----------

create or replace table orders_datawarehouse.stg_sales as select * from sales_new.orders

-- COMMAND ----------

create view orders_datawarehouse.stg_sales_view as select OrderID,OrderDate,CustomerID,CustomerName,CustomerEmail,ProductID,ProductName,ProductCategory,RegionID,RegionName,Country,Quantity*10 as Quantity,UnitPrice,TotalAmount from orders_datawarehouse.stg_sales

-- COMMAND ----------

select * from orders_datawarehouse.stg_sales_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dimension table
-- MAGIC

-- COMMAND ----------

CREATE or replace TABLE orders_datawarehouse.dimCustomers (
    CustomerID INT,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    DimCustomerKey INT
);

-- COMMAND ----------


create or replace view orders_datawarehouse.dimCustomers_view as select T.*,row_number() over(ORDER BY CustomerID) as DimCustomerKey FROM( select DISTINCT(CustomerID) as CustomerID,CustomerName,CustomerEmail from orders_datawarehouse.stg_sales_view) as T

-- COMMAND ----------

select * from orders_datawarehouse.dimCustomers_view

-- COMMAND ----------

 insert into orders_datawarehouse.dimcustomers select * from orders_datawarehouse.dimCustomers_view

-- COMMAND ----------

select * from orders_datawarehouse.dimcustomers

-- COMMAND ----------

CREATE or replace TABLE orders_datawarehouse.dimProduct (
    ProductID INT,
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(100),
    DimProductKey INT
);

-- COMMAND ----------

create or replace view orders_datawarehouse.dimProduct_view as select T.*,row_number() over(ORDER BY ProductID) as DimProductKey FROM( select DISTINCT(ProductId) as ProductID,ProductName,ProductCategory from orders_datawarehouse.stg_sales_view) as T

-- COMMAND ----------

insert into orders_datawarehouse.dimproduct select * from orders_datawarehouse.dimProduct_view

-- COMMAND ----------

select * from orders_datawarehouse.dimproduct

-- COMMAND ----------

CREATE or replace TABLE orders_datawarehouse.dimRegion (
    RegionID INT,
    RegionName VARCHAR(100),
    Country VARCHAR(100),
    DimRegionKey INT
);

-- COMMAND ----------

create or replace view orders_datawarehouse.dimRegion_view as select T.*,row_number() over(ORDER BY RegionID) as DimRegionKey FROM( select DISTINCT(RegionID) as RegionID,RegionName,Country from orders_datawarehouse.stg_sales_view) as T

-- COMMAND ----------

insert into orders_datawarehouse.dimregion select * from orders_datawarehouse.dimRegion_view


-- COMMAND ----------

select * from orders_datawarehouse.dimregion

-- COMMAND ----------

CREATE or replace TABLE orders_datawarehouse.dimDate (
    orderDate DATE,
    DimDateKey INT
);

-- COMMAND ----------

create or replace view orders_datawarehouse.dimDate_view as select T.*,row_number() over(ORDER BY orderDate) as DimDateKey FROM( select DISTINCT(OrderDate) as orderDate from orders_datawarehouse.stg_sales_view) as T

-- COMMAND ----------

insert into orders_datawarehouse.dimdate select * from orders_datawarehouse.dimDate_view

-- COMMAND ----------

select * from orders_datawarehouse.dimdate

-- COMMAND ----------

create table orders_datawarehouse.fact_sales(OrderID int, Quantity decimal(10,2), UnitPrice decimal(10,2), TotalAmount decimal(10,2), DimCustomerKey int, DimProductKey int, DimRegionKey int, DimDateKey int);

-- COMMAND ----------

insert into orders_datawarehouse.fact_sales select 
f.OrderID,
f.Quantity,
f.UnitPrice,
f.TotalAmount,
dc.DimCustomerKey,
dp.DimProductKey,
dr.DimRegionKey,
dd.DimDateKey
from orders_datawarehouse.stg_sales_view f left join orders_datawarehouse.dimcustomers dc on f.CustomerID=dc.CustomerID left join orders_datawarehouse.dimproduct dp on f.ProductID=dp.ProductID left join orders_datawarehouse.dimregion dr on f.Country=dr.Country left join orders_datawarehouse.dimdate dd on f.OrderDate=dd.orderDate;

-- COMMAND ----------

