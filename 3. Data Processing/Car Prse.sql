-- Databricks notebook source
SELECT * FROM `workspace`.`default`.`cars`;

SELECT 
MIN(sellingprice)  lowest_price,
MAX(sellingprice )   highest_price,
COUNT(vin)           Total_cars
FROM  `workspace`.`default`.`cars`;
