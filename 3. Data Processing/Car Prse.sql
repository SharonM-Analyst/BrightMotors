-- Databricks notebook source
SELECT * FROM `workspace`.`default`.`cars`;

SELECT 
MIN(sellingprice)  lowest_price,
MAX(sellingprice )   highest_price
FROM  `workspace`.`default`.`cars`;
