/*
Title: ETL Development using Pure SQL: 
Purpose: Load order details into denormalized order_summary reporting table
Written by: Scott Davis 2023-03-27
*/

-- Create example input and output tables (DDL)

CREATE TABLE orders (order_id int, customer_id int, order_date date, total_amount decimal(10,2));

CREATE TABLE order_details (order_id int, product_id int, quantity int, price decimal(10,2));

CREATE TABLE order_summary 
	(order_id int, customer_id int, order_date date, total_amount decimal(10,2), product_count int, total_quantity int, avg_price decimal(10,2), max_price decimal(10,2), min_price decimal(10,2) );

-- Insert example input data (DML)

INSERT INTO orders (order_id, customer_id, order_date, total_amount)
VALUES 
	(1, 1001, '2022-01-01', 100.00),
	(2, 1002, '2022-01-02', 200.00),
	(3, 1003, '2022-01-03', 150.00)
;

INSERT INTO order_details (order_id, product_id, quantity, price)
VALUES 
	(1, 1, 2, 10.00),
	(1, 2, 3, 20.00),
	(2, 1, 1, 50.00),
	(2, 2, 2, 30.00),
	(3, 1, 3, 15.00),
	(3, 2, 2, 25.00)
;

-- Load data into denormalized output table (ETL)

-- Delete existing data in output table and load new. (Not very efficient but totally reasonable for this data set at this time.)
TRUNCATE TABLE order_summary;

INSERT INTO order_summary
SELECT 
	od.order_id
	, o.customer_id
	, o.order_date
	, total_amount = SUM(od.price * od.quantity)	-- note that total_amount from orders does not equal the actual total from order_details *in the sample data*
	, product_count = COUNT(DISTINCT od.product_id) -- or could use COUNT(*) depending on business rules for counting products
	, total_quantity = SUM(od.quantity)
	, avg_price = ROUND( SUM(od.price * od.quantity) / SUM(od.quantity), 2)
	, max_price = MAX(od.price)
	, min_price = MIN(od.price)
FROM order_details od
	INNER JOIN orders o ON o.order_id = od.order_id
GROUP BY 
	od.order_id
	, o.customer_id
	, o.order_date
	, o.total_amount
;

-- Verify output

SELECT * FROM order_summary;

/*
-- Optional Cleanup:
DROP TABLE orders;
DROP TABLE order_details;
DROP TABLE order_summary;
*/
