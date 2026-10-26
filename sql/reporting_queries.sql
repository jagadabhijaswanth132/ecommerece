-- Gold reporting queries

-- 1. Final detailed sales
SELECT * FROM ecommerce.gold.sales_detail
ORDER BY order_date, order_id;

-- 2. Sales by day
SELECT * FROM ecommerce.gold.daily_sales_summary
ORDER BY order_date;

-- 3. Sales by state
SELECT * FROM ecommerce.gold.state_sales_summary
ORDER BY total_sales DESC;

-- 4. Sales by product category
SELECT * FROM ecommerce.gold.category_sales_summary
ORDER BY total_sales DESC;

-- 5. Check duplicate order IDs
SELECT order_id, COUNT(*) AS occurrences
FROM ecommerce.gold.sales_detail
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 6. Validate sales calculation
SELECT *
FROM ecommerce.gold.sales_detail
WHERE sales_amount <> quantity * price;

-- 7. Top customers by sales
SELECT customer_id, customer_name, SUM(sales_amount) AS total_sales
FROM ecommerce.gold.sales_detail
GROUP BY customer_id, customer_name
ORDER BY total_sales DESC;
