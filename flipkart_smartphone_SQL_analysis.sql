#dropping database if there was database in same name
DROP DATABASE flipkart;

#creating database
CREATE DATABASE flipkart; 
USE flipkart;

#importing data with MySQL(Table Data Import Wizard) from previously downloaded CSV file 
SELECT * FROM flipkart_smartphones;

#creating a duplicate table for analysis
CREATE TABLE flpk_smp LIKE flipkart_smartphones;
INSERT INTO flpk_smp SELECT * FROM flipkart_smartphones;

#checking for any duplicate value
SELECT brand,model,colour,original_price,discounted_price,ratings,rating_count,reviews,memory,storage,processor,
rear_camera,front_camera,display_size,battery_capacity,battery_type, COUNT(*) 
FROM flpk_smp 
GROUP BY brand,model,colour,original_price,discounted_price,ratings,rating_count,reviews,memory,storage,processor,
rear_camera,front_camera,display_size,battery_capacity,battery_type 
HAVING COUNT(*) > 1 ORDER BY model;

#making table without duplicate value
CREATE TABLE flipkart_smartphones_temp AS SELECT DISTINCT 
 brand,model,colour,original_price,discounted_price,ratings,rating_count,reviews,memory,storage,processor,
rear_camera,front_camera,display_size,battery_capacity,battery_type 
FROM flpk_smp;
DROP TABLE flpk_smp;
RENAME TABLE flipkart_smartphones_temp TO flpk_smp;
SELECT * FROM flpk_smp ;

#checking columns for any NULL or empty value
#filling these for better analysis
SELECT * FROM flpk_smp WHERE brand IS NULL OR brand = "";
SELECT * FROM flpk_smp WHERE model IS NULL OR model = "";
SELECT * FROM flpk_smp WHERE colour IS NULL OR colour = "";
SELECT * FROM flpk_smp WHERE original_price IS NULL OR original_price = "";
SELECT * FROM flpk_smp WHERE discounted_price IS NULL OR discounted_price = "";
SELECT * FROM flpk_smp WHERE ratings IS NULL OR ratings = "";
SELECT * FROM flpk_smp WHERE rating_count IS NULL OR rating_count = "";
SELECT * FROM flpk_smp WHERE reviews IS NULL OR reviews = "";

#filling null values with mode ie 4 usually phone memory(RAM) comes in 1GB, 2GB, 3GB, 4GB, 6GB  or 8GB in that case filling with mode makes more sense
SELECT * FROM flpk_smp WHERE memory IS NULL OR memory = "";
SELECT memory, COUNT(*) AS cnt
FROM flpk_smp
WHERE memory IS NOT NULL AND memory <> ""
GROUP BY memory
ORDER BY cnt DESC
LIMIT 1;
SET SQL_SAFE_UPDATES = 0;
UPDATE flpk_smp
SET memory = (
    SELECT memory
    FROM (
        SELECT memory
        FROM flpk_smp
        WHERE memory IS NOT NULL AND memory <> ""
        GROUP BY memory
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) AS sub
)
WHERE memory IS NULL OR memory = "";
SELECT * FROM flpk_smp WHERE memory IS NULL OR memory = "";

SELECT * FROM flpk_smp WHERE storage IS NULL OR storage = "";

#filling null  values with -
SELECT * FROM flpk_smp WHERE processor IS NULL OR processor = "";
UPDATE flpk_smp SET processor = "-" WHERE processor IS NULL OR processor = ""; 
SELECT * FROM flpk_smp WHERE processor IS NULL OR processor = "";

SELECT * FROM flpk_smp WHERE rear_camera IS NULL OR rear_camera = "";

#filling null values with 0 MP phone might come without front camera
SELECT * FROM flpk_smp WHERE front_camera IS NULL OR front_camera = "";
UPDATE flpk_smp SET front_camera = "0MP" WHERE front_camera IS NULL OR front_camera = "";

SELECT * FROM flpk_smp WHERE rear_camera IS NULL OR rear_camera = "";
SELECT * FROM flpk_smp WHERE display_size IS NULL OR display_size = "";

#filling null values with 0
SELECT * FROM flpk_smp WHERE battery_capacity IS NULL OR battery_capacity = "";
UPDATE flpk_smp SET battery_capacity = "0" WHERE battery_capacity IS NULL OR battery_capacity = "";
SELECT * FROM flpk_smp WHERE battery_capacity IS NULL OR battery_capacity = "";

#Dropping Unnecessary column
SELECT COUNT(*) FROM flpk_smp WHERE battery_type IS NULL OR battery_type = "";
ALTER TABLE flpk_smp DROP COLUMN battery_type;
SELECT * FROM flpk_smp;

#1 Write a query to list all products with their brand, model, and discounted price.
SELECT brand, model, discounted_price FROM flpk_smp ORDER BY brand;

#2 Find the total number of products available for each brand.
SELECT  brand, COUNT(*) AS total_product FROM flpk_smp GROUP BY brand ORDER BY brand ;

#3 Find the average discounted price of smartphones by brand.
SELECT brand, AVG(discounted_price) AS avg_price FROM flpk_smp GROUP BY brand ;

#4 Count how many smartphones have a rating above 4.5.
SELECT brand, model, ratings FROM flpk_smp WHERE ratings > 4.5 ORDER BY ratings DESC;

#5 List the top 5 models with the highest number of reviews.
SELECT brand, model, reviews FROM flpk_smp ORDER BY reviews DESC LIMIT 5;

#6 Show all smartphones where the discounted price is less than 70% of the original price.
SELECT brand, model, original_price, discounted_price, ((original_price - discounted_price) * 100 / original_price) AS 
discounted_percentage FROM flpk_smp WHERE discounted_price < (0.7 * original_price);

#7 Find the distinct storage options available in the dataset.
SELECT storage, COUNT(*) FROM flpk_smp GROUP BY storage ORDER BY storage;

#8 Identify the brand with the highest average rating using a subquery.
SELECT brand FROM (
SELECT brand, AVG(ratings) AS avg_rating FROM flpk_smp GROUP BY brand)
AS brand_avg ORDER BY avg_rating DESC LIMIT 1;

#9 Use a window function to rank models by rear_camera resolution within each brand.
SELECT brand, model, rear_camera, RANK() OVER(PARTITION BY brand ORDER BY rear_camera DESC ) AS camera_rank FROM flpk_smp
ORDER BY brand, camera_rank;

#10 Find the top 3 models with the best battery-to-price ratio.
SELECT brand, model, battery_capacity, discounted_price, (battery_capacity * 1.0 / discounted_price) AS battery_price_ratio
FROM flpk_smp ORDER BY battery_price_ratio DESC LIMIT 3;

#11 Write a query to calculate the correlation between rating_count and discounted_price (hint: group into ranges and compare averages).

SELECT 
    CASE 
        WHEN rating_count < 100 THEN 'Low Engagement (<100)'
        WHEN rating_count BETWEEN 100 AND 500 THEN 'Moderate Engagement (100–500)'
        WHEN rating_count BETWEEN 501 AND 1000 THEN 'High Engagement (501–1000)'
        ELSE 'Very High Engagement (>1000)'
    END AS rating_category,
    AVG(discounted_price) AS avg_discounted_price,
    AVG(rating_count) AS avg_rating_count
FROM flpk_smp
GROUP BY rating_category
ORDER BY avg_rating_count;

#12 For each brand, calculate total reviews, average rating, and average discount — then rank brands by overall performance score.

SELECT 
    brand,
    SUM(reviews) AS total_reviews,
    AVG(ratings) AS avg_rating,
    AVG((original_price - discounted_price) * 100.0 / original_price) AS avg_discount_pct,
    -- Performance score: weighted combination of reviews, rating, and discount
    (SUM(reviews) * 0.4 + AVG(ratings) * 0.4 + 
     AVG((original_price - discounted_price) * 100.0 / original_price) * 0.2) AS performance_score,
    RANK() OVER (ORDER BY 
        (SUM(reviews) * 0.4 + AVG(ratings) * 0.4 + 
         AVG((original_price - discounted_price) * 100.0 / original_price) * 0.2) DESC
    ) AS brand_rank
FROM flpk_smp
GROUP BY brand
ORDER BY brand_rank;

#13 Segment products into three categories based on original_price (Budget, Mid-range, Premium) using CASE. Show average ratings per segment.
SELECT 
    CASE 
        WHEN original_price < 10000 THEN 'Budget (<10,000)'
        WHEN original_price BETWEEN 10000 AND 25000 THEN 'Mid-range (10,000–25,000)'
        ELSE 'Premium (>25,000)'
    END AS price_segment,
    AVG(ratings) AS avg_rating
FROM flpk_smp
GROUP BY price_segment
ORDER BY avg_rating DESC;

#14 Find the processor type most commonly used in high-rated phones (ratings > 4.5).
SELECT processor, COUNT(*) AS model_count
FROM flpk_smp
WHERE ratings > 4.5
GROUP BY processor
ORDER BY model_count DESC LIMIT 1;

#15 Build a query that identifies the “flagship” model per brand (highest discounted_price) and compares its rating with the brand’s average rating.

WITH brand_avg AS (
    SELECT brand, AVG(ratings) AS avg_brand_rating
    FROM flpk_smp
    GROUP BY brand
),
flagship AS (
    SELECT brand, model, discounted_price, ratings,
           RANK() OVER (PARTITION BY brand ORDER BY discounted_price DESC) AS price_rank
    FROM flpk_smp
)
SELECT f.brand,
       f.model AS flagship_model,
       f.discounted_price,
       f.ratings AS flagship_rating,
       b.avg_brand_rating,
       (f.ratings - b.avg_brand_rating) AS rating_difference
FROM flagship f
JOIN brand_avg b ON f.brand = b.brand
WHERE f.price_rank = 1
ORDER BY f.discounted_price DESC;