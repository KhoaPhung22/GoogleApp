
  SELECT * FROM apps;
LOAD DATA LOCAL INFILE 'C:\\Kai\\data project\\Android_App Market_on_Google_PLay\\datasets\\apps.csv'
INTO TABLE Apps
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;
set global local_infile=1;
CREATE TABLE staging_apps
LIKE Apps;
SELECT * FROM staging_apps;
INSERT staging_apps
SELECT * from Apps;

-- 1. REMOVE DUPLICATE (IF ANY)
SELECT * ,row_number()over( PARTITION BY id,App) as row_num FROM staging_apps;
SELECT App,COUNT(*) AS 'duplicate' from staging_apps
WHERE 'duplicate' >2
GROUP BY App;

-- Create common table expression to find any duplicate value
WITH duplicate_cte AS(
	SELECT * ,row_number()over( PARTITION BY id,App,Category,Rating,Reviews,size,installs,typeOfApp,Price,Content_Rating,Genres,Last_Updated,Android_Ver) as row_num FROM staging_apps
)
select * from duplicate_cte
WHERE row_num >1;


-- 2. STANDARDIZE THE DATA
SELECT *from staging_apps;
SELECT App,TRIM(App)
from staging_apps;

-- Check if there is any word format error in the data
SELECT App FROM staging_apps;
SELECT App FROM staging_apps
where NOT App >'A' and App <'Z';

SELECT distinct category
FROM staging_apps
ORDER BY category ASC;

-- count how many distinct category
select count(distinct category) from staging_apps; -- 33

SELECT category FROM staging_apps;

SELECT * FROM staging_apps;

SELECT DISTINCT TRIM(category),category from staging_apps;

SELECT * FROM staging_apps
WHERE typeOfApp = 'Paid';


UPDATE staging_apps
SET size = null
WHERE size = 0;

select Last_Updated,STR_TO_DATE(Last_Updated,'%Y-%m-%d-%')
from staging_apps;

UPDATE staging_apps 
SET Last_Updated = STR_TO_DATE(Last_Updated,'%Y-%m-%d-%');
SELECT * FROM STAGING_APPS;
ALTER TABLE staging_apps
MODIFY COLUMN Last_Updated DATE;
-- 3. NULL VALUES OR BLANK VALUES
SELECT * FROM staging_apps
where size IS null;
-- 4. REMOVE ANY ROWS OR COLUMMN IF NECCESSARY 
-- REMOVE THE DUPLICATION IN CATEGORY AND GENRES IN THE TABLE
ALTER TABLE staging_apps
DROP COLUMN Genres; 


-- Explotary data analysis
select * FROM STAGING_aPPS;

SELECT MAX(installs),MAX(reviews) FROM staging_apps;

select * from staging_apps
WHERE Reviews = 78158306;

SELECT * FROM STAGING_APPS where installs = '1+'
ORDER BY Last_Updated ASC;

-- FILETERING MOST POPULAR APP with 1000000000+ download
SELECT App,Category FROM staging_apps
WHERE Rating >4.0 AND installs = '1000000000+' AND Content_Rating = 'Everyone'
ORDER BY Last_Updated ASC;

-- User's Review each year
SELECT YEAR(Last_Updated),SUM(reviews) as sum_reviews
FROM staging_apps
group by YEAR(Last_Updated)
order by 1 ;

SELECT year(Last_Updated),AVG(reviews)
FROM STAGING_APPS
GROUP BY Year(Last_Updated)
order by Year(Last_Updated) DESC;
-- The total reviews number of reviews in the year 2018 for 
SELECT SUBSTRING(Last_Updated,1,7) AS 'Month',SUM(reviews) 
from staging_apps
WHERE YEAR(Last_Updated) = 2018
GROUP BY 'Month'
ORDER BY Month ASC ;
WITH Review_Total AS
(
SELECT SUBSTRING(Last_Updated,1,7) AS `Month`,SUM(reviews) AS total_review
from staging_apps
GROUP BY `Month`
ORDER BY `Month` ASC
)
SELECT `Month`,total_review, SUM(total_review) Over (ORDER BY `month` ) as review_total from Review_Total;


WITH App_Year(App,years, reviews,Category) AS
(
SELECT App, YEAR(Last_Updated),Sum(reviews),Category
FROM staging_apps
group by App, Year(Last_Updated),Category
),App_Year_Rank AS(
SELECT * , DENSE_rank() over (PARTITION BY years ORDER BY reviews DESC) AS Ranking FROM App_Year 
WHERE years IS NOT NULL AND years = 2018
ORDER BY Ranking ASC
LIMIT 50
)
select * from App_Year_Rank;

SELECT * FROM staging_apps;






