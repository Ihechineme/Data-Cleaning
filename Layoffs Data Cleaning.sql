-- Data Cleaning 

CREATE TABLE clean_layoffs
SELECT *
FROM layoffs;

SELECT *
FROM clean_layoffs;

WITH duplicate_cte AS (
   SELECT *, 
          ROW_NUMBER() OVER(
              PARTITION BY company, location, total_laid_off, percentage_laid_off, `date`, country
          ) AS row_num
   FROM clean_layoffs
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1; -- This filters out duplicates, keeping only the first occurrence.

select count(*)
from layoffs;

-- Already removed duplicates from clean_layoffs table but removed the code. Will be documenting the process again below with a new table

CREATE TABLE clean_layoffs2
SELECT *
FROM layoffs;

WITH second_duplicates AS (
   SELECT *, 
          ROW_NUMBER() OVER(
              PARTITION BY company, location, total_laid_off, percentage_laid_off, `date`, country, stage, industry, funds_raised_millions
          ) AS row_num
   FROM clean_layoffs2
)
DELETE FROM clean_layoffs2
WHERE company IN (
    SELECT company
    FROM second_duplicates
    WHERE row_num > 1
);
-- SELECT * FROM second_duplicates WHERE row_num > 1; checking duplicates rows
select count(*)
from clean_layoffs2;

-- Trying to clean duplicates with another table.

CREATE TABLE layoffs_clean
SELECT *
FROM layoffs;

ALTER TABLE layoffs_clean
ADD COLUMN row_num INT;

SELECT *
FROM layoffs_clean;

WITH row_num_cte AS (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ORDER BY company) AS row_num
    FROM layoffs_clean
)
UPDATE layoffs_clean l
JOIN row_num_cte r
ON l.company = r.company
   AND l.location = r.location
   AND l.industry = r.industry
   AND l.total_laid_off = r.total_laid_off
   AND l.percentage_laid_off = r.percentage_laid_off
   AND l.`date` = r.`date`
   AND l.stage = r.stage
   AND l.country = r.country
   AND l.funds_raised_millions = r.funds_raised_millions
SET l.row_num = r.row_num;

ALTER TABLE layoffs_clean
DROP COLUMN row_num;

-- remove nulls first

SELECT COUNT(*) 
FROM(
SELECT *
FROM layoffs_clean
WHERE company IS NULL
  OR location IS NULL
  OR industry IS NULL
  OR total_laid_off IS NULL
  OR percentage_laid_off IS NULL
  OR `date` IS NULL
  OR stage IS NULL
  OR country IS NULL
  OR funds_raised_millions IS NULL
  ) no_of_nulls;
  
SELECT *
FROM layoffs_clean
WHERE industry IS NULL
OR industry = ''; 

SELECT *
FROM layoffs_clean
WHERE company LIKE 'Juul%'
OR company LIKE 'Airbnb'
OR company LIKE 'Carvana';

UPDATE layoffs_clean
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_clean t1
JOIN layoffs_clean t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_clean
WHERE total_laid_off IS NULL
OR total_laid_off = ''; 

-- removing duplicates

WITH duplicate_value AS (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_clean
)
DELETE
FROM layoffs_clean
WHERE company IN (
    SELECT company 
    FROM duplicate_value
    WHERE row_num > 1 AND company = 'Casper');
-- SELECT * FROM duplicate_valueWHERE row_num > 1;

DELETE t
FROM layoffs_clean t
JOIN duplicate_value d
ON t.company = d.company
AND t.location = d.location
AND t.industry = d.industry
AND t.total_laid_off = d.total_laid_off
AND t.percentage_laid_off = d.percentage_laid_off
AND t.`date` = d.`date`
AND t.stage = d.stage
AND t.country = d.country
AND t.funds_raised_millions = d.funds_raised_millions
WHERE d.row_num > 1;

-- change date column to correct data type
UPDATE layoffs_clean
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_clean
MODIFY COLUMN `date` DATE;

DELETE FROM layoffs_clean 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

