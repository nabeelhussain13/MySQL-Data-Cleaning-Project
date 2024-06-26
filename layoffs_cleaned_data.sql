/* DATA CLEANING PROJECT

OBJECTIVES
1. Removal of all duplicate values
2. Standardizing the data
3. Treatment of Null & Blank values
4. Removal of unnecessary columns 

Creation of a table named "staging1" from the main table, because we don't want to change our original table
*/
CREATE TABLE staging1
LIKE layoffs;

# Data insertion into "staging1" table
INSERT staging1
SELECT *
FROM layoffs;

# 1. Removal of all duplicate values

# Viewing all the data of table "staging1"
SELECT *
FROM staging1;

# Using window function ROW_NUMBER() to find duplicate rows.
# If it returns value > 1 in any row, then we have duplicate data/rows in our data
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
AS row_num
FROM staging1;

# Finding duplicate rows using CTE
WITH duplicate_row AS
(
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM staging1
)
SELECT *
FROM duplicate_row
WHERE row_num > 1;

# Creation of another table named "staging2" so that we can delete our duplicate rows from that table.
CREATE TABLE staging2
LIKE staging1;

#Adding a column named row_num in "staging2" which is also present in "staging1"
ALTER TABLE staging2
ADD COLUMN row_num INT;

# Data insertion into "staging2" table
INSERT staging2
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
AS row_num
FROM staging1;

# Deleting all duplicate values
DELETE 
FROM staging2
WHERE row_num > 1;

# 2. Standradizing the data

# Updating the company names, removing unnecessary spaces.
UPDATE staging2
SET company = TRIM(company);

#Analysing industry names, finding abnormal type of data
SELECT DISTINCT industry
FROM staging2
ORDER BY industry;

SELECT *
FROM staging2
WHERE industry LIKE "%Crypto%";

# Updating the industry name, some industry names were written in different texts like Crypto was written as a Crypto, Crypto currency and CryptoCurrency
UPDATE staging2
SET industry = "Crypto"
WHERE industry LIKE "%Crypto%";

#Analysing country names, finding abnormal type of data
SELECT DISTINCT country
FROM staging2
ORDER BY country;

SELECT * 
FROM staging2
WHERE country LIKE "United States";

# Updating country where country same country name was written in twwo different text
UPDATE staging2
SET country = "United States"
WHERE country LIKE "United States%";

# Analysing date column
SELECT `date`
FROM staging2;

#Converting date into SQL date format i.e. yyyy-mm-dd
SELECT `date`, STR_TO_DATE(`date`, "%m/%d/%Y")
FROM staging2;

UPDATE staging2
SET `date` = STR_TO_DATE(`date`, "%m/%d/%Y");

# Converting the date column data type to DATE from text
ALTER TABLE staging2
MODIFY COLUMN `date` DATE;

# 3. Treatment of Null and blank values

# Analysing the values of industry where values are null or blank
SELECT *
FROM staging2
WHERE industry IS NULL OR industry = "";

SELECT *
FROM staging2
WHERE company = "Carvana";

# Updating all the blank values of the industry column to Null values, so that we can perform update operation
UPDATE staging2
SET industry = null
WHERE industry = "";

# Analysing missing/null values of industry column using SELF INNER JOIN
SELECT stg.industry, stg_.industry
FROM staging2 stg
JOIN staging2 stg_
ON stg.company = stg_.company
AND stg.location = stg_.location
WHERE stg.industry IS NULL
AND stg_.industry IS NOT NULL;

# Updating missing/null values of industry column by analysing other rows of same data
UPDATE staging2 stg
INNER JOIN staging2 stg_
ON stg.company = stg_.company
AND
stg.location = stg_.location
SET stg.industry = stg_.industry
WHERE stg.industry IS NULL
AND stg_.industry IS NOT NULL;

# Analysing rows where total laid off and percentage laid off values are missing
SELECT *
FROM staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# Deleting those rows where total laid off and percentage laid off values are missing because these rows are unnecessary.
DELETE 
FROM staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# 4. Removing unnecessary column

# Removing the column "row_num" from the table because it was not in our raw dataset
ALTER TABLE staging2
DROP COLUMN row_num;