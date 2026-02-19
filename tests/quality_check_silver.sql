
-- Check For Nulls or Duplicates in Primary Key
-- Expectation : No Result

SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- Check for unwanted Spaces
-- Expectation : No Results

SELECT cst_lastname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

--Data Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

SELECT DISTINCT cst_material_status
from bronze.crm_cust_info

-- REPLACE WITH SILVER

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- Check for unwanted Spaces
-- Expectation : No Results

SELECT cst_lastname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

--Data Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_material_status
from silver.crm_cust_info

--==========================================

--Check For Nuls or Duplicates in Primary key
--Expectation : No Result

SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for NULLs OR Negative Numbers
-- Expectation : No results

SELECT prd_cost
FROM bronze.crm_prd_info
where prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for Invalid Date Orders

select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

select *,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt_test
from bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')


SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for NULLs OR Negative Numbers
-- Expectation : No results

SELECT prd_cost
FROM silver.crm_prd_info
where prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders

select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt

select *
from silver.crm_prd_info

--============================================

--Check for Invalid Dates

SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

--Check for Invalid Date Orders
select 
*
FROM bronze.crm_sales_details
Where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency : Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative.

SELECT DISTINCT
sls_sales AS old_sis_sales,
sls_quantity,
sls_price AS old_sis_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price is  NULL OR sls_price <= 0
	THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY  sls_sales, sls_quantity, sls_price



--Check for Invalid Dates

SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

--Check for Invalid Date Orders
select 
*
FROM silver.crm_sales_details
Where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency : Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative.

SELECT DISTINCT
sls_sales AS old_sis_sales,
sls_quantity,
sls_price AS old_sis_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY  sls_sales, sls_quantity, sls_price


SELECT *
FROM silver.crm_sales_details

--=========================================

SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12

-- Identify Out-Of-Range Dates

SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- DATE Standardization & Consistency

SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12



SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM silver.erp_cust_az12

-- Identify Out-Of-Range Dates

SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- DATE Standardization & Consistency

SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM silver.erp_cust_az12


--====================================

SELECT 
REPLACE(cid, '-','') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101

-- Data Standardization & Consistency

SELECT DISTINCT 
cntry AS old_cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

--============================

SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

--Check for unwanted Spaces

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)


-- Data Standardization & Consistency

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

