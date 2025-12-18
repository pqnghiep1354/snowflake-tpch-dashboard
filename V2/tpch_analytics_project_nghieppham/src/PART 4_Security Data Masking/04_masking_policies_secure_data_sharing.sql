-- =====================================================
-- PHẦN 4: SECURITY - MASKING POLICIES & DATA SHARING
-- Data Protection và Secure Sharing
-- =====================================================

USE ROLE TPCH_ADMIN;
USE DATABASE TPCH_ANALYTICS_DB;
USE WAREHOUSE TPCH_WH;

-- =====================================================
-- 4.1 TẠO BẢNG VỚI SENSITIVE DATA
-- =====================================================

USE SCHEMA ANALYTICS;

-- Tạo bảng customers với thông tin nhạy cảm
CREATE OR REPLACE TABLE CUSTOMER_SENSITIVE AS
SELECT 
    C.C_CUSTKEY,
    C.C_NAME,
    C.C_ADDRESS,
    C.C_PHONE,
    C.C_ACCTBAL,
    N.N_NAME AS NATION,
    R.R_NAME AS REGION,
    C.C_MKTSEGMENT,
    -- Thêm thông tin nhạy cảm (giả lập)
    'customer_' || C.C_CUSTKEY || '@company.com' AS EMAIL,
    LPAD(ABS(MOD(C.C_CUSTKEY * 123456789, 1000000000)), 9, '0') AS SSN_LAST_9,
    -- Credit card simulation (first 4 digits visible, rest masked)
    '4532-' || LPAD(ABS(MOD(C.C_CUSTKEY * 987654321, 10000)), 4, '0') || '-' ||
    LPAD(ABS(MOD(C.C_CUSTKEY * 456789123, 10000)), 4, '0') || '-' ||
    LPAD(ABS(MOD(C.C_CUSTKEY * 321654987, 10000)), 4, '0') AS CREDIT_CARD,
    -- Salary estimation based on account balance
    C.C_ACCTBAL * 10 AS ESTIMATED_ANNUAL_INCOME
FROM TPCH_ANALYTICS_DB.STAGING.CUSTOMER C
JOIN TPCH_ANALYTICS_DB.STAGING.NATION N ON C.C_NATIONKEY = N.N_NATIONKEY
JOIN TPCH_ANALYTICS_DB.STAGING.REGION R ON N.N_REGIONKEY = R.R_REGIONKEY;

-- Verify table creation
SELECT COUNT(*) AS TOTAL_CUSTOMERS FROM CUSTOMER_SENSITIVE;

-- Sample unmasked data (visible to ADMIN only)
SELECT * FROM CUSTOMER_SENSITIVE LIMIT 10;

-- =====================================================
-- 4.2 TẠO MASKING POLICIES
-- =====================================================

-- Masking Policy 1: EMAIL masking
-- Hiển thị đầy đủ cho ADMIN, partial cho ANALYST, full mask cho VIEWER
CREATE OR REPLACE MASKING POLICY EMAIL_MASK AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('TPCH_ADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() = 'TPCH_ANALYST' THEN 
            REGEXP_REPLACE(val, '(.{3})(.*)(@.*)', '\\1***\\3')  -- Show first 3 chars + domain
        WHEN CURRENT_ROLE() = 'TPCH_DEVELOPER' THEN 
            REGEXP_REPLACE(val, '(.{2})(.*)(@.*)', '\\1****\\3') -- Show first 2 chars + domain
        ELSE '***masked***'
    END;

-- Masking Policy 2: PHONE masking
-- ADMIN: Full phone, ANALYST: Last 4 digits, DEVELOPER: Last 4 digits, VIEWER: Fully masked
CREATE OR REPLACE MASKING POLICY PHONE_MASK AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('TPCH_ADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() IN ('TPCH_ANALYST', 'TPCH_DEVELOPER') THEN 
            '***-***-' || RIGHT(val, 4)
        ELSE '***-***-****'
    END;

-- Masking Policy 3: SSN masking
-- ADMIN: Full SSN, ANALYST: Last 4 digits, others: Fully masked
CREATE OR REPLACE MASKING POLICY SSN_MASK AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('TPCH_ADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() = 'TPCH_ANALYST' THEN 
            '***-**-' || RIGHT(val, 4)
        ELSE '***-**-****'
    END;

-- Masking Policy 4: ACCOUNT BALANCE masking
-- ADMIN: Full balance, ANALYST: Rounded to nearest 1000, DEVELOPER: Range category, VIEWER: Hidden
CREATE OR REPLACE MASKING POLICY BALANCE_MASK AS (val NUMBER) RETURNS NUMBER ->
    CASE 
        WHEN CURRENT_ROLE() IN ('TPCH_ADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() = 'TPCH_ANALYST' THEN ROUND(val, -3)  -- Round to nearest 1000
        WHEN CURRENT_ROLE() = 'TPCH_DEVELOPER' THEN ROUND(val, -2)  -- Round to nearest 100
        ELSE NULL  -- Hidden for VIEWER
    END;

-- Masking Policy 5: CREDIT CARD masking
-- ADMIN: Full number, ANALYST: First 4 and last 4, others: Only first 4
CREATE OR REPLACE MASKING POLICY CREDIT_CARD_MASK AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('TPCH_ADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() = 'TPCH_ANALYST' THEN 
            LEFT(val, 4) || '-****-****-' || RIGHT(val, 4)
        WHEN CURRENT_ROLE() = 'TPCH_DEVELOPER' THEN 
            LEFT(val, 4) || '-****-****-****'
        ELSE '****-****-****-****'
    END;

-- Masking Policy 6: INCOME masking
-- Similar to balance masking but with income ranges
CREATE OR REPLACE MASKING POLICY INCOME_MASK AS (val NUMBER) RETURNS NUMBER ->
    CASE 
        WHEN CURRENT_ROLE() IN ('TPCH_ADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() = 'TPCH_ANALYST' THEN ROUND(val, -4)  -- Round to nearest 10,000
        WHEN CURRENT_ROLE() = 'TPCH_DEVELOPER' THEN ROUND(val, -3)  -- Round to nearest 1,000
        ELSE NULL  -- Hidden
    END;


-- List all masking policies
SHOW MASKING POLICIES IN SCHEMA ANALYTICS;

-- =====================================================
-- 4.3 APPLY MASKING POLICIES
-- =====================================================

-- Apply EMAIL masking policy
ALTER TABLE CUSTOMER_SENSITIVE MODIFY COLUMN EMAIL 
    SET MASKING POLICY EMAIL_MASK;

-- Apply PHONE masking policy
ALTER TABLE CUSTOMER_SENSITIVE MODIFY COLUMN C_PHONE 
    SET MASKING POLICY PHONE_MASK;

-- Apply SSN masking policy
ALTER TABLE CUSTOMER_SENSITIVE MODIFY COLUMN SSN_LAST_9 
    SET MASKING POLICY SSN_MASK;

-- Apply BALANCE masking policy
ALTER TABLE CUSTOMER_SENSITIVE MODIFY COLUMN C_ACCTBAL 
    SET MASKING POLICY BALANCE_MASK;

-- Apply CREDIT CARD masking policy
ALTER TABLE CUSTOMER_SENSITIVE MODIFY COLUMN CREDIT_CARD 
    SET MASKING POLICY CREDIT_CARD_MASK;

-- Apply INCOME masking policy
ALTER TABLE CUSTOMER_SENSITIVE MODIFY COLUMN ESTIMATED_ANNUAL_INCOME 
    SET MASKING POLICY INCOME_MASK;

-- Xem policies đã apply
SELECT 
    POLICY_NAME,
    REF_ENTITY_NAME AS TABLE_NAME,
    REF_COLUMN_NAME AS COLUMN_NAME,
    POLICY_KIND
FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
WHERE POLICY_DB = 'TPCH_ANALYTICS_DB'
  AND POLICY_SCHEMA = 'ANALYTICS';

-- =====================================================
-- 4.4 TEST MASKING POLICIES
-- =====================================================

SELECT '═════════════════════════════════════════' AS SEPARATOR;
SELECT 'TESTING MASKING POLICIES WITH DIFFERENT ROLES' AS TEST_SECTION;
SELECT '═════════════════════════════════════════' AS SEPARATOR;

-- Test 1: As TPCH_ADMIN (should see FULL data)
USE ROLE TPCH_ADMIN;
SELECT 'TEST 1: TPCH_ADMIN Role - FULL ACCESS' AS TEST_NAME;
SELECT 
    C_CUSTKEY,
    C_NAME,
    EMAIL,
    C_PHONE,
    SSN_LAST_9,
    CREDIT_CARD,
    C_ACCTBAL,
    ESTIMATED_ANNUAL_INCOME
FROM CUSTOMER_SENSITIVE 
LIMIT 5;

-- Test 2: As TPCH_ANALYST (should see PARTIAL data)
USE ROLE TPCH_ANALYST;
SELECT 'TEST 2: TPCH_ANALYST Role - PARTIAL ACCESS' AS TEST_NAME;
SELECT 
    C_CUSTKEY,
    C_NAME,
    EMAIL,
    C_PHONE,
    SSN_LAST_9,
    CREDIT_CARD,
    C_ACCTBAL,
    ESTIMATED_ANNUAL_INCOME
FROM CUSTOMER_SENSITIVE 
LIMIT 5;

-- Test 3: As TPCH_DEVELOPER (should see CATEGORY data)
USE ROLE TPCH_DEVELOPER;
SELECT 'TEST 3: TPCH_DEVELOPER Role - CATEGORY ACCESS' AS TEST_NAME;
SELECT 
    C_CUSTKEY,
    C_NAME,
    EMAIL,
    C_PHONE,
    SSN_LAST_9,
    CREDIT_CARD,
    C_ACCTBAL,
    ESTIMATED_ANNUAL_INCOME
FROM CUSTOMER_SENSITIVE 
LIMIT 5;

-- Test 4: As TPCH_VIEWER (should see MASKED data)
USE ROLE TPCH_VIEWER;
SELECT 'TEST 4: TPCH_VIEWER Role - FULLY MASKED' AS TEST_NAME;
-- Note: VIEWER role may not have access to ANALYTICS schema
-- This query might fail, which is expected behavior
SELECT * FROM TPCH_ANALYTICS_DB.ANALYTICS.CUSTOMER_SENSITIVE LIMIT 5;

-- Switch back to ADMIN
USE ROLE TPCH_ADMIN;

-- =====================================================
-- 4.5 ROW ACCESS POLICIES (Bonus)
-- =====================================================

-- Create a row access policy to restrict data by region
-- Only show customers from user's assigned region

CREATE OR REPLACE ROW ACCESS POLICY REGIONAL_ACCESS_POLICY 
AS (region_name STRING) RETURNS BOOLEAN ->
    CASE 
        -- ADMIN sees all regions
        WHEN CURRENT_ROLE() IN ('TPCH_ADMIN', 'ACCOUNTADMIN') THEN TRUE
        -- ANALYST sees only AMERICA and EUROPE
        WHEN CURRENT_ROLE() = 'TPCH_ANALYST' AND region_name IN ('AMERICA', 'EUROPE') THEN TRUE
        -- DEVELOPER sees only AMERICA
        WHEN CURRENT_ROLE() = 'TPCH_DEVELOPER' AND region_name = 'AMERICA' THEN TRUE
        -- VIEWER sees nothing
        ELSE FALSE
    END;

-- Apply row access policy
ALTER TABLE CUSTOMER_SENSITIVE 
    ADD ROW ACCESS POLICY REGIONAL_ACCESS_POLICY ON (REGION);

-- Test row access policy
USE ROLE TPCH_ANALYST;
SELECT 'Row Access Policy Test - ANALYST (should see AMERICA & EUROPE only)' AS TEST_NAME;
SELECT 
    REGION,
    COUNT(*) AS CUSTOMER_COUNT,
    COUNT(DISTINCT NATION) AS NATION_COUNT
FROM CUSTOMER_SENSITIVE
GROUP BY REGION
ORDER BY REGION;

USE ROLE TPCH_DEVELOPER;
SELECT 'Row Access Policy Test - DEVELOPER (should see AMERICA only)' AS TEST_NAME;
SELECT 
    REGION,
    COUNT(*) AS CUSTOMER_COUNT,
    COUNT(DISTINCT NATION) AS NATION_COUNT
FROM CUSTOMER_SENSITIVE
GROUP BY REGION
ORDER BY REGION;

-- Switch back to ADMIN
USE ROLE TPCH_ADMIN;

-- =====================================================
-- 4.6 SECURE DATA SHARING
-- =====================================================

SELECT '═════════════════════════════════════════' AS SEPARATOR;
SELECT 'SECURE DATA SHARING SETUP' AS SECTION;
SELECT '═════════════════════════════════════════' AS SEPARATOR;

-- Create a secure view for external sharing
-- This view excludes sensitive PII fields
CREATE OR REPLACE SECURE VIEW CUSTOMER_SHARE_VIEW AS
SELECT 
    C_CUSTKEY AS CUSTOMER_ID,
    LEFT(C_NAME, 1) || '***' AS CUSTOMER_NAME_MASKED,  -- Masked name
    NATION,
    REGION,
    C_MKTSEGMENT AS MARKET_SEGMENT,
    -- Aggregate metrics only (no individual sensitive data)
    CASE 
        WHEN C_ACCTBAL < 0 THEN 'Negative'
        WHEN C_ACCTBAL < 1000 THEN 'Low'
        WHEN C_ACCTBAL < 5000 THEN 'Medium'
        WHEN C_ACCTBAL < 10000 THEN 'High'
        ELSE 'Very High'
    END AS BALANCE_CATEGORY
FROM CUSTOMER_SENSITIVE;

-- Test secure view
SELECT * FROM CUSTOMER_SHARE_VIEW LIMIT 10;

-- Create order summary view for sharing
CREATE OR REPLACE SECURE VIEW ORDER_SUMMARY_SHARE_VIEW AS
SELECT 
    O.O_ORDERKEY AS ORDER_ID,
    O.O_CUSTKEY AS CUSTOMER_ID,
    O.O_ORDERDATE AS ORDER_DATE,
    YEAR(O.O_ORDERDATE) AS ORDER_YEAR,
    QUARTER(O.O_ORDERDATE) AS ORDER_QUARTER,
    O.O_ORDERSTATUS AS STATUS,
    -- Rounded order value for privacy
    ROUND(O.O_TOTALPRICE, -2) AS ORDER_VALUE_ROUNDED,
    -- Item count instead of detailed line items
    COUNT(L.L_LINENUMBER) AS ITEM_COUNT,
    C.REGION,
    C.NATION
FROM ANALYTICS.ORDERS_SILVER O
JOIN ANALYTICS.LINEITEM_SILVER L ON O.O_ORDERKEY = L.L_ORDERKEY
JOIN CUSTOMER_SHARE_VIEW C ON O.O_CUSTKEY = C.CUSTOMER_ID
GROUP BY 
    O.O_ORDERKEY, O.O_CUSTKEY, O.O_ORDERDATE, 
    O.O_ORDERSTATUS, O.O_TOTALPRICE, C.REGION, C.NATION;

-- Test order summary view
SELECT * FROM ORDER_SUMMARY_SHARE_VIEW LIMIT 10;

-- =====================================================
-- 4.7 SECURE VIEW WITH AGGREGATIONS (Data Sharing Best Practice)
-- =====================================================

-- Create aggregated views that are safe for sharing
CREATE OR REPLACE SECURE VIEW REGIONAL_SALES_AGGREGATE AS
SELECT 
    REGION,
    NATION,
    YEAR(O_ORDERDATE) AS YEAR,
    QUARTER(O_ORDERDATE) AS QUARTER,
    COUNT(DISTINCT O_ORDERKEY) AS TOTAL_ORDERS,
    ROUND(SUM(O_TOTALPRICE), -3) AS TOTAL_REVENUE_ROUNDED,  -- Rounded for privacy
    COUNT(DISTINCT O_CUSTKEY) AS UNIQUE_CUSTOMERS,
    ROUND(AVG(O_TOTALPRICE), -2) AS AVG_ORDER_VALUE_ROUNDED
FROM ANALYTICS.ORDERS_SILVER O
JOIN CUSTOMER_SHARE_VIEW C ON O.O_CUSTKEY = C.CUSTOMER_ID
GROUP BY REGION, NATION, YEAR, QUARTER;

-- Test aggregated view
SELECT * FROM REGIONAL_SALES_AGGREGATE 
ORDER BY YEAR DESC, QUARTER DESC, TOTAL_REVENUE_ROUNDED DESC
LIMIT 20;

-- Create product performance view for sharing (no customer PII)
CREATE OR REPLACE SECURE VIEW PRODUCT_PERFORMANCE_SHARE_VIEW AS
SELECT 
    P.P_PARTKEY AS PRODUCT_ID,
    LEFT(P.P_NAME, 20) || '...' AS PRODUCT_NAME_TRUNCATED,
    P.P_MFGR AS MANUFACTURER,
    P.P_BRAND AS BRAND,
    P.P_TYPE AS PRODUCT_TYPE,
    COUNT(DISTINCT L.L_ORDERKEY) AS ORDER_COUNT,
    ROUND(SUM(L.L_QUANTITY), -1) AS TOTAL_QUANTITY_ROUNDED,
    ROUND(SUM(L.L_TOTAL_AMOUNT), -3) AS TOTAL_REVENUE_ROUNDED
FROM ANALYTICS.PART_SILVER P
JOIN ANALYTICS.LINEITEM_SILVER L ON P.P_PARTKEY = L.L_PARTKEY
GROUP BY P.P_PARTKEY, P.P_NAME, P.P_MFGR, P.P_BRAND, P.P_TYPE
HAVING COUNT(DISTINCT L.L_ORDERKEY) > 10  -- Only show products with sufficient volume
ORDER BY TOTAL_REVENUE_ROUNDED DESC;

-- Test product view
SELECT * FROM PRODUCT_PERFORMANCE_SHARE_VIEW LIMIT 20;

-- =====================================================
-- 4.8 AUDIT AND MONITORING
-- =====================================================

-- Create view to monitor policy applications
CREATE OR REPLACE VIEW SECURITY_POLICY_AUDIT AS
SELECT 
    POLICY_NAME,
    POLICY_KIND,
    POLICY_SCHEMA,
    REF_ENTITY_NAME AS TABLE_NAME,
    REF_COLUMN_NAME AS COLUMN_NAME,
    POLICY_STATUS
FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
WHERE POLICY_DB = 'TPCH_ANALYTICS_DB'
ORDER BY REF_ENTITY_NAME, REF_COLUMN_NAME;

-- View all applied policies
SELECT * FROM SECURITY_POLICY_AUDIT;

-- Create access log view (for monitoring)
SELECT 'Access Monitoring Setup:' AS INFO;
SELECT '1. Enable query history tracking' AS STEP
UNION ALL SELECT '2. Monitor CUSTOMER_SENSITIVE table access patterns'
UNION ALL SELECT '3. Review which roles access sensitive columns'
UNION ALL SELECT '4. Set up alerts for unusual access patterns';

-- =====================================================
-- TỔNG KẾT
-- =====================================================

USE ROLE TPCH_ADMIN;

SELECT '═════════════════════════════════════════' AS SEPARATOR;
SELECT '✅ PHẦN 4 HOÀN THÀNH!' AS STATUS;
SELECT '═════════════════════════════════════════' AS SEPARATOR;

SELECT 'Security Features Implemented:' AS SUMMARY;
SELECT '✓ Data Masking Policies:' AS FEATURE
UNION ALL SELECT '  - EMAIL_MASK: Progressive masking by role'
UNION ALL SELECT '  - PHONE_MASK: Last 4 digits visible to analysts'
UNION ALL SELECT '  - SSN_MASK: Secure SSN protection'
UNION ALL SELECT '  - BALANCE_MASK: Financial data protection'
UNION ALL SELECT '  - CREDIT_CARD_MASK: PCI compliance masking'
UNION ALL SELECT '  - INCOME_MASK: Salary data protection'
UNION ALL SELECT ''
UNION ALL SELECT '✓ Row Access Policies:'
UNION ALL SELECT '  - REGIONAL_ACCESS_POLICY: Region-based filtering'
UNION ALL SELECT ''
UNION ALL SELECT '✓ Secure Views for Data Sharing:'
UNION ALL SELECT '  - CUSTOMER_SHARE_VIEW: Masked customer data'
UNION ALL SELECT '  - ORDER_SUMMARY_SHARE_VIEW: Aggregated orders'
UNION ALL SELECT '  - REGIONAL_SALES_AGGREGATE: Regional metrics'
UNION ALL SELECT '  - PRODUCT_PERFORMANCE_SHARE_VIEW: Product insights'
UNION ALL SELECT ''
UNION ALL SELECT '✓ Role-Based Access Control:'
UNION ALL SELECT '  - TPCH_ADMIN: Full access to all data'
UNION ALL SELECT '  - TPCH_ANALYST: Partial access with masking'
UNION ALL SELECT '  - TPCH_DEVELOPER: Category-level data'
UNION ALL SELECT '  - TPCH_VIEWER: Heavily masked/restricted access';

