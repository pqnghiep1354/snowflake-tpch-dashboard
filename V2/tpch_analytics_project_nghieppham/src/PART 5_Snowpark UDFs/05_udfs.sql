-- =====================================================
-- PHẦN 5: USER-DEFINED FUNCTIONS (UDFs)
-- SQL UDFs và Python UDFs cho TPC-H Analytics
-- =====================================================

USE ROLE TPCH_DEVELOPER;
USE DATABASE TPCH_ANALYTICS_DB;
USE SCHEMA UDFS;
USE WAREHOUSE TPCH_WH;

-- =====================================================
-- 5.1 SQL UDFs
-- =====================================================

-- UDF 1: Phân loại khách hàng theo revenue
CREATE OR REPLACE FUNCTION CLASSIFY_CUSTOMER_BY_REVENUE(total_revenue NUMBER)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN total_revenue >= 500000 THEN 'VIP'
        WHEN total_revenue >= 200000 THEN 'GOLD'
        WHEN total_revenue >= 100000 THEN 'SILVER'
        WHEN total_revenue >= 50000 THEN 'BRONZE'
        ELSE 'STANDARD'
    END
$$;

-- Test UDF 1
SELECT 
    C_CUSTKEY,
    C_NAME,
    SUM(O_TOTALPRICE) AS TOTAL_REVENUE,
    CLASSIFY_CUSTOMER_BY_REVENUE(SUM(O_TOTALPRICE)) AS CUSTOMER_TIER
FROM TPCH_ANALYTICS_DB.ANALYTICS.CUSTOMER_SILVER C
JOIN TPCH_ANALYTICS_DB.ANALYTICS.ORDERS_SILVER O ON C.C_CUSTKEY = O.O_CUSTKEY
GROUP BY C_CUSTKEY, C_NAME
ORDER BY TOTAL_REVENUE DESC
LIMIT 20;

-- UDF 2: Validate phone number format
CREATE OR REPLACE FUNCTION VALIDATE_PHONE_NUMBER(phone_number STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    CASE
        WHEN phone_number IS NULL THEN FALSE
        WHEN LENGTH(TRIM(phone_number)) < 10 THEN FALSE
        WHEN REGEXP_LIKE(phone_number, '^[0-9+\\-\\(\\)\\s]+$') THEN TRUE
        ELSE FALSE
    END
$$;

-- Test UDF 2
SELECT 
    C_CUSTKEY,
    C_NAME,
    C_PHONE,
    VALIDATE_PHONE_NUMBER(C_PHONE) AS IS_VALID_PHONE
FROM TPCH_ANALYTICS_DB.STAGING.CUSTOMER
LIMIT 20;

-- UDF 3: Validate email format
CREATE OR REPLACE FUNCTION VALIDATE_EMAIL(email STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    CASE
        WHEN email IS NULL THEN FALSE
        WHEN REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') THEN TRUE
        ELSE FALSE
    END
$$;

-- Test UDF 3
SELECT 
    'test@example.com' AS EMAIL,
    VALIDATE_EMAIL('test@example.com') AS IS_VALID
UNION ALL
SELECT 'invalid.email', VALIDATE_EMAIL('invalid.email')
UNION ALL
SELECT 'user@domain', VALIDATE_EMAIL('user@domain')
UNION ALL
SELECT 'valid.email@company.co.uk', VALIDATE_EMAIL('valid.email@company.co.uk');

-- UDF 4: Calculate discount tier
CREATE OR REPLACE FUNCTION GET_DISCOUNT_TIER(discount NUMBER)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE
        WHEN discount IS NULL OR discount = 0 THEN 'No Discount'
        WHEN discount <= 0.05 THEN 'Low (0-5%)'
        WHEN discount <= 0.10 THEN 'Medium (5-10%)'
        WHEN discount <= 0.15 THEN 'High (10-15%)'
        ELSE 'Very High (>15%)'
    END
$$;

-- Test UDF 4
SELECT 
    L_ORDERKEY,
    L_LINENUMBER,
    L_DISCOUNT,
    GET_DISCOUNT_TIER(L_DISCOUNT) AS DISCOUNT_TIER,
    L_EXTENDEDPRICE,
    L_EXTENDEDPRICE * L_DISCOUNT AS DISCOUNT_AMOUNT
FROM TPCH_ANALYTICS_DB.ANALYTICS.LINEITEM_SILVER
LIMIT 20;

-- UDF 5: Format currency
CREATE OR REPLACE FUNCTION FORMAT_CURRENCY(amount NUMBER)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE
        WHEN amount IS NULL THEN '$0.00'
        WHEN ABS(amount) >= 1000000 THEN TO_VARCHAR(ROUND(amount / 1000000, 2), '999,999.99') || 'M'
        WHEN ABS(amount) >= 1000 THEN TO_VARCHAR(ROUND(amount / 1000, 2), '999,999.99') || 'K'
        ELSE TO_VARCHAR(amount, '$999,999.99')
    END
$$;

-- Test UDF 5
SELECT 
    FORMAT_CURRENCY(1234567.89) AS FORMATTED_1,
    FORMAT_CURRENCY(45678.90) AS FORMATTED_2,
    FORMAT_CURRENCY(123.45) AS FORMATTED_3,
    FORMAT_CURRENCY(1234567890.12) AS FORMATTED_4;

-- UDF 6: Calculate order priority score
CREATE OR REPLACE FUNCTION GET_PRIORITY_SCORE(order_priority STRING, total_price NUMBER)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
    CASE order_priority
        WHEN '1-URGENT' THEN 100 + (total_price / 1000)
        WHEN '2-HIGH' THEN 80 + (total_price / 1500)
        WHEN '3-MEDIUM' THEN 60 + (total_price / 2000)
        WHEN '4-NOT SPECIFIED' THEN 40 + (total_price / 2500)
        WHEN '5-LOW' THEN 20 + (total_price / 3000)
        ELSE 10
    END
$$;

-- Test UDF 6
SELECT 
    O_ORDERKEY,
    O_ORDERPRIORITY,
    O_TOTALPRICE,
    GET_PRIORITY_SCORE(O_ORDERPRIORITY, O_TOTALPRICE) AS PRIORITY_SCORE
FROM TPCH_ANALYTICS_DB.ANALYTICS.ORDERS_SILVER
ORDER BY PRIORITY_SCORE DESC
LIMIT 20;

-- UDF 7: Calculate shipping delay (days between commit and receipt)
CREATE OR REPLACE FUNCTION CALCULATE_SHIPPING_DELAY(
    ship_date DATE, 
    commit_date DATE, 
    receipt_date DATE
)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
    CASE
        WHEN receipt_date IS NULL OR commit_date IS NULL THEN NULL
        ELSE DATEDIFF('DAY', commit_date, receipt_date)
    END
$$;

-- Test UDF 7
SELECT 
    L_ORDERKEY,
    L_COMMITDATE,
    L_SHIPDATE,
    L_RECEIPTDATE,
    CALCULATE_SHIPPING_DELAY(L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE) AS DELAY_DAYS,
    CASE
        WHEN CALCULATE_SHIPPING_DELAY(L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE) <= 0 THEN 'On Time'
        WHEN CALCULATE_SHIPPING_DELAY(L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE) <= 7 THEN 'Slight Delay'
        WHEN CALCULATE_SHIPPING_DELAY(L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE) <= 14 THEN 'Moderate Delay'
        ELSE 'Severe Delay'
    END AS DELAY_CATEGORY
FROM TPCH_ANALYTICS_DB.ANALYTICS.LINEITEM_SILVER
WHERE L_RECEIPTDATE IS NOT NULL
LIMIT 20;

-- UDF 8: Get season from date
CREATE OR REPLACE FUNCTION GET_SEASON(order_date DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN MONTH(order_date) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(order_date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(order_date) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(order_date) IN (9, 10, 11) THEN 'Fall'
        ELSE 'Unknown'
    END
$$;

-- Test UDF 8
SELECT 
    O_ORDERKEY,
    O_ORDERDATE,
    GET_SEASON(O_ORDERDATE) AS SEASON,
    O_TOTALPRICE
FROM TPCH_ANALYTICS_DB.ANALYTICS.ORDERS_SILVER
LIMIT 20;

-- UDF 9: Calculate customer lifetime value score
CREATE OR REPLACE FUNCTION CALCULATE_CLV_SCORE(
    recency_days NUMBER,
    frequency NUMBER,
    monetary NUMBER
)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
    CASE
        WHEN recency_days IS NULL OR frequency IS NULL OR monetary IS NULL THEN 0
        ELSE 
            -- Weighted score: Recency (30%), Frequency (30%), Monetary (40%)
            ((365 - LEAST(recency_days, 365)) / 365 * 0.3) * 100 +
            (LEAST(frequency, 50) / 50 * 0.3) * 100 +
            (LEAST(monetary, 500000) / 500000 * 0.4) * 100
    END
$$;

-- Test UDF 9
SELECT 
    C_CUSTKEY,
    C_NAME,
    RECENCY_DAYS,
    FREQUENCY,
    MONETARY,
    CALCULATE_CLV_SCORE(RECENCY_DAYS, FREQUENCY, MONETARY) AS CLV_SCORE,
    RFM_SEGMENT
FROM TPCH_ANALYTICS_DB.REPORTS.CUSTOMER_METRICS
ORDER BY CLV_SCORE DESC
LIMIT 20;

-- UDF 10: Categorize product by price range
CREATE OR REPLACE FUNCTION CATEGORIZE_PRODUCT_PRICE(retail_price NUMBER)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE
        WHEN retail_price IS NULL THEN 'Unknown'
        WHEN retail_price < 1000 THEN 'Budget (< $1,000)'
        WHEN retail_price < 2000 THEN 'Economy ($1,000-$2,000)'
        WHEN retail_price < 3000 THEN 'Mid-Range ($2,000-$3,000)'
        WHEN retail_price < 4000 THEN 'Premium ($3,000-$4,000)'
        ELSE 'Luxury (> $4,000)'
    END
$$;

-- Test UDF 10
SELECT 
    P_PARTKEY,
    P_NAME,
    P_RETAILPRICE,
    CATEGORIZE_PRODUCT_PRICE(P_RETAILPRICE) AS PRICE_CATEGORY
FROM TPCH_ANALYTICS_DB.ANALYTICS.PART_SILVER
ORDER BY P_RETAILPRICE DESC
LIMIT 20;

-- =====================================================
-- 5.2 PYTHON UDFs
-- =====================================================

-- Python UDF 1: Calculate customer satisfaction score based on multiple factors
CREATE OR REPLACE FUNCTION CALCULATE_SATISFACTION_SCORE(
    on_time_delivery_rate FLOAT,
    avg_discount FLOAT,
    order_frequency NUMBER
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'calculate_score'
AS
$$
def calculate_score(on_time_rate, discount, frequency):
    """
    Calculate customer satisfaction score (0-100)
    Based on delivery performance, discount benefits, and engagement
    """
    if on_time_rate is None or discount is None or frequency is None:
        return 0.0
    
    # Normalize inputs
    delivery_score = min(on_time_rate * 100, 100) * 0.4  # 40% weight
    discount_score = min(discount * 1000, 100) * 0.3     # 30% weight
    frequency_score = min(frequency * 2, 100) * 0.3      # 30% weight
    
    total_score = delivery_score + discount_score + frequency_score
    
    return round(total_score, 2)
$$;

-- Test Python UDF 1
SELECT 
    CALCULATE_SATISFACTION_SCORE(0.95, 0.08, 15) AS HIGH_SATISFACTION,
    CALCULATE_SATISFACTION_SCORE(0.75, 0.05, 8) AS MEDIUM_SATISFACTION,
    CALCULATE_SATISFACTION_SCORE(0.50, 0.02, 3) AS LOW_SATISFACTION;

-- Python UDF 2: Clean and standardize phone numbers
CREATE OR REPLACE FUNCTION CLEAN_PHONE_NUMBER(phone STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'clean_phone'
AS
$$
def clean_phone(phone_str):
    """
    Clean and standardize phone number format
    """
    if phone_str is None:
        return None
    
    # Remove all non-digit characters
    digits = ''.join(c for c in phone_str if c.isdigit())
    
    # Format as (XXX) XXX-XXXX if we have 10+ digits
    if len(digits) >= 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    else:
        return digits
$$;

-- Test Python UDF 2
SELECT 
    C_PHONE AS ORIGINAL,
    CLEAN_PHONE_NUMBER(C_PHONE) AS CLEANED
FROM TPCH_ANALYTICS_DB.STAGING.CUSTOMER
LIMIT 10;

-- Python UDF 3: Calculate profitability index
CREATE OR REPLACE FUNCTION CALCULATE_PROFITABILITY_INDEX(
    revenue FLOAT,
    cost FLOAT,
    quantity NUMBER
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'calc_index'
AS
$$
def calc_index(rev, cst, qty):
    """
    Calculate profitability index
    Formula: (Revenue - Cost) / Cost * 100
    Adjusted for quantity volume
    """
    if rev is None or cst is None or qty is None or cst == 0:
        return 0.0
    
    profit_margin = ((rev - cst) / cst) * 100
    volume_factor = min(qty / 100, 2.0)  # Cap at 2x
    
    profitability_index = profit_margin * volume_factor
    
    return round(profitability_index, 2)
$$;

-- Test Python UDF 3
SELECT 
    PS_PARTKEY,
    PS_SUPPKEY,
    PS_SUPPLYCOST,
    PS_AVAILQTY,
    P_RETAILPRICE,
    CALCULATE_PROFITABILITY_INDEX(
        P_RETAILPRICE, 
        PS_SUPPLYCOST, 
        PS_AVAILQTY
    ) AS PROFITABILITY_INDEX
FROM TPCH_ANALYTICS_DB.STAGING.PARTSUPP PS
JOIN TPCH_ANALYTICS_DB.STAGING.PART P ON PS.PS_PARTKEY = P.P_PARTKEY
LIMIT 20;

-- Python UDF 4: Generate customer engagement score
CREATE OR REPLACE FUNCTION GENERATE_ENGAGEMENT_SCORE(
    days_since_last_order NUMBER,
    total_orders NUMBER,
    avg_order_value FLOAT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'engagement_score'
AS
$$
def engagement_score(days, orders, avg_value):
    """
    Generate customer engagement score and category
    Returns: JSON-like string with score and category
    """
    if days is None or orders is None or avg_value is None:
        return "{'score': 0, 'category': 'Unknown'}"
    
    # Calculate component scores
    recency_score = max(0, 100 - (days / 3.65))  # Decay over ~1 year
    frequency_score = min(orders * 5, 100)
    monetary_score = min(avg_value / 100, 100)
    
    # Weighted average
    total_score = (
        recency_score * 0.4 + 
        frequency_score * 0.3 + 
        monetary_score * 0.3
    )
    
    # Categorize
    if total_score >= 80:
        category = "Highly Engaged"
    elif total_score >= 60:
        category = "Engaged"
    elif total_score >= 40:
        category = "Moderately Engaged"
    elif total_score >= 20:
        category = "Low Engagement"
    else:
        category = "At Risk"
    
    return f"{{'score': {round(total_score, 1)}, 'category': '{category}'}}"
$$;

-- Test Python UDF 4
SELECT 
    C_CUSTKEY,
    C_NAME,
    RECENCY_DAYS,
    FREQUENCY,
    MONETARY / FREQUENCY AS AVG_ORDER_VALUE,
    GENERATE_ENGAGEMENT_SCORE(
        RECENCY_DAYS, 
        FREQUENCY, 
        MONETARY / FREQUENCY
    ) AS ENGAGEMENT_INFO
FROM TPCH_ANALYTICS_DB.REPORTS.CUSTOMER_METRICS
LIMIT 20;

-- =====================================================
-- 5.3 ADVANCED UDFs - TABLE FUNCTIONS
-- =====================================================

-- Table UDF: Split customer into cohorts
CREATE OR REPLACE FUNCTION GET_CUSTOMER_COHORTS()
RETURNS TABLE (
    COHORT_NAME STRING,
    CUSTOMER_COUNT NUMBER,
    AVG_LIFETIME_VALUE NUMBER,
    AVG_ORDER_FREQUENCY NUMBER
)
LANGUAGE SQL
AS
$$
    SELECT 
        RFM_SEGMENT AS COHORT_NAME,
        COUNT(C_CUSTKEY) AS CUSTOMER_COUNT,
        AVG(LIFETIME_VALUE) AS AVG_LIFETIME_VALUE,
        AVG(FREQUENCY) AS AVG_ORDER_FREQUENCY
    FROM TPCH_ANALYTICS_DB.REPORTS.CUSTOMER_METRICS
    GROUP BY RFM_SEGMENT
    ORDER BY AVG_LIFETIME_VALUE DESC
$$;

-- Test Table UDF
SELECT * FROM TABLE(GET_CUSTOMER_COHORTS());

-- =====================================================
-- 5.4 TESTING AND VALIDATION
-- =====================================================

-- Create a comprehensive test report
SELECT '═══════════════════════════════════════' AS SEPARATOR;
SELECT 'UDF TESTING REPORT' AS SECTION;
SELECT '═══════════════════════════════════════' AS SEPARATOR;

-- Test all SQL UDFs
SELECT 'SQL UDF Tests:' AS TEST_CATEGORY;

SELECT 
    'CLASSIFY_CUSTOMER_BY_REVENUE' AS UDF_NAME,
    CLASSIFY_CUSTOMER_BY_REVENUE(550000) AS TEST_VIP,
    CLASSIFY_CUSTOMER_BY_REVENUE(250000) AS TEST_GOLD,
    CLASSIFY_CUSTOMER_BY_REVENUE(75000) AS TEST_BRONZE;

SELECT 
    'VALIDATE_PHONE_NUMBER' AS UDF_NAME,
    VALIDATE_PHONE_NUMBER('555-123-4567') AS TEST_VALID,
    VALIDATE_PHONE_NUMBER('invalid') AS TEST_INVALID;

SELECT 
    'VALIDATE_EMAIL' AS UDF_NAME,
    VALIDATE_EMAIL('test@example.com') AS TEST_VALID,
    VALIDATE_EMAIL('not-an-email') AS TEST_INVALID;

SELECT 
    'FORMAT_CURRENCY' AS UDF_NAME,
    FORMAT_CURRENCY(1500000) AS TEST_MILLIONS,
    FORMAT_CURRENCY(75000) AS TEST_THOUSANDS;

-- Test Python UDFs
SELECT 'Python UDF Tests:' AS TEST_CATEGORY;

SELECT 
    'CALCULATE_SATISFACTION_SCORE' AS UDF_NAME,
    CALCULATE_SATISFACTION_SCORE(0.95, 0.10, 20) AS SCORE;

SELECT 
    'CLEAN_PHONE_NUMBER' AS UDF_NAME,
    CLEAN_PHONE_NUMBER('1-555-123-4567') AS CLEANED;

-- =====================================================
-- 5.5 UDF USAGE EXAMPLES IN QUERIES
-- =====================================================

-- Example 1: Classify all customers
SELECT 
    C.C_CUSTKEY,
    C.C_NAME,
    C.C_NATION_NAME,
    COUNT(O.O_ORDERKEY) AS ORDER_COUNT,
    SUM(O.O_TOTALPRICE) AS TOTAL_REVENUE,
    CLASSIFY_CUSTOMER_BY_REVENUE(SUM(O.O_TOTALPRICE)) AS CUSTOMER_TIER,
    FORMAT_CURRENCY(SUM(O.O_TOTALPRICE)) AS FORMATTED_REVENUE
FROM TPCH_ANALYTICS_DB.ANALYTICS.CUSTOMER_SILVER C
LEFT JOIN TPCH_ANALYTICS_DB.ANALYTICS.ORDERS_SILVER O ON C.C_CUSTKEY = O.O_CUSTKEY
GROUP BY C.C_CUSTKEY, C.C_NAME, C.C_NATION_NAME
ORDER BY TOTAL_REVENUE DESC
LIMIT 50;

-- Example 2: Analyze seasonal sales patterns
SELECT 
    GET_SEASON(O_ORDERDATE) AS SEASON,
    YEAR(O_ORDERDATE) AS YEAR,
    COUNT(O_ORDERKEY) AS ORDER_COUNT,
    SUM(O_TOTALPRICE) AS TOTAL_REVENUE,
    FORMAT_CURRENCY(SUM(O_TOTALPRICE)) AS FORMATTED_REVENUE,
    AVG(O_TOTALPRICE) AS AVG_ORDER_VALUE
FROM TPCH_ANALYTICS_DB.ANALYTICS.ORDERS_SILVER
GROUP BY GET_SEASON(O_ORDERDATE), YEAR(O_ORDERDATE)
ORDER BY YEAR, 
    CASE GET_SEASON(O_ORDERDATE)
        WHEN 'Winter' THEN 1
        WHEN 'Spring' THEN 2
        WHEN 'Summer' THEN 3
        WHEN 'Fall' THEN 4
    END;

-- Example 3: Product pricing analysis with categorization
SELECT 
    CATEGORIZE_PRODUCT_PRICE(P_RETAILPRICE) AS PRICE_CATEGORY,
    COUNT(P_PARTKEY) AS PRODUCT_COUNT,
    AVG(P_RETAILPRICE) AS AVG_PRICE,
    MIN(P_RETAILPRICE) AS MIN_PRICE,
    MAX(P_RETAILPRICE) AS MAX_PRICE
FROM TPCH_ANALYTICS_DB.ANALYTICS.PART_SILVER
GROUP BY CATEGORIZE_PRODUCT_PRICE(P_RETAILPRICE)
ORDER BY AVG_PRICE;

-- =====================================================
-- TỔNG KẾT
-- =====================================================

SELECT '═══════════════════════════════════════' AS SEPARATOR;
SELECT '✅ PHẦN 5 - UDFs HOÀN THÀNH!' AS STATUS;
SELECT '═══════════════════════════════════════' AS SEPARATOR;

SELECT 'UDF Summary:' AS SUMMARY;
SELECT '✓ SQL UDFs Created: 10' AS DETAIL
UNION ALL SELECT '  1. CLASSIFY_CUSTOMER_BY_REVENUE - Customer tier classification'
UNION ALL SELECT '  2. VALIDATE_PHONE_NUMBER - Phone validation'
UNION ALL SELECT '  3. VALIDATE_EMAIL - Email validation'
UNION ALL SELECT '  4. GET_DISCOUNT_TIER - Discount categorization'
UNION ALL SELECT '  5. FORMAT_CURRENCY - Currency formatting'
UNION ALL SELECT '  6. GET_PRIORITY_SCORE - Priority scoring'
UNION ALL SELECT '  7. CALCULATE_SHIPPING_DELAY - Delivery delay calculation'
UNION ALL SELECT '  8. GET_SEASON - Seasonal classification'
UNION ALL SELECT '  9. CALCULATE_CLV_SCORE - Customer lifetime value'
UNION ALL SELECT '  10. CATEGORIZE_PRODUCT_PRICE - Product price category'
UNION ALL SELECT ''
UNION ALL SELECT '✓ Python UDFs Created: 4'
UNION ALL SELECT '  1. CALCULATE_SATISFACTION_SCORE - Customer satisfaction'
UNION ALL SELECT '  2. CLEAN_PHONE_NUMBER - Phone number cleaning'
UNION ALL SELECT '  3. CALCULATE_PROFITABILITY_INDEX - Profitability analysis'
UNION ALL SELECT '  4. GENERATE_ENGAGEMENT_SCORE - Engagement scoring'
UNION ALL SELECT ''
UNION ALL SELECT '✓ Table Functions: 1'
UNION ALL SELECT '  1. GET_CUSTOMER_COHORTS - Cohort analysis';

-- List all UDFs in the schema
SHOW FUNCTIONS IN SCHEMA TPCH_ANALYTICS_DB.UDFS;
