"""
=============================================================================
PHẦN 5: SNOWPARK PYTHON ANALYTICS
TPC-H Analytics Project - Customer Segmentation & Sales Analysis
=============================================================================
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, max as max_, min as min_, sum as sum_, avg, count, 
    count_distinct, when, lit, current_date, datediff, 
    date_trunc, year, month, quarter, dayofweek,
    ntile, row_number, rank, dense_rank,
    round as round_, lag
)
from snowflake.snowpark.types import IntegerType, FloatType, StringType, DateType
from snowflake.snowpark.window import Window
import pandas as pd
import os
from datetime import datetime
import json

# =============================================================================
# CONNECTION SETUP
# =============================================================================

def create_snowpark_session():
    """
    Create Snowpark session by reading from config.json
    """
    # Đường dẫn đến file config.json (cùng thư mục với file py)
    config_file_path = 'config.json'
    
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"❌ Không tìm thấy file {config_file_path}. Hãy tạo file này trước!")

    try:
        with open(config_file_path, 'r') as f:
            connection_parameters = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Lỗi định dạng file {config_file_path}: {str(e)}")
        print("💡 Gợi ý: File JSON phải sử dụng dấu ngoặc kép (\") cho tên thuộc tính và chuỗi, không dùng dấu nháy đơn (').")
        raise e

    try:
        # Create session
        session = Session.builder.configs(connection_parameters).create()
        print(f"✅ Connected to Snowflake account: {connection_parameters['account']}")
        print(f"   Current role: {session.get_current_role()}")
        print(f"   Current database: {session.get_current_database()}")
        print(f"   Current schema: {session.get_current_schema()}")
        print(f"   Current warehouse: {session.get_current_warehouse()}")
    
        return session

    except Exception as e:
            print(f"❌ Lỗi kết nối: {str(e)}")
            raise e

# =============================================================================
# 5.1 CUSTOMER SEGMENTATION WITH RFM ANALYSIS
# =============================================================================

def calculate_rfm_segmentation(session):
    """
    Perform RFM (Recency, Frequency, Monetary) Analysis
    to segment customers based on their purchasing behavior
    """
    print("\n" + "="*80)
    print("CUSTOMER RFM SEGMENTATION")
    print("="*80)
    
    # Load tables
    customers = session.table("CUSTOMER_SILVER")
    orders = session.table("ORDERS_SILVER")
    lineitems = session.table("LINEITEM_SILVER")
    
    # Calculate RFM metrics
    print("\n📊 Calculating RFM metrics...")
    
    # Join orders with lineitems to get actual revenue
    order_revenue = (orders
        .join(lineitems, orders["O_ORDERKEY"] == lineitems["L_ORDERKEY"])
        .select(
            orders["O_ORDERKEY"],
            orders["O_CUSTKEY"],
            orders["O_ORDERDATE"],
            lineitems["L_TOTAL_AMOUNT"]
        )
    )
    
    # Calculate RFM values
    rfm_df = (customers
        .join(order_revenue, customers["C_CUSTKEY"] == order_revenue["O_CUSTKEY"], "left")
        .group_by("C_CUSTKEY", "C_NAME", "C_NATION_NAME", "C_REGION_NAME", "C_MKTSEGMENT")
        .agg([
            max_("O_ORDERDATE").alias("LAST_ORDER_DATE"),
            min_("O_ORDERDATE").alias("FIRST_ORDER_DATE"),
            count("O_ORDERKEY").alias("FREQUENCY"),
            sum_("L_TOTAL_AMOUNT").alias("MONETARY")
        ])
        .with_column("RECENCY_DAYS", 
            datediff("day", col("LAST_ORDER_DATE"), current_date()))
    )
    
    # Calculate RFM scores using NTILE (1-5 scale)
    print("📈 Calculating RFM scores (1-5 scale)...")
    
    # Define windows for scoring
    recency_window = Window.order_by(col("RECENCY_DAYS"))  # Lower recency is better
    frequency_window = Window.order_by(col("FREQUENCY").desc())  # Higher frequency is better
    monetary_window = Window.order_by(col("MONETARY").desc())  # Higher monetary is better
    
    rfm_scored = (rfm_df
        .with_column("R_SCORE", 6 - ntile(5).over(recency_window))  # Invert: 1=oldest, 5=newest
        .with_column("F_SCORE", ntile(5).over(frequency_window))
        .with_column("M_SCORE", ntile(5).over(monetary_window))
    )
    
    # Create RFM segment
    rfm_final = (rfm_scored
        .with_column("RFM_SCORE", 
            col("R_SCORE").cast(StringType()) + 
            col("F_SCORE").cast(StringType()) + 
            col("M_SCORE").cast(StringType()))
        .with_column("RFM_SEGMENT",
            when((col("R_SCORE") >= 4) & (col("F_SCORE") >= 4) & (col("M_SCORE") >= 4), 
                 lit("Champion"))
            .when((col("R_SCORE") >= 3) & (col("F_SCORE") >= 3) & (col("M_SCORE") >= 3), 
                  lit("Loyal"))
            .when((col("R_SCORE") >= 4) & (col("F_SCORE") <= 2), 
                  lit("Promising"))
            .when((col("R_SCORE") <= 2) & (col("F_SCORE") >= 3), 
                  lit("At Risk"))
            .when((col("R_SCORE") <= 2) & (col("F_SCORE") <= 2), 
                  lit("Lost"))
            .otherwise(lit("Need Attention"))
        )
        .with_column("LIFETIME_VALUE", col("MONETARY"))
        .with_column("AVG_ORDER_VALUE", 
            when(col("FREQUENCY") > 0, col("MONETARY") / col("FREQUENCY"))
            .otherwise(lit(0)))
    )
    
    # Save to Snowflake table
    print("\n💾 Saving RFM results to CUSTOMER_RFM_SCORES table...")
    rfm_final.write.mode("overwrite").save_as_table("CUSTOMER_RFM_SCORES")
    
    # Display summary statistics
    print(f"\n✅ RFM Segmentation completed!")
    print(f"   Total customers processed: {rfm_final.count()}")
    
    # Show segment distribution
    print("\n📊 Customer Segment Distribution:")
    segment_dist = (rfm_final
        .group_by("RFM_SEGMENT")
        .agg([
            count("C_CUSTKEY").alias("CUSTOMER_COUNT"),
            avg("RECENCY_DAYS").alias("AVG_RECENCY"),
            avg("FREQUENCY").alias("AVG_FREQUENCY"),
            avg("MONETARY").alias("AVG_MONETARY")
        ])
        .order_by(col("CUSTOMER_COUNT").desc())
    )
    segment_dist.show()
    
    # Show top 10 champions
    print("\n🏆 Top 10 Champion Customers:")
    champions = (rfm_final
        .filter(col("RFM_SEGMENT") == "Champion")
        .select(
            "C_CUSTKEY", "C_NAME", "C_NATION_NAME", "C_REGION_NAME",
            "FREQUENCY", "MONETARY", "RECENCY_DAYS", "RFM_SCORE"
        )
        .order_by(col("MONETARY").desc())
        .limit(10)
    )
    champions.show()
    
    return rfm_final

# =============================================================================
# 5.2 SALES TREND ANALYSIS
# =============================================================================

def analyze_sales_trends(session):
    """
    Analyze sales trends over time with various aggregations
    """
    print("\n" + "="*80)
    print("SALES TREND ANALYSIS")
    print("="*80)
    
    # Load tables
    orders = session.table("ORDERS_SILVER")
    lineitems = session.table("LINEITEM_SILVER")
    
    # Join orders with lineitems
    print("\n📊 Analyzing sales trends...")
    
    order_details = (orders
        .join(lineitems, orders["O_ORDERKEY"] == lineitems["L_ORDERKEY"])
        .select(
            orders["O_ORDERKEY"],
            orders["O_ORDERDATE"],
            orders["O_ORDER_YEAR"],
            orders["O_ORDER_MONTH"],
            orders["O_ORDER_QUARTER"],
            orders["O_CUSTKEY"],
            lineitems["L_QUANTITY"],
            lineitems["L_TOTAL_AMOUNT"]
        )
    )
    
    # Monthly aggregation
    print("\n📈 Monthly Sales Trends:")
    monthly_sales = (order_details
        .with_column("MONTH_START", date_trunc("month", col("O_ORDERDATE")))
        .group_by("MONTH_START", "O_ORDER_YEAR", "O_ORDER_MONTH")
        .agg([
            count_distinct("O_ORDERKEY").alias("ORDER_COUNT"),
            count_distinct("O_CUSTKEY").alias("UNIQUE_CUSTOMERS"),
            sum_("L_TOTAL_AMOUNT").alias("TOTAL_REVENUE"),
            avg("L_TOTAL_AMOUNT").alias("AVG_ORDER_ITEM_VALUE"),
            sum_("L_QUANTITY").alias("TOTAL_ITEMS_SOLD")
        ])
        .sort("MONTH_START")
    )
    
    # Calculate month-over-month growth
    window_spec = Window.order_by("MONTH_START")
    
    monthly_with_growth = (monthly_sales
        .with_column("PREV_MONTH_REVENUE", 
            lag(col("TOTAL_REVENUE"), 1).over(window_spec))
        .with_column("MOM_GROWTH_PCT",
            when(col("PREV_MONTH_REVENUE").is_not_null(),
                ((col("TOTAL_REVENUE") - col("PREV_MONTH_REVENUE")) / 
                 col("PREV_MONTH_REVENUE") * 100))
            .otherwise(lit(None))
        )
    )
    
    # Save monthly trends
    monthly_with_growth.write.mode("overwrite").save_as_table("MONTHLY_SALES_TRENDS")
    print(f"\n✅ Monthly sales trends saved to MONTHLY_SALES_TRENDS table")
    
    # Show recent months
    print("\n📊 Recent Monthly Performance (Last 12 months):")
    recent_months = monthly_with_growth.order_by(col("MONTH_START").desc()).limit(12)
    recent_months.show()
    
    # Quarterly aggregation
    print("\n📈 Quarterly Sales Trends:")
    quarterly_sales = (order_details
        .group_by("O_ORDER_YEAR", "O_ORDER_QUARTER")
        .agg([
            count_distinct("O_ORDERKEY").alias("ORDER_COUNT"),
            sum_("L_TOTAL_AMOUNT").alias("TOTAL_REVENUE"),
            avg("L_TOTAL_AMOUNT").alias("AVG_ORDER_VALUE"),
            sum_("L_QUANTITY").alias("TOTAL_ITEMS_SOLD")
        ])
        .sort("O_ORDER_YEAR", "O_ORDER_QUARTER")
    )
    
    quarterly_sales.write.mode("overwrite").save_as_table("QUARTERLY_SALES_TRENDS")
    print(f"\n✅ Quarterly sales trends saved to QUARTERLY_SALES_TRENDS table")
    quarterly_sales.show()
    
    # Day of week analysis
    print("\n📊 Sales by Day of Week:")
    dow_sales = (order_details
        .with_column("DAY_OF_WEEK", dayofweek(col("O_ORDERDATE")))
        .with_column("DAY_NAME",
            when(col("DAY_OF_WEEK") == 0, lit("Sunday"))
            .when(col("DAY_OF_WEEK") == 1, lit("Monday"))
            .when(col("DAY_OF_WEEK") == 2, lit("Tuesday"))
            .when(col("DAY_OF_WEEK") == 3, lit("Wednesday"))
            .when(col("DAY_OF_WEEK") == 4, lit("Thursday"))
            .when(col("DAY_OF_WEEK") == 5, lit("Friday"))
            .otherwise(lit("Saturday"))
        )
        .group_by("DAY_OF_WEEK", "DAY_NAME")
        .agg([
            count_distinct("O_ORDERKEY").alias("ORDER_COUNT"),
            sum_("L_TOTAL_AMOUNT").alias("TOTAL_REVENUE"),
            avg("L_TOTAL_AMOUNT").alias("AVG_ORDER_VALUE")
        ])
        .sort("DAY_OF_WEEK")
    )
    
    dow_sales.show()
    
    return monthly_with_growth

# =============================================================================
# 5.3 PRODUCT ANALYSIS
# =============================================================================

def analyze_product_performance(session):
    """
    Analyze product performance and identify top performers
    """
    print("\n" + "="*80)
    print("PRODUCT PERFORMANCE ANALYSIS")
    print("="*80)
    
    # Load tables
    parts = session.table("PART_SILVER")
    lineitems = session.table("LINEITEM_SILVER")
    
    # Product performance metrics
    print("\n📊 Calculating product metrics...")
    
    product_metrics = (lineitems
        .join(parts, lineitems["L_PARTKEY"] == parts["P_PARTKEY"])
        .group_by(
            parts["P_PARTKEY"], 
            parts["P_NAME"], 
            parts["P_MFGR"],
            parts["P_BRAND"],
            parts["P_TYPE"],
            parts["P_TYPE_CATEGORY"]
        )
        .agg([
            sum_("L_QUANTITY").alias("TOTAL_QUANTITY"),
            sum_("L_TOTAL_AMOUNT").alias("TOTAL_REVENUE"),
            avg("L_EXTENDEDPRICE").alias("AVG_PRICE"),
            avg("L_DISCOUNT").alias("AVG_DISCOUNT"),
            count_distinct("L_ORDERKEY").alias("ORDER_COUNT")
        ])
    )
    
    # Add rankings
    revenue_window = Window.order_by(col("TOTAL_REVENUE").desc())
    quantity_window = Window.order_by(col("TOTAL_QUANTITY").desc())
    
    product_ranked = (product_metrics
        .with_column("REVENUE_RANK", row_number().over(revenue_window))
        .with_column("QUANTITY_RANK", row_number().over(quantity_window))
    )
    
    # Save results
    product_ranked.write.mode("overwrite").save_as_table("PRODUCT_ANALYSIS_RESULTS")
    print(f"\n✅ Product analysis saved to PRODUCT_ANALYSIS_RESULTS table")
    
    # Show top 20 products by revenue
    print("\n🏆 Top 20 Products by Revenue:")
    top_products = product_ranked.filter(col("REVENUE_RANK") <= 20)
    top_products.show()
    
    # Category analysis
    print("\n📊 Performance by Product Type Category:")
    category_performance = (product_ranked
        .group_by("P_TYPE_CATEGORY")
        .agg([
            count("P_PARTKEY").alias("PRODUCT_COUNT"),
            sum_("TOTAL_REVENUE").alias("CATEGORY_REVENUE"),
            avg("TOTAL_REVENUE").alias("AVG_PRODUCT_REVENUE"),
            sum_("TOTAL_QUANTITY").alias("CATEGORY_QUANTITY")
        ])
        .order_by(col("CATEGORY_REVENUE").desc())
    )
    category_performance.show()
    
    return product_ranked

# =============================================================================
# 5.4 REGIONAL PERFORMANCE ANALYSIS
# =============================================================================

def analyze_regional_performance(session):
    """
    Analyze sales performance by region and nation
    """
    print("\n" + "="*80)
    print("REGIONAL PERFORMANCE ANALYSIS")
    print("="*80)
    
    # Load tables
    customers = session.table("CUSTOMER_SILVER")
    orders = session.table("ORDERS_SILVER")
    lineitems = session.table("LINEITEM_SILVER")
    
    # Regional metrics
    print("\n🌍 Calculating regional performance...")
    
    regional_metrics = (customers
        .join(orders, customers["C_CUSTKEY"] == orders["O_CUSTKEY"])
        .join(lineitems, orders["O_ORDERKEY"] == lineitems["L_ORDERKEY"])
        .group_by("C_REGION_NAME", "C_NATION_NAME", "C_MKTSEGMENT")
        .agg([
            count_distinct("C_CUSTKEY").alias("CUSTOMER_COUNT"),
            count_distinct("O_ORDERKEY").alias("ORDER_COUNT"),
            sum_("L_TOTAL_AMOUNT").alias("TOTAL_REVENUE"),
            avg("L_TOTAL_AMOUNT").alias("AVG_ORDER_LINE_VALUE"),
            sum_("L_QUANTITY").alias("TOTAL_QUANTITY")
        ])
    )
    
    # Calculate market share
    total_revenue = regional_metrics.select(sum_("TOTAL_REVENUE")).collect()[0][0]
    
    regional_with_share = (regional_metrics
        .with_column("MARKET_SHARE_PCT", 
            (col("TOTAL_REVENUE") / lit(total_revenue) * 100))
        .with_column("REVENUE_PER_CUSTOMER",
            col("TOTAL_REVENUE") / col("CUSTOMER_COUNT"))
    )
    
    # Save results
    regional_with_share.write.mode("overwrite").save_as_table("REGIONAL_PERFORMANCE_ANALYSIS")
    print(f"\n✅ Regional analysis saved to REGIONAL_PERFORMANCE_ANALYSIS table")
    
    # Show regional summary
    print("\n🌍 Top Regions by Revenue:")
    regional_summary = (regional_with_share
        .group_by("C_REGION_NAME")
        .agg([
            sum_("TOTAL_REVENUE").alias("REGION_REVENUE"),
            sum_("CUSTOMER_COUNT").alias("REGION_CUSTOMERS"),
            sum_("ORDER_COUNT").alias("REGION_ORDERS"),
            avg("MARKET_SHARE_PCT").alias("AVG_MARKET_SHARE")
        ])
        .order_by(col("REGION_REVENUE").desc())
    )
    regional_summary.show()
    
    return regional_with_share

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main execution function
    """
    print("\n" + "="*80)
    print("TPC-H ANALYTICS - SNOWPARK PYTHON")
    print("="*80)
    print(f"Execution started at: {datetime.now()}")
    
    try:
        # Create Snowpark session
        session = create_snowpark_session()
        
        # Run all analyses
        print("\n🚀 Starting analytics pipeline...")
        
        # 1. RFM Segmentation
        rfm_results = calculate_rfm_segmentation(session)
        
        # 2. Sales Trend Analysis
        sales_trends = analyze_sales_trends(session)
        
        # 3. Product Performance Analysis
        product_analysis = analyze_product_performance(session)
        
        # 4. Regional Performance Analysis
        regional_analysis = analyze_regional_performance(session)
        
        # Summary
        print("\n" + "="*80)
        print("✅ ALL ANALYSES COMPLETED SUCCESSFULLY!")
        print("="*80)
        print("\nGenerated Tables:")
        print("  1. CUSTOMER_RFM_SCORES")
        print("  2. MONTHLY_SALES_TRENDS")
        print("  3. QUARTERLY_SALES_TRENDS")
        print("  4. PRODUCT_ANALYSIS_RESULTS")
        print("  5. REGIONAL_PERFORMANCE_ANALYSIS")
        
        print(f"\nExecution completed at: {datetime.now()}")
        
        # Close session
        session.close()
        print("\n✅ Snowpark session closed")
        
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
