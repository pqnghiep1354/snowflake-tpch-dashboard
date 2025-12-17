import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from snowflake.snowpark.functions import col, sum as sum_, avg, count
from datetime import datetime
import json
import os
from snowflake.snowpark import Session


# =============================================================================
# 1. CẤU HÌNH TRANG & CSS
# =============================================================================

st.set_page_config(
    page_title="TPC-H Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .main > div { padding-top: 1rem; }
    .stMetric {
        background-color: #f5f7f9;
        border: 1px solid #e6e9ef;
        padding: 15px;
        border-radius: 5px;
    }
    </style>
""", unsafe_allow_html=True)

# =============================================================================
# 2. KẾT NỐI SNOWFLAKE (Dành riêng cho SiS)
# =============================================================================

@st.cache_resource
def create_session():
    """
    Lấy session hiện tại từ môi trường Snowflake.
    Không cần mật khẩu hay file config.
    """
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
# 3. HÀM LOAD DỮ LIỆU
# =============================================================================

@st.cache_data(ttl=3600)
def load_data(_session, table_name):
    """Load dữ liệu từ bảng Snowflake và chuyển sang Pandas DataFrame"""
    try:
        # Lưu ý: Cần đảm bảo Role chạy App có quyền SELECT trên schema REPORTS
        # Nếu bảng nằm ở database/schema khác, hãy sửa lại đường dẫn bên dưới
        # Ví dụ: table_path = f"TPCH_ANALYTICS_DB.REPORTS.{table_name}"
        table_path = table_name 
        df = _session.table(table_path).to_pandas()
        return df
    except Exception as e:
        st.error(f"Lỗi khi load bảng {table_name}: {e}")
        return pd.DataFrame()

# =============================================================================
# 4. CÁC COMPONENT HIỂN THỊ (Visualizations)
# =============================================================================

def show_executive_summary(monthly_sales, customer_metrics, regional_analysis):
    st.title("🏠 Executive Summary")
    st.markdown("---")
    
    if monthly_sales.empty:
        st.warning("Chưa có dữ liệu. Vui lòng kiểm tra lại Pipeline.")
        return

    # --- KPI CARDS ---
    total_revenue = monthly_sales['TOTAL_REVENUE'].sum()
    total_orders = monthly_sales['TOTAL_ORDERS'].sum()
    total_customers = len(customer_metrics)
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💰 Doanh Thu Tổng", f"${total_revenue:,.0f}")
    col2.metric("📦 Tổng Đơn Hàng", f"{total_orders:,}")
    col3.metric("👥 Tổng Khách Hàng", f"{total_customers:,}")
    
    if total_orders > 0:
        aov = total_revenue / total_orders
        col4.metric("💵 Giá Trị Đơn TB", f"${aov:,.2f}")
    else:
        col4.metric("💵 Giá Trị Đơn TB", "$0")

    st.markdown("---")

    # --- BIỂU ĐỒ ---
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("📈 Xu Hướng Doanh Thu")
        monthly_sales['REPORT_DATE'] = pd.to_datetime(monthly_sales['REPORT_DATE'])
        monthly_sales = monthly_sales.sort_values('REPORT_DATE')
        
        fig_trend = px.line(
            monthly_sales, 
            x='REPORT_DATE', 
            y='TOTAL_REVENUE',
            markers=True,
            title='Tăng trưởng doanh thu theo tháng'
        )
        fig_trend.update_traces(line_color='#1f77b4', line_width=3)
        st.plotly_chart(fig_trend, use_container_width=True)

    with col_right:
        st.subheader("🌍 Doanh Thu Theo Vùng")
        if not regional_analysis.empty:
            region_sum = regional_analysis.groupby('REGION_NAME')['TOTAL_REVENUE'].sum().reset_index()
            fig_pie = px.pie(
                region_sum, 
                values='TOTAL_REVENUE', 
                names='REGION_NAME',
                hole=0.4
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("Không có dữ liệu vùng")

def show_sales_analysis(monthly_sales):
    st.title("📈 Phân Tích Bán Hàng")
    st.markdown("---")
    
    if monthly_sales.empty: return

    # Filter theo năm
    years = sorted(monthly_sales['YEAR'].unique())
    selected_year = st.selectbox("Chọn Năm", years, index=len(years)-1)
    
    filtered_df = monthly_sales[monthly_sales['YEAR'] == selected_year]

    # Biểu đồ kết hợp (Combo Chart)
    st.subheader(f"Doanh thu & Tăng trưởng năm {selected_year}")
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Bar(x=filtered_df['MONTH_NAME'], y=filtered_df['TOTAL_REVENUE'], name="Doanh Thu"),
        secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(x=filtered_df['MONTH_NAME'], y=filtered_df['MOM_REVENUE_GROWTH'], name="Tăng Trưởng %", mode='lines+markers', line=dict(color='red')),
        secondary_y=True
    )
    
    fig.update_layout(title_text="Doanh thu hàng tháng vs Tăng trưởng MoM")
    fig.update_yaxes(title_text="Doanh Thu ($)", secondary_y=False)
    fig.update_yaxes(title_text="Tăng Trưởng (%)", secondary_y=True)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Bảng dữ liệu chi tiết
    st.dataframe(filtered_df[['REPORT_DATE', 'TOTAL_ORDERS', 'TOTAL_REVENUE', 'MOM_REVENUE_GROWTH']], use_container_width=True)

def show_customer_analytics(customer_metrics):
    st.title("👥 Phân Tích Khách Hàng")
    st.markdown("---")
    
    if customer_metrics.empty: return

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("🎯 Phân khúc RFM")
        # Lấy mẫu 1000 khách để vẽ cho nhanh
        fig_scatter = px.scatter(
            customer_metrics.head(1000), 
            x='RECENCY_DAYS',
            y='MONETARY',
            color='RFM_SEGMENT',
            size='FREQUENCY',
            hover_data=['C_NAME'],
            title="Recency vs Monetary (Kích thước = Tần suất mua)"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col2:
        st.subheader("📊 Tỷ lệ Phân khúc")
        seg_counts = customer_metrics['RFM_SEGMENT'].value_counts()
        fig_bar = px.bar(
            x=seg_counts.values,
            y=seg_counts.index,
            orientation='h',
            labels={'x': 'Số lượng', 'y': 'Phân khúc'}
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader("🏆 Top 10 Khách Hàng VIP")
    top_10 = customer_metrics.nlargest(10, 'LIFETIME_VALUE')[['C_NAME', 'C_NATION', 'RFM_SEGMENT', 'LIFETIME_VALUE', 'FREQUENCY']]
    st.dataframe(top_10, use_container_width=True)

def show_product_performance(product_performance):
    st.title("📦 Hiệu Suất Sản Phẩm")
    st.markdown("---")
    
    if product_performance.empty: return

    metric = st.radio("Sắp xếp theo:", ["TOTAL_REVENUE", "TOTAL_QUANTITY_SOLD"], horizontal=True)
    
    top_products = product_performance.nlargest(15, metric)
    
    fig = px.bar(
        top_products,
        x=metric,
        y='P_NAME',
        orientation='h',
        color='P_BRAND',
        title=f"Top 15 Sản phẩm theo {metric}"
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# 5. CHƯƠNG TRÌNH CHÍNH (MAIN)
# =============================================================================

def main():
    # Sidebar Navigation
    st.sidebar.title("📊 TPC-H Analytics")
    
    try:
        session = create_session()
        st.sidebar.success(f"✅ Đã kết nối: {session.get_current_database()}.{session.get_current_schema()}")
    except Exception as e:
        st.error("❌ Lỗi kết nối Session. Hãy đảm bảo bạn đang chạy trên Snowflake Streamlit App.")
        st.stop()

    page = st.sidebar.radio("Điều hướng", [
        "🏠 Executive Summary",
        "📈 Sales Analysis",
        "👥 Customer Analytics",
        "📦 Product Performance"
    ])

    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Làm mới dữ liệu"):
        st.cache_data.clear()
        st.experimental_rerun()

    # Load dữ liệu (giả định bảng nằm trong database/schema hiện tại của App)
    # Nếu App của bạn trỏ vào schema khác, hãy thêm tên DB.SCHEMA vào trước tên bảng
    with st.spinner("Đang tải dữ liệu..."):
        monthly_sales = load_data(session, "TPCH_ANALYTICS_DB.REPORTS.MONTHLY_SALES_REPORT")
        customer_metrics = load_data(session, "TPCH_ANALYTICS_DB.REPORTS.CUSTOMER_METRICS")
        product_performance = load_data(session, "TPCH_ANALYTICS_DB.REPORTS.PRODUCT_PERFORMANCE")
        regional_analysis = load_data(session, "TPCH_ANALYTICS_DB.REPORTS.REGIONAL_ANALYSIS")

    # Routing trang
    if page == "🏠 Executive Summary":
        show_executive_summary(monthly_sales, customer_metrics, regional_analysis)
    elif page == "📈 Sales Analysis":
        show_sales_analysis(monthly_sales)
    elif page == "👥 Customer Analytics":
        show_customer_analytics(customer_metrics)
    elif page == "📦 Product Performance":
        show_product_performance(product_performance)

if __name__ == "__main__":
    main()