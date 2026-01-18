import streamlit as st
import akshare as ak
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# 设置页面配置
st.set_page_config(
    page_title="基金布林带分析",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 侧边栏：输入与设置
with st.sidebar:
    st.header("🔍 查询设置")
    fund_code = st.text_input("基金代码", value="017057", help="输入6位基金代码")
    lookback_days = st.slider("分析天数", min_value=30, max_value=365, value=120, step=10)
    st.info("提示：手机浏览器访问时，点击左上角箭头展开此菜单。")

# 主函数
def main():
    st.title(f"📈 基金实时分析看板")
    
    if len(fund_code) != 6:
        st.error("请输入正确的6位基金代码")
        return

    # 获取数据
    with st.spinner(f'正在获取 {fund_code} 数据...'):
        realtime_data, hist_data, error_msg = get_data(fund_code)
    
    if error_msg:
        st.error(error_msg)
        return

    # --- 调试模式：显示原始数据 ---
    # 仅当数据看起来异常时（比如净值是整数序列），或者用户手动展开时显示
    is_abnormal = False
    if hist_data is not None and not hist_data.empty:
        # 简单判断：如果单位净值是整数且连续，很可能是读成索引了
        vals = hist_data['单位净值'].head(10).tolist()
        if all(isinstance(x, (int, float)) and x == int(x) for x in vals):
            is_abnormal = True
    
    with st.expander("🔧 数据调试面板 (如果图表是一条直线，请点开截图发给我)", expanded=is_abnormal):
        st.write("程序读取到的前5行数据：")
        st.write(hist_data.head() if hist_data is not None else "无数据")
        st.write("数据列名：", hist_data.columns.tolist() if hist_data is not None else "无")
    # ---------------------------

    # 1. 实时数据展示
    display_realtime_metrics(realtime_data, hist_data)

    # 2. 布林带图表
    st.subheader("📊 布林带趋势图")
    fig = plot_bollinger_plotly(hist_data, lookback_days)
    st.plotly_chart(fig, use_container_width=True)

    # 3. 数据表格与导出
    st.subheader("📋 历史数据明细")
    with st.expander("查看详细数据"):
        display_df = hist_data.sort_values('净值日期', ascending=False).head(lookback_days)
        st.dataframe(display_df.style.format({
            "单位净值": "{:.4f}",
            "UB": "{:.4f}",
            "MB": "{:.4f}",
            "LB": "{:.4f}"
        }))
        
        # 导出CSV
        csv = display_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 导出为Excel/CSV",
            data=csv,
            file_name=f'fund_{fund_code}_{datetime.now().strftime("%Y%m%d")}.csv',
            mime='text/csv',
        )

# 数据获取函数
@st.cache_data(ttl=300) # 缓存5分钟
def get_data(code):
    realtime_info = None
    hist_df = None
    error = None

    try:
        # 1. 实时估值
        try:
            df_est = ak.fund_value_estimation_em()
            target = df_est[df_est['基金代码'] == code]
            if not target.empty:
                realtime_info = target.iloc[0].to_dict()
        except Exception as e:
            pass # 实时数据获取失败不影响历史数据

        # 2. 历史净值
        hist_df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
        
        # --- 强力修复逻辑 ---
        # 如果列名不对，尝试按位置重命名
        if '单位净值' not in hist_df.columns and len(hist_df.columns) >= 2:
            hist_df.columns = ['净值日期', '单位净值'] + list(hist_df.columns[2:])
            
        # 3. 数据处理
        hist_df['净值日期'] = pd.to_datetime(hist_df['净值日期'])
        hist_df = hist_df.sort_values('净值日期')
        
        # 再次检查：如果单位净值列全是整数（可能是索引），尝试找真正的浮点数列
        try:
             # 先尝试转数字
            hist_df['单位净值'] = pd.to_numeric(hist_df['单位净值'], errors='coerce')
            
            # 检查前10行是否都是整数
            head_vals = hist_df['单位净值'].head(10).dropna()
            if len(head_vals) > 0 and all(x == int(x) for x in head_vals):
                # 可能是索引，尝试寻找真正的净值列（通常是浮点数，且不是第一列）
                for col in hist_df.columns:
                    if col in ['净值日期', '单位净值']: continue
                    
                    # 尝试转换这一列
                    temp_series = pd.to_numeric(hist_df[col], errors='coerce')
                    temp_head = temp_series.head(10).dropna()
                    
                    # 如果这列是浮点数（包含小数），那它才是真正的净值
                    if len(temp_head) > 0 and any(x != int(x) for x in temp_head):
                        hist_df['单位净值'] = temp_series
                        break
        except:
            pass
            
        hist_df = hist_df.dropna(subset=['单位净值']) # 去除空值
        
        # 4. 计算布林带
        window = 20
        k = 2
        hist_df['MB'] = hist_df['单位净值'].rolling(window=window).mean()
        hist_df['STD'] = hist_df['单位净值'].rolling(window=window).std()
        hist_df['UB'] = hist_df['MB'] + k * hist_df['STD']
        hist_df['LB'] = hist_df['MB'] - k * hist_df['STD']

    except Exception as e:
        error = f"数据获取失败: {str(e)}"
    
    return realtime_info, hist_df, error

# 实时指标展示组件
def display_realtime_metrics(realtime_info, hist_df):
    latest_hist = hist_df.iloc[-1]
    
    # 准备显示数据
    fund_name = realtime_info['基金名称'] if realtime_info else "未知基金"
    
    # 获取实时估值或最新净值
    if realtime_info:
        est_val_col = [c for c in realtime_info.keys() if '估算值' in c][0]
        est_rate_col = [c for c in realtime_info.keys() if '估算增长率' in c][0]
        est_time_col = [c for c in realtime_info.keys() if '估算时间' in c][0] if any('估算时间' in c for c in realtime_info.keys()) else None
        
        curr_price = float(realtime_info[est_val_col])
        curr_rate = realtime_info[est_rate_col]
        curr_time = realtime_info[est_time_col] if est_time_col else "实时"
    else:
        curr_price = latest_hist['单位净值']
        curr_rate = "0.00"
        curr_time = latest_hist['净值日期'].strftime('%Y-%m-%d')

    # 计算技术指标
    ub = latest_hist['UB']
    lb = latest_hist['LB']
    mb = latest_hist['MB']
    
    # 位置百分比 (0=LB, 50=MB, 100=UB)
    if (ub - lb) != 0:
        position_pct = (curr_price - lb) / (ub - lb) * 100
    else:
        position_pct = 50.0

    # 状态判断
    if curr_price > ub:
        status = "⚠️ 严重高估 (卖出)"
        status_color = "red"
    elif curr_price > mb + (ub-mb)*0.8:
        status = "⚠️ 偏高 (风险区)"
        status_color = "orange"
    elif curr_price < lb:
        status = "💎 严重低估 (抄底)"
        status_color = "green"
    elif curr_price < lb + (mb-lb)*0.2:
        status = "💎 偏低 (机会区)"
        status_color = "lightgreen"
    else:
        status = "⚖️ 正常持有"
        status_color = "gray"

    # --- 布局展示 ---
    
    # 第一行：核心信息
    st.markdown(f"### 📊 {fund_name} ({fund_code})")
    
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("当前价格", f"{curr_price:.4f}", f"{curr_rate}%")
    with c2:
        st.metric("更新时间", curr_time.split(' ')[-1] if ' ' in str(curr_time) else str(curr_time))
    with c3:
        st.metric("操作建议", status)
    with c4:
        st.metric("布林带位置", f"{position_pct:.1f}%", help="0%为下轨，100%为上轨，超过100%为突破上轨")

    # 第二行：技术位详情
    st.markdown("#### 🎯 今日关键点位")
    k1, k2, k3, k4 = st.columns(4)
    
    # 计算距离
    dist_ub = (ub - curr_price) / curr_price * 100
    dist_lb = (curr_price - lb) / curr_price * 100
    
    with k1:
        st.metric("上轨 (压力)", f"{ub:.4f}", f"距现价 {dist_ub:.2f}%")
    with k2:
        st.metric("中轨 (趋势)", f"{mb:.4f}")
    with k3:
        st.metric("下轨 (支撑)", f"{lb:.4f}", f"距现价 {-dist_lb:.2f}%")
    with k4:
        width = (ub - lb) / mb * 100
        st.metric("通道带宽", f"{width:.2f}%", help="带宽越窄说明即将变盘，越宽说明波动剧烈")

    st.divider()

# Plotly绘图函数
def plot_bollinger_plotly(df, days):
    plot_data = df.tail(days)
    
    fig = go.Figure()

    # 1. 绘制下轨 (作为填充基准)
    fig.add_trace(go.Scatter(
        x=plot_data['净值日期'], 
        y=plot_data['LB'], 
        mode='lines', 
        name='下轨 (支撑)', 
        line=dict(color='red', dash='dash', width=1)
    ))

    # 2. 绘制上轨 (填充到下轨)
    fig.add_trace(go.Scatter(
        x=plot_data['净值日期'], 
        y=plot_data['UB'], 
        mode='lines', 
        name='上轨 (阻力)', 
        line=dict(color='green', dash='dash', width=1),
        fill='tonexty', # 关键修改：使用 tonexty 填充到上一条线(下轨)
        fillcolor='rgba(128,128,128,0.1)'
    ))

    # 3. 绘制中轨
    fig.add_trace(go.Scatter(
        x=plot_data['净值日期'], 
        y=plot_data['MB'], 
        mode='lines', 
        name='中轨 (趋势)', 
        line=dict(color='gray', dash='dot', width=1)
    ))

    # 4. 绘制净值线
    fig.add_trace(go.Scatter(
        x=plot_data['净值日期'], 
        y=plot_data['单位净值'], 
        mode='lines', 
        name='单位净值', 
        line=dict(color='black', width=2)
    ))

    # 5. 标记买卖点
    high_points = plot_data[plot_data['单位净值'] > plot_data['UB']]
    low_points = plot_data[plot_data['单位净值'] < plot_data['LB']]

    fig.add_trace(go.Scatter(
        x=high_points['净值日期'], y=high_points['单位净值'],
        mode='markers', marker=dict(color='red', size=8, symbol='circle'),
        name='高估信号'
    ))

    fig.add_trace(go.Scatter(
        x=low_points['净值日期'], y=low_points['单位净值'],
        mode='markers', marker=dict(color='green', size=8, symbol='circle'),
        name='低估信号'
    ))

    fig.update_layout(
        hovermode="x unified",
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_title="日期",
        yaxis_title="净值",
        dragmode="pan"
    )
    
    return fig

if __name__ == "__main__":
    main()
