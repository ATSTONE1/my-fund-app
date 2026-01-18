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
        hist_df['净值日期'] = pd.to_datetime(hist_df['净值日期'])
        hist_df = hist_df.sort_values('净值日期')
        hist_df['单位净值'] = hist_df['单位净值'].astype(float)

        # 3. 计算布林带
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
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(label="基金名称", value=fund_name)
    
    with col2:
        if realtime_info:
            # 动态获取列名
            est_val_col = [c for c in realtime_info.keys() if '估算值' in c][0]
            est_rate_col = [c for c in realtime_info.keys() if '估算增长率' in c][0]
            val = float(realtime_info[est_val_col])
            rate = realtime_info[est_rate_col]
            st.metric(label="实时估值 (GZ)", value=val, delta=f"{rate}%")
        else:
            st.metric(label="最新净值 (JZ)", value=f"{latest_hist['单位净值']:.4f}", delta="无实时数据")

    with col3:
        # 状态判断
        curr_price = float(realtime_info[[c for c in realtime_info.keys() if '估算值' in c][0]]) if realtime_info else latest_hist['单位净值']
        status = "正常持仓"
        color = "normal"
        
        if curr_price > latest_hist['UB']:
            status = "⚠️ 高估 (卖出信号)"
            color = "inverse"
        elif curr_price < latest_hist['LB']:
            status = "💎 低估 (买入信号)"
            color = "normal"
            
        st.metric(label="当前状态", value=status)

# Plotly绘图函数
def plot_bollinger_plotly(df, days):
    plot_data = df.tail(days)
    
    fig = go.Figure()

    # 1. 绘制轨道区域 (UB和LB之间)
    fig.add_trace(go.Scatter(
        x=pd.concat([plot_data['净值日期'], plot_data['净值日期'][::-1]]),
        y=pd.concat([plot_data['UB'], plot_data['LB'][::-1]]),
        fill='toself',
        fillcolor='rgba(128,128,128,0.1)',
        line=dict(color='rgba(255,255,255,0)'),
        hoverinfo="skip",
        name='布林带通道'
    ))

    # 2. 绘制三条线
    fig.add_trace(go.Scatter(x=plot_data['净值日期'], y=plot_data['UB'], mode='lines', name='上轨 (阻力)', line=dict(color='green', dash='dash', width=1)))
    fig.add_trace(go.Scatter(x=plot_data['净值日期'], y=plot_data['MB'], mode='lines', name='中轨 (趋势)', line=dict(color='gray', dash='dot', width=1)))
    fig.add_trace(go.Scatter(x=plot_data['净值日期'], y=plot_data['LB'], mode='lines', name='下轨 (支撑)', line=dict(color='red', dash='dash', width=1)))

    # 3. 绘制净值线
    fig.add_trace(go.Scatter(x=plot_data['净值日期'], y=plot_data['单位净值'], mode='lines', name='单位净值', line=dict(color='black', width=2)))

    # 4. 标记买卖点
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
        dragmode="pan" # 适合手机
    )
    
    return fig

if __name__ == "__main__":
    main()
