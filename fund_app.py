import streamlit as st
import akshare as ak
import pandas as pd
import altair as alt
from datetime import datetime

# ==========================================
# 1. 页面配置
# ==========================================
st.set_page_config(
    page_title="基金分析重制版",
    page_icon="📈",
    layout="wide"
)

# ==========================================
# 2. 核心数据获取 (极简稳健版)
# ==========================================
@st.cache_data(ttl=300)
def get_fund_data_v2(code):
    """
    重写的获取函数，不搞复杂的猜测，只做标准处理。
    """
    history_df = pd.DataFrame()
    realtime_data = None
    error_msg = None
    
    try:
        # --- A. 获取历史净值 ---
        # akshare 返回的标准列名通常是: '净值日期', '单位净值', '日增长率', ...
        raw_df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
        
        if raw_df is None or raw_df.empty:
            return None, None, "接口未返回任何数据，请检查基金代码是否正确。"

        # 强制重命名列，防止列名带空格或不可见字符
        # 我们假设前两列大概率是 日期 和 净值，但为了保险，我们用列名匹配
        col_map = {}
        for c in raw_df.columns:
            c_str = str(c).strip()
            if "日期" in c_str:
                col_map[c] = "date"
            elif "单位净值" in c_str:
                col_map[c] = "value"
        
        df = raw_df.rename(columns=col_map)
        
        # 必须要有 date 和 value
        if "date" not in df.columns or "value" not in df.columns:
            # 如果找不到名字匹配的，尝试回退到按位置 (慎用，但作为最后兜底)
            # 只有当列数 >= 2 时才敢这么做
            if len(df.columns) >= 2:
                df = df.iloc[:, :2]
                df.columns = ["date", "value"]
            else:
                return None, None, f"数据列名识别失败，原始列名: {raw_df.columns.tolist()}"

        # 类型转换
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        
        # 清洗无效行
        df = df.dropna(subset=["date", "value"])
        df = df.sort_values("date")
        df = df.reset_index(drop=True)
        
        # --- 这里的逻辑：如果单位净值全是 0,1,2 这种整数序列，说明数据源确实错了 ---
        # 但我们先不做自动修正，直接展示，由用户看调试面板
        
        # 计算布林带
        # N=20, K=2
        if len(df) >= 20:
            df["MB"] = df["value"].rolling(window=20).mean()
            df["STD"] = df["value"].rolling(window=20).std()
            df["UB"] = df["MB"] + 2 * df["STD"]
            df["LB"] = df["MB"] - 2 * df["STD"]
            
            # 计算信号
            def get_signal(row):
                if pd.isna(row['UB']) or pd.isna(row['LB']):
                    return "数据不足"
                if row['value'] > row['UB']:
                    return "卖出"
                elif row['value'] < row['LB']:
                    return "买入"
                else:
                    return "持有"
            
            df["信号"] = df.apply(get_signal, axis=1)
        
        history_df = df

        # --- B. 获取实时估值 (可选) ---
        try:
            est_df = ak.fund_value_estimation_em()
            # 找到对应代码的那一行
            target = est_df[est_df["基金代码"] == code]
            if not target.empty:
                realtime_data = target.iloc[0].to_dict()
        except Exception:
            pass # 实时数据挂了不影响历史数据

    except Exception as e:
        error_msg = f"发生未预期的错误: {str(e)}"
        
    return history_df, realtime_data, error_msg

# ==========================================
# 3. 绘图函数 (Altair 版)
# ==========================================
def plot_chart(df, days):
    # 截取最近 N 天
    plot_data = df.tail(days).copy()
    
    if plot_data.empty:
        st.warning("没有足够的数据用于绘图")
        return None

    # 基础图表对象
    base = alt.Chart(plot_data).encode(
        x=alt.X('date:T', title='日期')
    )

    # 1. 布林带区域 (UB 和 LB 之间)
    band = base.mark_area(opacity=0.3, color='#C0C0C0').encode(
        y='LB:Q',
        y2='UB:Q'
    )

    # 2. 线条
    # 净值线
    line_val = base.mark_line(color='black', strokeWidth=2).encode(
        y=alt.Y('value:Q', title='单位净值', scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip('date', title='日期', format='%Y-%m-%d'),
            alt.Tooltip('value', title='单位净值'),
            alt.Tooltip('UB', title='上轨', format='.4f'),
            alt.Tooltip('LB', title='下轨', format='.4f')
        ]
    )
    
    # 上轨 (虚线)
    line_ub = base.mark_line(color='green', strokeDash=[5, 5], opacity=0.7).encode(y='UB:Q')
    
    # 下轨 (虚线)
    line_lb = base.mark_line(color='red', strokeDash=[5, 5], opacity=0.7).encode(y='LB:Q')
    
    # 中轨 (点线)
    line_mb = base.mark_line(color='gray', strokeDash=[2, 2], opacity=0.5).encode(y='MB:Q')

    # 组合图表
    chart = (band + line_ub + line_lb + line_mb + line_val).properties(
        title='布林带趋势分析',
        height=400
    )
    
    # 3. 买卖信号点 (新增)
    # 筛选出有买卖信号的点
    buy_points = plot_data[plot_data['信号'] == '买入']
    sell_points = plot_data[plot_data['信号'] == '卖出']
    
    if not buy_points.empty:
        buy_layer = alt.Chart(buy_points).mark_point(
            shape='triangle-up', size=100, color='red', fill='red'
        ).encode(
            x='date:T',
            y='value:Q',
            tooltip=['date', 'value', '信号']
        )
        chart = chart + buy_layer
        
    if not sell_points.empty:
        sell_layer = alt.Chart(sell_points).mark_point(
            shape='triangle-down', size=100, color='green', fill='green'
        ).encode(
            x='date:T',
            y='value:Q',
            tooltip=['date', 'value', '信号']
        )
        chart = chart + sell_layer

    return chart.interactive()

# ==========================================
# 4. 主程序
# ==========================================
def main():
    # 侧边栏
    with st.sidebar:
        st.header("设置")
        code = st.text_input("基金代码", value="017057", max_chars=6)
        days = st.slider("显示天数", 30, 365, 120)
        
        if st.button("清除缓存"):
            st.cache_data.clear()
            st.rerun()

    # 标题
    st.title(f"📊 基金分析看板 ({code})")

    if len(code) != 6:
        st.warning("请输入6位基金代码")
        return

    # 获取数据
    with st.spinner("正在拉取最新数据..."):
        df, rt_data, err = get_fund_data_v2(code)

    if err:
        st.error(f"❌ {err}")
        return

    if df is None or df.empty:
        st.warning("未获取到历史数据")
        return

    # --- 实时/最新信息展示 ---
    latest = df.iloc[-1]
    
    # 尝试从实时数据里拿，拿不到就用历史数据最新的
    curr_val = latest["value"]
    curr_date = latest["date"].strftime("%Y-%m-%d")
    curr_rate = "0.00%"
    
    if rt_data:
        # 也就是 '估算值' 和 '估算增长率'，但也可能是别的名字，这里做个模糊匹配
        try:
            # 找 key 中包含 '估算值' 的
            k_val = next((k for k in rt_data.keys() if "估算值" in k), None)
            k_rate = next((k for k in rt_data.keys() if "估算增长率" in k), None)
            
            if k_val: curr_val = float(rt_data[k_val])
            if k_rate: 
                # 修复可能重复的百分号
                raw_rate = str(rt_data[k_rate]).replace("%", "")
                curr_rate = f"{raw_rate}%"
            curr_date = "实时估算"
        except:
            pass

    # 计算状态
    ub = latest["UB"] if "UB" in df.columns else 0
    lb = latest["LB"] if "LB" in df.columns else 0
    
    # --- 扩展指标计算 ---
    # 1. 区间涨跌幅 (基于显示天数)
    period_df = df.tail(days)
    if not period_df.empty:
        start_val = period_df.iloc[0]["value"]
        end_val = period_df.iloc[-1]["value"]
        period_change = (end_val - start_val) / start_val * 100
    else:
        period_change = 0

    # 2. 最大回撤
    # 计算公式：(当前值 - 之前最高值) / 之前最高值
    roll_max = period_df["value"].cummax()
    drawdown = (period_df["value"] - roll_max) / roll_max
    max_drawdown = drawdown.min() * 100

    # 3. 布林带位置 (%B)
    # 0=下轨, 1=上轨, >1=突破上轨, <0=跌破下轨
    if ub != lb:
        pct_b = (curr_val - lb) / (ub - lb)
    else:
        pct_b = 0.5

    # 指标栏 - 第一行 (基础信息)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("当前净值/估值", f"{curr_val:.4f}", curr_rate)
    c2.metric("更新时间", curr_date)
    c3.metric("布林上轨 (阻力)", f"{ub:.4f}" if ub else "-")
    c4.metric("布林下轨 (支撑)", f"{lb:.4f}" if lb else "-")

    # 指标栏 - 第二行 (进阶分析)
    st.markdown("---") # 分割线
    k1, k2, k3, k4 = st.columns(4)
    
    k1.metric(f"近{len(period_df)}天涨跌", f"{period_change:.2f}%", 
              delta_color="normal" if period_change > 0 else "inverse")
    
    k2.metric("区间最大回撤", f"{max_drawdown:.2f}%", 
              delta_color="off") # 回撤通常是负数，用灰色或红色表示风险
              
    k3.metric("相对位置 (%B)", f"{pct_b:.2f}", 
              help=">1: 突破上轨 (超买); <0: 跌破下轨 (超卖)")
    
    # 信号状态
    signal_color = "gray"
    if curr_val > ub:
        signal_text = "🚫 卖出信号 (高估)"
        signal_color = "red"
    elif curr_val < lb:
        signal_text = "✅ 买入信号 (低估)"
        signal_color = "green"
    else:
        signal_text = "☕ 持有观望"
        signal_color = "blue"
        
    k4.markdown(f"**操作建议**:<br><span style='color:{signal_color};font-size:1.2em;font-weight:bold'>{signal_text}</span>", unsafe_allow_html=True)
    st.markdown("---") # 分割线

    # 图表
    if "UB" in df.columns:
        st.caption("💡 提示：点击图表右上角的 **...** 按钮，选择 **Save as PNG** 即可下载高清趋势图")
        chart = plot_chart(df, days)
        if chart:
            st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("数据不足，无法计算布林带 (至少需要20天数据)")

    # 原始数据查看 (放在折叠栏里，方便查错)
    st.subheader("📋 历史数据明细")
    
    # 格式化一下显示的 DataFrame
    display_df = df.copy()
    display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
    # 只保留主要列，并按日期倒序
    cols = ['date', 'value', '信号', 'UB', 'LB', 'MB', '日增长率']
    # 过滤掉不存在的列
    cols = [c for c in cols if c in display_df.columns]
    
    st.dataframe(
        display_df[cols].sort_values('date', ascending=False),
        use_container_width=True,
        column_config={
            "date": "日期",
            "value": "单位净值",
            "信号": st.column_config.TextColumn("操作信号", help="基于布林带策略的建议"),
            "UB": st.column_config.NumberColumn("阻力位 (上轨)", format="%.4f"),
            "LB": st.column_config.NumberColumn("支撑位 (下轨)", format="%.4f"),
            "MB": st.column_config.NumberColumn("趋势位 (中轨)", format="%.4f"),
            "日增长率": "日涨幅(%)"
        }
    )

    # 下载
    csv = df.to_csv(index=False).encode('utf-8-sig')
    st.download_button("📥 下载完整数据 (CSV)", csv, f"fund_{code}.csv", "text/csv", use_container_width=True)

    # 调试信息 (已移除，如需恢复请取消注释)
    # with st.expander("🛠️ 调试信息"):
    #    ...

if __name__ == "__main__":
    main()
