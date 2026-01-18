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
    ).interactive() # 开启交互 (缩放、平移)
    
    return chart

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
            if k_rate: curr_rate = f"{rt_data[k_rate]}%"
            curr_date = "实时估算"
        except:
            pass

    # 计算状态
    ub = latest["UB"] if "UB" in df.columns else 0
    lb = latest["LB"] if "LB" in df.columns else 0
    
    status = "持有"
    color = "off"
    if ub > 0 and lb > 0:
        if curr_val > ub:
            status = "高估 (卖出信号)"
            color = "inverse" # Streamlit metric doesn't support color directly, but we use delta
        elif curr_val < lb:
            status = "低估 (买入信号)"
            color = "normal"

    # 指标栏
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("当前净值/估值", f"{curr_val:.4f}", curr_rate)
    c2.metric("更新时间", curr_date)
    c3.metric("布林上轨 (阻力)", f"{ub:.4f}" if ub else "-")
    c4.metric("布林下轨 (支撑)", f"{lb:.4f}" if lb else "-")

    # 图表
    if "UB" in df.columns:
        chart = plot_chart(df, days)
        if chart:
            st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("数据不足，无法计算布林带 (至少需要20天数据)")

    # 原始数据查看 (放在折叠栏里，方便查错)
    with st.expander("📋 查看原始数据 & 调试", expanded=True):
        st.write(f"数据总行数: {len(df)}")
        
        st.write("### 数据预览 (文本模式)")
        st.code(df.head().to_string(), language="text")
        st.code(df.tail().to_string(), language="text")

        st.write("### 数据预览 (表格模式)")
        st.dataframe(df.head())
        
        # 下载
        csv = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button("下载数据 CSV", csv, f"fund_{code}.csv", "text/csv")

if __name__ == "__main__":
    main()
