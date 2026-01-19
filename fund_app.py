import streamlit as st
import akshare as ak
import pandas as pd
import altair as alt
from datetime import datetime

# ==========================================
# 1. 页面配置
# ==========================================
st.set_page_config(
    page_title="基金分析",
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
    增加重试机制 (3次)
    """
    history_df = pd.DataFrame()
    realtime_data = None
    error_msg = None
    
    # 重试装饰器逻辑
    def fetch_with_retry(func, *args, retries=3):
        last_err = None
        for i in range(retries):
            try:
                res = func(*args)
                if res is not None and not (isinstance(res, pd.DataFrame) and res.empty):
                    return res
            except Exception as e:
                last_err = e
            import time
            time.sleep(0.5) # 稍微歇一下
        raise last_err if last_err else Exception("获取数据为空")

    try:
        # --- A. 获取历史净值 ---
        # akshare 返回的标准列名通常是: '净值日期', '单位净值', '日增长率', ...
        try:
            raw_df = fetch_with_retry(ak.fund_open_fund_info_em, code, "单位净值走势")
        except:
            raw_df = None
        
        if raw_df is None or raw_df.empty:
            return None, None, "接口未返回任何数据 (重试3次失败)，请检查基金代码是否正确或网络状态。"

        # 强制重命名列，防止列名带空格或不可见字符
        # 我们假设前两列大概率是 日期 和 净值，但为了保险，我们用列名匹配
        col_map = {}
        for c in raw_df.columns:
            c_str = str(c).strip()
            if "日期" in c_str:
                col_map[c] = "date"
            elif "单位净值" in c_str:
                col_map[c] = "value"
            elif "日增长率" in c_str:
                 col_map[c] = "日增长率"
        
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
        if "日增长率" in df.columns:
            df["日增长率"] = pd.to_numeric(df["日增长率"], errors="coerce")
        
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
            # 同样重试3次
            est_df = fetch_with_retry(ak.fund_value_estimation_em)
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
def plot_chart(df, days, title="布林带趋势分析", subtitle=None, enable_interactive=False):
    # 截取最近 N 天
    plot_data = df.tail(days).copy()
    
    if plot_data.empty:
        st.warning("没有足够的数据用于绘图")
        return None

    # 定义交互选择器 (Crosshair 核心)
    # nearest=True 表示选择最近的数据点
    # on='mouseover' 对应鼠标悬停
    # 增加 mousemove touchmove 以支持移动端滑动查数
    # empty=False 确保未交互时不显示任何辅助线
    nearest = alt.selection_point(
        nearest=True, 
        on='mouseover mousemove touchmove', 
        fields=['date'], 
        clear='mouseout',
        empty=False
    )

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
        y=alt.Y('value:Q', title='单位净值', scale=alt.Scale(zero=False))
    )
    
    # 上轨 (虚线)
    line_ub = base.mark_line(color='green', strokeDash=[5, 5], opacity=0.7).encode(y='UB:Q')
    
    # 下轨 (虚线)
    line_lb = base.mark_line(color='red', strokeDash=[5, 5], opacity=0.7).encode(y='LB:Q')
    
    # 中轨 (点线)
    line_mb = base.mark_line(color='gray', strokeDash=[2, 2], opacity=0.5).encode(y='MB:Q')

    # --- Crosshair 交互层 ---
    # 透明的选择层：负责捕捉鼠标/触摸位置
    selectors = base.mark_point().encode(
        x='date:T',
        opacity=alt.value(0),
        tooltip=[
            alt.Tooltip('date', title='日期', format='%Y-%m-%d'),
            alt.Tooltip('value', title='单位净值'),
            alt.Tooltip('UB', title='上轨', format='.4f'),
            alt.Tooltip('LB', title='下轨', format='.4f'),
            alt.Tooltip('信号', title='操作信号')
        ]
    ).add_params(
        nearest
    )

    # 垂直辅助线：根据选择显示
    rule = base.mark_rule(color='gray', strokeWidth=1).encode(
        x='date:T'
    ).transform_filter(
        nearest
    )

    # 选中点的圆点高亮
    points = line_val.mark_point(filled=True, size=50, color='black').transform_filter(
        nearest
    )

    # 组合图表
    # 注意层级顺序：selectors 最好在上面以捕捉事件，或者至少在图层中存在
    layers = [band, line_ub, line_lb, line_mb, line_val, selectors, rule, points]
    
    # 3. 买卖信号点 (新增) - 保持原有逻辑
    # 筛选出有买卖信号的点
    buy_points = plot_data[plot_data['信号'] == '买入']
    sell_points = plot_data[plot_data['信号'] == '卖出']
    
    if not buy_points.empty:
        buy_layer = alt.Chart(buy_points).mark_point(
            shape='triangle-up', size=100, color='green', fill='green'
        ).encode(
            x='date:T',
            y='value:Q',
            tooltip=['date', 'value', '信号']
        )
        layers.append(buy_layer)
        
    if not sell_points.empty:
        sell_layer = alt.Chart(sell_points).mark_point(
            shape='triangle-down', size=100, color='red', fill='red'
        ).encode(
            x='date:T',
            y='value:Q',
            tooltip=['date', 'value', '信号']
        )
        layers.append(sell_layer)

    # 合并所有层
    chart = alt.layer(*layers).properties(
        title=alt.TitleParams(
            text=title,
            subtitle=subtitle if subtitle else [],
            fontSize=20,
            subtitleFontSize=14,
            subtitleColor="gray",
            anchor='start',
            offset=20
        ),
        height=400
    )
    
    # 根据开关决定是否开启缩放平移
    if enable_interactive:
        return chart.interactive()
    else:
        return chart

# ==========================================
# 4. 概览页逻辑
# ==========================================
@st.cache_data(ttl=600)
def get_all_fund_estimation():
    """获取所有基金的实时估值数据 (缓存10分钟)"""
    for _ in range(3):
        try:
            res = ak.fund_value_estimation_em()
            if res is not None and not res.empty:
                return res
        except Exception:
            import time
            time.sleep(0.5)
    return None

def render_overview_page():
    # 标题栏 + 刷新按钮
    c1, c2 = st.columns([6, 1])
    with c1:
        st.title("📊 基金批量概览")
    with c2:
        if st.button("🔄 刷新", use_container_width=True, help="清除缓存并强制重新拉取数据"):
            st.cache_data.clear()
            st.rerun()
    
    # 输入区域
    st.subheader("📝 基金代码输入 (批量)")
    
    with st.form(key="search_form"):
        default_codes = "017057, 005827, 161725, 012414, 161028"
        input_text = st.text_area(
            "请输入基金代码 (支持逗号、空格或换行分隔)", 
            value=default_codes,
            height=100,
            label_visibility="collapsed" # 隐藏label，因为上面已经有subheader了
        )
        submit_btn = st.form_submit_button("🔍 开始分析", use_container_width=True)
    
    # 解析代码
    import re
    codes = list(set(re.findall(r"\d{6}", input_text)))
    st.caption(f"已识别 {len(codes)} 个有效基金代码")
        
    if not codes:
        st.info("请输入基金代码以开始分析")
        return

    # 获取全量数据并筛选
    with st.spinner("正在获取实时行情和计算指标..."):
        all_est_df = get_all_fund_estimation()
        
        # 预先计算指标 (UB, LB, 信号)
        # 使用并行计算加速历史数据获取
        import concurrent.futures
        
        stats_list = []
        progress_bar = st.progress(0)
        
        def fetch_single_fund_stats(code):
            # 默认值
            stats = {
                "基金代码": code,
                "UB": None,
                "LB": None, 
                "建议": "数据不足",
                "昨日涨跌幅": None
            }
            try:
                # 获取历史数据 (利用缓存)
                hist_df, _, _ = get_fund_data_v2(code)
                if hist_df is not None and not hist_df.empty:
                    # 获取 UB/LB
                    if "UB" in hist_df.columns:
                        latest = hist_df.iloc[-1]
                        stats["UB"] = latest["UB"]
                        stats["LB"] = latest["LB"]
                    
                    # 获取昨日涨跌幅 (兜底用)
                    if "日增长率" in hist_df.columns:
                         stats["昨日涨跌幅"] = hist_df.iloc[-1]["日增长率"]
            except:
                pass
            return stats

        # 使用线程池并发请求 (最大50个线程)
        # 注意：过高的并发可能会导致被数据源(东方财富)封禁 IP，50 属于激进设置，请谨慎使用
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            # 提交所有任务
            future_to_code = {executor.submit(fetch_single_fund_stats, code): code for code in codes}
            
            # 处理结果
            for i, future in enumerate(concurrent.futures.as_completed(future_to_code)):
                stats = future.result()
                stats_list.append(stats)
                # 更新进度条
                progress_bar.progress((i + 1) / len(codes))
            
        progress_bar.empty() # 清除进度条
        stats_df = pd.DataFrame(stats_list)

    if all_est_df is None or all_est_df.empty:
        # 如果获取不到实时数据，至少展示历史计算结果
        st.warning("无法获取实时行情数据，仅显示历史分析结果")
        # 创建一个空的实时数据结构以供后续合并
        all_est_df = pd.DataFrame(columns=["基金代码", "基金名称", "估算值", "估算增长率"])
    
    # 筛选
    # 构造基础 DataFrame，确保所有输入代码都在列表中
    input_df = pd.DataFrame({"基金代码": codes})
    
    # 处理列名动态变化的问题 (比如 '2026-01-19-估算数据-估算值')
    col_mapping = {}
    for c in all_est_df.columns:
        if "估算值" in c and "估算数据" in c:
            col_mapping[c] = "估算值"
        elif "估算增长率" in c and "估算数据" in c:
            col_mapping[c] = "估算增长率"
        elif "单位净值" in c and "公布数据" in c:
            col_mapping[c] = "单位净值"
        elif "日增长率" in c and "公布数据" in c:
            col_mapping[c] = "日增长率"
        elif "估算时间" in c:
            col_mapping[c] = "估算时间"
            
    # 重命名列
    all_est_df = all_est_df.rename(columns=col_mapping)
    
    # 左连接，保留所有输入代码
    all_est_df["基金代码"] = all_est_df["基金代码"].astype(str)
    
    # 1. 合并输入代码和实时数据
    merged_df = pd.merge(input_df, all_est_df, on="基金代码", how="left")
    
    # 2. 合并计算指标 (UB, LB)
    final_df = pd.merge(merged_df, stats_df, on="基金代码", how="left")
    
    # 如果没匹配到，填充默认值
    final_df["基金名称"] = final_df["基金名称"].fillna("未知/无实时数据")
    # 不要过早 fillna("-")，因为还需要计算
    
    # 计算最终信号 (实时值 vs UB/LB)
    def calculate_final_signal(row):
        try:
            # 获取当前值：优先用实时估算值，没有则用单位净值(如果有的话，但在all_est_df里可能没有最新的，这里主要靠实时)
            # 如果实时估算值是 NaN，尝试用单位净值
            curr_val = row.get("估算值")
            if pd.isna(curr_val) or curr_val == "":
                 curr_val = row.get("单位净值")
            
            # 如果还是拿不到数值，就没法比较
            if pd.isna(curr_val) or curr_val == "-":
                return "数据不足"
                
            val = float(curr_val)
            ub = float(row["UB"])
            lb = float(row["LB"])
            
            if pd.isna(ub) or pd.isna(lb):
                return "数据不足"
                
            if val > ub:
                return "卖出 (高估)"
            elif val < lb:
                return "买入 (低估)"
            else:
                return "持有"
        except:
            return "数据不足"

    final_df["建议"] = final_df.apply(calculate_final_signal, axis=1)

    # 处理估算涨跌幅为空的情况 (使用昨日数据兜底)
    def fix_rate_display(row):
        rate = row.get("估算增长率")
        if pd.isna(rate) or rate == "" or rate == "-":
            # 尝试用昨日涨跌幅
            y_rate = row.get("昨日涨跌幅")
            if pd.notna(y_rate):
                return f"{y_rate}% (昨日)"
            return "-"
        return rate

    final_df["估算增长率"] = final_df.apply(fix_rate_display, axis=1)

    # 格式化展示
    display_cols = ["基金代码", "基金名称", "建议", "估算值", "估算增长率", "UB", "LB"]
    # 确保列存在
    display_cols = [c for c in display_cols if c in final_df.columns]
    
    final_df = final_df[display_cols]
    final_df = final_df.fillna("-")
    
    # 样式优化：高亮涨跌
    def highlight_change(val):
        try:
            val_str = str(val).replace('%', '').replace(' (昨日)', '')
            val_num = float(val_str)
            if val_num > 0:
                return 'color: red'
            elif val_num < 0:
                return 'color: green'
            else:
                return '' # 0 不变色
        except:
            return ''

    # 显示表格 (支持选择)
    st.subheader(f"📈 实时行情 ({len(final_df)}只)")
    
    # 操作模式切换
    col_help, col_toggle = st.columns([3, 1])
    with col_help:
        st.caption("💡 **默认模式**：点击表格行 **直接查看详情**。")
    with col_toggle:
        is_batch = st.toggle("🛠️ 批量导出模式", value=False)
        
    if is_batch:
        st.caption("✅ **批量模式已开启**：勾选多行可批量导出，点击行不会跳转。")
        selection_mode = "multi-row"
    else:
        selection_mode = "single-row"
    
    # 使用 st.dataframe 的 selection 功能
    selection = st.dataframe(
        final_df,
        key="overview_table",  # 添加固定 key 保持状态
        use_container_width=True,
        hide_index=True,
        selection_mode=selection_mode, 
        on_select="rerun",
        column_config={
            "建议": st.column_config.TextColumn("操作建议"),
            "估算增长率": st.column_config.TextColumn("估算涨幅"),
            "估算值": st.column_config.NumberColumn("实时估值", format="%.4f"),
            "UB": st.column_config.NumberColumn("阻力位(UB)", format="%.4f"),
            "LB": st.column_config.NumberColumn("支撑位(LB)", format="%.4f"),
        }
    )
    
    # 逻辑分流
    if is_batch:
        # 批量模式：只处理导出
        selected_rows = []
        if selection and selection.selection and selection.selection.rows:
            selected_rows = selection.selection.rows
            
        export_df = final_df
        export_label = f"📥 导出全部 ({len(final_df)}只)"
        
        if selected_rows:
            export_df = final_df.iloc[selected_rows]
            export_label = f"📥 导出选中 ({len(export_df)}只)"
            
        csv = export_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            export_label, 
            csv, 
            f"fund_overview_{datetime.now().strftime('%Y%m%d')}.csv", 
            "text/csv", 
            use_container_width=True
        )
    else:
        # 默认模式：点击即跳转
        if selection and selection.selection and selection.selection.rows:
            selected_idx = selection.selection.rows[0]
            selected_code = final_df.iloc[selected_idx]["基金代码"]
            st.session_state.selected_code = selected_code
            st.session_state.page = "detail"
            st.rerun()
            
        # 默认模式下也保留一个导出全部按钮，方便不切模式也能导
        csv = final_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            "📥 导出今日概览数据 (CSV)", 
            csv, 
            f"fund_overview_{datetime.now().strftime('%Y%m%d')}.csv", 
            "text/csv", 
            use_container_width=True
        )

# ==========================================
# 5. 详情页逻辑 (原 main 函数)
# ==========================================
def render_detail_page(code):
    # 顶部导航: 返回 | 标题 | 刷新
    c_back, c_title, c_refresh = st.columns([1, 5, 1])
    with c_back:
        if st.button("⬅️ 返回", use_container_width=True):
            st.query_params.clear() # 清除 URL 参数防止死循环
            st.session_state.page = "overview"
            st.rerun()
            
    with c_title:
        st.markdown(f"<h3 style='text-align: center; margin: 0; padding-top: 10px;'>📊 基金分析看板 ({code})</h3>", unsafe_allow_html=True)

    with c_refresh:
        if st.button("🔄 刷新", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # 详情页设置
    with st.expander("⚙️ 图表设置", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
            days = st.slider("显示天数", 30, 365, 120)
        with c2:
            st.write("") # 占位
            st.write("") 
            enable_zoom = st.checkbox("开启图表缩放/平移 (手机端建议关闭)", value=False)

    # ... (后续逻辑复用原代码，只需把 code, days, enable_zoom 传入或在函数内使用) ...
    # 为了减少缩进改动，我们把后面的逻辑直接搬过来，稍微调整缩进
    
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
    curr_rate = "-" # 默认为横杠，避免误导为 0.00%
    
    if rt_data:
        try:
            # 模糊匹配 key，防止列名变动
            # 常见列名: "估算值", "gsz"; "估算增长率", "gszzl"
            k_val = next((k for k in rt_data.keys() if "估算值" in str(k) or "gsz" in str(k)), None)
            k_rate = next((k for k in rt_data.keys() if "估算增长率" in str(k) or "gszzl" in str(k)), None)
            
            if k_val: curr_val = float(rt_data[k_val])
            if k_rate: 
                raw_rate = str(rt_data[k_rate]).replace("%", "")
                curr_rate = f"{raw_rate}%"
            curr_date = "实时估算"
        except:
            pass
            
    # 如果实时没拿到涨幅，尝试用历史数据的"日增长率" (如果是今天的数据)
    # 但通常历史数据是昨天的。为了不留空，可以显示昨天的，但要标明。
    # 这里我们简单处理：如果还是 "-"，且历史数据里有日增长率，就显示历史的，但日期已经是"昨天"了
    if curr_rate == "-" and "日增长率" in latest:
         r = latest["日增长率"]
         if pd.notna(r):
             curr_rate = f"{r}% (昨日)"

    # 计算状态
    ub = latest["UB"] if "UB" in df.columns else 0
    lb = latest["LB"] if "LB" in df.columns else 0
    
    # --- 扩展指标计算 ---
    period_df = df.tail(days)
    if not period_df.empty:
        start_val = period_df.iloc[0]["value"]
        end_val = period_df.iloc[-1]["value"]
        period_change = (end_val - start_val) / start_val * 100
    else:
        period_change = 0

    # 最大回撤
    roll_max = period_df["value"].cummax()
    drawdown = (period_df["value"] - roll_max) / roll_max
    max_drawdown = drawdown.min() * 100

    # 布林带位置 (%B)
    if ub != lb:
        pct_b = (curr_val - lb) / (ub - lb)
    else:
        pct_b = 0.5

    # 指标栏 - 第一行 (基础信息)
    c1, c2, c3, c4 = st.columns(4)
    # 涨跌幅颜色逻辑: 涨红(inverse) 跌绿(inverse)
    c1.metric("当前净值/估值", f"{curr_val:.4f}", curr_rate, delta_color="inverse")
    c2.metric("更新时间", curr_date)
    c3.metric("布林上轨 (阻力)", f"{ub:.4f}" if ub else "-")
    c4.metric("布林下轨 (支撑)", f"{lb:.4f}" if lb else "-")

    # 指标栏 - 第二行 (进阶分析)
    st.markdown("---") 
    k1, k2, k3, k4 = st.columns(4)
    
    k1.metric(f"近{len(period_df)}天涨跌", f"{period_change:.2f}%", 
              delta_color="inverse")
    
    k2.metric("区间最大回撤", f"{max_drawdown:.2f}%", 
              delta_color="off") 
              
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
    st.markdown("---") 

    # 图表
    if "UB" in df.columns:
        st.caption("💡 提示：点击图表右上角的 **...** 按钮，选择 **Save as PNG** 即可下载高清趋势图")
        
        chart_title = f"基金 {code} 趋势分析 ({days}天)"
        chart_subtitle = [
            f"最新: {curr_val:.4f} ({curr_rate}) | {curr_date}",
            f"建议: {signal_text} | 区间涨跌: {period_change:.2f}% | 最大回撤: {max_drawdown:.2f}%"
        ]
        
        chart = plot_chart(df, days, title=chart_title, subtitle=chart_subtitle, enable_interactive=enable_zoom)
        if chart:
            st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("数据不足，无法计算布林带 (至少需要20天数据)")

    # 原始数据查看
    st.subheader("📋 历史数据明细")
    
    display_df = df.copy()
    display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
    cols = ['date', 'value', '信号', 'UB', 'LB', 'MB', '日增长率']
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

    csv = df.to_csv(index=False).encode('utf-8-sig')
    st.download_button("📥 下载完整数据 (CSV)", csv, f"fund_{code}.csv", "text/csv", use_container_width=True)

# ==========================================
# 6. 主程序入口
# ==========================================
def main():
    # 检查 URL 参数以支持直接导航 (配合表格中的链接按钮)
    if "code" in st.query_params:
        code_param = st.query_params["code"]
        st.session_state.page = "detail"
        st.session_state.selected_code = code_param

    # 初始化 session state
    if 'page' not in st.session_state:
        st.session_state.page = "overview"
    if 'selected_code' not in st.session_state:
        st.session_state.selected_code = "017057"

    # 路由
    if st.session_state.page == "overview":
        # 如果在概览页，清除可能残留的 code 参数
        if "code" in st.query_params:
             st.query_params.clear()
        render_overview_page()
    elif st.session_state.page == "detail":
        render_detail_page(st.session_state.selected_code)


if __name__ == "__main__":
    main()
