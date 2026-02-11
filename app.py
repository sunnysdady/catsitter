import streamlit as st
import pandas as pd
import requests
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import time

# --- 1. 核心连接配置 (自动从 Secrets 读取) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 核心算法：路径优化与地理计算 ---

def get_distance(p1, p2):
    """计算两点间的经纬度物理距离"""
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """最近邻算法：实现『不走回头路』的作业顺序排列"""
    if len(df_sitter) <= 1:
        df_sitter['拟定顺序'] = range(1, len(df_sitter) + 1)
        return df_sitter
    
    unvisited = df_sitter.to_dict('records')
    # 默认从第一个点位开始作为起点
    current_node = unvisited.pop(0)
    optimized_list = [current_node]
    
    while unvisited:
        # 寻找距离当前点最近的下一个未访问点
        next_node = min(unvisited, key=lambda x: get_distance(
            (current_node['lng'], current_node['lat']), 
            (x['lng'], x['lat'])
        ))
        unvisited.remove(next_node)
        optimized_list.append(next_node)
        current_node = next_node
        
    res_df = pd.DataFrame(optimized_list)
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

# --- 3. 飞书 API 交互逻辑 (全量补全) ---

def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        res = r.json()
        return res.get("tenant_access_token") if res.get("code") == 0 else None
    except: return None

def fetch_feishu_data():
    token = get_feishu_token()
    if not token: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, params={"page_size": 500}, timeout=15).json()
        items = r.get("data", {}).get("items", [])
        if not items: return pd.DataFrame()
        # 提取字段并注入系统 ID
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        # 字段标准化对齐
        required_cols = ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '投喂频率', '喂猫师', '备注']
        for col in required_cols:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def add_feishu_record(fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        response = requests.post(url, headers=headers, json={"fields": fields}, timeout=10)
        return response.json().get("code") == 0
    except: return False

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    clean_fields = {k: ("" if pd.isna(v) else v) for k, v in fields.items()}
    try:
        response = requests.patch(url, headers=headers, json={"fields": clean_fields}, timeout=10)
        return response.json().get("code") == 0
    except: return False

# --- 4. 视觉与地理工具 (30px 巨幕适配) ---

def set_ui():
    st.markdown("""
        <style>
        /* 强制背景色与文字对比 */
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; }
        
        /* 巨幕 30px 适配按钮：极致粗体黑框 */
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important;
            height: 100px !important;
            background-color: #FFFFFF !important;
            color: #000000 !important;
            border: 3px solid #000000 !important;
            border-radius: 15px !important;
            font-size: 30px !important;
            font-weight: 900 !important;
            margin-bottom: 20px !important;
            transition: all 0.2s ease;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        [data-testid="stSidebar"] div.stButton > button:hover {
            background-color: #000000 !important;
            color: #FFFFFF !important;
        }
        
        /* 表格文字强化 */
        .stDataFrame { font-size: 16px !important; }
        [data-testid="stSidebar"] { border-right: 1px solid #E9ECEF !important; }
        </style>
        """, unsafe_allow_html=True)

@st.cache_data(show_spinner=False)
def get_coords(address):
    """接入高德 API 获取经纬度"""
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_API_KEY}&address=深圳市{address}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            lng, lat = r['geocodes'][0]['location'].split(',')
            return float(lng), float(lat)
    except: return None, None

# --- 5. 页面控制中心 ---

st.set_page_config(page_title="小猫直喂-调度指挥中心", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "数据中心"

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    
    # 30px 巨幕导航按钮
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    
    if st.session_state['page'] == "智能看板":
        st.divider()
        st.subheader("⚙️ 快速调度配置")
        active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 调度日期范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))
    else:
        current_active = ["梦蕊", "依蕊"]
        date_range = (datetime.now(), datetime.now() + timedelta(days=2))

# 缓存机制：减少飞书 API 调用
if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

# --- 6. 核心业务逻辑渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (数据录入与云端预览)")
    
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量导入 Excel (支持 30px 巨幕点击)"):
            up_file = st.file_uploader("选择 Excel 文件", type=["xlsx"])
            if up_file and st.button("🚀 启动批量录入云端"):
                df_up = pd.read_excel(up_file)
                p_bar = st.progress(0)
                for i, (_, row) in enumerate(df_up.iterrows()):
                    # 时间戳转换
                    s_ts = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                    e_ts = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                    payload = {
                        "详细地址": str(row['详细地址']).strip(),
                        "宠物名字": str(row.get('宠物名字', '小猫')).strip(),
                        "投喂频率": int(row.get('投喂频率', 1)),
                        "服务开始日期": s_ts,
                        "服务结束日期": e_ts,
                        "喂猫师": str(row.get('喂猫师', '')).strip(), # 导入时保留原有喂猫师
                        "备注": str(row.get('备注', ''))
                    }
                    add_feishu_record(payload)
                    p_bar.progress((i + 1) / len(df_up))
                st.success("批量同步成功！")
                st.session_state['feishu_cache'] = fetch_feishu_data()

    with c2:
        with st.expander("单条手动录入"):
            with st.form("manual_entry_form"):
                addr = st.text_input("详细地址*")
                cat_name = st.text_input("宠物名字")
                s_date = st.date_input("服务开始")
                e_date = st.date_input("服务结束")
                sitter_name = st.text_input("指定喂猫师 (可选)")
                if st.form_submit_button("保存至云端"):
                    payload = {
                        "详细地址": addr.strip(),
                        "宠物名字": cat_name.strip(),
                        "投喂频率": 1,
                        "服务开始日期": int(datetime.combine(s_date, datetime.min.time()).timestamp()*1000),
                        "服务结束日期": int(datetime.combine(e_date, datetime.min.time()).timestamp()*1000),
                        "喂猫师": sitter_name.strip()
                    }
                    if add_feishu_record(payload):
                        st.success("单条录入成功！")
                        st.session_state['feishu_cache'] = fetch_feishu_data()

    st.divider()
    if st.button("🔄 强制刷新预览云端数据"):
        st.session_state.pop('feishu_cache', None)
        st.session_state['feishu_cache'] = fetch_feishu_data()
    
    st.subheader("📊 当前云端文档快照")
    df_snapshot = st.session_state['feishu_cache'].copy()
    if not df_snapshot.empty:
        # 格式化日期显示
        for c in ['服务开始日期', '服务结束日期']:
            df_snapshot[c] = pd.to_datetime(df_snapshot[c], unit='ms', errors='coerce').dt.strftime('%Y-%m-%d')
        st.dataframe(df_snapshot.drop(columns=['_system_id'], errors='ignore'), use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能调度排单看板 (三级算法+路径优化版)")
    df_raw = st.session_state['feishu_cache'].copy()
    
    if not df_raw.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        # 转换日期格式进行计算
        for col in ['服务开始日期', '服务结束日期']:
            df_raw[col] = pd.to_datetime(df_raw[col], unit='ms', errors='coerce')
        
        if st.button(f"✨ 拟定全周期最优排单方案"):
            all_day_plans = []
            # 建立【地址 -> 喂猫师】映射，实现老客户优先绑定
            address_sitter_binding = {}
            
            days = pd.date_range(start_d, end_d).tolist()
            p_bar_calc = st.progress(0)
            
            for i, current_day in enumerate(days):
                cur_ts = pd.Timestamp(current_day)
                # 过滤出当日有效的任务
                day_df = df_raw[(df_raw['服务开始日期'] <= cur_ts) & (df_raw['服务结束日期'] >= cur_ts)].copy()
                
                if not day_df.empty:
                    # 频率过滤逻辑
                    day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    
                    if not day_df.empty:
                        # 批量并行获取经纬度坐标
                        with ThreadPoolExecutor(max_workers=10) as executor:
                            coords_list = list(executor.map(get_coords, day_df['详细地址']))
                        day_df[['lng', 'lat']] = pd.DataFrame(coords_list, index=day_df.index)
                        valid_df = day_df.dropna(subset=['lng', 'lat']).copy()
                        
                        if not valid_df.empty:
                            # --- 三级调度大脑算法 ---
                            sitter_load_today = {s: 0 for s in current_active}
                            
                            def core_assign_algorithm(row):
                                addr = row['详细地址']
                                manual_val = str(row.get('喂猫师', '')).strip()
                                
                                # 级别 1: 人工指定优先 (Excel/云端已有数据)
                                if manual_val and manual_val != "nan" and manual_val != "":
                                    address_sitter_binding[addr] = manual_val
                                    return manual_val
                                
                                # 级别 2: 客户粘性绑定 (同一地址优先分配给之前定过的人)
                                if addr in address_sitter_binding:
                                    return address_sitter_binding[addr]
                                
                                # 级别 3: 出勤人员负载均衡 (选当日活最少的人)
                                if current_active:
                                    best_choice = min(sitter_load_today, key=sitter_load_today.get)
                                    sitter_load_today[best_choice] += 1
                                    address_sitter_binding[addr] = best_choice
                                    return best_choice
                                return "待分配"

                            valid_df['喂猫师'] = valid_df.apply(core_assign_algorithm, axis=1)
                            valid_df['作业日期'] = current_day.strftime('%Y-%m-%d')
                            
                            # --- 路径算法接入：分人执行最优路径排列 ---
                            optimized_results = []
                            for sitter in current_active:
                                sitter_tasks = valid_df[valid_df['喂猫师'] == sitter].copy()
                                if not sitter_tasks.empty:
                                    optimized_results.append(optimize_route(sitter_tasks))
                            
                            if optimized_results:
                                all_day_plans.append(pd.concat(optimized_results))
                
                p_bar_calc.progress((i + 1) / len(days))
            
            if all_day_plans:
                st.session_state['period_plan_data'] = pd.concat(all_day_plans)
                st.success("✅ 全周期调度拟定完成！(已应用路径算法与三级分配逻辑)")

        # 结果展示区
        if 'period_plan_data' in st.session_state:
            res_df = st.session_state['period_plan_data']
            
            f_col1, f_col2 = st.columns(2)
            with f_col1:
                target_day = st.selectbox("📅 1. 选择查看日期", sorted(res_df['作业日期'].unique()))
            with f_col2:
                sitters_list = ["全部"] + sorted(res_df[res_df['作业日期'] == target_day]['喂猫师'].unique().tolist())
                target_sitter = st.selectbox("👤 2. 筛选具体喂猫师", sitters_list)
            
            # 数据过滤
            display_data = res_df[res_df['作业日期'] == target_day]
            if target_sitter != "全部":
                display_data = display_data[display_data['喂猫师'] == target_sitter]
            
            if not display_data.empty:
                # 地图呈现
                st.pydeck_chart(pdk.Deck(
                    map_style=pdk.map_styles.LIGHT,
                    initial_view_state=pdk.ViewState(longitude=display_data['lng'].mean(), latitude=display_data['lat'].mean(), zoom=11),
                    layers=[pdk.Layer("ScatterplotLayer", display_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=350)]
                ))
                
                # 任务明细表
                st.markdown(f"### 📋 {target_day} 任务流水 (按路径优化顺序排列)")
                st.data_editor(display_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                
                # 同步功能
                sync_col1, sync_col2 = st.columns(2)
                with sync_col1:
                    if st.button("📋 导出简报至微信"):
                        summary_txt = f"📢 任务清单 ({target_day})\n\n"
                        for s in (current_active if target_sitter == "全部" else [target_sitter]):
                            s_data = display_data[display_data['喂猫师'] == s].sort_values('拟定顺序')
                            if not s_data.empty:
                                summary_txt += f"👤 喂猫师：{s}\n"
                                for _, t in s_data.iterrows():
                                    summary_txt += f"   {t['拟定顺序']}. {t['宠物名字']} - {t['详细地址']}\n"
                                summary_txt += "\n"
                        st.text_area("复制以下内容：", summary_txt, height=200)

                with sync_col2:
                    if st.button("✅ 确认并回写飞书喂猫师字段"):
                        # 全周期同步
                        total_recs = len(res_df)
                        sync_bar = st.progress(0)
                        for idx, (_, r_data) in enumerate(res_df.iterrows()):
                            # 回写飞书：关键字段“喂猫师”
                            update_feishu_record(r_data['_system_id'], {"喂猫师": r_data['喂猫师']})
                            sync_bar.progress((idx + 1) / total_recs)
                        st.success("🎉 同步完成！飞书文档已成功更新。")
                        st.session_state.pop('feishu_cache', None)

else:
    st.title("📖 系统使用手册 (V2.9)")
    st.info("作为运营经理，您可以通过此系统实现对深圳一线喂猫师的高效异地调度。")
    st.markdown("""
    ### ⚙️ 核心逻辑说明
    1. **三级分配算法**：
       - **一级（指定）**：若 Excel 已手动指定喂猫师，系统绝不更改。
       - **二级（绑定）**：同一客户（地址）的所有猫由同一位喂猫师负责。
       - **三级（动态）**：剩余订单根据当日“梦蕊”、“依蕊”的负载情况进行均衡分配。
    2. **路径优化**：
       - 接入**最近邻算法**。系统会计算每日所有服务点的地理坐标，并按路程最短原则生成`拟定顺序`，杜绝往返跑。
    3. **巨幕适配**：
       - 侧边栏按钮高度提升至 100px，字体统一为 **30px 极致粗体**，方便在高分辨率大屏上快速点击。
    """)
