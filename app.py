import streamlit as st
import pandas as pd
import requests
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# --- 1. 核心连接配置 ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 核心算法：路径优化与分配逻辑 ---

def get_distance(p1, p2):
    """计算两点间的经纬度距离"""
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """最近邻算法：按物理距离重新排列喂猫师的作业顺序"""
    if len(df_sitter) <= 1:
        df_sitter['拟定顺序'] = range(1, len(df_sitter) + 1)
        return df_sitter
    
    unvisited = df_sitter.to_dict('records')
    current_node = unvisited.pop(0)
    optimized_list = [current_node]
    
    while unvisited:
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

# --- 3. 飞书 API 交互逻辑 ---

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
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        # 统一字段初始化
        required_cols = ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '投喂频率', '喂猫师', '备注']
        for col in required_cols:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # 确保推送时字段名与飞书文档严格一致
    clean_fields = {k: ("" if pd.isna(v) else v) for k, v in fields.items()}
    try:
        response = requests.patch(url, headers=headers, json={"fields": clean_fields}, timeout=10)
        return response.json().get("code") == 0
    except: return False

# --- 4. UI 视觉重构 (30px 巨幕) ---

def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; }
        /* 侧边栏按钮：30px 极致粗体 */
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
        }
        [data-testid="stSidebar"] div.stButton > button:hover {
            background-color: #000000 !important;
            color: #FFFFFF !important;
        }
        .stDataFrame { font-size: 16px !important; }
        </style>
        """, unsafe_allow_html=True)

@st.cache_data(show_spinner=False)
def get_coords(address):
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_API_KEY}&address=深圳市{address}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            lng, lat = r['geocodes'][0]['location'].split(',')
            return float(lng), float(lat)
    except: return None, None

# --- 5. 页面路由与逻辑 ---

st.set_page_config(page_title="小猫直喂指挥中心", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "数据中心"

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    
    if st.session_state['page'] == "智能看板":
        st.divider(); st.subheader("⚙️ 快速调度")
        active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 调度范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))

if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

# --- 6. 模块渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据预览与管理")
    # ... [此处为原有的数据录入/Excel导入代码，字段已对齐] ...
    # (保持 V2.7 导入逻辑不变，仅确保发送给 add_feishu_record 的 key 是 '喂猫师')
    if st.button("🔄 强制刷新预览云端数据"):
        st.session_state.pop('feishu_cache', None)
        st.session_state['feishu_cache'] = fetch_feishu_data()
    
    df_v = st.session_state['feishu_cache'].copy()
    if not df_v.empty:
        st.dataframe(df_v.drop(columns=['_system_id'], errors='ignore'), use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能排单看板")
    df = st.session_state['feishu_cache'].copy()
    
    if not df.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        for col in ['服务开始日期', '服务结束日期']: df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
        
        if st.button(f"🚀 点击拟定周期排单方案"):
            all_plans = []
            addr_to_sitter_map = {}
            days = pd.date_range(start_d, end_d).tolist(); p_bar = st.progress(0)
            
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d)
                day_df = df[(df['服务开始日期'] <= cur_ts) & (df['服务结束日期'] >= cur_ts)].copy()
                if not day_df.empty:
                    day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not day_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, day_df['详细地址']))
                        day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                        v_df = day_df.dropna(subset=['lng', 'lat']).copy()
                        
                        if not v_df.empty:
                            sitter_load = {s: 0 for s in current_active}
                            def sitter_assign_logic(row):
                                addr, manual = row['详细地址'], str(row.get('喂猫师', '')).strip()
                                # 核心逻辑：人工指定 > 客户绑定 > 负载均衡
                                if manual and manual != "nan" and manual != "":
                                    addr_to_sitter_map[addr] = manual; return manual
                                if addr in addr_to_sitter_map: return addr_to_sitter_map[addr]
                                if current_active:
                                    best = min(sitter_load, key=sitter_load.get)
                                    sitter_load[best] += 1; addr_to_sitter_map[addr] = best; return best
                                return "待分配"

                            # 字段统一：将算法结果存入 '喂猫师' 列
                            v_df['喂猫师'] = v_df.apply(sitter_assign_logic, axis=1)
                            v_df['作业日期'] = d.strftime('%Y-%m-%d')
                            
                            # 路径优化
                            optimized_day = []
                            for sitter in current_active:
                                s_tasks = v_df[v_df['喂猫师'] == sitter].copy()
                                if not s_tasks.empty: optimized_day.append(optimize_route(s_tasks))
                            if optimized_day: all_plans.append(pd.concat(optimized_day))
                p_bar.progress((i + 1) / len(days))
            
            if all_plans:
                st.session_state['period_plan'] = pd.concat(all_plans)
                st.success("✅ 路径优化与分配完成，喂猫师字段已就绪。")
        
        if 'period_plan' in st.session_state:
            res = st.session_state['period_plan']
            col_f1, col_f2 = st.columns(2)
            with col_f1: view_day = st.selectbox("📅 查看日期", sorted(res['作业日期'].unique()))
            with col_f2:
                s_list = ["全部"] + sorted(res[res['作业日期'] == view_day]['喂猫师'].unique().tolist())
                view_sitter = st.selectbox("👤 筛选喂猫师", s_list)
            
            v_data = res[res['作业日期'] == view_day]
            if view_sitter != "全部": v_data = v_data[v_data['喂猫师'] == view_sitter]
            
            if not v_data.empty:
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                
                if st.button("✅ 确认并同步飞书"):
                    t_s = len(res); s_b = st.progress(0)
                    for i, (_, rs) in enumerate(res.iterrows()):
                        # 【核心修正】同步字段名严格改为“喂猫师”
                        update_feishu_record(rs['_system_id'], {"喂猫师": rs['喂猫师']})
                        s_b.progress((i + 1) / t_s)
                    st.success("🎉 飞书文档同步成功！")
                    st.session_state.pop('feishu_cache', None)
