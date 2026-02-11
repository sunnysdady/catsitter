import streamlit as st
import pandas as pd
import requests
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# --- 1. 核心连接配置 (从 Secrets 读取) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 核心算法：路径优化 ---
def get_distance(p1, p2):
    """计算经纬度直线距离"""
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """最近邻算法：按物理距离重新排列顺序"""
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
        # 确保关键列存在
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '喂猫师']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # 确保推送的字段名为“喂猫师”
    payload = {"fields": {k: ("" if pd.isna(v) else v) for k, v in fields.items()}}
    try:
        requests.patch(url, headers=headers, json=payload, timeout=10)
        return True
    except: return False

# --- 4. UI 视觉重构 (30px 巨幕适配) ---
def set_ui():
    st.markdown("""
        <style>
        /* 侧边栏按钮：巨幕 30px 极致适配 */
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important; height: 100px !important;
            background-color: #FFFFFF !important; color: #000000 !important;
            border: 3px solid #000000 !important; border-radius: 15px !important;
            font-size: 30px !important; font-weight: 900 !important;
            margin-bottom: 20px !important;
        }
        [data-testid="stSidebar"] div.stButton > button:hover { background-color: #000000 !important; color: #FFFFFF !important; }
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

# --- 5. 调度核心大脑 ---
def execute_smart_dispatch(df, active_sitters):
    # 1. 宠物与喂猫师绑定映射 (一只猫固定一人)
    cat_to_sitter = {}
    
    # 先扫描全表：如果这只猫以前有喂猫师，存入字典
    for _, row in df[df['喂猫师'] != ""].iterrows():
        key = f"{row['宠物名字']}_{row['详细地址']}"
        cat_to_sitter[key] = row['喂猫师']

    # 2. 统计当前每个人负责的猫的数量，用于负载均衡
    sitter_load = {s: 0 for s in active_sitters}
    for sitter in df['喂猫师']:
        if sitter in sitter_load: sitter_load[sitter] += 1

    # 3. 逐行分配
    for i, row in df.iterrows():
        # 如果当前单子已经有喂猫师，跳过
        if row['喂猫师'] != "": continue
        
        cat_key = f"{row['宠物名字']}_{row['详细地址']}"
        
        # 优先级 A：查看这只猫是否已经绑定过人
        if cat_key in cat_to_sitter:
            df.at[i, '喂猫师'] = cat_to_sitter[cat_key]
        else:
            # 优先级 B：新猫，分配给当前活最少的人
            if active_sitters:
                best_sitter = min(sitter_load, key=sitter_load.get)
                df.at[i, '喂猫师'] = best_sitter
                cat_to_sitter[cat_key] = best_sitter # 记录绑定关系
                sitter_load[best_sitter] += 1
            else:
                df.at[i, '喂猫师'] = "无人出勤"
    return df

# --- 6. 页面控制 ---
st.set_page_config(page_title="小猫直喂指挥中心", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"

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
        date_range = st.date_input("📅 范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))

if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

# --- 7. 看板渲染 ---
if st.session_state['page'] == "智能看板":
    st.title("🚀 智能调度排单看板")
    df = st.session_state['feishu_cache'].copy()
    
    if not df.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        # 转换日期格式进行计算
        for col in ['服务开始日期', '服务结束日期']:
            df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
        
        if st.button(f"✨ 1. 拟定分配方案 (含路径优化)"):
            all_plans = []
            days = pd.date_range(start_d, end_d).tolist()
            p_bar = st.progress(0)
            
            # 先跑全表的分配逻辑，确保“一只猫固定一人”
            df = execute_smart_dispatch(df, current_active)
            
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d)
                day_df = df[(df['服务开始日期'] <= cur_ts) & (df['服务结束日期'] >= cur_ts)].copy()
                
                if not day_df.empty:
                    # 坐标获取
                    with ThreadPoolExecutor(max_workers=10) as ex:
                        coords = list(ex.map(get_coords, day_df['详细地址']))
                    day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                    day_df = day_df.dropna(subset=['lng', 'lat'])
                    
                    # 路径优化
                    day_plans = []
                    for s in current_active:
                        s_tasks = day_df[day_df['喂猫师'] == s].copy()
                        if not s_tasks.empty: day_plans.append(optimize_route(s_tasks))
                    if day_plans:
                        res_day = pd.concat(day_plans)
                        res_day['作业日期'] = d.strftime('%Y-%m-%d')
                        all_plans.append(res_day)
                p_bar.progress((i + 1) / len(days))
            
            if all_plans:
                st.session_state['final_plan'] = pd.concat(all_plans)
                st.success("✅ 分配与路径优化拟定完成！")

        if 'final_plan' in st.session_state:
            res = st.session_state['final_plan']
            c1, c2 = st.columns(2)
            with c1: view_day = st.selectbox("📅 查看日期", sorted(res['作业日期'].unique()))
            with c2: view_sitter = st.selectbox("👤 筛选喂猫师", ["全部"] + sorted(res['喂猫师'].unique().tolist()))
            
            v_data = res[res['作业日期'] == view_day]
            if view_sitter != "全部": v_data = v_data[v_data['喂猫师'] == view_sitter]
            
            st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
            
            if st.button("✅ 2. 确认并将分配结果同步至飞书"):
                t_recs = len(res)
                sync_p = st.progress(0)
                # 按照系统 ID 回传喂猫师数据
                for i, (_, row) in enumerate(res.iterrows()):
                    # 这里是关键：将计算出的“喂猫师”写回飞书对应的 record_id
                    update_feishu_record(row['_system_id'], {"喂猫师": row['喂猫师']})
                    sync_p.progress((i + 1) / t_recs)
                st.success("🎉 数据回传成功！飞书文档已更新每只猫对应的喂猫师。")
                st.session_state.pop('feishu_cache', None)

else:
    st.title("📂 数据中心")
    if st.button("🔄 刷新预览云端数据"):
        st.session_state.pop('feishu_cache', None)
        st.session_state['feishu_cache'] = fetch_feishu_data()
    st.dataframe(st.session_state['feishu_cache'].drop(columns=['_system_id'], errors='ignore'), use_container_width=True)
