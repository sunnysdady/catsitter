import streamlit as st
import pandas as pd
import requests
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# --- 1. 核心连接配置 (Secrets 自动读取) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 核心算法大脑：路径、分配与预警 ---

def get_distance(p1, p2):
    """计算物理直线距离"""
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """最近邻路径算法：优化喂猫师的作业顺序"""
    if len(df_sitter) <= 1:
        df_sitter['拟定顺序'] = range(1, len(df_sitter) + 1)
        return df_sitter
    unvisited = df_sitter.to_dict('records')
    current_node = unvisited.pop(0)
    optimized_list = [current_node]
    while unvisited:
        next_node = min(unvisited, key=lambda x: get_distance(
            (current_node['lng'], current_node['lat']), (x['lng'], x['lat'])
        ))
        unvisited.remove(next_node)
        optimized_list.append(next_node)
        current_node = next_node
    res_df = pd.DataFrame(optimized_list)
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

def execute_smart_dispatch(df, active_sitters):
    """三级分配逻辑：人工优先 > 宠物绑定 > 负载均衡"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    cat_to_sitter_map = {}
    for _, row in df[df['喂猫师'] != ""].iterrows():
        cat_to_sitter_map[f"{row['宠物名字']}_{row['详细地址']}"] = row['喂猫师']
    sitter_load = {s: 0 for s in active_sitters}
    for sitter in df['喂猫师']:
        if sitter in sitter_load: sitter_load[sitter] += 1
    for i, row in df.iterrows():
        if row['喂猫师'] != "": continue
        cat_key = f"{row['宠物名字']}_{row['详细地址']}"
        if cat_key in cat_to_sitter_map:
            df.at[i, '喂猫师'] = cat_to_sitter_map[cat_key]
        elif active_sitters:
            best = min(sitter_load, key=sitter_load.get)
            df.at[i, '喂猫师'] = best
            cat_to_sitter_map[cat_key] = best
            sitter_load[best] += 1
    return df

def detect_duplicates(df):
    """检测重复订单与地址预警"""
    if df.empty: return []
    dups = df[df.duplicated(subset=['宠物名字', '详细地址'], keep=False)]
    return [f"⚠️ 预警：宠物 [{row['宠物名字']}] 在 [{row['详细地址']}] 存在重复单！" for _, row in dups.iterrows()]

# --- 3. 飞书 API 交互逻辑 ---

def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("tenant_access_token")
    except: return None

def fetch_feishu_data():
    token = get_feishu_token()
    if not token: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, params={"page_size": 500}, timeout=15).json()
        items = r.get("data", {}).get("items", [])
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '喂猫师', '投喂频率', 'lng', 'lat']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fields": {k: ("" if pd.isna(v) else v) for k, v in fields.items()}}
    try:
        return requests.patch(url, headers=headers, json=payload, timeout=10).json().get("code") == 0
    except: return False

# --- 4. UI 与地理工具 (30px 巨幕) ---

def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; }
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important; height: 100px !important;
            border: 4px solid #000 !important; border-radius: 15px !important;
            font-size: 30px !important; font-weight: 900 !important;
            box-shadow: 5px 5px 0px #000;
        }
        [data-testid="stSidebar"] div.stButton > button:hover { background-color: #000 !important; color: #FFF !important; }
        .patch-box { background: #f0f5ff; border: 2px dashed #1890ff; padding: 20px; border-radius: 15px; margin-top: 20px; }
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

# --- 5. 页面路由 ---

st.set_page_config(page_title="小猫直喂调度中心", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    if st.session_state['page'] == "智能看板":
        st.divider(); active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))

if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

# --- 6. 模块渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (云端录入与坐标修正)")
    
    # 1. 坐标修正补丁 (新增功能)
    st.markdown('<div class="patch-box">', unsafe_allow_html=True)
    st.subheader("🌐 经纬度手动修正补丁")
    df_fix = st.session_state['feishu_cache'].copy()
    target_rec = st.selectbox("选择需要修正坐标的宠物订单", df_fix['宠物名字'] + " - " + df_fix['详细地址'])
    if target_rec:
        rec_id = df_fix.iloc[df_fix[df_fix['宠物名字'] + " - " + df_fix['详细地址'] == target_rec].index[0]]['_system_id']
        c_fix1, c_fix2 = st.columns(2)
        new_lng = c_fix1.text_input("修正经度 (Longitude)")
        new_lat = c_fix2.text_input("修正纬度 (Latitude)")
        if st.button("💾 应用并更新云端坐标"):
            if update_feishu_record(rec_id, {"lng": new_lng, "lat": new_lat}):
                st.success("✅ 坐标已修正并回写云端！"); st.session_state.pop('feishu_cache', None); st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # 2. 重复预警与数据表格
    st.divider()
    warns = detect_duplicates(st.session_state['feishu_cache'])
    for w in warns: st.error(w)
    st.dataframe(st.session_state['feishu_cache'].drop(columns=['_system_id'], errors='ignore'), use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能调度看板")
    res_raw = st.session_state['feishu_cache'].copy()
    
    if not res_raw.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        for c in ['服务开始日期', '服务结束日期']: res_raw[c] = pd.to_datetime(res_raw[c], unit='ms', errors='coerce')
        
        if st.button("✨ 拟定方案 (接入路径算法)"):
            all_plans = []
            days = pd.date_range(date_range[0], date_range[1]).tolist()
            res_raw = execute_smart_dispatch(res_raw, current_active) # 三级分配
            
            p_bar = st.progress(0)
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d)
                day_df = res_raw[(res_raw['服务开始日期'] <= cur_ts) & (res_raw['服务结束日期'] >= cur_ts)].copy()
                if not day_df.empty:
                    day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not day_df.empty:
                        # 坐标逻辑：优先取飞书里的 lng/lat，没有再去高德查
                        def fill_coords(row):
                            if row['lng'] and row['lat']: return float(row['lng']), float(row['lat'])
                            return get_coords(row['详细地址'])
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(fill_coords, [r for _, r in day_df.iterrows()]))
                        day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                        day_df = day_df.dropna(subset=['lng', 'lat'])
                        
                        day_res = []
                        for s in current_active:
                            s_tasks = day_df[day_df['喂猫师'] == s].copy()
                            if not s_tasks.empty: day_res.append(optimize_route(s_tasks))
                        if day_res:
                            concat_day = pd.concat(day_res)
                            concat_day['作业日期'] = d.strftime('%Y-%m-%d')
                            all_plans.append(concat_day)
                p_bar.progress((i + 1) / len(days))
            st.session_state['final_plan'] = pd.concat(all_plans) if all_plans else None
            st.success("✅ 全周期调度完成！")

        if st.session_state.get('final_plan') is not None:
            res_final = st.session_state['final_plan']
            col_f1, col_f2 = st.columns(2)
            # 修复 NameError：在 col 作用域内确保赋值
            view_day = col_f1.selectbox("📅 查看日期", sorted(res_final['作业日期'].unique()))
            view_sit = col_f2.selectbox("👤 筛选喂猫师", ["全部"] + sorted(res_final['喂猫师'].unique().tolist()))
            
            v_data = res_final[res_final['作业日期'] == view_day]
            if view_sit != "全部": v_data = v_data[v_data['喂猫师'] == view_sit]
            
            if not v_data.empty:
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                if st.button("✅ 确认并回写飞书"):
                    sync_p = st.progress(0); total = len(res_final)
                    for i, (_, row) in enumerate(res_final.iterrows()):
                        update_feishu_record(row['_system_id'], {"喂猫师": row['喂猫师']})
                        sync_p.progress((i + 1) / total)
                    st.success("🎉 飞书更新完成！"); st.session_state.pop('feishu_cache', None)
