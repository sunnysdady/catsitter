import streamlit as st
import pandas as pd
import requests
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# --- 1. 核心配置与授权 (Secrets 读取) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 核心算法大脑：调度与优化 ---

def get_distance(p1, p2):
    """计算物理直线距离"""
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """最近邻路径算法：按物理距离排列作业顺序"""
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
    """三级派单规则：人工指定 > 一只猫固定一人 > 负载均衡"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    
    # 建立猫与人的绑定映射
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

# --- 3. 飞书与地理 API 逻辑 ---

def get_feishu_token():
    url = "https://open.feisku.cn/open-apis/auth/v3/tenant_access_token/internal"
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
        # 强制补全关键字段，防止 KeyError
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

@st.cache_data(show_spinner=False)
def get_coords(address):
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_API_KEY}&address=深圳市{address}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            lng, lat = r['geocodes'][0]['location'].split(',')
            return float(lng), float(lat)
    except: return None, None

# --- 4. 视觉方案 (30px 巨幕) ---

def set_ui():
    st.markdown("""
        <style>
        /* 侧边栏 30px 巨幕按钮适配 */
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important; height: 100px !important;
            border: 4px solid #000 !important; border-radius: 15px !important;
            font-size: 30px !important; font-weight: 900 !important;
            box-shadow: 6px 6px 0px #000;
        }
        .stDataFrame { font-size: 16px !important; }
        .patch-box { background: #e6f7ff; border: 2px dashed #1890ff; padding: 20px; border-radius: 15px; margin-bottom: 25px; }
        </style>
        """, unsafe_allow_html=True)

# --- 5. 流程控制 ---

st.set_page_config(page_title="指挥中心 V3.4", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    if st.session_state['page'] == "智能看板":
        st.divider(); sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 调度范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))

# --- 6. 模块渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (坐标修正补丁)")
    
    # 坐标手动补丁模块
    st.markdown('<div class="patch-box">', unsafe_allow_html=True)
    st.subheader("🌐 经纬度手动修正补丁")
    df_fix = st.session_state['feishu_cache'].copy()
    if not df_fix.empty:
        target = st.selectbox("选择需修正的订单", df_fix['宠物名字'] + " | " + df_fix['详细地址'])
        rec_id = df_fix.iloc[df_fix[df_fix['宠物名字'] + " | " + df_fix['详细地址'] == target].index[0]]['_system_id']
        c1, c2 = st.columns(2)
        n_lng = c1.text_input("修正经度")
        n_lat = c2.text_input("修正纬度")
        if st.button("💾 确认更新坐标"):
            if update_feishu_record(rec_id, {"lng": n_lng, "lat": n_lat}):
                st.success("✅ 坐标已同步云端！"); st.session_state.pop('feishu_cache', None); st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)
    
    if st.button("🔄 刷新预览"):
        st.session_state.pop('feishu_cache', None); st.session_state['feishu_cache'] = fetch_feishu_data(); st.rerun()
    st.dataframe(st.session_state['feishu_cache'].drop(columns=['_system_id'], errors='ignore'), use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能调度看板 (路径优化版)")
    df_kb = st.session_state['feishu_cache'].copy()
    
    if not df_kb.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        for c in ['服务开始日期', '服务结束日期']: df_kb[c] = pd.to_datetime(df_kb[c], unit='ms', errors='coerce')
        
        if st.button("✨ 拟定方案 (接入路径算法)"):
            all_plans = []
            days = pd.date_range(date_range[0], date_range[1]).tolist()
            # 执行分配大脑
            df_kb = execute_smart_dispatch(df_kb, current_active)
            
            p_bar = st.progress(0)
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d)
                day_df = df_kb[(df_kb['服务开始日期'] <= cur_ts) & (df_kb['服务结束日期'] >= cur_ts)].copy()
                if not day_df.empty:
                    day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not day_df.empty:
                        # 修复 KeyError 的关键：增强型 fill_coords
                        def fill_coords(row):
                            # 优先检查是否存在该键，再检查内容是否为空
                            try:
                                if 'lng' in row and 'lat' in row and row['lng'] and row['lat']:
                                    return float(row['lng']), float(row['lat'])
                            except: pass
                            return get_coords(row['详细地址'])

                        with ThreadPoolExecutor(max_workers=10) as ex:
                            coords = list(ex.map(fill_coords, [r for _, r in day_df.iterrows()]))
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
            st.success("✅ 路径优化调度完成！")

        if st.session_state.get('final_plan') is not None:
            res_final = st.session_state['final_plan']
            col_f1, col_f2 = st.columns(2)
            v_day = col_f1.selectbox("📅 日期", sorted(res_final['作业日期'].unique()))
            v_sit = col_f2.selectbox("👤 喂猫师", ["全部"] + sorted(res_final['喂猫师'].unique().tolist()))
            
            v_data = res_final[res_final['作业日期'] == v_day]
            if v_sit != "全部": v_data = v_data[v_data['喂猫师'] == v_sit]
            
            if not v_data.empty:
                # 展现路径优化后的顺序表格
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                if st.button("✅ 确认并同步回写飞书"):
                    sync_p = st.progress(0); total = len(res_final)
                    for i, (_, row) in enumerate(res_final.iterrows()):
                        # 将计算出的“喂猫师”字段回写
                        update_feishu_record(row['_system_id'], {"喂猫师": row['喂猫师']})
                        sync_p.progress((i + 1) / total)
                    st.success("🎉 云端更新已完成！字段：喂猫师。")
                    st.session_state.pop('feishu_cache', None)
