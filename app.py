import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import re

# --- 1. 配置清洗 ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'(bas|tbl|rec)[a-zA-Z0-9]+', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "")) 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 调度大脑逻辑 ---

def get_distance(p1, p2):
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
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
    """一只猫固定一人：锁定绑定关系"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    cat_to_sitter_map = {}
    for _, row in df.iterrows():
        s_val = str(row.get('喂猫师', '')).strip()
        if s_val and s_val not in ["nan", ""]:
            cat_to_sitter_map[f"{row['宠物名字']}_{row['详细地址']}"] = s_val
    sitter_load = {s: 0 for s in active_sitters}
    for s in df['喂猫师']:
        if s in sitter_load: sitter_load[s] += 1
    for i, row in df.iterrows():
        if str(row.get('喂猫师', '')).strip() not in ["", "nan"]: continue
        key = f"{row['宠物名字']}_{row['详细地址']}"
        if key in cat_to_sitter_map:
            df.at[i, '喂猫师'] = cat_to_sitter_map[key]
        elif active_sitters:
            best = min(sitter_load, key=sitter_load.get)
            df.at[i, '喂猫师'] = best
            cat_to_sitter_map[key] = best
            sitter_load[best] += 1
    return df

# --- 3. 飞书 API 交互 ---

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
        if not items: return pd.DataFrame()
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        # 强制格式转换：数字戳转日期
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        for col in ['宠物名字', '详细地址', '喂猫师', '投喂频率', '备注', 'lng', 'lat']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_final(record_id, sitter_name):
    token = get_feishu_token()
    clean_rid = str(record_id).strip()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{clean_rid}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fields": {"喂猫师": str(sitter_name)}}
    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            res = r.json()
            return (True, "成功") if res.get("code") == 0 else (False, f"API:{res.get('msg')}")
        return False, f"HTTP {r.status_code}"
    except Exception as e: return False, str(e)

@st.cache_data(show_spinner=False)
def get_coords(address):
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_API_KEY}&address=深圳市{address}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return float(loc[0]), float(loc[1])
    except: pass
    return None, None

# --- 4. UI 视觉方案 (30px) ---

def set_ui():
    st.markdown("""
        <style>
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important; height: 100px !important;
            border: 4px solid #000 !important; border-radius: 15px !important;
            font-size: 30px !important; font-weight: 900 !important;
            box-shadow: 6px 6px 0px #000;
            background-color: #FFFFFF !important;
        }
        .stDataFrame { font-size: 16px !important; }
        .stat-card { background: #f0f2f5; border-radius: 10px; padding: 15px; margin-bottom: 20px; border-left: 5px solid #1890ff; }
        </style>
        """, unsafe_allow_html=True)

# --- 5. 流程中心 ---

st.set_page_config(page_title="指挥中心 V26.0", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"

# --- 6. 模块渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (导入、录入与快照)")
    col_in1, col_in2 = st.columns(2)
    with col_in1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("Excel 文件", type=["xlsx"])
            if up_file and st.button("🚀 启动批量录入"):
                df_up = pd.read_excel(up_file); p_bar = st.progress(0); tok = get_feishu_token()
                for i, (_, row) in enumerate(df_up.iterrows()):
                    f = {"详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(), "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tok}"}, json={"fields": f})
                    p_bar.progress((i + 1) / len(df_up))
                st.success("批量同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()
    with col_in2:
        with st.expander("✍️ 单条手动录入"):
            with st.form("manual_cat"):
                a = st.text_input("地址*"); n = st.text_input("宠物名"); sd = st.date_input("开始日期"); ed = st.date_input("结束日期")
                if st.form_submit_button("💾 保存至云端"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "投喂频率": 1, "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                    st.success("录入完成！"); st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    if st.button("🔄 强制刷新数据预览"):
        st.session_state.pop('feishu_cache', None); st.session_state['feishu_cache'] = fetch_feishu_data(); st.rerun()
    
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        # --- 净化：隐藏坐标，格式化日期 ---
        disp = df_p.drop(columns=['lng', 'lat', '_system_id'], errors='ignore')
        for c in ['服务开始日期', '服务结束日期']:
            if c in disp.columns: disp[c] = pd.to_datetime(disp[c]).dt.strftime('%Y-%m-%d')
        st.dataframe(disp, use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能调度看板 (排单透视 V26.0)")
    df_kb = st.session_state['feishu_cache'].copy()
    
    sitters = ["梦蕊", "依蕊"]
    current_active = [s for s in sitters if st.sidebar.checkbox(f"{s} (出勤)", value=True)]
    date_range = st.sidebar.date_input("📅 调度全周期", value=(datetime.now(), datetime.now() + timedelta(days=2)))

    if not df_kb.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        for c in ['服务开始日期', '服务结束日期']: df_kb[c] = pd.to_datetime(df_kb[c], unit='ms', errors='coerce')
        
        # --- 核心排错：排单逻辑诊断 ---
        if st.button("✨ 执行全周期排单"):
            all_plans = []
            days = pd.date_range(date_range[0], date_range[1]).tolist()
            
            # 分配规则审计
            df_kb = execute_smart_dispatch(df_kb, current_active)
            
            total_filtered_expired = 0
            total_filtered_frequency = 0
            
            p_bar = st.progress(0)
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d)
                # 1. 过滤不在日期范围内的
                day_df = df_kb[(df_kb['服务开始日期'] <= cur_ts) & (df_kb['服务结束日期'] >= cur_ts)].copy()
                total_filtered_expired += (len(df_kb) - len(day_df))
                
                if not day_df.empty:
                    # 2. 频率过滤
                    day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not day_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, day_df['详细地址']))
                        day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index); day_df = day_df.dropna(subset=['lng', 'lat'])
                        
                        day_res = []
                        for s in current_active:
                            s_tasks = day_df[day_df['喂猫师'] == s].copy()
                            if not s_tasks.empty: day_res.append(optimize_route(s_tasks))
                        if day_res:
                            cd = pd.concat(day_res); cd['作业日期'] = d.strftime('%Y-%m-%d'); all_plans.append(cd)
                p_bar.progress((i + 1) / len(days))
            
            st.session_state['final_plan_v26'] = pd.concat(all_plans) if all_plans else None
            st.session_state['diag_info'] = f"总数据库记录: {len(df_kb)} 条 | 生成总任务数: {len(st.session_state['final_plan_v26']) if all_plans else 0}"
            st.success("✅ 方案拟定完成！")

        # --- 排单数量透视 ---
        if 'diag_info' in st.session_state:
            st.markdown(f'<div class="stat-card">📊 {st.session_state["diag_info"]}</div>', unsafe_allow_html=True)

        if st.session_state.get('final_plan_v26') is not None:
            res_f = st.session_state['final_plan_v26']
            c_f1, c_f2 = st.columns(2)
            v_day = c_f1.selectbox("📅 1. 选择查看日期", sorted(res_f['作业日期'].unique()))
            v_sitters = ["全部"] + sorted(res_f[res_f['作业日期'] == v_day]['喂猫师'].unique().tolist())
            v_sit = c_f2.selectbox("👤 2. 筛选具体喂猫师", v_sitters)
            
            v_data = res_f[res_f['作业日期'] == v_day]
            if v_sit != "全部": v_data = v_data[v_data['喂猫师'] == v_sit]
            
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11), layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=350)]))
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                
                if st.button("✅ 3. 确认并同步飞书"):
                    suc = 0; sync_p = st.progress(0)
                    for i, (_, row) in enumerate(res_f.iterrows()):
                        if row.get('_system_id') and row.get('喂猫师'):
                            if update_feishu_final(row['_system_id'], row['喂猫师']): suc += 1
                        sync_p.progress((i + 1) / len(res_f))
                    st.success(f"🎉 同步完成！回写 {suc} 条记录。")
                    st.session_state.pop('feishu_cache', None)
