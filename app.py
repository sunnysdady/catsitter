import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import json

# --- 1. 核心配置清洗 (解决 404 关键) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() # 多维表格 ID
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() # 数据表 ID
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 核心算法：调度大脑 ---

def get_distance(p1, p2):
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """路径算法：锁定 ID 传递"""
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
    """一只猫固定一人逻辑"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    
    # 建立【宠物名_地址 -> 喂猫师】映射
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

# --- 3. 飞书 API 交互逻辑 (URL 路径修复) ---

def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("tenant_access_token")
    except: return None

def fetch_feishu_data():
    token = get_feishu_token()
    if not token: return pd.DataFrame()
    # 路径构造检查
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, params={"page_size": 500}, timeout=15).json()
        items = r.get("data", {}).get("items", [])
        if not items: return pd.DataFrame()
        # 记录关键 record_id
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '喂猫师', '投喂频率', '备注', 'lng', 'lat']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_final(record_id, sitter_name):
    """彻底解决 404：清洗所有路径变量"""
    token = get_feishu_token()
    clean_rid = str(record_id).strip()
    # 构建精准 URL 路径
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{clean_rid}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fields": {"喂猫师": str(sitter_name)}}
    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            res = r.json()
            if res.get("code") == 0: return True, "成功"
            return False, f"API 逻辑错误: {res.get('msg')}"
        return False, f"URL错误(404)或权限不足({r.status_code})"
    except Exception as e:
        return False, f"异常: {str(e)}"

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

# --- 4. 视觉风格与 UI (30px) ---

def set_ui():
    st.markdown("""
        <style>
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important; height: 100px !important;
            border: 4px solid #000 !important; border-radius: 15px !important;
            font-size: 30px !important; font-weight: 900 !important;
            box-shadow: 6px 6px 0px #000;
            background-color: #FFFFFF !important; color: #000000 !important;
        }
        .stDataFrame { font-size: 16px !important; }
        .diag-box { background: #fff1f0; border: 1px solid #ffa39e; padding: 10px; border-radius: 5px; font-family: monospace; }
        </style>
        """, unsafe_allow_html=True)

# --- 5. 流程中心 ---

st.set_page_config(page_title="指挥中心 V21.0", layout="wide")
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
        date_range = st.date_input("📅 调度范围", value=(datetime.now(), datetime.now() + timedelta(days=1)))

# --- 6. 模块渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (全功能管理)")
    col_in1, col_in2 = st.columns(2)
    with col_in1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("Excel", type=["xlsx"])
            if up_file and st.button("🚀 启动数据导入"):
                df_up = pd.read_excel(up_file); p_bar = st.progress(0); tok = get_feishu_token()
                for i, (_, row) in enumerate(df_up.iterrows()):
                    f = {"详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(), "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000), "备注": str(row.get('备注', ''))}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tok}"}, json={"fields": f})
                    p_bar.progress((i + 1) / len(df_up))
                st.success("批量完成！"); st.session_state.pop('feishu_cache', None); st.rerun()
    with col_in2:
        with st.expander("✍️ 单条信息手动录入"):
            with st.form("single"):
                a = st.text_input("详细地址*"); n = st.text_input("名字"); s = st.date_input("开始日期"); e = st.date_input("结束日期")
                if st.form_submit_button("保存至云端"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "投喂频率": 1, "服务开始日期": int(datetime.combine(s, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(e, datetime.min.time()).timestamp()*1000)}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                    st.success("录入完成！"); st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    if st.button("🔄 强制刷新预览数据"):
        st.session_state.pop('feishu_cache', None); st.session_state['feishu_cache'] = fetch_feishu_data(); st.rerun()
    
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        # --- 预览净化：日期格式修复 ---
        disp = df_p.drop(columns=['lng', 'lat', '_system_id'], errors='ignore')
        for c in ['服务开始日期', '服务结束日期']:
            disp[c] = pd.to_datetime(disp[c], unit='ms', errors='coerce').dt.strftime('%Y-%m-%d')
        st.dataframe(disp, use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度看板 (链路加固 V21.0)")
    df_kb = st.session_state['feishu_cache'].copy()
    if not df_kb.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        for c in ['服务开始日期', '服务结束日期']: df_kb[c] = pd.to_datetime(df_kb[c], unit='ms', errors='coerce')
        if st.button("✨ 拟定方案"):
            all_plans = []
            days = pd.date_range(date_range[0], date_range[1]).tolist()
            df_kb = execute_smart_dispatch(df_kb, current_active)
            p_bar = st.progress(0)
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d); d_df = df_kb[(df_kb['服务开始日期'] <= cur_ts) & (df_kb['服务结束日期'] >= cur_ts)].copy()
                if not d_df.empty:
                    d_df = d_df[d_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not d_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, d_df['详细地址']))
                        d_df[['lng', 'lat']] = pd.DataFrame(coords, index=d_df.index); d_df = d_df.dropna(subset=['lng', 'lat'])
                        d_res = []
                        for s in current_active:
                            s_tasks = d_df[d_df['喂猫师'] == s].copy()
                            if not s_tasks.empty: d_res.append(optimize_route(s_tasks))
                        if d_res:
                            cd = pd.concat(d_res); cd['作业日期'] = d.strftime('%Y-%m-%d'); all_plans.append(cd)
                p_bar.progress((i + 1) / len(days))
            st.session_state['final_plan_v21'] = pd.concat(all_plans) if all_plans else None
            st.success("✅ 方案拟定完成！")

        if st.session_state.get('final_plan_v21') is not None:
            res_f = st.session_state['final_plan_v21']
            v_day = st.selectbox("📅 选择日期", sorted(res_f['作业日期'].unique()))
            v_data = res_f[res_f['作业日期'] == v_day]
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11), layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]))
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                
                # 同步回写按钮 (核心修复 404)
                if st.button("✅ 确认并同步飞书"):
                    logs = []; suc = 0; tot = len(res_f); sync_p = st.progress(0)
                    for i, (_, row) in enumerate(res_f.iterrows()):
                        if row.get('_system_id') and row.get('喂猫师'):
                            ok, msg = update_feishu_final(row['_system_id'], row['喂猫师'])
                            if ok: suc += 1
                            else: logs.append(f"猫[{row['宠物名字']}]: {msg}")
                        sync_p.progress((i + 1) / tot)
                    st.success(f"🎉 同步完成！回写 {suc} 条记录。")
                    if logs:
                        st.error("同步异常报告：")
                        for l in logs: st.markdown(f'<div class="diag-box">{l}</div>', unsafe_allow_html=True)
                    st.session_state.pop('feishu_cache', None)
