import streamlit as st
import pandas as pd
import requests
from sklearn.cluster import KMeans
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import time

# --- 1. 核心配置 (Secrets) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "")
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "")
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "") 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "") 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "")

# --- 2. 飞书 API 交互逻辑 ---
def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET})
    return r.json().get("app_access_token")

def fetch_feishu_data():
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, params={"page_size": 500}).json()
        items = r.get("data", {}).get("items", [])
        if not items: return pd.DataFrame()
        df = pd.DataFrame([dict(i['fields'], record_id=i['record_id']) for i in items])
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '投喂频率', '备注', '建议顺序']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def check_duplicate_robust(fields, df):
    if df.empty: return False
    new_addr, new_name = str(fields['详细地址']).strip(), str(fields['宠物名字']).strip()
    new_date = pd.to_datetime(fields['服务开始日期'], unit='ms').strftime('%Y-%m-%d')
    temp_df = df.copy()
    temp_df['服务开始日期_std'] = pd.to_datetime(temp_df['服务开始日期'], unit='ms', errors='coerce').dt.strftime('%Y-%m-%d')
    match = temp_df[
        (temp_df['详细地址'].str.strip() == new_addr) & 
        (temp_df['宠物名字'].str.strip() == new_name) & 
        (temp_df['服务开始日期_std'] == new_date)
    ]
    return not match.empty

def add_feishu_record(fields):
    current_df = fetch_feishu_data()
    if check_duplicate_robust(fields, current_df): return "duplicate"
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json={"fields": fields}, timeout=10)
    return "success" if response.json().get("code") == 0 else "error"

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    requests.patch(url, headers=headers, json={"fields": fields})

# --- 3. UI 设置 ---
def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', Arial !important; }
        header { visibility: hidden !important; }
        div.stButton > button { background-color: #FFFFFF !important; color: #000000 !important; border: 1px solid #000000 !important; border-radius: 4px !important; width: 100% !important; font-weight: bold !important; }
        [data-testid="stSidebar"] { background-color: #F8F9FA !important; border-right: 1px solid #E9ECEF !important; }
        [data-testid="stSidebar"] label { color: #000000 !important; font-weight: bold !important; }
        h1, h2, h3 { color: #000000 !important; border-bottom: 2px solid #000000; padding-bottom: 5px; }
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
    except: pass
    return None, None

# --- 4. 页面主体 ---
st.set_page_config(page_title="小猫直喂-调度中心", layout="wide")
set_ui()

# 侧边栏导航
with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    # 核心改动：侧边栏入口
    menu = st.radio("功能导航", ["📂 数据中心 (导入/录入)", "🚀 智能调度看板"])

# 数据缓存初始化
if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

if menu == "📂 数据中心 (导入/录入)":
    st.title("📂 数据录入中心")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("➕ 批量导入 Excel"):
            up_file = st.file_uploader("选择 Excel", type=["xlsx"])
            if up_file and st.button("🚀 启动查重同步"):
                df_up = pd.read_excel(up_file)
                total, success, skipped = len(df_up), 0, 0
                p_bar = st.progress(0)
                p_text = st.empty()
                for i, (_, row) in enumerate(df_up.iterrows()):
                    p_text.text(f"同步中: {i+1}/{total}")
                    s_ts = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                    e_ts = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                    payload = {
                        "详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(),
                        "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": s_ts, "服务结束日期": e_ts,
                        "备注": str(row.get('备注', ''))
                    }
                    res = add_feishu_record(payload)
                    if res == "success": success += 1
                    elif res == "duplicate": skipped += 1
                    p_bar.progress((i + 1) / total)
                p_text.empty(); p_bar.empty()
                st.success(f"✅ 完成！同步 {success} 条，跳过重复 {skipped} 条。")
                st.session_state['feishu_cache'] = fetch_feishu_data()
    with c2:
        with st.expander("➕ 单条补单"):
            with st.form("manual", clear_on_submit=True):
                addr = st.text_input("详细地址*")
                cat = st.text_input("宠物名", value="小胖猫")
                f1, f2 = st.columns(2)
                sd, ed = f1.date_input("开始"), f2.date_input("结束")
                freq = st.number_input("频率", min_value=1, value=1)
                if st.form_submit_button("保存到云端"):
                    with st.spinner("查重校验中..."):
                        payload = {
                            "详细地址": addr.strip(), "宠物名字": cat.strip(), "投喂频率": freq,
                            "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000),
                            "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)
                        }
                        res = add_feishu_record(payload)
                        if res == "success": st.balloons(); st.success("✅ 存入成功！")
                        elif res == "duplicate": st.error("❌ 该单已存在。")
                        st.session_state['feishu_cache'] = fetch_feishu_data()
    st.divider()
    if st.button("🔄 刷新查看云端数据预览"):
        st.session_state['feishu_cache'] = fetch_feishu_data()
        df_view = st.session_state['feishu_cache'].copy()
        for c in ['服务开始日期', '服务结束日期']:
            if not df_view.empty: df_view[c] = pd.to_datetime(df_view[c], unit='ms').dt.strftime('%Y-%m-%d')
        st.dataframe(df_view.drop(columns=['record_id'], errors='ignore'), use_container_width=True)

else:
    st.title("🚀 智能调度看板")
    # 核心改动：将看板设置挪到侧边栏
    with st.sidebar:
        st.divider()
        st.subheader("⚙️ 看板配置")
        active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (今日出勤)", value=True)]
        date_range = st.date_input("📅 选择作业日期/周期", value=(datetime.now(), datetime.now() + timedelta(days=2)))
    
    df = st.session_state['feishu_cache'].copy()
    if not df.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        for col in ['服务开始日期', '服务结束日期']: df[col] = pd.to_datetime(df[col], unit='ms')
        start_d, end_d = date_range
        if st.button(f"🚀 点击执行：{start_d} 至 {end_d} 周期均衡排单"):
            all_plans = []
            days = pd.date_range(start_d, end_d).tolist()
            with st.spinner("正在计算地理路径与单量平衡..."):
                for d in days:
                    cur_ts = pd.Timestamp(d)
                    day_df = df[(df['服务开始日期'] <= cur_ts) & (df['服务结束日期'] >= cur_ts)].copy()
                    if not day_df.empty:
                        day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not day_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex:
                            coords = list(ex.map(get_coords, day_df['详细地址']))
                        day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                        v_df = day_df.dropna(subset=['lng', 'lat']).copy()
                        if not v_df.empty:
                            v_df['拟定人'] = current_active[0] 
                            v_df['拟定顺序'] = v_df.groupby('拟定人').cumcount() + 1
                            v_df['作业日期'] = d.strftime('%Y-%m-%d')
                            all_plans.append(v_df)
            if all_plans:
                st.session_state['period_plan'] = pd.concat(all_plans)
                st.success("✅ 周期排单拟定完成！")
        
        if 'period_plan' in st.session_state:
            res = st.session_state['period_plan']
            c_view1, c_view2 = st.columns(2)
            view_day = c_view1.selectbox("📅 切换显示日期", sorted(res['作业日期'].unique()))
            worker = c_view2.selectbox("👤 查看师视角", current_active)
            v_data = res[(res['作业日期'] == view_day) & (res['拟定人'] == worker)]
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                                        layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]))
                st.data_editor(v_data[['拟定顺序', '宠物名字', '详细地址', '备注']], use_container_width=True)
                if st.button("✅ 确认并全周期同步至飞书"):
                    t_sync = len(res); s_bar = st.progress(0); s_text = st.empty()
                    for i, (_, r_sync) in enumerate(res.iterrows()):
                        s_text.text(f"回写云端: {i+1}/{t_sync}")
                        update_feishu_record(r_sync['record_id'], {"喂猫师": r_sync['拟定人'], "建议顺序": r_sync['拟定顺序']})
                        s_bar.progress((i + 1) / t_sync)
                    s_text.empty(); s_bar.empty(); st.success("🎉 全周期方案已成功同步。")
