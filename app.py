import streamlit as st
import pandas as pd
import requests
from sklearn.cluster import KMeans
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np

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
        
        # 补齐列名，防止后续 KeyErr
        required_cols = ['宠物名字', '服务开始日期', '服务结束日期', '投喂频率', '详细地址', '喂猫师', '备注', '建议顺序']
        for col in required_cols:
            if col not in df.columns: df[col] = ""

        # 日期转换处理
        for col in ['服务开始日期', '服务结束日期']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], unit='ms').dt.strftime('%Y-%m-%d')
        
        return df
    except: return pd.DataFrame()

# --- 核心新增：去重校验函数 ---
def is_duplicate(fields, existing_df):
    if existing_df.empty: return False
    # 定义唯一标识：地址 + 宠物名 + 开始日期
    new_date_str = pd.to_datetime(fields['服务开始日期'], unit='ms').strftime('%Y-%m-%d')
    match = existing_df[
        (existing_df['详细地址'] == fields['详细地址']) & 
        (existing_df['宠物名字'] == fields['宠物名字']) & 
        (existing_df['服务开始日期'] == new_date_str)
    ]
    return not match.empty

def add_feishu_record(fields, existing_df=None):
    # 如果传入了现有数据，则先查重
    if existing_df is not None and is_duplicate(fields, existing_df):
        return "duplicate"
    
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

# --- 3. UI 视觉适配 ---
def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', '微软雅黑', Arial !important; }
        header { visibility: hidden !important; }
        div.stButton > button { background-color: #FFFFFF !important; color: #000000 !important; border: 1px solid #000000 !important; border-radius: 4px !important; width: 100% !important; font-weight: bold !important; }
        [data-testid="stSidebar"] { background-color: #F8F9FA !important; border-right: 1px solid #E9ECEF !important; }
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

# --- 4. 页面执行 ---
st.set_page_config(page_title="小猫直喂-调度中心", layout="wide")
set_ui()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    active_sitters = ["梦蕊", "依蕊"]
    current_active = [s for s in active_sitters if st.checkbox(f"{s} (今日出勤)", value=True)]
    st.divider()
    date_range = st.date_input("📅 选择作业周期", value=(datetime.now(), datetime.now() + timedelta(days=2)))

st.title("🐱 小猫直喂-云端大脑 (查重增强版)")
tab1, tab2 = st.tabs(["📂 数据中心", "🚀 智能调度看板"])

# --- Tab 1: 录入逻辑优化 ---
with tab1:
    st.subheader("📝 订单同步")
    c1, c2 = st.columns(2)
    
    # 获取当前数据用于查重
    if 'feishu_cache' not in st.session_state:
        st.session_state['feishu_cache'] = fetch_feishu_data()
    current_data = st.session_state['feishu_cache']

    with c1:
        with st.expander("➕ 批量导入 Excel"):
            up_file = st.file_uploader("选择 Excel", type=["xlsx"])
            if up_file and st.button("🚀 启动查重导入"):
                df_up = pd.read_excel(up_file)
                success, skipped = 0, 0
                for _, row in df_up.iterrows():
                    s_ts = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                    e_ts = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                    payload = {
                        "详细地址": str(row['详细地址']), "宠物名字": str(row.get('宠物名字', '小猫')),
                        "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": s_ts, "服务结束日期": e_ts,
                        "备注": str(row.get('备注', ''))
                    }
                    res = add_feishu_record(payload, current_data)
                    if res == "success": success += 1
                    elif res == "duplicate": skipped += 1
                st.success(f"✅ 导入完毕：成功 {success} 条，跳过重复 {skipped} 条。")
                st.session_state['feishu_cache'] = fetch_feishu_data()

    with c2:
        with st.expander("➕ 单条补单"):
            with st.form("manual", clear_on_submit=True):
                addr = st.text_input("详细地址*")
                cat = st.text_input("宠物名", value="小胖猫")
                f_c1, f_c2 = st.columns(2)
                sd, ed = f_c1.date_input("开始"), f_c2.date_input("结束")
                freq = st.number_input("频率", min_value=1, value=1)
                if st.form_submit_button("保存到云端"):
                    payload = {
                        "详细地址": addr, "宠物名字": cat, "投喂频率": freq,
                        "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000),
                        "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)
                    }
                    res = add_feishu_record(payload, current_data)
                    if res == "success": st.info("✅ 已存入飞书。")
                    elif res == "duplicate": st.error("❌ 订单重复！该地址及宠物在同一日期已存在单据。")
                    st.session_state['feishu_cache'] = fetch_feishu_data()

    st.divider()
    if st.button("🔄 刷新预览"):
        st.session_state['feishu_cache'] = fetch_feishu_data()
        st.dataframe(st.session_state['feishu_cache'].drop(columns=['record_id'], errors='ignore'), use_container_width=True)

# --- Tab 2: 看板 ---
with tab2:
    if 'feishu_cache' in st.session_state and not st.session_state['feishu_cache'].empty:
        df = st.session_state['feishu_cache'].copy()
        # 内部逻辑转回 datetime
        for col in ['服务开始日期', '服务结束日期']: df[col] = pd.to_datetime(df[col])
        
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_d, end_d = date_range
            if st.button(f"🚀 执行 {start_d} 至 {end_d} 周期均衡排单"):
                all_plans = []
                days = pd.date_range(start_d, end_d).tolist()
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
                            v_df['拟定人'] = current_active[0] # 示例简化
                            v_df['拟定顺序'] = v_df.groupby('拟定人').cumcount() + 1
                            v_df['作业日期'] = d.strftime('%Y-%m-%d')
                            all_plans.append(v_df)
                if all_plans:
                    st.session_state['period_plan'] = pd.concat(all_plans)
                    st.success("✅ 周期排单完成！")

            if 'period_plan' in st.session_state:
                res = st.session_state['period_plan']
                view_day = st.selectbox("📅 切换显示日期", sorted(res['作业日期'].unique()))
                worker = st.selectbox("👤 查看师视角", current_active)
                v_data = res[(res['作业日期'] == view_day) & (res['拟定人'] == worker)]
                if not v_data.empty:
                    st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                                            layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]))
                    st.data_editor(v_data[['拟定顺序', '宠物名字', '详细地址', '备注']], use_container_width=True)
