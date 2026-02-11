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

# --- 1. 核心连接配置 (已加入自动去空格逻辑) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 飞书 API 交互逻辑 ---
def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("app_access_token")
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
        df = pd.DataFrame([dict(i['fields'], record_id=i['record_id']) for i in items])
        # 补齐必要列
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '投喂频率', '喂猫师', '建议顺序', '备注']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    # 强制核对路径
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # 净化数据类型
    clean_fields = {}
    for k, v in fields.items():
        if pd.isna(v): clean_fields[k] = ""
        elif isinstance(v, (np.int64, np.int32)): clean_fields[k] = int(v)
        else: clean_fields[k] = v

    try:
        response = requests.patch(url, headers=headers, json={"fields": clean_fields}, timeout=10)
        if response.status_code == 404:
            st.error(f"❌ 404 错误：找不到目标记录。请核对下方打印的 URL 是否与飞书实际 ID 一致。")
            st.code(f"请求URL: {url}")
            return False
        res_json = response.json()
        if res_json.get("code") != 0:
            st.error(f"❌ 飞书拒绝同步: {res_json.get('msg')} (代码: {res_json.get('code')})")
            return False
        return True
    except: return False

# --- 3. UI 视觉设置 (白底黑字) ---
def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', Arial !important; }
        header { visibility: hidden !important; }
        h1, h2, h3 { color: #000000 !important; border-bottom: 2px solid #000000; padding-bottom: 5px; }
        [data-testid="stSidebar"] { background-color: #FFFFFF !important; border-right: 1px solid #E9ECEF !important; }
        [data-testid="stSidebar"] div[role="radiogroup"] { display: flex; flex-direction: column; gap: 15px; width: 100% !important; }
        [data-testid="stSidebar"] div[role="radiogroup"] label {
            background-color: #F8F9FA !important; border: 1px solid #E0E0E0 !important;
            padding: 30px 10px !important; border-radius: 14px !important; cursor: pointer; transition: all 0.2s ease;
        }
        [data-testid="stSidebar"] div[role="radiogroup"] [data-baseweb="radio"] div:first-child { display: none !important; }
        [data-testid="stSidebar"] div[role="radiogroup"] label[data-baseweb="radio"]:has(input:checked) {
            background-color: #FFFFFF !important; border: 2px solid #000000 !important; box-shadow: 0 8px 18px rgba(0,0,0,0.12) !important;
        }
        [data-testid="stSidebar"] div[role="radiogroup"] label p { font-size: 20px !important; font-weight: bold !important; text-align: center !important; }
        .stProgress > div > div > div > div { background-color: #000000 !important; }
        div.stButton > button { background-color: #FFFFFF !important; color: #000000 !important; border: 1px solid #000000 !important; border-radius: 8px !important; font-weight: bold !important; }
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

# --- 4. 页面主体 ---
st.set_page_config(page_title="小猫直喂-指挥中心", layout="wide")
set_ui()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    menu = st.radio("功能选择", ["📂 数据中心", "🚀 智能看板"], label_visibility="collapsed")

if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

if menu == "📂 数据中心":
    st.title("📂 数据录入中心")
    # ... 此处录入逻辑与之前保持一致 ...
    if st.button("🔄 刷新预览云端数据"):
        st.session_state['feishu_cache'] = fetch_feishu_data()
        st.dataframe(st.session_state['feishu_cache'].drop(columns=['record_id'], errors='ignore'), use_container_width=True)

else:
    st.title("🚀 智能调度看板")
    with st.sidebar:
        st.divider()
        st.subheader("⚙️ 调度设置")
        active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 选择作业日期/范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))
    
    df = st.session_state['feishu_cache'].copy()
    if not df.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        # ... 拟定方案逻辑保持一致 ...
        
        if 'period_plan' in st.session_state:
            res = st.session_state['period_plan']
            # ... 视角切换逻辑保持一致 ...
            
            if st.button("✅ 确认同步全周期方案至飞书"):
                t_s = len(res); s_b = st.progress(0); s_t = st.empty(); fail_count = 0
                for i, (_, rs) in enumerate(res.iterrows()):
                    s_t.text(f"正在向飞书回写数据: {i+1}/{t_s}")
                    if not update_feishu_record(rs['record_id'], {"喂猫师": rs['拟定人'], "建议顺序": rs['拟定顺序']}):
                        fail_count += 1
                    s_b.progress((i + 1) / t_s)
                s_t.empty(); s_b.empty()
                if fail_count == 0: st.success("🎉 全周期方案已成功回写！")
                else: st.warning(f"⚠️ 同步结束，其中 {fail_count} 条记录失败。")
