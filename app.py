import streamlit as st
import pandas as pd
import requests
from sklearn.cluster import KMeans
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import re
import numpy as np

# --- 1. 核心连接配置 (请确保 Secrets 已配置) ---
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
    r = requests.get(url, headers=headers, params={"page_size": 500}).json()
    items = r.get("data", {}).get("items", [])
    data = []
    for i in items:
        row = i['fields']
        row['record_id'] = i['record_id'] # 记录 ID 用于回写
        data.append(row)
    return pd.DataFrame(data) if data else pd.DataFrame()

def add_feishu_record(fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    requests.post(url, headers=headers, json={"fields": fields})

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}"}
    requests.patch(url, headers=headers, json={"fields": fields})

# --- 3. 视觉优化：极简白底黑字 (微软雅黑 + Arial) ---
def set_minimalist_ui():
    st.markdown("""
         <style>
         html, body, [data-testid="stAppViewContainer"] {
             background-color: #FFFFFF !important;
             color: #000000 !important;
             font-family: 'Microsoft YaHei', '微软雅黑', Arial, sans-serif !important;
         }
         header { visibility: hidden !important; height: 0px !important; }
         [data-testid="stSidebar"] {
             background-color: #F8F9FA !important;
             border-right: 1px solid #E9ECEF !important;
         }
         div.stButton > button {
             background-color: #FFFFFF !important;
             color: #000000 !important;
             border: 1px solid #000000 !important;
             border-radius: 4px !important;
         }
         div.stButton > button:hover {
             background-color: #000000 !important;
             color: #FFFFFF !important;
         }
         h1, h2, h3 { color: #000000 !important; border-bottom: 2px solid #000000; padding-bottom: 5px; }
         .stTabs [data-baseweb="tab-list"] { background-color: #FFFFFF !important; }
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

# --- 4. 页面初始化 ---
st.set_page_config(page_title="小猫直喂-调度指挥中心", layout="wide", page_icon="🐱")
set_minimalist_ui()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password") != "xiaomaozhiwei666": st.stop()
    
    st.divider()
    active_sitters = ["梦蕊", "依蕊"]
    current_active = [s for s in active_sitters if st.checkbox(f"{s} (今日出勤)", value=True)]
    
    st.divider()
    target_date = st.date_input("查看作业日期", value=datetime.now())

st.title("🐱 小猫直喂-云端同步系统")
tab1, tab2 = st.tabs(["📂 数据同步中心", "🚀 智能派单看板"])

# --- Tab 1: 重新加入上传入口 ---
with tab1:
    st.subheader("📝 订单录入与同步")
    
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("➕ 批量导入 Excel (自动同步飞书)"):
            up_file = st.file_uploader("选择 Excel 模板文件", type=["xlsx"])
            if up_file and st.button("🚀 确认批量导入并存入云端"):
                df_up = pd.read_excel(up_file)
                for _, row in df_up.iterrows():
                    # 转换日期为飞书所需的毫秒戳
                    s_date = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                    e_date = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                    add_feishu_record({
                        "详细地址": str(row['详细地址']),
                        "宠物名字": str(row.get('宠物名字', '小猫')),
                        "投喂频率": int(row.get('投喂频率', 1)),
                        "喂猫师": row.get('喂猫师') if pd.notna(row.get('喂猫师')) else None,
                        "服务开始日期": s_date, "服务结束日期
