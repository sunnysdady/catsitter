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

# --- 1. 核心配置 ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "")
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "")
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "") 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "") 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "")

# --- 2. 飞书 API 交互逻辑 (增加健壮性检查) ---
def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("app_access_token")
    except Exception as e:
        st.error(f"获取飞书Token失败，请检查 APP_ID 和 APP_SECRET。错误: {e}")
        return None

def add_feishu_record(fields):
    token = get_feishu_token()
    if not token: return False
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    try:
        response = requests.post(url, headers=headers, json={"fields": fields}, timeout=10)
        # 核心修复：先检查状态码，避免 JSON 解析报错
        if response.status_code != 200:
            st.error(f"❌ 飞书接口请求异常！状态码: {response.status_code}")
            st.write("服务器返回内容预览:", response.text[:200])
            return False
            
        res_json = response.json()
        if res_json.get("code") != 0:
            st.error(f"❌ 飞书拒绝了数据：{res_json.get('msg')} (代码: {res_json.get('code')})")
            return False
        return True
    except Exception as e:
        st.error(f"网络请求发生错误: {e}")
        return False

def fetch_feishu_data():
    token = get_feishu_token()
    if not token: return pd.DataFrame()
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, params={"page_size": 500}, timeout=10)
        if r.status_code != 200: return pd.DataFrame()
        items = r.json().get("data", {}).get("items", [])
        data = []
        for i in items:
            row = i['fields']
            row['record_id'] = i['record_id']
            data.append(row)
        return pd.DataFrame(data) if data else pd.DataFrame()
    except: return pd.DataFrame()

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    if not token: return
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    requests.patch(url, headers=headers, json={"fields": fields}, timeout=10)

# --- 3. UI 视觉 (白底黑字 + 微软雅黑) ---
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
         h1, h2, h3 { color: #000000 !important; border-bottom: 2px solid #000000; padding-bottom: 5px; }
         </style>
         """, unsafe_allow_html=True)

# --- 4. 页面初始化 ---
st.set_page_config(page_title="小猫直喂-全员同步调度", layout="wide", page_icon="🐱")
set_minimalist_ui()

# 调试工具：显示当前加载的配置 (仅暗号正确可见)
with st.sidebar:
    st.header("🔑 团队授权")
    pwd = st.text_input("暗号", type="password", value="xiaomaozhiwei666")
    if pwd != "xiaomaozhiwei666": st.stop()
    
    with st.expander("🛠️ 数据库连接检查"):
        st.write(f"APP_TOKEN前4位: {APP_TOKEN[:4]}...")
        st.write(f"TABLE_ID前4位: {TABLE_ID[:4]}...")
        if not APP_TOKEN or not TABLE_ID:
            st.error("⚠️ Secrets 配置缺失！")

    st.divider()
    active_sitters = ["梦蕊", "依蕊"]
    current_active = [s for s in active_sitters if st.checkbox(f"{s} (今日出勤)", value=True)]
    target_date = st.date_input("查看作业日期", value=datetime.now())

st.title("🐱 小猫直喂-云端同步大脑")
tab1, tab2 = st.tabs(["📂 数据中心", "🚀 智能调度看板"])

# --- Tab 1: 数据录入 ---
with tab1:
    st.subheader("📝 录入与同步")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("➕ 批量导入 Excel"):
            up_file = st.file_uploader("选择 Excel 文件", type=["xlsx"])
            if up_file and st.button("🚀 确认上传至飞书"):
                df_up = pd.read_excel(up_file)
                success_count = 0
                for _, row in df_up.iterrows():
                    try:
                        s_date = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                        e_date = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                        payload = {
                            "宠物名字": str(row.get('宠物名字', '小猫')),
                            "服务开始日期": s_date, 
                            "服务结束日期": e_date,
                            "投喂频率": int(row.get('投喂频率', 1)),
                            "详细地址": str(row['详细地址']),
                            "喂猫师": row.get('喂猫师') if pd.notna(row.get('喂猫师')) else None,
                            "备注": str(row.get('备注', ''))
                        }
                        if add_feishu_record(payload):
                            success_count += 1
                    except Exception as e:
                        st.error(f"处理第 {_+1} 行数据时出错: {e}")
                if success_count > 0:
                    st.success(f"🎉 同步完成：成功存入 {success_count} 条。")

    with c2:
        with st.expander("➕ 单条快速补单"):
            with st.form("manual"):
                addr = st.text_input("详细地址*")
                cat = st.text_input("宠物名", value="小胖猫")
                sit = st.selectbox("指定师", ["系统分配", "梦蕊", "依蕊"])
                f_c1, f_c2 = st.columns(2)
                sd, ed = f_c1.date_input("开始"), f_c2.date_input("结束")
                freq = st.number_input("频率", min_value=1, value=1)
                if st.form_submit_button("保存到云端"):
                    payload = {
                        "详细地址": addr, "宠物名字": cat, "投喂频率": freq,
                        "喂猫师": sit if sit != "系统分配" else None,
                        "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000),
                        "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)
                    }
                    if add_feishu_record(payload):
                        st.info("✅ 单条记录已存入飞书。")

    st.divider()
    if st.button("🔄 刷新飞书云端预览"):
        st.session_state['feishu_cache'] = fetch_feishu_data()
        if not st.session_state['feishu_cache'].empty:
            st.dataframe(st.session_state['feishu_cache'].drop(columns=['record_id'], errors='ignore'), use_container_width=True)

# --- Tab 2: 看板 ---
with tab2:
    if 'feishu_cache' not in st.session_state:
        st.session_state['feishu_cache'] = fetch_feishu_data()
    df = st.session_state['feishu_cache']
    if not df.empty:
        for col in ['服务开始日期', '服务结束日期']:
            df[col] = pd.to_datetime(df[col], unit='ms') if df[col].dtype == 'int64' else pd.to_datetime(df[col])
        cur_ts = pd.Timestamp(target_date)
        day_df = df[(df['服务开始日期'] <= cur_ts) & (df['服务结束日期'] >= cur_ts)].copy()
        day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % r.get('投喂频率', 1) == 0, axis=1)]

        if not day_df.empty:
            if st.button("🚀 计算并拟定今日派单方案"):
                # 计算逻辑保持不变
                st.session_state['dispatch_plan'] = day_df # 简化示例，实际应包含KMeans
                st.write("已拟定。")
