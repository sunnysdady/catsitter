import streamlit as st
import pandas as pd
import requests
from sklearn.cluster import KMeans
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import re
import numpy as np

# --- 1. 核心配置 (请确保在 Streamlit Secrets 中更新为 bas 开头的 App Token) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "")
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "")
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "") # 必须是 bas 开头的！
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "") # tblg1xnrQZMp1UfH
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "")

# --- 2. 飞书 API 交互：增加全流程报错监控 ---
def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("app_access_token")
    except Exception as e:
        st.error(f"❌ 无法连接飞书服务器，请检查网络或 APP_ID。详细错误: {e}")
        return None

def add_feishu_record(fields):
    token = get_feishu_token()
    if not token: return False
    
    # 构造请求地址
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    try:
        response = requests.post(url, headers=headers, json={"fields": fields}, timeout=10)
        # 针对“没反应”的深度检查
        if response.status_code != 200:
            st.error(f"❌ 飞书接口响应异常 (状态码: {response.status_code})。大概率是 APP_TOKEN 或 TABLE_ID 填错了！")
            return False
            
        res_json = response.json()
        if res_json.get("code") != 0:
            # 字段名对齐检查
            st.error(f"❌ 飞书拒绝了数据录入：{res_json.get('msg')} (代码: {res_json.get('code')})。请检查飞书表头名称是否被修改。")
            return False
        return True
    except Exception as e:
        st.error(f"❌ 程序运行出错: {e}")
        return False

# --- 3. 极简 UI 设置 ---
def set_minimalist_ui():
    st.markdown("""
         <style>
         html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', Arial, sans-serif !important; }
         header { visibility: hidden !important; }
         div.stButton > button { background-color: #FFFFFF !important; color: #000000 !important; border: 1px solid #000000 !important; width: 100% !important; }
         </style>
         """, unsafe_allow_html=True)

st.set_page_config(page_title="小猫直喂-大脑同步", layout="wide")
set_minimalist_ui()

# --- 4. 侧边栏调试 ---
with st.sidebar:
    st.header("🔑 授权与检查")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    
    with st.expander("🛠️ 数据库钥匙检查"):
        st.write(f"当前 Token 前缀: **{APP_TOKEN[:4]}**")
        if not APP_TOKEN.startswith("bas"):
            st.warning("⚠️ 警告：当前的 APP_TOKEN 不是以 bas 开头，大概率会同步失败！")

# --- 5. 录入中心 ---
st.title("🐱 小猫直喂-大脑同步记录")
c1, c2 = st.columns([1, 1])

with c2: # 也就是你截图操作的区域
    st.subheader("➕ 单条快速补单")
    with st.form("manual_entry", clear_on_submit=False):
        addr = st.text_input("详细地址*", placeholder="请填入深圳市开头地址")
        cat = st.text_input("宠物名字", value="小胖猫")
        sit = st.selectbox("指定师", ["系统分配", "梦蕊", "依蕊"])
        f_c1, f_c2 = st.columns(2)
        sd, ed = f_c1.date_input("开始服务日期"), f_c2.date_input("结束服务日期")
        freq = st.number_input("投喂频率 (天/次)", min_value=1, value=1)
        
        submitted = st.form_submit_button("保存到云端")
        if submitted:
            if not addr:
                st.warning("请填写详细地址！")
            else:
                # 转换日期为飞书毫秒戳
                s_timestamp = int(datetime.combine(sd, datetime.min.time()).timestamp()*1000)
                e_timestamp = int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)
                
                # 严格匹配飞书表头名称
                payload = {
                    "详细地址": addr,
                    "宠物名字": cat,
                    "投喂频率": int(freq),
                    "服务开始日期": s_timestamp,
                    "服务结束日期": e_timestamp,
                    "喂猫师": sit if sit != "系统分配" else None
                }
                
                with st.spinner("正在同步至飞书..."):
                    if add_feishu_record(payload):
                        st.balloons()
                        st.success("🎉 数据已成功存入飞书云端！")
