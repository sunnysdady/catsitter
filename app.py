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

# --- 1. 核心连接配置 ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "")
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "")
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "") 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "") 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "")

# --- 2. 核心功能函数 ---
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
        # 补齐必要列
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
    match = temp_df[(temp_df['详细地址'].str.strip() == new_addr) & (temp_df['宠物名字'].str.strip() == new_name) & (temp_df['服务开始日期_std'] == new_date)]
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

# --- 3. 视觉强化：全宽卡片导航 CSS ---
def set_ui():
    st.markdown("""
        <style>
        /* 基础设置 */
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', Arial !important; }
        header { visibility: hidden !important; }
        h1, h2, h3 { color: #000000 !important; border-bottom: 2px solid #000000; padding-bottom: 5px; }

        /* 侧边栏宽度与内边距适配 */
        [data-testid="stSidebar"] { background-color: #FFFFFF !important; border-right: 1px solid #E9ECEF !important; }
        [data-testid="stSidebarUserContent"] { padding-top: 20px !important; }
        
        /* 导航卡片列表 */
        [data-testid="stSidebar"] div[role="radiogroup"] { display: flex; flex-direction: column; gap: 15px; width: 100% !important; }
        
        /* 单个导航卡片的基础态 */
        [data-testid="stSidebar"] div[role="radiogroup"] label {
            background-color: #FBFBFB !important; 
            border: 1px solid #E0E0E0 !important;
            padding: 30px 10px !important; 
            border-radius: 14px !important;
            cursor: pointer;
            transition: all 0.2s ease-in-out;
            width: 100% !important;
            display: flex !important;
            justify-content: center !important;
        }
        
        /* 导航卡片内的文字显影 */
        [data-testid="stSidebar"] div[role="radiogroup"] label div[data-testid="stMarkdownContainer"] p {
            font-size: 20px !important;
            color: #333333 !important;
            font-weight: bold !important;
            text-align: center !important;
            margin: 0 !important;
        }

        /* 隐藏原生单选圈 */
        [data-testid="stSidebar"] div[role="radiogroup"] [data-baseweb="radio"] div:first-child { display: none !important; }

        /* 选中态：高对比度黑边框与阴影 */
        [data-testid="stSidebar"] div[role="radiogroup"] label[data-baseweb="radio"]:has(input:checked) {
            background-color: #FFFFFF !important;
            border: 2px solid #000000 !important;
            box-shadow: 0 8px 20px rgba(0,0,0,0.18) !important;
        }
        [data-testid="stSidebar"] div[role="radiogroup"] label[data-baseweb="radio"]:has(input:checked) p {
            color: #000000 !important;
        }

        /* 进度条样式 */
        .stProgress > div > div > div > div { background-color: #000000 !important; }
        
        /* 按钮与输入框 */
        div.stButton > button { background-color: #FFFFFF !important; color: #000000 !important; border: 1px solid #000000 !important; border-radius: 8px !important; font-weight: bold !important; }
        div.stButton > button:hover { background-color: #000000 !important; color: #FFFFFF !important; }
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
st.set_page_config(page_title="小猫直喂-调度指挥中心", layout="wide")
set_ui()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    # 修复后的导航菜单，包含文字描述
    menu = st.radio("导航选择", ["📂 数据中心", "🚀 智能看板"], label_visibility="collapsed")

# 缓存初始化
if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

if menu == "📂 数据中心":
    st.title("📂 数据录入与管理")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("选择文件", type=["xlsx"])
            if up_file and st.button("确认查重同步"):
                df_up = pd.read_excel(up_file)
                total, success, skipped = len(df_up), 0, 0
                p_bar = st.progress(0); p_text = st.empty()
                for i, (_, row) in enumerate(df_up.iterrows()):
                    p_text.text(f"处理中: {i+1}/{total}")
                    s_ts = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                    e_ts = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                    payload = {"详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(), "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": s_ts, "服务结束日期": e_ts, "备注": str(row.get('备注', ''))}
                    res = add_feishu_record(payload)
                    if res == "success": success += 1
                    elif res == "duplicate": skipped += 1
                    p_bar.progress((i + 1) / total)
                p_text.empty(); p_bar.empty()
                st.success(f"完成！录入 {success} 条，跳过重复 {skipped} 条。")
                st.session_state['feishu_cache'] = fetch_feishu_data()
    with c2:
        with st.expander("单条快速补单"):
            with st.form("manual", clear_on_submit=True):
                addr = st.text_input("详细地址*")
                cat = st.text_input("宠物名", value="小胖猫")
                f1, f2 = st.columns(2)
                sd, ed = f1.date_input("开始"), f2.date_input("结束")
                freq = st.number_input("频率", min_value=1, value=1)
                if st.form_submit_button("保存到云端"):
                    payload = {"详细地址": addr.strip(), "宠物名字": cat.strip(), "投喂频率": freq, "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)}
                    res = add_feishu_record(payload)
                    if res == "success": st.balloons(); st.success("录入成功！")
                    elif res == "duplicate": st.error("查重：该记录已存在。")
                    st.session_state['feishu_cache'] = fetch_feishu_data()
    st.divider()
    if st.button("🔄 刷新预览云端数据"):
        st.session_state['feishu_cache'] = fetch_feishu_data()
        df_v = st.session_state['feishu_cache'].copy()
        if not df_v.empty:
            for c in ['服务开始日期', '服务结束日期']: df_v[c] = pd.to_datetime(df_v[c], unit='ms').dt.strftime('%Y-%m-%d')
            st.dataframe(df_v.drop(columns=['record_id'], errors='ignore'), use_container_width=True)

else:
    st.title("🚀 智能调度看板")
    with st.sidebar:
        st.divider()
        st.subheader("⚙️ 调度设置")
        active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (今日出勤)", value=True)]
        date_range = st.date_input("📅 选择作业日期/范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))
    
    df = st.session_state['feishu_cache'].copy()
    if not df.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        for col in ['服务开始日期', '服务结束日期']: df[col] = pd.to_datetime(df[col], unit='ms')
        start_d, end_d = date_range
        if st.button(f"🚀 点击拟定周期方案 ({start_d} ~ {end_d})"):
            all_plans = []
            days = pd.date_range(start_d, end_d).tolist()
            with st.spinner("计算路径与均衡负载..."):
                for d in days:
                    cur_ts = pd.Timestamp(d)
                    day_df = df[(df['服务开始日期'] <= cur_ts) & (df['服务结束日期'] >= cur_ts)].copy()
                    if not day_df.empty:
                        day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        if not day_df.empty:
                            with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, day_df['详细地址']))
                            day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                            v_df = day_df.dropna(subset=['lng', 'lat']).copy()
                            if not v_df.empty:
                                v_df['拟定人'] = current_active[0]; v_df['拟定顺序'] = v_df.groupby('拟定人').cumcount() + 1; v_df['作业日期'] = d.strftime('%Y-%m-%d')
                                all_plans.append(v_df)
            if all_plans: st.session_state['period_plan'] = pd.concat(all_plans); st.success("方案拟定完成！")
        
        if 'period_plan' in st.session_state:
            res = st.session_state['period_plan']
            view_day = st.selectbox("📅 切换查看日期", sorted(res['作业日期'].unique()))
            worker = st.selectbox("👤 查看伙伴视角", current_active)
            v_data = res[(res['作业日期'] == view_day) & (res['拟定人'] == worker)]
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11), layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]))
                st.data_editor(v_data[['拟定顺序', '宠物名字', '详细地址', '备注']], use_container_width=True)
                if st.button("✅ 确认同步全周期方案至飞书"):
                    t_s = len(res); s_b = st.progress(0); s_t = st.empty()
                    for i, (_, rs) in enumerate(res.iterrows()):
                        s_t.text(f"回写中: {i+1}/{t_s}")
                        update_feishu_record(rs['record_id'], {"喂猫师": rs['拟定人'], "建议顺序": rs['拟定顺序']})
                        s_b.progress((i + 1) / t_s)
                    s_t.empty(); s_b.empty(); st.success("🎉 全周期同步完成！")
