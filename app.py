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

# --- 1. 核心连接配置 (请确保 Secrets 中 APP_TOKEN 为 bas 开头) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "")
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "")
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "") 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "") 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "")

# --- 2. 飞书 API 交互逻辑 (增强报错监控) ---
def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("app_access_token")
    except Exception as e:
        st.error(f"无法获取飞书Token: {e}")
        return None

def fetch_feishu_data():
    token = get_feishu_token()
    if not token: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, params={"page_size": 500}, timeout=10).json()
        items = r.get("data", {}).get("items", [])
        data = []
        for i in items:
            row = i['fields']
            row['record_id'] = i['record_id']
            data.append(row)
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"数据抓取异常: {e}")
        return pd.DataFrame()

def add_feishu_record(fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json={"fields": fields}, timeout=10)
    res_json = response.json()
    if res_json.get("code") != 0:
        st.error(f"❌ 同步失败: {res_json.get('msg')}")
        return False
    return True

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    requests.patch(url, headers=headers, json={"fields": fields}, timeout=10)

# --- 3. 极简雅致 UI (微软雅黑 + Arial) ---
def set_minimalist_ui():
    st.markdown("""
         <style>
         html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', '微软雅黑', Arial, sans-serif !important; }
         header { visibility: hidden !important; height: 0px !important; }
         [data-testid="stSidebar"] { background-color: #F8F9FA !important; border-right: 1px solid #E9ECEF !important; }
         [data-testid="stSidebar"] .stMarkdown p { color: #000000 !important; font-weight: 600 !important; }
         div.stButton > button { background-color: #FFFFFF !important; color: #000000 !important; border: 1px solid #000000 !important; border-radius: 4px !important; }
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
st.set_page_config(page_title="小猫直喂-调度中心", layout="wide", page_icon="🐱")
set_minimalist_ui()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    
    st.divider()
    active_sitters = ["梦蕊", "依蕊"]
    current_active = [s for s in active_sitters if st.checkbox(f"{s} (今日出勤)", value=True)]
    target_date = st.date_input("查看作业日期", value=datetime.now())
    
    with st.expander("🛠️ 数据库连接检查"):
        st.write(f"Token前缀: {APP_TOKEN[:4]}")
        if not APP_TOKEN.startswith("bas"): st.warning("⚠️ Token格式可能不对，请使用bas开头的ID")

st.title("🐱 小猫直喂-飞书同步调度系统")
tab1, tab2 = st.tabs(["📂 数据录入与同步", "🚀 智能调度看板"])

# --- Tab 1: 数据中心 (批量+单条) ---
with tab1:
    st.subheader("📝 订单录入")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("➕ 批量导入 Excel"):
            up_file = st.file_uploader("上传 Excel", type=["xlsx"])
            if up_file and st.button("🚀 确认批量同步至飞书"):
                df_up = pd.read_excel(up_file)
                success = 0
                for _, row in df_up.iterrows():
                    s_ts = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                    e_ts = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                    payload = {
                        "详细地址": str(row['详细地址']), "宠物名字": str(row.get('宠物名字', '小猫')),
                        "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": s_ts, "服务结束日期": e_ts,
                        "喂猫师": row.get('喂猫师') if pd.notna(row.get('喂猫师')) else None, "备注": str(row.get('备注', ''))
                    }
                    if add_feishu_record(payload): success += 1
                if success > 0: st.success(f"✅ 成功同步 {success} 条数据！")

    with c2:
        with st.expander("➕ 单条快速补单"):
            with st.form("manual", clear_on_submit=True):
                addr = st.text_input("详细地址*")
                cat = st.text_input("宠物名字", value="小胖猫")
                sit = st.selectbox("指定喂猫师", ["系统分配", "梦蕊", "依蕊"])
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
                    if add_feishu_record(payload): st.info("✅ 已保存。")

    st.divider()
    if st.button("🔄 刷新并预览云端数据"):
        st.session_state['feishu_cache'] = fetch_feishu_data()
        if not st.session_state['feishu_cache'].empty:
            st.dataframe(st.session_state['feishu_cache'].drop(columns=['record_id'], errors='ignore'), use_container_width=True)

# --- Tab 2: 派单看板 (双优先级+负载均衡) ---
with tab2:
    if 'feishu_cache' not in st.session_state:
        st.session_state['feishu_cache'] = fetch_feishu_data()
    df = st.session_state['feishu_cache']
    
    if not df.empty:
        # 预处理日期
        for col in ['服务开始日期', '服务结束日期']:
            df[col] = pd.to_datetime(df[col], unit='ms') if df[col].dtype == 'int64' else pd.to_datetime(df[col])
        
        cur_ts = pd.Timestamp(target_date)
        day_df = df[(df['服务开始日期'] <= cur_ts) & (df['服务结束日期'] >= cur_ts)].copy()
        day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % r.get('投喂频率', 1) == 0, axis=1)]

        if not day_df.empty:
            st.info(f"今日待服务单量：{len(day_df)}")
            if st.button("🚀 计算今日均衡排单"):
                with ThreadPoolExecutor(max_workers=10) as ex:
                    coords = list(ex.map(get_coords, day_df['详细地址']))
                day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                v_df = day_df.dropna(subset=['lng', 'lat']).copy()
                
                if not v_df.empty:
                    # 1. 第一优先级：指定绑定
                    v_df['拟定人'] = v_df.get('喂猫师', np.nan)
                    free_mask = v_df['拟定人'].isna() | (~v_df['拟定人'].isin(current_active))
                    
                    # 2. 第二优先级：距离均衡
                    if free_mask.any():
                        free_df = v_df[free_mask].copy()
                        sc = len(current_active)
                        km = KMeans(n_clusters=sc, random_state=42, n_init='auto')
                        free_df['组'] = km.fit_predict(free_df[['lng', 'lat']])
                        
                        # 负载均衡：单量差 ≤ 2
                        while sc > 1:
                            tots = [len(v_df[v_df['拟定人'] == s]) + len(free_df[free_df['组'] == current_active.index(s)]) for s in current_active]
                            if abs(tots[0] - tots[1]) <= 2: break
                            src, dst = (0, 1) if tots[0] > tots[1] else (1, 0)
                            idx = free_df[free_df['组'] == src].index
                            dist = ((free_df.loc[idx, 'lng'] - km.cluster_centers_[dst][0])**2 + (free_df.loc[idx, 'lat'] - km.cluster_centers_[dst][1])**2)
                            free_df.loc[dist.idxmin(), '组'] = dst
                        v_df.loc[free_mask, '拟定人'] = free_df['组'].map(lambda x: current_active[x])
                    
                    v_df['拟定人'] = v_df['拟定人'].fillna(current_active[0])
                    v_df['拟定顺序'] = v_df.groupby('拟定人').cumcount() + 1
                    st.session_state['dispatch_plan'] = v_df
            
            if 'dispatch_plan' in st.session_state:
                res = st.session_state['dispatch_plan']
                st.dataframe(res[['拟定人', '拟定顺序', '宠物名字', '详细地址']], use_container_width=True)
                if st.button("✅ 确认同步方案至飞书 (全员实时可见)"):
                    for _, row in res.iterrows():
                        update_feishu_record(row['record_id'], {"喂猫师": row['拟定人'], "建议顺序": row['拟定顺序']})
                    st.success("🎉 同步成功！伙伴们刷新即可看到统一路径。")
                    st.session_state['feishu_cache'] = fetch_feishu_data()

            # 最终路线图与清单
            st.divider()
            worker = st.selectbox("👤 查看作业视角", current_active)
            w_data = df[df.get('喂猫师') == worker] if '喂猫师' in df.columns else pd.DataFrame()
            if not w_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=114.05, latitude=22.54, zoom=11),
                                        layers=[pdk.Layer("ScatterplotLayer", w_data, get_position='[lng, lat]', get_color=[0, 123, 255], get_radius=300)]))
                st.data_editor(w_data[['建议顺序', '宠物名字', '详细地址', '备注']], use_container_width=True)
