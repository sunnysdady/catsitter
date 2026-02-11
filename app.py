import streamlit as st
import pandas as pd
import requests
from sklearn.cluster import KMeans
import io
import pydeck as pdk
from datetime import datetime, timedelta
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
import re
import numpy as np

# --- 1. 核心连接配置 (请在 Streamlit Secrets 中填写) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "")
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "")
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "") 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "") 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "c26fc76dd582c32e4406552df8ba40ff")

# --- 2. 飞书 API 核心交互 ---
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
        return pd.DataFrame([i['fields'] for i in items]) if items else pd.DataFrame()
    except: return pd.DataFrame()

def add_feishu_record(fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    requests.post(url, headers=headers, json={"fields": fields})

# --- 3. 视觉优化：全域暗黑精修版 UI ---
def set_pro_ui():
    st.markdown("""
         <style>
         /* 全局基础设定 */
         .stApp {
             background-color: #121212 !important; /* 最底层背景：纯黑 */
             color: #E0E0E0 !important; /* 全局文字：浅灰 */
         }
         
         /* 侧边栏深度定制 */
         [data-testid="stSidebar"] {
             background-color: #1E1E1E !important; /* 侧边栏背景：深灰 */
             border-right: 1px solid #333;
         }
         /* 强制侧边栏所有文字和标签为高亮白 */
         [data-testid="stSidebar"] .stMarkdown p,
         [data-testid="stSidebar"] label, 
         [data-testid="stSidebar"] .stCheckbox label p,
         [data-testid="stSidebar"] .stDateInput label p {
             color: #FFFFFF !important;
             font-weight: 600 !important;
         }

         /* 主内容区域容器 */
         .block-container {
             background-color: #1E1E1E !important; /* 主内容背景：深灰，与侧边栏统一 */
             padding: 2rem;
             border-radius: 12px;
             box-shadow: 0 4px 12px rgba(0,0,0,0.5); /* 增加深色阴影 */
         }

         /* 标题颜色 */
         h1, h2, h3, h4, h5, h6 {
             color: #FF9F43 !important; /* 活力橙，醒目 */
         }

         /* 修复 Tab 标签页的白色背景问题 */
         .stTabs [data-baseweb="tab-list"] {
             background-color: #1E1E1E !important;
             border-bottom: 2px solid #333;
         }
         .stTabs [data-baseweb="tab"] {
             color: #AAAAAA !important; /* 未选中 Tab 文字颜色 */
             background-color: transparent !important;
         }
         .stTabs [aria-selected="true"] {
             color: #FF9F43 !important; /* 选中 Tab 文字颜色 */
             border-bottom-color: #FF9F43 !important;
         }

         /* 修复 Expander (折叠面板) 的白色背景问题 */
         .streamlit-expanderHeader {
             background-color: #262626 !important; /* 折叠头背景 */
             color: #FFFFFF !important;
             border-radius: 8px;
         }
         [data-testid="stExpanderDetails"] {
             background-color: #1E1E1E !important; /* 折叠内容背景 */
             border: 1px solid #333;
             border-top: none;
             border-radius: 0 0 8px 8px;
         }
         
         /* 修复 Dataframe 和 DataEditor 的背景 */
         [data-testid="stDataFrame"], [data-testid="stDataEditor"] {
             background-color: #1E1E1E !important;
         }
         /* 表格内的文字颜色适配 */
         [data-testid="stDataFrame"] div, [data-testid="stDataEditor"] div {
             color: #E0E0E0 !important;
         }

         /* 输入框和选择框的背景适配 */
         .stTextInput input, .stSelectbox div[data-baseweb="select"] div, .stNumberInput input, .stDateInput input {
             background-color: #262626 !important;
             color: #FFFFFF !important;
             border-color: #444 !important;
         }
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
st.set_page_config(page_title="小猫直喂-飞书同步系统", layout="wide", page_icon="🐱")
set_pro_ui()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password") != "xiaomaozhiwei666": st.stop()
    
    st.divider()
    active_sitters = []
    if st.checkbox("梦蕊 (出勤)", value=True): active_sitters.append("梦蕊")
    if st.checkbox("依蕊 (出勤)", value=True): active_sitters.append("依蕊")
    
    st.divider()
    date_range = st.date_input("查看区间", value=(datetime.now(), datetime.now() + timedelta(days=6)))

st.title("🐱 小猫直喂-飞书智能大脑")
tab1, tab2 = st.tabs(["📂 飞书同步中心", "🚀 智能排单看板"])

with tab1:
    st.subheader("📊 飞书云端记录")
    if st.button("🔄 同步飞书最新订单数据"):
        st.session_state['feishu_data'] = fetch_feishu_data()
        if not st.session_state['feishu_data'].empty:
            st.success(f"同步成功！共获取 {len(st.session_state['feishu_data'])} 条记录。")
            st.dataframe(st.session_state['feishu_data'], use_container_width=True)

    with st.expander("➕ 单条手动补单"):
        with st.form("add_one"):
            c1, c2 = st.columns(2)
            addr = c1.text_input("详细地址*")
            cat = c2.text_input("宠物名字", value="小猫咪")
            sitter = st.selectbox("指定喂猫师 (选填)", ["系统分配", "梦蕊", "依蕊"])
            f1, f2 = st.columns(2)
            sd, ed = f1.date_input("开始日期"), f2.date_input("结束日期")
            freq = st.number_input("投喂频率", min_value=1, value=1)
            if st.form_submit_button("立即同步至飞书"):
                new_fields = {
                    "详细地址": addr, "宠物名字": cat, "投喂频率": freq,
                    "喂猫师": sitter if sitter != "系统分配" else None,
                    "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000),
                    "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)
                }
                add_feishu_record(new_fields)
                st.info("数据已发送至飞书，请刷新同步。")

with tab2:
    if 'feishu_data' not in st.session_state or st.session_state['feishu_data'].empty:
        st.warning("请先在 Tab 1 完成同步")
    else:
        if st.button("🚀 执行双优先级均衡排单"):
            df = st.session_state['feishu_data']
            # 日期标准化处理
            for col in ['服务开始日期', '服务结束日期']:
                df[col] = pd.to_datetime(df[col], unit='ms') if df[col].dtype == 'int64' else pd.to_datetime(df[col])
            
            start_d, end_d = date_range
            dates = pd.date_range(start_d, end_d).tolist()
            final_dispatch = []
            
            for d in dates:
                cur_ts = pd.Timestamp(d)
                today_df = df[(df['服务开始日期'] <= cur_ts) & (df['服务结束日期'] >= cur_ts)].copy()
                # 频率过滤逻辑
                today_df = today_df[today_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % r.get('投喂频率', 1) == 0, axis=1)]
                
                if not today_df.empty:
                    with ThreadPoolExecutor(max_workers=10) as ex:
                        coords = list(ex.map(get_coords, today_df['详细地址']))
                    today_df[['lng', 'lat']] = pd.DataFrame(coords, index=today_df.index)
                    today_df = today_df.dropna(subset=['lng', 'lat'])
                    
                    if not today_df.empty:
                        # P1: 优先固定绑定
                        today_df['最终人'] = today_df.get('喂猫师', np.nan)
                        free_m = today_df['最终人'].isna() | (~today_df['最终人'].isin(active_sitters))
                        
                        # P2: 距离均衡
                        if free_m.any():
                            free_df = today_df[free_m].copy()
                            sc = len(active_sitters)
                            if len(free_df) >= sc:
                                km = KMeans(n_clusters=sc, random_state=42, n_init='auto')
                                free_df['组'] = km.fit_predict(free_df[['lng', 'lat']])
                                # 强制均衡：单量差 ≤ 2
                                while sc > 1:
                                    tots = [len(today_df[today_df['最终人'] == s]) + len(free_df[free_df['组'] == active_sitters.index(s)]) for s in active_sitters]
                                    if abs(tots[0] - tots[1]) <= 2: break
                                    src, dst = (0, 1) if tots[0] > tots[1] else (1, 0)
                                    target_idx = free_df[free_df['组'] == src].index
                                    dist = ((free_df.loc[target_idx, 'lng'] - km.cluster_centers_[dst][0])**2 + (free_df.loc[target_idx, 'lat'] - km.cluster_centers_[dst][1])**2)
                                    free_df.loc[dist.idxmin(), '组'] = dst
                                today_df.loc[free_m, '最终人'] = free_df['组'].map(lambda x: active_sitters[x])
                        
                        today_df['最终人'] = today_df['最终人'].fillna(active_sitters[0])
                        today_df['派单日期'] = d.strftime('%Y-%m-%d')
                        final_dispatch.append(today_df)
            
            if final_dispatch:
                st.session_state['dispatch'] = pd.concat(final_dispatch)
                st.success("智能均衡排单已完成！")

        if 'dispatch' in st.session_state:
            res = st.session_state['dispatch']
            c1, c2 = st.columns(2)
            sd = c1.selectbox("📅 日期", sorted(res['派单日期'].unique()))
            ss = c2.selectbox("👤 伙伴视角", sorted(res['最终人'].unique()))
            v_data = res[(res['派单日期'] == sd) & (res['最终人'] == ss)]
            
            # 地图渲染
            st.pydeck_chart(pdk.Deck(
                map_style=pdk.map_styles.DARK,
                initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=12),
                layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[255, 159, 67], get_radius=250)]
            ))
            st.data_editor(v_data[['宠物名字', '详细地址', '备注']], use_container_width=True)
            
            # 下载功能
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                res.to_excel(writer, index=False, sheet_name='汇总')
                for s in res['最终人'].unique():
                    res[res['最终人'] == s].to_excel(writer, index=False, sheet_name=s)
            st.download_button("📥 下载下周专属 Excel 周报表", data=output.getvalue(), file_name=f"小猫直喂_周计划_{datetime.now().strftime('%m%d')}.xlsx")
