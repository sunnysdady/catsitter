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

# --- 1. 核心连接配置 ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "")
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "")
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "") 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "") 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "c26fc76dd582c32e4406552df8ba40ff")

# --- 2. 飞书 API 交互 ---
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

# --- 3. 最终完美版视觉适配 CSS ---
def set_final_pro_ui():
    st.markdown("""
         <style>
         /* 1. 彻底消灭顶部白色装饰条 */
         header { visibility: hidden !important; height: 0px !important; }
         .stApp { background-color: #121212 !important; color: #E0E0E0 !important; }
         
         /* 2. 侧边栏完美适配 */
         [data-testid="stSidebar"] { background-color: #1A1A1A !important; border-right: 1px solid #333; }
         [data-testid="stSidebar"] .stMarkdown p, [data-testid="stSidebar"] label {
             color: #FFFFFF !important; font-weight: 700 !important; font-size: 1.1rem !important;
         }
         
         /* 3. 主内容区深色容器 */
         .block-container { background-color: #121212 !important; padding-top: 1rem !important; }
         div[data-testid="stVerticalBlock"] > div { background-color: transparent !important; }

         /* 4. 强力按钮美化：告别白色块 */
         div.stButton > button {
             background-color: #262626 !important;
             color: #FF9F43 !important;
             border: 2px solid #FF9F43 !important;
             border-radius: 8px !important;
             padding: 0.5rem 1rem !important;
             width: 100% !important;
             font-weight: bold !important;
         }
         div.stButton > button:hover {
             background-color: #FF9F43 !important;
             color: #121212 !important;
         }

         /* 5. Tab 标签页深色化 */
         .stTabs [data-baseweb="tab-list"] { background-color: #121212 !important; border-bottom: 1px solid #333; }
         .stTabs [data-baseweb="tab"] { color: #888 !important; }
         .stTabs [aria-selected="true"] { color: #FF9F43 !important; border-bottom-color: #FF9F43 !important; }

         /* 6. 折叠面板 (Expander) 视觉修正 */
         .streamlit-expanderHeader {
             background-color: #262626 !important;
             color: #FFFFFF !important;
             border: 1px solid #333 !important;
             border-radius: 8px !important;
         }
         [data-testid="stExpanderDetails"] {
             background-color: #1A1A1A !important;
             border: 1px solid #333;
             border-top: none;
         }

         /* 7. 输入框全局暗黑化 */
         input, textarea, [data-baseweb="select"] {
             background-color: #262626 !important;
             color: #FFFFFF !important;
             border: 1px solid #444 !important;
         }
         
         /* 8. 标题醒目处理 */
         h1, h2, h3 { color: #FF9F43 !important; padding-bottom: 1rem !important; }
         </style>
         """, unsafe_allow_html=True)

# --- 4. 辅助函数 ---
def extract_room(addr):
    if pd.isna(addr): return ""
    match = re.search(r'([a-zA-Z0-9-]{2,})$', str(addr).strip())
    return match.group(1) if match else ""

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

# --- 5. 页面逻辑 ---
st.set_page_config(page_title="小猫直喂-调度指挥中心", layout="wide", page_icon="🐱")
set_final_pro_ui()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password") != "xiaomaozhiwei666": st.stop()
    
    st.divider()
    st.header("👤 伙伴出勤")
    active_sitters = []
    if st.checkbox("梦蕊 (出勤)", value=True): active_sitters.append("梦蕊")
    if st.checkbox("依蕊 (出勤)", value=True): active_sitters.append("依蕊")
    
    st.divider()
    date_range = st.date_input("查看区间", value=(datetime.now(), datetime.now() + timedelta(days=6)))

st.title("🐱 小猫直喂-飞书智能大脑")
tab1, tab2 = st.tabs(["📂 飞书同步中心", "🚀 智能排单看板"])

with tab1:
    st.subheader("📊 飞书云端记录")
    # 强制重写按钮，解决白色块问题
    if st.button("🔄 同步飞书最新订单数据"):
        st.session_state['feishu_data'] = fetch_feishu_data()
        if not st.session_state['feishu_data'].empty:
            st.success(f"同步成功！共获取 {len(st.session_state['feishu_data'])} 条记录。")
            st.dataframe(st.session_state['feishu_data'], use_container_width=True)

    with st.expander("➕ 单条手动补单"):
        with st.form("add_one", clear_on_submit=True):
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
                st.info("数据已成功发送至飞书！请点击上方‘同步’按钮刷新。")

with tab2:
    if 'feishu_data' not in st.session_state or st.session_state['feishu_data'].empty:
        st.warning("请先在 Tab 1 完成同步")
    else:
        if st.button("🚀 执行双优先级均衡排单"):
            df = st.session_state['feishu_data']
            # 日期转换逻辑
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
                        # 均衡派单逻辑
                        today_df['最终人'] = today_df.get('喂猫师', np.nan)
                        free_m = today_df['最终人'].isna() | (~today_df['最终人'].isin(active_sitters))
                        if free_m.any():
                            free_df = today_df[free_m].copy()
                            sc = len(active_sitters)
                            if len(free_df) >= sc:
                                km = KMeans(n_clusters=sc, random_state=42, n_init='auto')
                                free_df['组'] = km.fit_predict(free_df[['lng', 'lat']])
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
                st.success("智能均衡排单计算完成！")

        if 'dispatch' in st.session_state:
            res = st.session_state['dispatch']
            c1, c2 = st.columns(2)
            sd = c1.selectbox("📅 日期", sorted(res['派单日期'].unique()))
            ss = c2.selectbox("👤 伙伴", sorted(res['最终人'].unique()))
            v_data = res[(res['派单日期'] == sd) & (res['最终人'] == ss)]
            
            st.pydeck_chart(pdk.Deck(
                map_style=pdk.map_styles.DARK,
                initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=12),
                layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[255, 159, 67], get_radius=250)]
            ))
            st.data_editor(v_data[['宠物名字', '详细地址', '备注']], use_container_width=True)
            
            # Excel 导出功能
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                res.to_excel(writer, index=False, sheet_name='汇总')
                for s in res['最终人'].unique():
                    res[res['最终人'] == s].to_excel(writer, index=False, sheet_name=s)
            st.download_button("📥 导出分人 Excel 报表", data=output.getvalue(), file_name=f"小猫直喂_周计划_{datetime.now().strftime('%m%d')}.xlsx")
