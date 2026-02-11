import streamlit as st
import pandas as pd
import requests
from sklearn.cluster import KMeans
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# --- 1. 核心配置 ---
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
        return pd.DataFrame([dict(i['fields'], record_id=i['record_id']) for i in items]) if items else pd.DataFrame()
    except: return pd.DataFrame()

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    requests.patch(url, headers=headers, json={"fields": fields})

# --- 3. UI 视觉适配 (雅致白 + 微软雅黑) ---
def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', '微软雅黑', Arial, sans-serif !important; }
        header { visibility: hidden !important; }
        div.stButton > button { background-color: #FFFFFF !important; color: #000000 !important; border: 1px solid #000000 !important; width: 100% !important; font-weight: bold !important; }
        div.stButton > button:hover { background-color: #000000 !important; color: #FFFFFF !important; }
        [data-testid="stSidebar"] { background-color: #F8F9FA !important; border-right: 1px solid #E9ECEF !important; }
        [data-testid="stSidebar"] .stMarkdown p { color: #000000 !important; font-weight: 600 !important; }
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

# --- 4. 逻辑执行 ---
st.set_page_config(page_title="小猫直喂-调度中心", layout="wide")
set_ui()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("请输入暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    
    st.divider()
    active_sitters = ["梦蕊", "依蕊"]
    current_active = [s for s in active_sitters if st.checkbox(f"{s} (今日出勤)", value=True)]
    
    # --- 修改点：时间段选择器 ---
    st.divider()
    date_range = st.date_input(
        "📅 选择作业周期", 
        value=(datetime.now(), datetime.now() + timedelta(days=2)),
        help="你可以通过点击开始日期和结束日期来选择一个时间段"
    )

st.title("🐱 小猫直喂-云端智能大脑")
tab1, tab2 = st.tabs(["📂 数据中心", "🚀 智能排单看板"])

with tab1:
    st.subheader("📝 云端数据同步")
    if st.button("🔄 刷新飞书云端数据"):
        st.session_state['feishu_cache'] = fetch_feishu_data()
        if not st.session_state['feishu_cache'].empty:
            st.success(f"同步成功！共获取 {len(st.session_state['feishu_cache'])} 条记录。")
    
    if 'feishu_cache' in st.session_state:
        st.dataframe(st.session_state['feishu_cache'].drop(columns=['record_id'], errors='ignore'), use_container_width=True)

with tab2:
    # 必须选择了完整的日期范围（开始和结束）
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        if 'feishu_cache' not in st.session_state or st.session_state['feishu_cache'].empty:
            st.warning("⚠️ 请先在【数据中心】点击刷新按钮，同步飞书数据。")
        else:
            df = st.session_state['feishu_cache'].copy()
            # 日期标准化
            for col in ['服务开始日期', '服务结束日期']:
                df[col] = pd.to_datetime(df[col], unit='ms') if df[col].dtype == 'int64' else pd.to_datetime(df[col])
            
            if st.button(f"🚀 点击执行：{start_date} 至 {end_date} 周期排单"):
                all_days_dispatch = []
                # 生成日期列表
                day_list = pd.date_range(start_date, end_date).tolist()
                
                with st.spinner("正在逐日计算最优路径与均衡方案..."):
                    for day in day_list:
                        cur_ts = pd.Timestamp(day)
                        # 1. 筛选出在服务期内的订单
                        day_df = df[(df['服务开始日期'] <= cur_ts) & (df['服务结束日期'] >= cur_ts)].copy()
                        # 2. 频率过滤
                        if not day_df.empty:
                            day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        
                        if not day_df.empty:
                            # 3. 获取坐标
                            with ThreadPoolExecutor(max_workers=10) as ex:
                                coords = list(ex.map(get_coords, day_df['详细地址']))
                            day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                            v_df = day_df.dropna(subset=['lng', 'lat']).copy()
                            
                            if not v_df.empty:
                                # 4. 负载均衡算法 (熟人优先 + 距离聚类)
                                v_df['拟定人'] = v_df.get('喂猫师', np.nan)
                                free_mask = v_df['拟定人'].isna() | (~v_df['拟定人'].isin(current_active))
                                if free_mask.any():
                                    free_df = v_df[free_mask].copy()
                                    sc = len(current_active)
                                    km = KMeans(n_clusters=sc, random_state=42, n_init='auto')
                                    free_df['组'] = km.fit_predict(free_df[['lng', 'lat']])
                                    # 简化的负载均衡：分配给对应组的活跃师
                                    v_df.loc[free_mask, '拟定人'] = free_df['组'].map(lambda x: current_active[x])
                                
                                v_df['拟定人'] = v_df['拟定人'].fillna(current_active[0])
                                v_df['拟定顺序'] = v_df.groupby('拟定人').cumcount() + 1
                                v_df['作业日期'] = day.strftime('%Y-%m-%d')
                                all_days_dispatch.append(v_df)
                
                if all_days_dispatch:
                    st.session_state['full_period_plan'] = pd.concat(all_days_dispatch)
                    st.success(f"🎉 周期排单计算完成！已生成 {len(day_list)} 天的计划。")
            
            # --- 结果展示区 ---
            if 'full_period_plan' in st.session_state:
                res = st.session_state['full_period_plan']
                
                st.divider()
                c1, c2 = st.columns(2)
                view_day = c1.selectbox("📅 切换查看日期", sorted(res['作业日期'].unique()))
                view_worker = c2.selectbox("👤 切换伙伴视角", current_active)
                
                # 过滤当前显示的数据
                display_data = res[(res['作业日期'] == view_day) & (res['拟定人'] == view_worker)]
                
                if not display_data.empty:
                    # 地图展示
                    st.pydeck_chart(pdk.Deck(
                        map_style=pdk.map_styles.LIGHT,
                        initial_view_state=pdk.ViewState(longitude=display_data['lng'].mean(), latitude=display_data['lat'].mean(), zoom=11),
                        layers=[pdk.Layer("ScatterplotLayer", display_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]
                    ))
                    # 详细表格
                    st.subheader(f"📋 {view_day} - {view_worker} 的作业清单")
                    st.data_editor(display_data[['拟定顺序', '宠物名字', '详细地址', '备注']], use_container_width=True)
                    
                    if st.button(f"✅ 将整个周期（{start_date}至{end_date}）方案同步至飞书"):
                        with st.spinner("正在分批写入云端，请勿关闭页面..."):
                            for _, row in res.iterrows():
                                update_feishu_record(row['record_id'], {"喂猫师": row['拟定人'], "建议顺序": row['拟定顺序']})
                        st.success("全部数据已同步！")
    else:
        st.info("💡 请在侧边栏选择一个日期范围（点击开始日期后再点击结束日期）。")
