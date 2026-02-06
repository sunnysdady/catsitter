import streamlit as st
import pandas as pd
import requests
from sklearn.cluster import KMeans
import io
import pydeck as pdk
from datetime import datetime, timedelta
import urllib.parse

# --- 1. 安全配置：从云端 Secrets 读取 Key ---
AMAP_API_KEY = st.secrets["AMAP_KEY"]

def get_coords(address, city, api_key):
    full_address = f"{city}{address}" if city not in str(address) else address
    url = f"https://restapi.amap.com/v3/geocode/geo?key={api_key}&address={full_address}"
    try:
        response = requests.get(url, timeout=5).json()
        if response['status'] == '1' and response['geocodes']:
            location = response['geocodes'][0]['location']
            lng, lat = location.split(',')
            return float(lng), float(lat), "成功"
    except: return None, None, "异常"
    return None, None, "未匹配"

# --- 2. 团队权限管理 ---
st.set_page_config(page_title="太阳爸爸内部派单助手", layout="wide")

with st.sidebar:
    st.header("🔑 团队授权")
    access_code = st.text_input("请输入内部授权码", type="password")
    if access_code != "sunnysdady666": # 这里设置你的团队密码
        st.warning("授权码不正确，请向张鹏申请。")
        st.stop()
    st.success("验证通过！")
    
    st.divider()
    date_range = st.date_input("选择日期区间", value=(datetime.now(), datetime.now() + timedelta(days=2)))
    default_city = st.text_input("默认城市", value="深圳市")
    sitter_count = st.number_input("喂猫师人数", min_value=1, value=3)
    uploaded_file = st.file_uploader("上传《客户主表》Excel", type=["xlsx"])

# --- 3. 核心逻辑 ---
if uploaded_file and isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
    date_list = pd.date_range(start=start_date, end=end_date).tolist()
    raw_df = pd.read_excel(uploaded_file)
    raw_df.columns = raw_df.columns.str.strip() 

    if st.button("🚀 生成并开启路线看板"):
        all_days_results = []
        progress_bar = st.progress(0)
        for idx, current_date in enumerate(date_list):
            current_ts = pd.Timestamp(current_date)
            def filter_task(row):
                if not (row['服务开始日期'] <= current_ts <= row['服务结束日期']): return False
                delta = (current_ts - row['服务开始日期']).days
                freq = row['投喂频率'] if row['投喂频率'] > 0 else 1
                return delta % freq == 0
            
            day_df = raw_df[raw_df.apply(filter_task, axis=1)].copy()
            if not day_df.empty:
                coords_list = []
                for addr in day_df['详细地址']:
                    lng, lat, status = get_coords(addr, default_city, AMAP_API_KEY)
                    coords_list.append({'lng': lng, 'lat': lat, '解析状态': status})
                day_df = pd.concat([day_df.reset_index(drop=True), pd.DataFrame(coords_list)], axis=1)
                valid_df = day_df.dropna(subset=['lng', 'lat']).copy()
                if not valid_df.empty:
                    kmeans = KMeans(n_clusters=min(len(valid_df), sitter_count), random_state=42, n_init='auto')
                    valid_df['派单组别'] = kmeans.fit_predict(valid_df[['lng', 'lat']])
                    valid_df['喂猫师'] = valid_df['派单组别'].map(lambda x: f"喂猫师_{x+1}")
                    valid_df = valid_df.sort_values(by=['喂猫师', 'lat'], ascending=False)
                    valid_df['顺序'] = valid_df.groupby('喂猫师').cumcount() + 1
                    valid_df['派单日期'] = current_date.strftime('%Y-%m-%d')
                    all_days_results.append(valid_df)
            progress_bar.progress((idx + 1) / len(date_list))
        
        if all_days_results:
            st.session_state['dispatch_data'] = pd.concat(all_days_results)

# --- 4. 路径看板展示 ---
if 'dispatch_data' in st.session_state:
    st.divider()
    df_view = st.session_state['dispatch_data']
    c1, c2 = st.columns(2)
    with c1: sel_date = st.selectbox("📅 派单日期", sorted(df_view['派单日期'].unique()))
    with c2: sel_sitter = st.selectbox("👤 喂猫师", sorted(df_view['喂猫师'].unique()))
    
    view_data = df_view[(df_view['派单日期'] == sel_date) & (df_view['喂猫师'] == sel_sitter)].copy()
    if not view_data.empty:
        # 地图与导航
        view_data['导航'] = view_data.apply(lambda r: f"https://uri.amap.com/marker?position={r['lng']},{r['lat']}&name={urllib.parse.quote(r['详细地址'])}", axis=1)
        
        st.pydeck_chart(pdk.Deck(
            map_style=pdk.map_styles.CARTO_LIGHT,
            initial_view_state=pdk.ViewState(longitude=view_data['lng'].mean(), latitude=view_data['lat'].mean(), zoom=11),
            layers=[
                pdk.Layer("PathLayer", [{"path": view_data[['lng', 'lat']].values.tolist()}], get_path="path", get_width=15, get_color=[0, 100, 255, 200]),
                pdk.Layer("ScatterplotLayer", view_data, get_position='[lng, lat]', get_color=[255, 50, 0, 200], get_radius=150)
            ],
            tooltip={"text": "顺序: {顺序}\n地址: {详细地址}"}
        ))
        st.dataframe(view_data[['顺序', '详细地址', '导航']], column_config={"导航": st.column_config.LinkColumn("点击开启导航")}, hide_index=True)