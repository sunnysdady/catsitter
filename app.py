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

# --- 核心配置 ---
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "")

def extract_room(addr):
    if pd.isna(addr): return ""
    match = re.search(r'([a-zA-Z0-9-]{2,})$', str(addr).strip())
    return match.group(1) if match else ""

@st.cache_data(show_spinner=False)
def get_coords_cached(address, city, api_key):
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

st.set_page_config(page_title="太阳爸爸-均衡版-修复", layout="wide")

with st.sidebar:
    st.header("🔑 团队授权")
    access_code = st.text_input("暗号", type="password", value="sunnysdady666")
    if access_code != "sunnysdady666": st.stop()
    
    st.divider()
    st.header("👤 伙伴出勤")
    active_sitters = []
    if st.checkbox("梦蕊 (出勤)", value=True): active_sitters.append("梦蕊")
    if st.checkbox("依蕊 (出勤)", value=True): active_sitters.append("依蕊")
    
    st.divider()
    date_range = st.date_input("派单日期区间", value=(datetime.now(), datetime.now() + timedelta(days=2)))
    uploaded_file = st.file_uploader("上传《客户主表》Excel", type=["xlsx"])

if uploaded_file and len(active_sitters) > 0:
    raw_df = pd.read_excel(uploaded_file)
    raw_df.columns = raw_df.columns.str.strip()
    
    # 自动补全宠物名称等列
    if '房号' not in raw_df.columns: raw_df['房号'] = raw_df['详细地址'].apply(extract_room)
    if '宠物名字' not in raw_df.columns: raw_df['宠物名字'] = "猫主子"
    if '喂养备注' not in raw_df.columns: raw_df['喂养备注'] = "无"
    if '投喂频率' not in raw_df.columns: raw_df['投喂频率'] = 1

    if st.button("🚀 生成均衡派单看板"):
        start_date, end_date = date_range
        date_list = pd.date_range(start=start_date, end=end_date).tolist()
        all_results = []
        
        for current_date in date_list:
            current_ts = pd.Timestamp(current_date)
            day_df = raw_df[raw_df.apply(lambda r: (r['服务开始日期'] <= current_ts <= r['服务结束日期']) and ((current_ts - r['服务开始日期']).days % (r['投喂频率'] if r['投喂频率']>0 else 1) == 0), axis=1)].copy()
            
            if not day_df.empty:
                with ThreadPoolExecutor(max_workers=10) as executor:
                    coords = list(executor.map(lambda a: get_coords_cached(a, "深圳市", AMAP_API_KEY), day_df['详细地址'].tolist()))
                day_df[['lng', 'lat', 'status']] = pd.DataFrame(coords, index=day_df.index)
                valid_df = day_df.dropna(subset=['lng', 'lat']).copy()
                
                if not valid_df.empty:
                    sitter_count = len(active_sitters)
                    # --- 智能分配保护逻辑：处理单量少于人数的情况 ---
                    if len(valid_df) < sitter_count:
                        valid_df['组'] = np.arange(len(valid_df))
                    else:
                        kmeans = KMeans(n_clusters=sitter_count, random_state=42, n_init='auto')
                        valid_df['组'] = kmeans.fit_predict(valid_df[['lng', 'lat']])
                        # 负载均衡
                        while sitter_count > 1:
                            counts = valid_df['组'].value_counts()
                            g0, g1 = counts.get(0, 0), counts.get(1, 0)
                            if abs(g0 - g1) <= 2: break
                            src, dst = (0, 1) if g0 > g1 else (1, 0)
                            dst_center = kmeans.cluster_centers_[dst]
                            src_idx = valid_df[valid_df['组'] == src].index
                            dists = ((valid_df.loc[src_idx, 'lng'] - dst_center[0])**2 + (valid_df.loc[src_idx, 'lat'] - dst_center[1])**2)
                            valid_df.loc[dists.idxmin(), '组'] = dst

                    valid_df['喂猫师'] = valid_df['组'].map(lambda x: active_sitters[x] if x < sitter_count else active_sitters[0])
                    valid_df = valid_df.sort_values(by=['喂猫师', 'lat'], ascending=False)
                    valid_df['顺序'] = valid_df.groupby('喂猫师').cumcount() + 1
                    valid_df['派单日期'] = current_date.strftime('%Y-%m-%d')
                    all_results.append(valid_df)
        
        if all_results:
            st.session_state['cloud_data'] = pd.concat(all_results)
            st.success("✅ 均衡派单已就绪")

if 'cloud_data' in st.session_state:
    df = st.session_state['cloud_data']
    st.divider()
    c1, c2 = st.columns(2)
    with c1: cur_date = st.selectbox("📅 日期", sorted(df['派单日期'].unique()))
    with c2: cur_sitter = st.selectbox("👤 伙伴视角", sorted(df['喂猫师'].unique()))
    
    worker_data = df[(df['派单日期'] == cur_date) & (df['喂猫师'] == cur_sitter)].copy()
    if not worker_data.empty:
        # 地图看板
        st.pydeck_chart(pdk.Deck(
            map_style=pdk.map_styles.CARTO_LIGHT,
            initial_view_state=pdk.ViewState(longitude=worker_data['lng'].mean(), latitude=worker_data['lat'].mean(), zoom=12),
            layers=[
                pdk.Layer("PathLayer", [{"path": worker_data[['lng', 'lat']].values.tolist()}], get_path="path", get_width=18, get_color=[0, 100, 255, 180]),
                pdk.Layer("ScatterplotLayer", worker_data, get_position='[lng, lat]', get_color=[255, 70, 0], get_radius=220)
            ],
            tooltip={"text": "顺序: {顺序}\n宠物: {宠物名字}\n地址: {详细地址}"}
        ))

        st.subheader(f"📋 {cur_sitter} 的今日清单 (包含宠物名)")
        display_df = worker_data.copy()
        display_df['完成'] = False 
        if '联系电话' in display_df.columns:
            display_df['拨号'] = display_df['联系电话'].apply(lambda x: f"tel:{x}")
        
        target_cols = ['完成', '顺序', '宠物名字', '房号', '详细地址', '投喂频率', '拨号', '喂养备注']
        existing = [c for c in target_cols if c in display_df.columns or c == '完成']
        
        st.data_editor(
            display_df[existing],
            column_config={
                "完成": st.column_config.CheckboxColumn("核销", default=False),
                "宠物名字": st.column_config.TextColumn("🐱 宠物名"),
                "拨号": st.column_config.LinkColumn("📞 拨打"),
                "喂养备注": st.column_config.TextColumn("⚠️ 备注", width="large")
            },
            hide_index=True, use_container_width=True
        )
        
        st.write("📍 **导航助手**")
        nav_cols = st.columns(3)
        for i, row in enumerate(worker_data.itertuples()):
            nav_url = f"https://uri.amap.com/marker?position={row.lng},{row.lat}&name={urllib.parse.quote(row.详细地址)}"
            nav_cols[i % 3].link_button(f"{row.顺序}. {getattr(row,'房号','导航')}", nav_url)
