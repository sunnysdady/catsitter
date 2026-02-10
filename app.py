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

# --- 1. 核心配置 ---
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "c26fc76dd582c32e4406552df8ba40ff")

# --- 2. 极简专业 UI：深灰黑风格 (去除背景图) ---
def set_professional_ui():
    st.markdown(f"""
         <style>
         /* 整体背景：深灰黑色，保护视力 */
         .stApp {{
             background-color: #121212 !important;
             color: #E0E0E0 !important;
         }}
         /* 侧边栏：稍浅的灰色，区分功能区 */
         [data-testid="stSidebar"] {{
             background-color: #1E1E1E !important;
             border-right: 1px solid #333;
         }}
         /* 强制所有文字颜色，确保高对比度 */
         [data-testid="stSidebar"] .stMarkdown p, 
         [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p,
         [data-testid="stSidebar"] .stCheckbox label p,
         .stMarkdown p {{
             color: #FFFFFF !important; 
             font-weight: 500 !important;
         }}
         /* 内容容器：半透明深色，增加层次感 */
         .block-container {{
             background-color: rgba(30, 30, 30, 0.8);
             padding: 2.5rem;
             border-radius: 12px;
             margin-top: 1rem;
         }}
         /* 标题：醒目的活力橙 */
         h1, h2, h3 {{
             color: #FF9F43 !important;
             font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
         }}
         /* 表格编辑器适配 */
         .stDataEditor {{
             background-color: #1E1E1E !important;
         }}
         </style>
         """, unsafe_allow_html=True)

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

# --- 3. 初始化 ---
st.set_page_config(page_title="小猫直喂-调度中心", layout="wide", page_icon="🐱")
set_professional_ui()

with st.sidebar:
    st.header("🔑 团队授权")
    access_code = st.text_input("请输入暗号", type="password")
    if access_code != "xiaomaozhiwei666": 
        st.info("💡 请输入暗号 xiaomaozhiwei666 启动系统")
        st.stop()
    
    st.divider()
    st.header("👤 伙伴出勤")
    active_sitters = []
    if st.checkbox("梦蕊 (出勤)", value=True): active_sitters.append("梦蕊")
    if st.checkbox("依蕊 (出勤)", value=True): active_sitters.append("依蕊")
    
    st.divider()
    date_range = st.date_input("派单日期区间", value=(datetime.now(), datetime.now() + timedelta(days=6)))
    uploaded_file = st.file_uploader("上传《客户主表》Excel", type=["xlsx"])

# --- 4. 派单逻辑核心 (双优先级 + 均衡算法) ---
if uploaded_file and len(active_sitters) > 0:
    raw_df = pd.read_excel(uploaded_file)
    raw_df.columns = raw_df.columns.str.strip()
    
    # 智能预处理
    if '房号' not in raw_df.columns: raw_df['房号'] = raw_df['详细地址'].apply(extract_room)
    if '宠物名字' not in raw_df.columns: raw_df['宠物名字'] = "小猫咪"
    if '指定喂猫师' not in raw_df.columns: raw_df['指定喂猫师'] = np.nan
    if '投喂频率' not in raw_df.columns: raw_df['投喂频率'] = 1

    if st.button("🚀 生成小猫直喂派单方案"):
        start_date, end_date = date_range
        date_list = pd.date_range(start=start_date, end=end_date).tolist()
        all_results = []
        
        for current_date in date_list:
            current_ts = pd.Timestamp(current_date)
            # 过滤今日任务
            day_df = raw_df[raw_df.apply(lambda r: (r['服务开始日期'] <= current_ts <= r['服务结束日期']) and ((current_ts - r['服务开始日期']).days % (r['投喂频率'] if r['投喂频率']>0 else 1) == 0), axis=1)].copy()
            
            if not day_df.empty:
                with ThreadPoolExecutor(max_workers=10) as executor:
                    coords = list(executor.map(lambda a: get_coords_cached(a, "深圳市", AMAP_API_KEY), day_df['详细地址'].tolist()))
                day_df[['lng', 'lat', 'status']] = pd.DataFrame(coords, index=day_df.index)
                valid_df = day_df.dropna(subset=['lng', 'lat']).copy()
                
                if not valid_df.empty:
                    # 优先级 1: 指定绑定
                    valid_df['喂猫师'] = valid_df['指定喂猫师']
                    free_mask = valid_df['喂猫师'].isna() | (~valid_df['喂猫师'].isin(active_sitters))
                    
                    # 优先级 2: 距离聚类与均衡调拨
                    if free_mask.any():
                        free_df = valid_df[free_mask].copy()
                        s_count = len(active_sitters)
                        if len(free_df) < s_count:
                            valid_df.loc[free_mask, '喂猫师'] = active_sitters[0]
                        else:
                            kmeans = KMeans(n_clusters=s_count, random_state=42, n_init='auto')
                            free_df['组'] = kmeans.fit_predict(free_df[['lng', 'lat']])
                            while s_count > 1:
                                totals = [len(valid_df[valid_df['喂猫师'] == s]) + len(free_df[free_df['组'] == active_sitters.index(s)]) for s in active_sitters]
                                if abs(totals[0] - totals[1]) <= 2: break
                                src, dst = (0, 1) if totals[0] > totals[1] else (1, 0)
                                targets = free_df[free_df['组'] == src].index
                                dists = ((free_df.loc[targets, 'lng'] - kmeans.cluster_centers_[dst][0])**2 + (free_df.loc[targets, 'lat'] - kmeans.cluster_centers_[dst][1])**2)
                                free_df.loc[dists.idxmin(), '组'] = dst
                            valid_df.loc[free_mask, '喂猫师'] = free_df['组'].map(lambda x: active_sitters[x])

                    valid_df['喂猫师'] = valid_df['喂猫师'].fillna(active_sitters[0])
                    valid_df = valid_df.sort_values(by=['喂猫师', 'lat'], ascending=False)
                    valid_df['顺序'] = valid_df.groupby('喂猫师').cumcount() + 1
                    valid_df['派单日期'] = current_date.strftime('%Y-%m-%d')
                    all_results.append(valid_df)
        
        if all_results:
            st.session_state['cloud_data'] = pd.concat(all_results)
            st.success("✅ 均衡派单方案已就绪")

# --- 5. 展现与下载 ---
if 'cloud_data' in st.session_state:
    df = st.session_state['cloud_data']
    st.divider()
    
    # 导出 Excel 报告
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='全量汇总表')
        for s in df['喂猫师'].unique():
            df[df['喂猫师'] == s].to_excel(writer, index=False, sheet_name=s)
    
    st.download_button(label="📥 下载 Excel 专属排期报告 (分人分表)", data=output.getvalue(), file_name=f"小猫直喂_排期_{datetime.now().strftime('%m%d')}.xlsx")
    
    st.divider()
    c1, c2 = st.columns(2)
    with c1: cur_date = st.selectbox("📅 作业日期", sorted(df['派单日期'].unique()))
    with c2: cur_sitter = st.selectbox("👤 视角切换", sorted(df['喂猫师'].unique()))
    
    worker_data = df[(df['派单日期'] == cur_date) & (df['喂猫师'] == cur_sitter)].copy()
    if not worker_data.empty:
        st.pydeck_chart(pdk.Deck(
            map_style=pdk.map_styles.DARK, # 地图也换成深色模式，便于看清路径
            initial_view_state=pdk.ViewState(longitude=worker_data['lng'].mean(), latitude=worker_data['lat'].mean(), zoom=12),
            layers=[
                pdk.Layer("PathLayer", [{"path": worker_data[['lng', 'lat']].values.tolist()}], get_path="path", get_width=18, get_color=[255, 159, 67, 200]),
                pdk.Layer("ScatterplotLayer", worker_data, get_position='[lng, lat]', get_color=[255, 70, 0], get_radius=220)
            ],
            tooltip={"text": "{顺序}. {宠物名字}\n地址: {详细地址}"}
        ))

        st.subheader(f"📋 {cur_sitter} 的待办清单")
        worker_data['完成'] = False 
        target_cols = ['完成', '顺序', '宠物名字', '房号', '详细地址', '投喂频率', '喂养备注']
        existing = [c for c in target_cols if c in worker_data.columns or c == '完成']
        st.data_editor(worker_data[existing], column_config={"完成": st.column_config.CheckboxColumn("核销")}, hide_index=True, use_container_width=True)
