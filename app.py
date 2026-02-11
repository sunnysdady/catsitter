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
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 核心算法：最近邻路径优化 ---
def get_distance(p1, p2):
    """计算两点间的经纬度距离 (欧几里得简化版)"""
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """
    最近邻算法实现：
    从第一个点开始，每次寻找距离当前点最近的下一个未访问点，重新生成拟定顺序。
    """
    if len(df_sitter) <= 1:
        df_sitter['拟定顺序'] = range(1, len(df_sitter) + 1)
        return df_sitter
    
    unvisited = df_sitter.to_dict('records')
    # 默认从第一个点开始
    current_node = unvisited.pop(0)
    optimized_list = [current_node]
    
    while unvisited:
        # 寻找距离当前节点最近的下一个节点
        next_node = min(unvisited, key=lambda x: get_distance(
            (current_node['lng'], current_node['lat']), 
            (x['lng'], x['lat'])
        ))
        unvisited.remove(next_node)
        optimized_list.append(next_node)
        current_node = next_node
        
    res_df = pd.DataFrame(optimized_list)
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

# --- 3. 飞书 API 交互逻辑 ---
def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        res = r.json()
        if res.get("code") != 0: return None
        return res.get("tenant_access_token")
    except: return None

def fetch_feishu_data():
    token = get_feishu_token()
    if not token: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, params={"page_size": 500}, timeout=15).json()
        items = r.get("data", {}).get("items", [])
        if not items: return pd.DataFrame()
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        required_cols = ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '投喂频率', '喂猫师', '备注']
        for col in required_cols:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def add_feishu_record(fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        response = requests.post(url, headers=headers, json={"fields": fields}, timeout=10)
        return response.json().get("code") == 0
    except: return False

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    clean_fields = {k: ("" if pd.isna(v) else v) for k, v in fields.items()}
    try:
        response = requests.patch(url, headers=headers, json={"fields": clean_fields}, timeout=10)
        return response.json().get("code") == 0
    except: return False

# --- 4. UI 视觉重构 (30px 巨幕按钮适配) ---
def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', Arial !important; }
        header { visibility: hidden !important; }
        h1, h2, h3 { color: #000000 !important; border-bottom: 2px solid #000000; padding-bottom: 5px; }
        
        /* 侧边栏按钮：巨幕 30px 适配 */
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important;
            height: 100px !important;
            background-color: #FFFFFF !important;
            color: #000000 !important;
            border: 3px solid #000000 !important;
            border-radius: 15px !important;
            font-size: 30px !important;
            font-weight: 900 !important;
            margin-bottom: 20px !important;
            transition: all 0.2s ease;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        [data-testid="stSidebar"] div.stButton > button:hover {
            background-color: #000000 !important;
            color: #FFFFFF !important;
            transform: scale(1.02);
        }
        
        [data-testid="stSidebar"] { background-color: #FFFFFF !important; border-right: 1px solid #E9ECEF !important; }
        .stDataFrame { font-size: 16px !important; }
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
    except: return None, None

# --- 5. 页面控制中心 ---
st.set_page_config(page_title="小猫直喂-指挥中心", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "数据中心"

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    
    if st.session_state['page'] == "智能看板":
        st.divider()
        st.subheader("⚙️ 快速调度")
        active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))
    else:
        current_active = ["梦蕊", "依蕊"]
        date_range = (datetime.now(), datetime.now() + timedelta(days=2))

    st.markdown('<div style="height: 30vh;"></div>', unsafe_allow_html=True)
    st.divider()
    if st.button("📖 使用帮助"): st.session_state['page'] = "帮助"

# 数据缓存
if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

# --- 6. 业务模块渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (导入与预览)")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("上传 Excel", type=["xlsx"])
            if up_file and st.button("🚀 启动数据录入"):
                df_up = pd.read_excel(up_file); p_bar = st.progress(0)
                for i, (_, row) in enumerate(df_up.iterrows()):
                    s_ts = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                    e_ts = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                    payload = {"详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(), "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": s_ts, "服务结束日期": e_ts, "备注": str(row.get('备注', ''))}
                    add_feishu_record(payload); p_bar.progress((i + 1) / len(df_up))
                st.success("批量同步完成！"); st.session_state['feishu_cache'] = fetch_feishu_data()
    with c2:
        with st.expander("单条手动录入"):
            with st.form("single"):
                addr = st.text_input("地址*"); cat = st.text_input("宠物名"); sd = st.date_input("开始"); ed = st.date_input("结束")
                if st.form_submit_button("保存"):
                    payload = {"详细地址": addr.strip(), "宠物名字": cat.strip(), "投喂频率": 1, "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)}
                    if add_feishu_record(payload): st.success("录入成功！"); st.session_state['feishu_cache'] = fetch_feishu_data()
    
    st.divider()
    if st.button("🔄 强制刷新预览云端数据"):
        st.session_state.pop('feishu_cache', None); st.session_state['feishu_cache'] = fetch_feishu_data()
    
    df_v = st.session_state['feishu_cache'].copy()
    if not df_v.empty:
        for c in ['服务开始日期', '服务结束日期']: df_v[c] = pd.to_datetime(df_v[c], unit='ms', errors='coerce').dt.strftime('%Y-%m-%d')
        st.dataframe(df_v.drop(columns=['_system_id'], errors='ignore'), use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能调度排单看板 (V2.7)")
    df = st.session_state['feishu_cache'].copy()
    
    if not df.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        for col in ['服务开始日期', '服务结束日期']: df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
        
        if st.button(f"🚀 点击拟定周期排单方案"):
            all_plans = []
            days = pd.date_range(start_d, end_d).tolist(); p_bar = st.progress(0)
            
            # --- 核心调度映射：用于实现客户绑定 ---
            addr_to_sitter_map = {}
            
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d)
                day_df = df[(df['服务开始日期'] <= cur_ts) & (df['服务结束日期'] >= cur_ts)].copy()
                if not day_df.empty:
                    day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not day_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, day_df['详细地址']))
                        day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                        v_df = day_df.dropna(subset=['lng', 'lat']).copy()
                        
                        if not v_df.empty:
                            # --- 注入三级分配算法 ---
                            sitter_load = {s: 0 for s in current_active} # 统计今日负载
                            
                            def sitter_assign_logic(row):
                                addr, manual = row['详细地址'], str(row.get('喂猫师', '')).strip()
                                # 规则 1: 手动指定优先
                                if manual and manual != "nan" and manual != "":
                                    addr_to_sitter_map[addr] = manual; return manual
                                # 规则 2: 老客户/同地址绑定
                                if addr in addr_to_sitter_map: return addr_to_sitter_map[addr]
                                # 规则 3: 负载均衡 (仅从出勤人员中选)
                                if current_active:
                                    best = min(sitter_load, key=sitter_load.get)
                                    sitter_load[best] += 1
                                    addr_to_sitter_map[addr] = best; return best
                                return "待分配"

                            v_df['拟定人'] = v_df.apply(sitter_assign_logic, axis=1)
                            v_df['作业日期'] = d.strftime('%Y-%m-%d')
                            
                            # --- 注入路径优化算法 ---
                            optimized_day = []
                            for sitter in current_active:
                                s_tasks = v_df[v_df['拟定人'] == sitter].copy()
                                if not s_tasks.empty: optimized_day.append(optimize_route(s_tasks))
                            if optimized_day: all_plans.append(pd.concat(optimized_day))
                p_bar.progress((i + 1) / len(days))
            
            if all_plans: st.session_state['period_plan'] = pd.concat(all_plans); st.success("✅ 优化完成！")
        
        if 'period_plan' in st.session_state:
            res = st.session_state['period_plan']
            col_f1, col_f2 = st.columns(2)
            with col_f1: view_day = st.selectbox("📅 选择查看日期", sorted(res['作业日期'].unique()))
            with col_f2:
                s_list = ["全部"] + sorted(res[res['作业日期'] == view_day]['拟定人'].unique().tolist())
                view_sitter = st.selectbox("👤 筛选喂猫师", s_list)
            
            v_data = res[res['作业日期'] == view_day]
            if view_sitter != "全部": v_data = v_data[v_data['拟定人'] == view_sitter]
            
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11), layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]))
                st.data_editor(v_data[['拟定顺序', '拟定人', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                
                c1, c2 = st.columns(2)
                if c1.button("📋 导出今日简报"):
                    today_tasks = v_data.sort_values(['拟定人', '拟定顺序'])
                    summary = f"📢 清单 ({view_day})\n\n"
                    for s in (current_active if view_sitter == "全部" else [view_sitter]):
                        s_tasks = today_tasks[today_tasks['拟定人'] == s]
                        if not s_tasks.empty:
                            summary += f"👤 喂猫师：{s}\n"
                            for _, t in s_tasks.iterrows(): summary += f"   {t['拟定顺序']}. {t['宠物名字']} - {t['详细地址']}\n"
                            summary += "\n"
                    st.text_area("复制发到微信：", summary, height=200)

                if c2.button("✅ 确认并同步飞书"):
                    t_s = len(res); s_b = st.progress(0)
                    for i, (_, rs) in enumerate(res.iterrows()):
                        update_feishu_record(rs['_system_id'], {"喂猫师": rs['拟定人']})
                        s_b.progress((i + 1) / t_s)
                    st.success("🎉 同步完成！"); st.session_state.pop('feishu_cache', None)

else:
    st.title("📖 使用帮助与日志")
    st.info("**核心流程**：飞书存档 -> 智能排期 -> 路径优化 -> 一键同步。")
    st.markdown("""
    - **1. 人工干预优先**：若 Excel 已填喂猫师，系统不再自动分配。
    - **2. 客户绑定**：同一地址订单自动分配给同一位喂猫师，增强服务连续性。
    - **3. 路径自动优化**：接入最近邻算法，自动按距离排列作业顺序，不走回头路。
    - **4. 巨幕适配**：所有导航按钮字体已提升至 30px，适配高分辨率显示器。
    """)
