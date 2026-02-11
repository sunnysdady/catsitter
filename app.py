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

# --- 1. 核心连接配置 (自动清理 Secrets 空格) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 飞书 API 交互逻辑 ---
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
        # 隔离系统 ID
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

# --- 3. UI 视觉重构 (30px 巨幕按钮适配) ---
def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', Arial !important; }
        header { visibility: hidden !important; }
        h1, h2, h3 { color: #000000 !important; border-bottom: 2px solid #000000; padding-bottom: 5px; }
        
        /* 侧边栏按钮：巨幕 30px 适配 */
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important;
            height: 100px !important;  /* 适配 30px 字体的高度 */
            background-color: #FFFFFF !important;
            color: #000000 !important;
            border: 3px solid #000000 !important; /* 加粗边框至 3px */
            border-radius: 15px !important;
            font-size: 30px !important; /* 字体提升至 30px */
            font-weight: 900 !important; /* 极致粗体 */
            margin-bottom: 20px !important;
            transition: all 0.2s ease;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            line-height: 1.2 !important;
        }
        [data-testid="stSidebar"] div.stButton > button:hover {
            background-color: #000000 !important;
            color: #FFFFFF !important;
            box-shadow: 0 10px 20px rgba(0,0,0,0.25);
            transform: scale(1.02);
        }
        
        [data-testid="stSidebar"] { background-color: #FFFFFF !important; border-right: 1px solid #E9ECEF !important; }
        [data-testid="stSidebarUserContent"] { padding-top: 15px !important; }
        
        /* 设置项文字强化 */
        [data-testid="stSidebar"] .stMarkdown p { font-weight: bold !important; font-size: 16px !important; }
        .stProgress > div > div > div > div { background-color: #000000 !important; }
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

# --- 4. 页面控制中心 ---
st.set_page_config(page_title="小猫直喂-指挥中心", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "数据中心"

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    
    # 1. 顶部主按钮
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    
    # 2. 调度配置置顶 (看板页专用)
    if st.session_state['page'] == "智能看板":
        st.divider()
        st.subheader("⚙️ 快速调度")
        active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))
    else:
        current_active = ["梦蕊", "依蕊"]
        date_range = (datetime.now(), datetime.now() + timedelta(days=2))

    # 3. 动态间距保持帮助置底
    spacer_val = "10vh" if st.session_state['page'] == "智能看板" else "40vh"
    st.markdown(f'<div style="height: {spacer_val};"></div>', unsafe_allow_html=True)
    
    st.divider()
    if st.button("📖 使用帮助"): st.session_state['page'] = "帮助"

# 数据缓存
if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

# --- 逻辑渲染分发 ---

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
    st.title("🚀 智能调度排单看板")
    df = st.session_state['feishu_cache'].copy()
    if not df.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        for col in ['服务开始日期', '服务结束日期']: df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
        if st.button(f"🚀 点击拟定周期排单方案"):
            all_plans = []
            days = pd.date_range(start_d, end_d).tolist(); p_bar = st.progress(0)
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
                            v_df['拟定人'] = current_active[0] if current_active else "待分配"
                            v_df['拟定顺序'] = v_df.groupby('拟定人').cumcount() + 1
                            v_df['作业日期'] = d.strftime('%Y-%m-%d'); all_plans.append(v_df)
                p_bar.progress((i + 1) / len(days))
            if all_plans: st.session_state['period_plan'] = pd.concat(all_plans); st.success("✅ 拟定完成！")
        
        if 'period_plan' in st.session_state:
            res = st.session_state['period_plan']
            view_day = st.selectbox("📅 查看具体日期", sorted(res['作业日期'].unique()))
            v_data = res[res['作业日期'] == view_day]
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11), layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]))
                st.data_editor(v_data[['拟定顺序', '宠物名字', '详细地址', '备注']], use_container_width=True)
                
                c1, c2 = st.columns(2)
                if c1.button("📋 导出今日简报"):
                    today_str = datetime.now().strftime('%Y-%m-%d'); today_tasks = res[res['作业日期'] == today_str].sort_values(['拟定人', '拟定顺序'])
                    if not today_tasks.empty:
                        summary = f"📢 今日清单 ({today_str})\n\n"
                        for s in current_active:
                            s_tasks = today_tasks[today_tasks['拟定人'] == s]; summary += f"👤 喂猫师：{s}\n"
                            for _, t in s_tasks.iterrows(): summary += f"   {t['拟定顺序']}. {t['宠物名字']} - {t['详细地址']}\n"
                            summary += "\n"
                        st.text_area("复制发到微信：", summary, height=200)
                if c2.button("✅ 确认并同步飞书"):
                    t_s = len(res); s_b = st.progress(0)
                    for i, (_, rs) in enumerate(res.iterrows()):
                        update_feishu_record(rs['_system_id'], {"喂猫师": rs['拟定人']})
                        s_b.progress((i + 1) / t_s)
                    st.success("🎉 同步已完成！"); st.session_state.pop('feishu_cache', None)

else:
    st.title("📖 使用帮助与日志")
    tab_sop, tab_logs = st.tabs(["💡 操作 SOP", "📜 更新日志"])
    with tab_sop:
        st.info("**核心流程**：飞书存档，看板排期。每日按简报顺序作业。")
        st.markdown("- **1. 录入**：地址关键词需包含区域辅助高德定位\n- **2. 排单**：上午选定伙伴后一键拟定\n- **3. 同步**：仅同步喂猫师归属字段")
    with tab_logs:
        st.subheader("📅 2026-02-11 (V2.4)")
        st.markdown("""
        * **字体巨幕化**：侧边栏按钮字体由 20px 暴增至 **30px**，匹配 100px 高度方框。
        * **边框强化**：导航按钮边框加粗至 3px，黑色对比度拉满，提升“专业终端”手感。
        * **布局对齐**：延续 V2.3 的调度配置置顶逻辑，确保进入看板页无需滚动。
        """)
