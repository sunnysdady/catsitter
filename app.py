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

# --- 1. 核心连接配置 (自动清理空格，防止 404) ---
# 请确保 Secrets 中的 APP_TOKEN 是 bas 开头的
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 飞书 API 交互逻辑 ---
def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("app_access_token")
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
        df = pd.DataFrame([dict(i['fields'], record_id=i['record_id']) for i in items])
        # 补齐列名，防止 KeyError
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '投喂频率', '喂猫师', '建议顺序', '备注']:
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
    try:
        response = requests.post(url, headers=headers, json={"fields": fields}, timeout=10)
        return "success" if response.json().get("code") == 0 else "error"
    except: return "error"

# 修复 404 问题的关键回传函数
def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    # 构造请求 URL
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # 净化数据类型
    clean_fields = {}
    for k, v in fields.items():
        if pd.isna(v): clean_fields[k] = ""
        elif isinstance(v, (np.int64, np.int32)): clean_fields[k] = int(v)
        else: clean_fields[k] = v

    try:
        response = requests.patch(url, headers=headers, json={"fields": clean_fields}, timeout=10)
        if response.status_code == 404:
            st.error(f"❌ 找不到记录 (404)！请核对 Secrets 中的 APP_TOKEN 或 TABLE_ID 是否填错。")
            return False
        res_json = response.json()
        if res_json.get("code") != 0:
            st.error(f"❌ 飞书拒绝回写: {res_json.get('msg')} (代码: {res_json.get('code')})")
            return False
        return True
    except Exception as e:
        st.error(f"❌ 回写网络异常: {e}")
        return False

# --- 3. 视觉强化：卡片式导航 (带 Emoji 和阴影) ---
def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', Arial !important; }
        header { visibility: hidden !important; }
        h1, h2, h3 { color: #000000 !important; border-bottom: 2px solid #000000; padding-bottom: 5px; }

        /* 侧边栏适配 */
        [data-testid="stSidebar"] { background-color: #FFFFFF !important; border-right: 1px solid #E9ECEF !important; }
        [data-testid="stSidebarUserContent"] { padding-top: 20px !important; }
        
        /* 导航卡片 */
        [data-testid="stSidebar"] div[role="radiogroup"] { display: flex; flex-direction: column; gap: 15px; width: 100% !important; }
        [data-testid="stSidebar"] div[role="radiogroup"] label {
            background-color: #F8F9FA !important; border: 1px solid #E0E0E0 !important;
            padding: 30px 10px !important; border-radius: 14px !important; cursor: pointer;
            transition: all 0.2s ease-in-out; width: 100% !important;
        }
        
        /* 隐藏原生单选圈 */
        [data-testid="stSidebar"] div[role="radiogroup"] [data-baseweb="radio"] div:first-child { display: none !important; }
        
        /* 选中态：阴影与加粗边框 */
        [data-testid="stSidebar"] div[role="radiogroup"] label[data-baseweb="radio"]:has(input:checked) {
            background-color: #FFFFFF !important; border: 2px solid #000000 !important;
            box-shadow: 0 10px 20px rgba(0,0,0,0.15) !important;
        }
        
        /* 文字描述放大 */
        [data-testid="stSidebar"] div[role="radiogroup"] label p {
            font-size: 20px !important; font-weight: bold !important; text-align: center !important; margin: 0 !important;
        }

        .stProgress > div > div > div > div { background-color: #000000 !important; }
        div.stButton > button { background-color: #FFFFFF !important; color: #000000 !important; border: 1px solid #000000 !important; border-radius: 8px !important; font-weight: bold !important; }
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

# --- 4. 页面主体 ---
st.set_page_config(page_title="小猫直喂-调度指挥", layout="wide")
set_ui()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    # 带 Emoji 和文字的导航卡片
    menu = st.radio("导航选择", ["📂 数据中心", "🚀 智能看板"], label_visibility="collapsed")

if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

if menu == "📂 数据中心":
    st.title("📂 数据录入与管理")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("选择文件", type=["xlsx"])
            if up_file and st.button("确认同步"):
                df_up = pd.read_excel(up_file)
                total, success = len(df_up), 0
                p_bar = st.progress(0); p_text = st.empty()
                for i, (_, row) in enumerate(df_up.iterrows()):
                    p_text.text(f"正在录入云端: {i+1}/{total}")
                    s_ts = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                    e_ts = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                    payload = {"详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(), "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": s_ts, "服务结束日期": e_ts, "备注": str(row.get('备注', ''))}
                    if add_feishu_record(payload) == "success": success += 1
                    p_bar.progress((i + 1) / total)
                st.success(f"完成！录入 {success} 条数据。")
                st.session_state['feishu_cache'] = fetch_feishu_data()

    with c2:
        with st.expander("单条快速补单"):
            with st.form("manual", clear_on_submit=True):
                addr = st.text_input("详细地址*")
                cat = st.text_input("宠物名", value="小胖猫")
                f1, f2 = st.columns(2)
                sd, ed = f1.date_input("开始日期"), f2.date_input("结束日期")
                freq = st.number_input("投喂频率", min_value=1, value=1)
                if st.form_submit_button("保存到云端"):
                    payload = {"详细地址": addr.strip(), "宠物名字": cat.strip(), "投喂频率": freq, "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)}
                    res = add_feishu_record(payload)
                    if res == "success": st.balloons(); st.success("录入成功！")
                    elif res == "duplicate": st.error("查重提醒：云端已有该笔记录。")
                    st.session_state['feishu_cache'] = fetch_feishu_data()
    st.divider()
    if st.button("🔄 刷新预览云端数据"):
        st.session_state['feishu_cache'] = fetch_feishu_data()
        df_v = st.session_state['feishu_cache'].copy()
        if not df_v.empty:
            for c in ['服务开始日期', '服务结束日期']:
                df_v[c] = pd.to_datetime(df_v[c], unit='ms', errors='coerce').dt.strftime('%Y-%m-%d')
            st.dataframe(df_v.drop(columns=['record_id'], errors='ignore'), use_container_width=True)

else:
    st.title("🚀 智能调度看板")
    with st.sidebar:
        st.divider()
        st.subheader("⚙️ 调度设置")
        active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (今日出勤)", value=True)]
        date_range = st.date_input("📅 选择作业周期", value=(datetime.now(), datetime.now() + timedelta(days=2)))
    
    df = st.session_state['feishu_cache'].copy()
    if not df.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        for col in ['服务开始日期', '服务结束日期']: df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
        start_d, end_d = date_range
        
        if st.button(f"🚀 点击拟定周期排单方案"):
            all_plans = []
            days = pd.date_range(start_d, end_d).tolist()
            # 增加拟定方案进度条
            p_bar_calc = st.progress(0); p_text_calc = st.empty()
            
            for i, d in enumerate(days):
                p_text_calc.text(f"正在分析 {d.strftime('%Y-%m-%d')} 的作业单量...")
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
                            v_df['作业日期'] = d.strftime('%Y-%m-%d')
                            all_plans.append(v_df)
                p_bar_calc.progress((i + 1) / len(days))
            p_text_calc.empty(); p_bar_calc.empty()
            if all_plans: st.session_state['period_plan'] = pd.concat(all_plans); st.success("拟定完成！")
        
        if 'period_plan' in st.session_state:
            res = st.session_state['period_plan']
            view_day = st.selectbox("📅 切换查看日期", sorted(res['作业日期'].unique()))
            worker = st.selectbox("👤 查看师视角", current_active)
            v_data = res[(res['作业日期'] == view_day) & (res['拟定人'] == worker)]
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11), layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]))
                st.data_editor(v_data[['拟定顺序', '宠物名字', '详细地址', '备注']], use_container_width=True)
                
                # 同步回写进度条与 404 诊断
                if st.button("✅ 确认并同步全周期方案至飞书"):
                    t_s = len(res); s_b = st.progress(0); s_t = st.empty(); fail_count = 0
                    for i, (_, rs) in enumerate(res.iterrows()):
                        s_t.text(f"回写进度: {i+1}/{t_s}")
                        if not update_feishu_record(rs['record_id'], {"喂猫师": rs['拟定人'], "建议顺序": rs['拟定顺序']}):
                            fail_count += 1
                        s_b.progress((i + 1) / t_s)
                    s_t.empty(); s_b.empty()
                    if fail_count == 0: st.success("🎉 全周期方案已成功回写至飞书！"); st.session_state.pop('feishu_cache', None)
                    else: st.warning(f"⚠️ 同步结束，其中 {fail_count} 条同步失败。请检查侧边栏的 404 报错提示。")
