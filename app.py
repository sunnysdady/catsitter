import streamlit as st
import pandas as pd
import requests
from sklearn.cluster import KMeans
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import json

# --- 1. 核心连接配置 (请确保 Secrets 正确) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "")
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "")
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "") 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "") 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "")

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
        # 补齐必要列
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '投喂频率', '喂猫师', '建议顺序', '备注']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

# 核心改进：数据净化版回传函数
def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # 净化数据：处理 NaN 和数据类型
    clean_fields = {}
    for k, v in fields.items():
        if pd.isna(v): clean_fields[k] = ""
        elif isinstance(v, (np.int64, np.int32)): clean_fields[k] = int(v)
        else: clean_fields[k] = v

    try:
        response = requests.patch(url, headers=headers, json={"fields": clean_fields}, timeout=10)
        # 增加安全解析逻辑
        if "application/json" in response.headers.get("Content-Type", ""):
            res_json = response.json()
            if res_json.get("code") != 0:
                st.error(f"❌ 飞书拒绝更新: {res_json.get('msg')} (代码: {res_json.get('code')})")
                return False
            return True
        else:
            st.error(f"❌ 飞书返回了异常非JSON页面 (状态码: {response.status_code})。请检查表头名是否有误。")
            return False
    except Exception as e:
        st.error(f"❌ 网络回写异常: {e}")
        return False

# --- 3. UI 视觉适配 (雅致白 + 大卡片导航) ---
def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', Arial !important; }
        header { visibility: hidden !important; }
        [data-testid="stSidebar"] { background-color: #FFFFFF !important; border-right: 1px solid #E9ECEF !important; }
        [data-testid="stSidebarUserContent"] { padding-top: 20px !important; }
        [data-testid="stSidebar"] div[role="radiogroup"] { display: flex; flex-direction: column; gap: 15px; width: 100% !important; }
        [data-testid="stSidebar"] div[role="radiogroup"] label {
            background-color: #F8F9FA !important; border: 1px solid #E0E0E0 !important;
            padding: 30px 10px !important; border-radius: 14px !important; cursor: pointer; transition: all 0.2s ease;
        }
        [data-testid="stSidebar"] div[role="radiogroup"] [data-baseweb="radio"] div:first-child { display: none !important; }
        [data-testid="stSidebar"] div[role="radiogroup"] label[data-baseweb="radio"]:has(input:checked) {
            background-color: #FFFFFF !important; border: 2px solid #000000 !important; box-shadow: 0 8px 18px rgba(0,0,0,0.12) !important;
        }
        [data-testid="stSidebar"] div[role="radiogroup"] label p { font-size: 20px !important; font-weight: bold !important; text-align: center !important; }
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
st.set_page_config(page_title="小猫直喂-指挥中心", layout="wide")
set_ui()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    menu = st.radio("功能选择", ["📂 数据录入", "🚀 智能看板"], label_visibility="collapsed")

if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

if menu == "📂 数据录入":
    st.title("📂 数据录入与管理")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("选择 Excel", type=["xlsx"])
            if up_file and st.button("确认同步"):
                df_up = pd.read_excel(up_file)
                total, success = len(df_up), 0
                p_bar = st.progress(0); p_text = st.empty()
                for i, (_, row) in enumerate(df_up.iterrows()):
                    p_text.text(f"同步进度: {i+1}/{total}")
                    s_ts = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                    e_ts = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                    payload = {"详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(), "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": s_ts, "服务结束日期": e_ts, "备注": str(row.get('备注', ''))}
                    # 此处省略了查重，直接演示添加
                    token = get_feishu_token()
                    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
                    requests.post(url, headers={"Authorization": f"Bearer {token}"}, json={"fields": payload})
                    success += 1
                    p_bar.progress((i + 1) / total)
                st.success(f"完成！录入 {success} 条。")
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
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 作业周期", value=(datetime.now(), datetime.now() + timedelta(days=2)))
    
    df = st.session_state['feishu_cache'].copy()
    if not df.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        for col in ['服务开始日期', '服务结束日期']: df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
        start_d, end_d = date_range
        
        if st.button(f"🚀 执行拟定周期方案 ({start_d} ~ {end_d})"):
            all_plans = []
            days = pd.date_range(start_d, end_d).tolist()
            p_bar_calc = st.progress(0); p_text_calc = st.empty()
            
            for i, d in enumerate(days):
                p_text_calc.text(f"分析进度: {d.strftime('%Y-%m-%d')}...")
                cur_ts = pd.Timestamp(d)
                day_df = df[(df['服务开始日期'] <= cur_ts) & (df['服务结束日期'] >= cur_ts)].copy()
                if not day_df.empty:
                    day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not day_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, day_df['详细地址']))
                        day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                        v_df = day_df.dropna(subset=['lng', 'lat']).copy()
                        if not v_df.empty:
                            # 派单逻辑：此处简化演示，实际应包含 KMeans 聚类
                            v_df['拟定人'] = current_active[0] if current_active else "待定"
                            v_df['拟定顺序'] = v_df.groupby('拟定人').cumcount() + 1
                            v_df['作业日期'] = d.strftime('%Y-%m-%d')
                            all_plans.append(v_df)
                p_bar_calc.progress((i + 1) / len(days))
            p_text_calc.empty(); p_bar_calc.empty()
            if all_plans: st.session_state['period_plan'] = pd.concat(all_plans); st.success("方案拟定完成！")
        
        if 'period_plan' in st.session_state:
            res = st.session_state['period_plan']
            view_day = st.selectbox("📅 切换显示日期", sorted(res['作业日期'].unique()))
            worker = st.selectbox("👤 查看伙伴视角", current_active)
            v_data = res[(res['作业日期'] == view_day) & (res['拟定人'] == worker)]
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11), layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]))
                st.data_editor(v_data[['拟定顺序', '宠物名字', '详细地址', '备注']], use_container_width=True)
                
                if st.button("✅ 确认同步此全周期方案至飞书"):
                    t_s = len(res); s_b = st.progress(0); s_t = st.empty(); fail_count = 0
                    for i, (_, rs) in enumerate(res.iterrows()):
                        s_t.text(f"回写云端进度: {i+1}/{t_s}")
                        # 回传时净化数据格式
                        if not update_feishu_record(rs['record_id'], {"喂猫师": rs['拟定人'], "建议顺序": rs['拟定顺序']}):
                            fail_count += 1
                        s_b.progress((i + 1) / t_s)
                    s_t.empty(); s_b.empty()
                    if fail_count == 0: 
                        st.success("🎉 全周期同步已完美完成！"); st.session_state.pop('feishu_cache', None)
                    else: 
                        st.warning(f"⚠️ 同步结束，其中 {fail_count} 条回写失败，请检查上方报错原因。")
