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
        if res.get("code") != 0:
            st.error(f"❌ 飞书身份授权失败: {res.get('msg')}")
            return None
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
        df = pd.DataFrame([dict(i['fields'], record_id=i['record_id']) for i in items])
        # 补齐必要列
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '投喂频率', '喂猫师', '建议顺序', '备注']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

# 诊断版回传函数：专门处理 404 路径问题
def update_feishu_record(record_id, fields):
    if not record_id or len(str(record_id)) < 5:
        st.error("⚠️ 跳过更新：记录 ID 格式无效。")
        return False

    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # 数据格式标准化
    clean_fields = {k: (int(v) if isinstance(v, (np.int64, np.int32)) else ("" if pd.isna(v) else v)) for k, v in fields.items()}

    try:
        response = requests.patch(url, headers=headers, json={"fields": clean_fields}, timeout=10)
        if response.status_code == 404:
            st.error(f"❌ 404 报错路径诊断: 请检查 Secrets 里的 APP_TOKEN 或 TABLE_ID 是否填错。")
            st.info(f"正在尝试访问的路径: .../apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}")
            return False
        res_json = response.json()
        if res_json.get("code") != 0:
            st.error(f"❌ 飞书拒绝回写: {res_json.get('msg')} (代码: {res_json.get('code')})")
            return False
        return True
    except Exception as e:
        st.error(f"❌ 网络异常: {e}")
        return False

# --- 3. UI 视觉适配 (白底黑字 + 全宽导航) ---
def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; font-family: 'Microsoft YaHei', Arial !important; }
        header { visibility: hidden !important; }
        h1, h2, h3 { color: #000000 !important; border-bottom: 2px solid #000000; padding-bottom: 5px; }

        /* 修复侧边栏白块问题 */
        [data-testid="stSidebar"] { background-color: #FFFFFF !important; border-right: 1px solid #E9ECEF !important; }
        [data-testid="stSidebar"] div[role="radiogroup"] { display: flex; flex-direction: column; gap: 15px; padding: 10px; width: 100% !important; }
        
        [data-testid="stSidebar"] div[role="radiogroup"] label {
            background-color: #F8F9FA !important; border: 1px solid #E0E0E0 !important;
            padding: 25px 10px !important; border-radius: 12px !important; cursor: pointer;
            transition: all 0.2s ease; width: 100% !important; display: flex !important; justify-content: center !important;
        }
        
        /* 强制显影文字描述 */
        [data-testid="stSidebar"] div[role="radiogroup"] label div[data-testid="stMarkdownContainer"] p {
            font-size: 18px !important; color: #000000 !important; font-weight: bold !important; text-align: center !important; margin: 0 !important;
        }

        [data-testid="stSidebar"] div[role="radiogroup"] [data-baseweb="radio"] div:first-child { display: none !important; }

        /* 选中态阴影 */
        [data-testid="stSidebar"] div[role="radiogroup"] label[data-baseweb="radio"]:has(input:checked) {
            background-color: #FFFFFF !important; border: 2px solid #000000 !important; box-shadow: 0 8px 15px rgba(0,0,0,0.1) !important;
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
    menu = st.radio("功能切换", ["📂 数据中心", "🚀 智能看板"], label_visibility="collapsed")

if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

if menu == "📂 数据中心":
    st.title("📂 数据录入与管理")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("选择 Excel", type=["xlsx"])
            if up_file and st.button("🚀 启动数据同步"):
                st.info("同步中...")
                # 此处保持之前的 Excel 录入逻辑
    
    st.divider()
    if st.button("🔄 强制刷新云端预览"):
        st.session_state.pop('feishu_cache', None)
        st.session_state['feishu_cache'] = fetch_feishu_data()
        df_v = st.session_state['feishu_cache'].copy()
        if not df_v.empty:
            for c in ['服务开始日期', '服务结束日期']:
                df_v[c] = pd.to_datetime(df_v[c], unit='ms', errors='coerce').dt.strftime('%Y-%m-%d')
            st.dataframe(df_v.drop(columns=['record_id'], errors='ignore'), use_container_width=True)

else:
    st.title("🚀 智能调度排单看板")
    with st.sidebar:
        st.divider()
        st.subheader("⚙️ 调度设置")
        active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 周期范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))
    
    df = st.session_state['feishu_cache'].copy()
    if not df.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        for col in ['服务开始日期', '服务结束日期']: df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')

        if st.button(f"🚀 点击拟定排单方案 ({start_d} ~ {end_d})"):
            all_plans = []
            days = pd.date_range(start_d, end_d).tolist()
            p_bar_calc = st.progress(0)
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
                            v_df['作业日期'] = d.strftime('%Y-%m-%d')
                            all_plans.append(v_df)
                p_bar_calc.progress((i + 1) / len(days))
            if all_plans: st.session_state['period_plan'] = pd.concat(all_plans); st.success("✅ 方案拟定完成！")
        
        if 'period_plan' in st.session_state:
            res = st.session_state['period_plan']
            view_day = st.selectbox("📅 切换查看日期", sorted(res['作业日期'].unique()))
            v_data = res[(res['作业日期'] == view_day)]
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                                        layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]))
                st.data_editor(v_data[['拟定顺序', '宠物名字', '详细地址', '备注']], use_container_width=True)
                
                if st.button("✅ 确认同步全周期方案至飞书"):
                    t_s = len(res); s_b = st.progress(0); fail_count = 0
                    for i, (_, rs) in enumerate(res.iterrows()):
                        if not update_feishu_record(rs['record_id'], {"喂猫师": rs['拟定人'], "建议顺序": rs['拟定顺序']}):
                            fail_count += 1
                        s_b.progress((i + 1) / t_s)
                    if fail_count == 0: 
                        st.balloons()
                        st.success("🎉 全周期同步已成功！请刷新飞书。")
                        st.session_state.pop('feishu_cache', None) # 同步后强制清理本地缓存
                    else: st.warning(f"⚠️ 同步结束，其中 {fail_count} 条同步失败。")
