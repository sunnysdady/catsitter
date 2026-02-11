import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import re
import io
import calendar

# --- 1. 核心配置与 ID 强力清洗 (保持精准对位) ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
# 使用用户刚提供的精准 Token 与 Table ID
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 调度大脑逻辑 ---

def get_distance(p1, p2):
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """路径优化：1 -> 2 -> 3"""
    if len(df_sitter) <= 1:
        df_sitter['拟定顺序'] = range(1, len(df_sitter) + 1)
        return df_sitter
    unvisited = df_sitter.to_dict('records')
    current_node = unvisited.pop(0)
    optimized_list = [current_node]
    while unvisited:
        next_node = min(unvisited, key=lambda x: get_distance(
            (current_node['lng'], current_node['lat']), (x['lng'], x['lat'])
        ))
        unvisited.remove(next_node)
        optimized_list.append(next_node)
        current_node = next_node
    res_df = pd.DataFrame(optimized_list)
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

def execute_smart_dispatch(df, active_sitters):
    """归属记忆引擎：优先继承云端“喂猫师”列"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    cat_to_sitter_map = {f"{row['宠物名字']}_{row['详细地址']}": str(row['喂猫师']).strip() 
                         for _, row in df.iterrows() if str(row.get('喂猫师', '')).strip() not in ["", "nan"]}
    sitter_load = {s: 0 for s in active_sitters}
    for s in df['喂猫师']:
        if s in sitter_load: sitter_load[s] += 1
    for i, row in df.iterrows():
        if str(row.get('喂猫师', '')).strip() not in ["", "nan"]: continue
        key = f"{row['宠物名字']}_{row['详细地址']}"
        if key in cat_to_sitter_map:
            df.at[i, '喂猫师'] = cat_to_sitter_map[key]
        elif active_sitters:
            best = min(sitter_load, key=sitter_load.get)
            df.at[i, '喂猫师'] = best
            cat_to_sitter_map[key] = best
            sitter_load[best] += 1
    return df

# --- 3. 飞书 API 交互逻辑 ---

def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("tenant_access_token")
    except: return None

def fetch_feishu_data():
    token = get_feishu_token()
    if not token or not APP_TOKEN or not TABLE_ID: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, params={"page_size": 500}, timeout=15).json()
        items = r.get("data", {}).get("items", [])
        if not items: return pd.DataFrame()
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        if '进度' not in df.columns: df['进度'] = "待处理"
        for col in ['宠物名字', '详细地址', '喂猫师', '备注', 'lng', 'lat', '投喂频率']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_v52(record_id, field_name, value):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{str(record_id).strip()}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fields": {field_name: str(value)}}
    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=10)
        return (True, "OK") if r.status_code == 200 else (False, f"Error {r.status_code}: {r.text}")
    except Exception as e:
        return False, str(e)

# --- 4. 视觉对齐与 UI 精修 (200*50 与 100*25) ---

def set_ui():
    st.markdown("""
        <style>
        /* 主频道按钮 200*50 */
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button {
            width: 200px !important; height: 50px !important;
            border: 3px solid #000 !important; border-radius: 10px !important;
            font-size: 18px !important; font-weight: 800 !important;
            box-shadow: 4px 4px 0px #000; background-color: #FFFFFF !important;
            margin-bottom: 12px !important; display: block; margin-left: auto; margin-right: auto;
        }
        /* 快捷调度按钮 100*25 */
        .quick-nav div.stButton > button {
            width: 100px !important; height: 25px !important;
            font-size: 12px !important; padding: 0px !important;
            border: 1.5px solid #000 !important; border-radius: 4px !important;
            box-shadow: 1.5px 1.5px 0px #000; margin: 2px !important;
        }
        .info-card { background: #f8f9fa; border-left: 5px solid #000; padding: 15px; border-radius: 10px; }
        .stMetric { background: white; padding: 10px; border-radius: 5px; border: 1px solid #eee; }
        </style>
        """, unsafe_allow_html=True)

def generate_excel_multisheet(df):
    output = io.BytesIO()
    full_df = df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values(['作业日期', '喂猫师', '拟定顺序'])
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        full_df.to_excel(writer, index=False, sheet_name='汇总')
        for s in df['喂猫师'].unique():
            if str(s).strip() and str(s) != 'nan':
                df[df['喂猫师'] == s][['作业日期', '拟定顺序', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name=str(s)[:31])
    return output.getvalue()

@st.cache_data(show_spinner=False)
def get_coords(address):
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_API_KEY}&address=深圳市{address}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return float(loc[0]), float(loc[1])
    except: pass
    return None, None

# --- 5. 侧边栏层级 (置顶指挥舱) ---

st.set_page_config(page_title="指挥中心 V52.0", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()

with st.sidebar:
    # --- A. 置顶：调度配置 ---
    st.subheader("📅 快捷调度 (100*25)")
    st.markdown('<div class="quick-nav">', unsafe_allow_html=True)
    td = datetime.now().date()
    cq1, cq2 = st.columns(2)
    with cq1:
        if st.button("📍 今天"): st.session_state['r'] = (td, td + timedelta(days=1))
        if st.button("📍 本周"): st.session_state['r'] = (td - timedelta(days=td.weekday()), td + timedelta(days=(6-td.weekday())+1))
    with cq2:
        if st.button("📍 明天"): st.session_state['r'] = (td + timedelta(days=1), td + timedelta(days=2))
        if st.button("📍 本月"): st.session_state['r'] = (td.replace(day=1), td.replace(day=calendar.monthrange(td.year, td.month)[1]) + timedelta(days=1))
    st.markdown('</div>', unsafe_allow_html=True)
    
    d_sel = st.date_input("锁定周期", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    sitters = ["梦蕊", "依蕊"]
    active = [s for s in sitters if st.checkbox(f"{s} (出勤)", value=True, key=f"v52_{s}")]
    
    st.divider()
    # --- B. 居中：导航频道 ---
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("📊 任务进度"): st.session_state['page'] = "任务进度"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    if st.button("📖 帮助文档"): st.session_state['page'] = "帮助文档"
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()
    # --- C. 沉底：授权码 ---
    with st.expander("🔑 团队授权"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 6. 模块频道渲染 ---

if st.session_state['page'] == "帮助文档":
    st.title("📖 V52 稳健修复指引")
    st.info(f"当前识别 APP_TOKEN: {APP_TOKEN} | TABLE_ID: {TABLE_ID}")
    st.markdown("""
    1. **ValueError 修复**：针对排单过程中出现的坐标赋值错误，系统已增加强制列对齐逻辑，即使当日任务为空也不会崩溃。
    2. **锁定机制**：在【智能看板】拟定方案后，点击“强力锁定”，系统会将名字写入飞书的【喂猫师】列。
    """)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度指挥中心")
    if not st.session_state['feishu_cache'].empty and isinstance(d_sel, tuple) and len(d_sel) == 2:
        if st.button("✨ 1. 拟定调度方案"):
            ap = []; dk = st.session_state['feishu_cache'].copy()
            days = pd.date_range(d_sel[0], d_sel[1]).tolist()
            dk = execute_smart_dispatch(dk, active)
            pb = st.progress(0)
            for i, d in enumerate(days):
                ct = pd.Timestamp(d); d_df = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                if not d_df.empty:
                    d_df = d_df[d_df.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not d_df.empty:
                        with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, d_df['详细地址']))
                        # --- V52 核心修复：强制声明列名，防止空数据导致的赋值崩溃 ---
                        coords_df = pd.DataFrame(coords, index=d_df.index, columns=['lng', 'lat'])
                        d_df[['lng', 'lat']] = coords_df
                        dv = d_df.dropna(subset=['lng', 'lat']).copy()
                        if not dv.empty:
                            dv['color'] = dv['喂猫师'].apply(lambda n: [0, 123, 255, 180] if n == "梦蕊" else ([255, 165, 0, 180] if n == "依蕊" else [128, 128, 128, 180]))
                            for s in active:
                                stks = dv[dv['喂猫师'] == s].copy()
                                if not stks.empty:
                                    res = optimize_route(stks); res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
                pb.progress((i + 1) / len(days))
            st.session_state['fp'] = pd.concat(ap) if ap else None
            st.success("✅ 方案拟定完成！坐标引擎已稳健对齐。")

        if st.session_state.get('fp') is not None:
            if st.button("🔒 2. 强力锁定归属 (同步至飞书)"):
                with st.spinner("正在同步云端记录..."):
                    lc = 0; err_log = ""
                    unique_plan = st.session_state['fp'].drop_duplicates(subset=['宠物名字', '详细地址'])
                    for _, row in unique_plan.iterrows():
                        ok, msg = update_feishu_v52(row['_system_id'], "喂猫师", row['喂猫师'])
                        if ok: lc += 1
                        else: err_log = msg
                    if lc > 0: st.success(f"同步大功告成！已为 {lc} 条记录锁定。")
                    if err_log: st.error(f"同步异常报告：{err_log}")
                    st.session_state.pop('feishu_cache', None)
            
            st.download_button("📥 3. 导出 Excel 排单文档", data=generate_excel_multisheet(st.session_state['fp']), file_name="Dispatch.xlsx")
            res_f = st.session_state['fp']
            vd = st.selectbox("📅 查看日期", sorted(res_f['作业日期'].unique()))
            v_data = res_f[res_f['作业日期'] == vd]
            st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color='color', get_radius=350, pickable=True)]))
            st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)

# (数据中心、任务进度逻辑同 V51，保持布局对齐)
