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

# --- 1. 核心配置与 ID 强力清洗 ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 调度与财务对账引擎 ---

def get_distance(p1, p2):
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def calculate_billing_days(row, start_range, end_range):
    """计算计费天数：1=每天, 2=隔天"""
    try:
        s_date = pd.to_datetime(row['服务开始日期']).date()
        e_date = pd.to_datetime(row['服务结束日期']).date()
        freq = int(row.get('投喂频率', 1))
        actual_start = max(s_date, start_range)
        actual_end = min(e_date, end_range)
        if actual_start > actual_end: return 0
        count = 0
        current = actual_start
        while current <= actual_end:
            if (current - s_date).days % freq == 0: count += 1
            current += timedelta(days=1)
        return count
    except: return 0

def optimize_route(df_sitter):
    """路径优化：1 -> 2 -> 3"""
    if len(df_sitter) <= 1:
        df_sitter['拟定顺序'] = range(1, len(df_sitter) + 1)
        return df_sitter
    unvisited = df_sitter.to_dict('records')
    current_node = unvisited.pop(0)
    optimized_list = [current_node]
    while unvisited:
        next_node = min(unvisited, key=lambda x: get_distance((current_node['lng'], current_node['lat']), (x['lng'], x['lat'])))
        unvisited.remove(next_node)
        optimized_list.append(next_node)
        current_node = next_node
    res_df = pd.DataFrame(optimized_list)
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

def execute_smart_dispatch(df, active_sitters):
    """负载均衡分配"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    sitter_load = {s: 0 for s in active_sitters}
    for s in df['喂猫师']:
        if s in sitter_load: sitter_load[s] += 1
    for i, row in df.iterrows():
        if str(row.get('喂猫师', '')).strip() not in ["", "nan"]: continue
        if active_sitters:
            best = min(sitter_load, key=sitter_load.get)
            df.at[i, '喂猫师'] = best
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
        if '订单状态' not in df.columns: df['订单状态'] = "进行中" # 预设默认列
        for col in ['宠物名字', '详细地址', '喂猫师', '备注', 'lng', 'lat', '投喂频率']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_status(record_id, status_val):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{str(record_id).strip()}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fields": {"进度": str(status_val)}}
    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=10)
        return r.status_code == 200
    except: return False

def generate_excel_v63(df):
    output = io.BytesIO()
    full_df = df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values(['作业日期', '喂猫师', '拟定顺序'])
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        full_df.to_excel(writer, index=False, sheet_name='汇总')
        mapping_df = df.drop_duplicates(subset=['宠物名字', '详细地址'])[['宠物名字', '详细地址', '喂猫师', '备注']]
        mapping_df.to_excel(writer, index=False, sheet_name='宠物归属明细')
        for s in df['喂猫师'].unique():
            if str(s).strip() and str(s) != 'nan':
                df[df['喂猫师'] == s][['作业日期', '拟定顺序', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name=str(s)[:31])
    return output.getvalue()

# --- 4. UI 视觉方案 (200*50 与 100*25 对齐) ---

def set_ui():
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button {
            width: 200px !important; height: 50px !important;
            border: 3px solid #000 !important; border-radius: 10px !important;
            font-size: 18px !important; font-weight: 800 !important;
            box-shadow: 4px 4px 0px #000; background-color: #FFFFFF !important;
            margin-bottom: 12px !important; display: block; margin-left: auto; margin-right: auto;
        }
        .quick-nav div.stButton > button {
            width: 100px !important; height: 25px !important;
            font-size: 11px !important; padding: 0px !important;
            border: 1.5px solid #000 !important; border-radius: 4px !important;
            box-shadow: 1.5px 1.5px 0px #000; margin: 2px !important;
        }
        .stMetric { background: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #ddd; }
        </style>
        """, unsafe_allow_html=True)

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

# --- 5. 侧边栏布局 (V44 对齐：指挥舱置顶) ---

st.set_page_config(page_title="指挥中心 V63.0", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()

with st.sidebar:
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
    
    d_sel = st.date_input("调度日期范围", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    
    # --- 新增订单状态筛选器 ---
    st.divider()
    all_statuses = ["进行中", "已结束", "待处理"]
    status_filter = st.multiselect("🔍 订单状态过滤", options=all_statuses, default=["进行中"])
    
    sitters = ["梦蕊", "依蕊"]
    active = [s for s in sitters if st.checkbox(f"{s} (出勤)", value=True, key=f"v63_{s}")]
    
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("📊 任务进度"): st.session_state['page'] = "任务进度"
    if st.button("📝 订单信息"): st.session_state['page'] = "订单信息"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()
    with st.expander("🔑 团队授权"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 6. 核心频道渲染 ---

# 订单信息：计费汇总展示
if st.session_state['page'] == "订单信息":
    st.title("📝 订单全景分析 (财务级统计)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        # 执行状态过滤
        df_i = df_raw[df_raw['订单状态'].isin(status_filter)] if status_filter else df_raw
        
        # 计算计费天数
        if isinstance(d_sel, tuple) and len(d_sel) == 2:
            df_i['计费天数'] = df_i.apply(lambda r: calculate_billing_days(r, d_sel[0], d_sel[1]), axis=1)
        else: df_i['计费天数'] = 0
        
        # --- 计费天数合计 ---
        total_billing = df_i['计费天数'].sum()
        st.metric("📊 当前周期内全量计费天数汇总", f"{total_billing} 次上门", help="已根据筛选出的订单和频率自动对账")
        
        for c in ['服务开始日期', '服务结束日期']:
            if c in df_i.columns: df_i[c] = pd.to_datetime(df_i[c]).dt.strftime('%Y-%m-%d')
            
        s = st.text_input("🔍 宠物检索", placeholder="搜索猫咪...")
        if s: df_i = df_i[df_i['宠物名字'].str.contains(s, na=False)]
        
        with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, df_i['详细地址']))
        df_i[['lng', 'lat']] = pd.DataFrame(coords, index=df_i.index, columns=['lng', 'lat'])
        dm = df_i.dropna(subset=['lng', 'lat'])
        if not dm.empty:
            st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=dm['lng'].mean(), latitude=dm['lat'].mean(), zoom=10),
                layers=[pdk.Layer("HeatmapLayer", dm, get_position='[lng, lat]', radius_pixels=60, intensity=1)]))
        st.dataframe(df_i[['宠物名字', '计费天数', '服务开始日期', '服务结束日期', '投喂频率', '喂猫师', '详细地址', '备注']], use_container_width=True)

# 智能看板：作业单量合计
elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度指挥中心")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty and isinstance(d_sel, tuple) and len(d_sel) == 2:
        # 执行状态过滤
        dk_filtered = df_raw[df_raw['订单状态'].isin(status_filter)] if status_filter else df_raw
        
        if st.button("✨ 1. 拟定最优方案"):
            ap = []; dk = execute_smart_dispatch(dk_filtered, active)
            days = pd.date_range(d_sel[0], d_sel[1]).tolist()
            for d in days:
                ct = pd.Timestamp(d); d_v = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                if not d_v.empty:
                    d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not d_v.empty:
                        with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, d_v['详细地址']))
                        d_v[['lng', 'lat']] = pd.DataFrame(coords, index=d_v.index, columns=['lng', 'lat'])
                        dv = d_v.dropna(subset=['lng', 'lat']).copy()
                        for s in active:
                            stks = dv[dv['喂猫师'] == s].copy()
                            if not stks.empty:
                                res = optimize_route(stks); res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
            st.session_state['fp'] = pd.concat(ap) if ap else None
            st.success("✅ 调度方案拟定完成！")

        if st.session_state.get('fp') is not None:
            # --- 方案拟定完成后的单量合计 ---
            plan_total = len(st.session_state['fp'])
            st.metric("📊 周期内总作业单量 (合计计费点)", f"{plan_total} 单", delta=f"覆盖 {len(active)} 名人员")
            
            st.download_button("📥 2. 导出 Excel (含归属明细)", data=generate_excel_v63(st.session_state['fp']), file_name="Dispatch_V63.xlsx")
            res_f = st.session_state['fp']
            vd = st.selectbox("📅 日期", sorted(res_f['作业日期'].unique()))
            v_data = res_f[res_f['作业日期'] == vd]
            st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color='color', get_radius=350, pickable=True)]))
            st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)

# 数据中心、任务进度、帮助文档逻辑对齐 V62 (完整全量)
elif st.session_state['page'] == "数据中心":
    st.title("📂 云端数据同步")
    up = st.file_uploader("Excel 导入", type=["xlsx"])
    if up and st.button("🚀 推送云端"):
        du = pd.read_excel(up); tk = get_feishu_token()
        for _, r in du.iterrows():
            f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')).strip(), "投喂频率": int(r.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000)}
            requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk}"}, json={"fields": f})
        st.success("批量成功！"); st.session_state.pop('feishu_cache', None); st.rerun()
    st.button("🔄 刷新预览", on_click=lambda: st.session_state.pop('feishu_cache', None))
    st.dataframe(st.session_state['feishu_cache'].drop(columns=['lng', 'lat', '_system_id'], errors='ignore'), use_container_width=True)

elif st.session_state['page'] == "任务进度":
    st.title("📊 进度反馈 (实时反馈)")
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        edit = st.data_editor(df_p[['宠物名字', '详细地址', '喂猫师', '进度']], column_config={"进度": st.column_config.SelectboxColumn("状态", options=["未开始", "已出发", "服务中", "已完成"], required=True)}, use_container_width=True)
        if st.button("🚀 提交同步"):
            for i, row in edit.iterrows():
                if row['进度'] != df_p.iloc[i]['进度']: update_feishu_status(df_p.iloc[i]['_system_id'], row['进度'])
            st.success("同步成功！"); st.session_state.pop('feishu_cache', None)

elif st.session_state['page'] == "帮助文档":
    st.title("📖 V63 财务级操作指引")
    st.markdown("""
    1. **状态筛选**：侧边栏新增“订单状态过滤”，默认只排“进行中”的任务，避免过期单据干扰计费。
    2. **计费汇总**：在【订单信息】顶部查看本区间的总计费天数，在【智能看板】拟定后查看总作业单量。
    3. **财务口径**：计费天数合计 = 所有被筛选订单的实际服务次数总和，直接用于客户结算。
    """)
