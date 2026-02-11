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

# --- 1. 核心配置 ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 调度与对账引擎 ---

def calculate_billing_days(row, start_range, end_range):
    """计算周期内计费天数：1=每天, 2=隔天"""
    try:
        s_date = pd.to_datetime(row['服务开始日期']).date()
        e_date = pd.to_datetime(row['服务结束日期']).date()
        freq = int(row.get('投喂频率', 1))
        actual_start, actual_end = max(s_date, start_range), min(e_date, end_range)
        if actual_start > actual_end: return 0
        count = 0
        current = actual_start
        while current <= actual_end:
            if (current - s_date).days % freq == 0: count += 1
            current += timedelta(days=1)
        return count
    except: return 0

def optimize_route(df_sitter):
    if len(df_sitter) <= 1:
        df_sitter['拟定顺序'] = range(1, len(df_sitter) + 1)
        return df_sitter
    unvisited = df_sitter.to_dict('records')
    current_node = unvisited.pop(0)
    optimized_list = [current_node]
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((current_node['lng']-x['lng'])**2 + (current_node['lat']-x['lat'])**2))
        unvisited.remove(next_node)
        optimized_list.append(next_node)
        current_node = next_node
    res_df = pd.DataFrame(optimized_list)
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

def execute_smart_dispatch(df, active_sitters):
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

# --- 3. 飞书 API 交互 ---

def get_feishu_token():
    try:
        r = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("tenant_access_token")
    except: return None

def fetch_feishu_data():
    token = get_feishu_token()
    if not token: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"page_size": 500}, timeout=15).json()
        items = r.get("data", {}).get("items", [])
        if not items: return pd.DataFrame()
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        if '进度' not in df.columns: df['进度'] = "未完成"
        if '订单状态' not in df.columns: df['订单状态'] = "进行中"
        for col in ['宠物名字', '详细地址', '喂猫师', '备注', 'lng', 'lat', '投喂频率']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_field(record_id, field_name, value):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{str(record_id).strip()}"
    payload = {"fields": {field_name: str(value)}}
    try:
        r = requests.patch(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json=payload, timeout=10)
        return r.status_code == 200
    except: return False

def generate_excel_v64(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name='汇总')
        df.drop_duplicates(subset=['宠物名字', '详细地址'])[['宠物名字', '详细地址', '喂猫师', '备注']].to_excel(writer, index=False, sheet_name='宠物归属明细')
        for s in df['喂猫师'].unique():
            if str(s).strip() and str(s) != 'nan':
                df[df['喂猫师'] == s][['作业日期', '拟定顺序', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name=str(s)[:31])
    return output.getvalue()

# --- 4. 视觉对齐 ---

st.set_page_config(page_title="指挥中心 V64.0", layout="wide")

def set_ui():
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button {
            width: 200px !important; height: 50px !important; border-radius: 10px !important;
            font-size: 18px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000;
            background-color: #FFFFFF !important; margin-bottom: 12px !important; display: block; margin-left: auto; margin-right: auto;
        }
        .quick-nav div.stButton > button {
            width: 100px !important; height: 25px !important; font-size: 12px !important;
            border: 1.5px solid #000 !important; border-radius: 4px !important; box-shadow: 1.5px 1.5px 0px #000;
        }
        .stMetric { background: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #ddd; }
        </style>
        """, unsafe_allow_html=True)

set_ui()

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

# --- 5. 侧边栏布局 (V44 对齐) ---

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
    
    d_sel = st.date_input("调度区间", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    
    # 状态筛选器
    st.divider()
    s_filter = st.multiselect("🔍 订单状态过滤", options=["进行中", "已结束", "待处理"], default=["进行中"])
    
    sitters = ["梦蕊", "依蕊"]
    active = [s for s in sitters if st.checkbox(f"{s} (出勤)", value=True, key=f"active_{s}")]
    
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("📊 任务进度"): st.session_state['page'] = "任务进度"
    if st.button("📝 订单信息"): st.session_state['page'] = "订单信息"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    if st.button("📖 帮助文档"): st.session_state['page'] = "帮助文档"
    st.markdown('</div>', unsafe_allow_html=True)

# --- 6. 各频道逻辑渲染 ---

# 订单信息：找回筛选与对账
if st.session_state['page'] == "订单信息":
    st.title("📝 订单全景分析 (财务对账)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        df_i = df_raw[df_raw['订单状态'].isin(s_filter)] if s_filter else df_raw
        if isinstance(d_sel, tuple) and len(d_sel) == 2:
            df_i['计费天数'] = df_i.apply(lambda r: calculate_billing_days(r, d_sel[0], d_sel[1]), axis=1)
            st.metric("📊 当前周期总计费天数", f"{df_i['计费天数'].sum()} 次上门")
        
        s_query = st.text_input("🔍 秒搜宠物", placeholder="输入小猫名...")
        if s_query: df_i = df_i[df_i['宠物名字'].str.contains(s_query, na=False)]
        
        with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, df_i['详细地址']))
        df_i[['lng', 'lat']] = pd.DataFrame(coords, index=df_i.index, columns=['lng', 'lat'])
        st.dataframe(df_i[['宠物名字', '计费天数', '服务开始日期', '服务结束日期', '投喂频率', '订单状态', '喂猫师', '详细地址']], use_container_width=True)

# 智能看板：找回人员筛选功能
elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度指挥大屏")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty and isinstance(d_sel, tuple) and len(d_sel) == 2:
        df_kb = df_raw[df_raw['订单状态'].isin(s_filter)] if s_filter else df_raw
        if st.button("✨ 拟定最优调度方案"):
            ap = []; dk = execute_smart_dispatch(df_kb, active)
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
            st.success("✅ 方案拟定完成！")

        if st.session_state.get('fp') is not None:
            st.download_button("📥 导出 Excel (含归属明细)", data=generate_excel_v64(st.session_state['fp']), file_name="Cat_Dispatch_V64.xlsx")
            c1, c2 = st.columns(2)
            vd = c1.selectbox("📅 查看日期", sorted(st.session_state['fp']['作业日期'].unique()))
            # 恢复人员筛选器
            vs = c2.selectbox("👤 筛选人员", ["全部"] + sorted(st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]['喂猫师'].unique().tolist()))
            v_data = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
            if vs != "全部": v_data = v_data[v_data['喂猫师'] == vs]
            
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                    layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color='[0, 123, 255, 180]' if vs=="梦蕊" else '[255, 165, 0, 180]', get_radius=350)]))
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)

# 数据中心：支持订单状态修改
elif st.session_state['page'] == "数据中心":
    st.title("📂 数据快照与状态控制台")
    df_dc = st.session_state['feishu_cache'].copy()
    if not df_dc.empty:
        st.subheader("🛠️ 云端订单状态维护 (实时同步)")
        edit_dc = st.data_editor(df_dc[['宠物名字', '详细地址', '订单状态']], 
                                 column_config={"订单状态": st.column_config.SelectboxColumn("修改状态", options=["进行中", "已结束", "待处理"], required=True)}, 
                                 use_container_width=True)
        if st.button("🚀 提交状态修改至飞书"):
            sc = 0
            for i, row in edit_dc.iterrows():
                if row['订单状态'] != df_dc.iloc[i]['订单状态']:
                    if update_feishu_field(df_dc.iloc[i]['_system_id'], "订单状态", row['订单状态']): sc += 1
            st.success(f"已同步 {sc} 个订单状态。"); st.session_state.pop('feishu_cache', None); st.rerun()
    
    st.divider()
    with st.expander("批量导入 Excel"):
        up = st.file_uploader("上传文件", type=["xlsx"])
        if up and st.button("🚀 推送云端"):
            du = pd.read_excel(up); pb = st.progress(0); tk = get_feishu_token()
            for i, (_, r) in enumerate(du.iterrows()):
                f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')).strip(), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000)}
                requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk}"}, json={"fields": f})
                pb.progress((i + 1) / len(du))
            st.success("同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()

# 任务进度与帮助文档逻辑保持一致
elif st.session_state['page'] == "任务进度":
    st.title("📊 任务执行实时反馈")
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        edit = st.data_editor(df_p[['宠物名字', '详细地址', '进度']], column_config={"进度": st.column_config.SelectboxColumn("执行状态", options=["未开始", "已出发", "服务中", "已完成"])}, use_container_width=True)
        if st.button("🚀 同步进度"):
            for i, row in edit.iterrows():
                if row['进度'] != df_p.iloc[i]['进度']: update_feishu_field(df_p.iloc[i]['_system_id'], "进度", row['进度'])
            st.success("同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()

elif st.session_state['page'] == "帮助文档":
    st.title("📖 V64.0 操作指引")
    st.markdown("""
    1. **如何管理订单？**：在【数据中心】的上方编辑器直接修改订单状态为“已结束”，点击提交即可，**不需要去飞书勾选**。
    2. **筛选在哪？**：进入【智能看板】拟定方案后，日期下方会出现【👤 筛选人员】下拉框，支持只看梦蕊或依蕊。
    3. **财务对账**：【订单信息】顶部会显示总计费天数，已自动根据状态过滤结果计算。
    """)
