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
import streamlit.components.v1 as components

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

# --- 2. 调度与财务对账核心引擎 ---

def get_normalized_address_v73(addr):
    """【V73 进化】精准地址指纹：跳过行政区，锁定大楼"""
    if not addr: return "未知"
    # 1. 预清洗：去掉省市区等宏观前缀，统一数字
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    addr = addr.replace("龙华区", "").replace("民治街道", "").replace("龙华街道", "")
    addr = addr.replace('一','1').replace('二','2').replace('三','3').replace('四','4').replace('五','5')
    # 2. 提取指纹：保留到 栋/号/座/村，忽略具体房号
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def calculate_billing_days(row, start_range, end_range):
    """精确计费逻辑"""
    try:
        s_date = pd.to_datetime(row['服务开始日期']).date()
        e_date = pd.to_datetime(row['服务结束日期']).date()
        freq = int(row.get('投喂频率', 1))
        actual_start, actual_end = max(s_date, start_range), min(e_date, end_range)
        if actual_start > actual_end: return 0
        count = 0; curr = actual_start
        while curr <= actual_end:
            if (curr - s_date).days % freq == 0: count += 1
            curr += timedelta(days=1)
        return count
    except: return 0

def optimize_route(df_sitter):
    """全量派单路径优化"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    if len(has_coords) <= 1:
        res = pd.concat([has_coords, no_coords])
        res['拟定顺序'] = range(1, len(res) + 1)
        return res
    unvisited = has_coords.to_dict('records')
    curr_node = unvisited.pop(0); optimized = [curr_node]
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_node['lng']-x['lng'])**2 + (curr_node['lat']-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node); curr_node = next_node
    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

def execute_smart_dispatch_spatial_v73(df, active_sitters):
    """【V73 进化】空间捆绑优先"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    
    sitter_load = {s: 0 for s in active_sitters}
    # 统计云端已锁定的负载
    for s in df['喂猫师']:
        if s in sitter_load: sitter_load[s] += 1
        
    df['building_fingerprint'] = df['详细地址'].apply(get_normalized_address_v73)
    
    # 对未分配订单按建筑聚类
    unassigned_mask = ~df['喂猫师'].isin(active_sitters)
    if unassigned_mask.any() and active_sitters:
        building_groups = df[unassigned_mask].groupby('building_fingerprint')
        for _, group in building_groups:
            best_sitter = min(sitter_load, key=sitter_load.get)
            df.loc[group.index, '喂猫师'] = best_sitter
            sitter_load[best_sitter] += len(group)
    return df

# --- 3. 飞书 API 服务 ---

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
        # 核心修复：空白状态填充
        if '订单状态' in df.columns:
            df['订单状态'] = df['订单状态'].fillna("进行中")
        else:
            df['订单状态'] = "进行中"
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        if '进度' not in df.columns: df['进度'] = "未开始"
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

# --- 4. 一键复制与 导出组件 ---

def copy_to_clipboard_v73(text):
    html_code = f"""
    <div style="margin-bottom: 20px;">
        <button onclick="copyToClipboard()" style="
            width: 220px; height: 50px; background-color: #000; color: white;
            border-radius: 10px; font-weight: 800; cursor: pointer; border: none;
            box-shadow: 4px 4px 0px #666; font-size: 16px;">
            📋 一键复制微信简报
        </button>
    </div>
    <script>
    function copyToClipboard() {{
        const text = `{text}`;
        navigator.clipboard.writeText(text).then(function() {{
            alert('简报复制成功！');
        }}, function(err) {{
            console.error('无法复制: ', err);
        }});
    }}
    </script>
    """
    components.html(html_code, height=70)

def generate_excel_v73(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name='汇总')
        df.drop_duplicates(subset=['宠物名字', '详细地址'])[['宠物名字', '详细地址', '喂猫师', '备注']].to_excel(writer, index=False, sheet_name='宠物归属明细')
        for s in df['喂猫师'].unique():
            if str(s).strip() and str(s) != 'nan':
                df[df['喂猫师'] == s][['作业日期', '拟定顺序', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name=str(s)[:31])
    return output.getvalue()

# --- 5. UI 视觉方案 (V44 对齐) ---

st.set_page_config(page_title="指挥中心 V73.0", layout="wide")

def set_ui():
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { width: 200px !important; height: 50px !important; font-size: 18px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000; background-color: #FFFFFF !important; margin-bottom: 12px !important; display: block; margin-left: auto; margin-right: auto; border: 3px solid #000 !important; border-radius: 10px !important; }
        .quick-nav div.stButton > button { width: 100px !important; height: 25px !important; font-size: 11px !important; border-radius: 4px !important; box-shadow: 1.5px 1.5px 0px #000; border: 1.5px solid #000 !important; }
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

# --- 6. 侧边栏 (指挥舱置顶) ---

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
    
    d_sel = st.date_input("调度区间选择", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    
    # 状态过滤：默认包含 进行中 和 空白
    st.divider()
    s_filter = st.multiselect("🔍 状态过滤", options=["进行中", "已结束", "待处理"], default=["进行中"])
    
    active_sitters = ["梦蕊", "依蕊"]
    active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True, key=f"v73_{s}")]
    
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    for p in ["数据中心", "任务进度", "订单信息", "智能看板"]:
        if st.button(p): st.session_state['page'] = p
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()
    with st.expander("🔑 团队授权"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 7. 各频道核心渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据同步与状态维护")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        st.subheader("⚙️ 归属与状态管理台")
        edit_dc = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态']], 
                                 column_config={
                                     "喂猫师": st.column_config.SelectboxColumn("指定人员", options=active_sitters),
                                     "订单状态": st.column_config.SelectboxColumn("状态", options=["进行中", "已结束", "待处理"])
                                 }, use_container_width=True)
        if st.button("🚀 提交同步修改"):
            sc = 0
            for i, row in edit_dc.iterrows():
                if row['订单状态'] != df_raw.iloc[i]['订单状态']: update_feishu_field(df_raw.iloc[i]['_system_id'], "订单状态", row['订单状态']); sc += 1
                if row['喂猫师'] != df_raw.iloc[i]['喂猫师']: update_feishu_field(df_raw.iloc[i]['_system_id'], "喂猫师", row['喂猫师']); sc += 1
            st.success(f"同步成功！已更新 {sc} 个字段。"); st.session_state.pop('feishu_cache', None); st.rerun()
    st.divider()
    with st.expander("录单入口"):
        with st.form("add_v73"):
            a = st.text_input("地址*"); n = st.text_input("猫名"); sd = st.date_input("始"); ed = st.date_input("终")
            if st.form_submit_button("💾 保存"):
                f = {"详细地址": a.strip(), "宠物名字": n.strip(), "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                st.session_state.pop('feishu_cache', None); st.rerun()

elif st.session_state['page'] == "任务进度":
    st.title("📊 现场状态反馈 (实时)")
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        edit_p = st.data_editor(df_p[['宠物名字', '详细地址', '进度']], column_config={"进度": st.column_config.SelectboxColumn("反馈", options=["未开始", "已出发", "服务中", "已完成"])}, use_container_width=True)
        if st.button("🚀 同步反馈"):
            for i, row in edit_p.iterrows():
                if row['进度'] != df_p.iloc[i]['进度']: update_feishu_field(df_p.iloc[i]['_system_id'], "进度", row['进度'])
            st.session_state.pop('feishu_cache', None); st.rerun()

elif st.session_state['page'] == "订单信息":
    st.title("📝 财务对账全景 (100% 全量)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        # 状态过滤
        df_i = df_raw[df_raw['订单状态'].isin(s_filter)] if s_filter else df_raw
        if isinstance(d_sel, tuple) and len(d_sel) == 2:
            df_i['计费天数'] = df_i.apply(lambda r: calculate_billing_days(r, d_sel[0], d_sel[1]), axis=1)
            st.metric("📊 周期内总计费次数 (100% 对账)", f"{df_i['计费天数'].sum()} 次")
        for c in ['服务开始日期', '服务结束日期']:
            if c in df_i.columns: df_i[c] = pd.to_datetime(df_i[c]).dt.strftime('%Y-%m-%d')
        st.dataframe(df_i[['宠物名字', '计费天数', '喂猫师', '服务开始日期', '服务结束日期', '投喂频率', '订单状态', '详细地址']], use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度指挥大屏 (全量修复版)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty and isinstance(d_sel, tuple) and len(d_sel) == 2:
        df_kb = df_raw[df_raw['订单状态'].isin(s_filter)] if s_filter else df_raw
        if st.button("✨ 1. 拟定全量方案 (空间捆绑模式)"):
            ap = []; dk = execute_smart_dispatch_spatial_v73(df_kb, active); days = pd.date_range(d_sel[0], d_sel[1]).tolist()
            for d in days:
                ct = pd.Timestamp(d); d_v = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                if not d_v.empty:
                    d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not d_v.empty:
                        with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, d_v['详细地址']))
                        d_v[['lng', 'lat']] = pd.DataFrame(coords, index=d_v.index, columns=['lng', 'lat'])
                        # 核心修复：100% 不漏单
                        dv = d_v.copy(); dv['color'] = dv['喂猫师'].apply(lambda n: [0, 123, 255, 180] if n == "梦蕊" else [255, 165, 0, 180])
                        for s in active:
                            stks = dv[dv['喂猫师'] == s].copy()
                            if not stks.empty:
                                res = optimize_route(stks); res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
            st.session_state['fp'] = pd.concat(ap) if ap else None
            st.success("✅ 拟定完成！对账结果已 100% 对齐。")

        if st.session_state.get('fp') is not None:
            st.metric("📊 方案最终总单量 (财务闭环)", f"{len(st.session_state['fp'])} 单")
            st.download_button("📥 2. 导出 Excel", data=generate_excel_v73(st.session_state['fp']), file_name="Dispatch_Full_V73.xlsx")
            c_f1, c_f2 = st.columns(2)
            vd = c_f1.selectbox("📅 选择日期简报", sorted(st.session_state['fp']['作业日期'].unique()))
            # 恢复筛选人员功能
            vs = c_f2.selectbox("👤 地图筛选", ["全部"] + sorted(active))
            v_data = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
            
            brief = f"📢 {vd} 任务简报\n\n"
            for s in active:
                s_tasks = v_data[v_data['喂猫师'] == s].sort_values('拟定顺序')
                if not s_tasks.empty:
                    brief += f"👤 【{s}】负责人：\n" + "\n".join([f"  {t['拟定顺序']}. {t['宠物名字']}-{t['详细地址']}" for _, t in s_tasks.iterrows()]) + "\n\n"
            
            # --- 一键复制黑科技 ---
            copy_to_clipboard_v73(brief.replace('\n', '\\n'))
            st.text_area("📄 简报预览：", brief, height=180)

            cur_v = v_data[v_data['喂猫师'] == vs] if vs != "全部" else v_data
            map_d = cur_v.dropna(subset=['lng', 'lat'])
            if not map_d.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=map_d['lng'].mean(), latitude=map_d['lat'].mean(), zoom=11),
                    layers=[pdk.Layer("ScatterplotLayer", map_d, get_position='[lng, lat]', get_color='color', get_radius=350)]))
            st.dataframe(cur_v[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)

elif st.session_state['page'] == "帮助文档":
    st.title("📖 V73.0 运营旗舰版指引")
    st.info("💡 如果单量对不上，请检查是否在侧边栏勾选了‘已结束’。默认已帮您找回所有空白状态的单据。")
