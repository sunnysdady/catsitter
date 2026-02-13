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

# --- 2. 空间优先调度引擎 ---

def get_normalized_address(addr):
    """【V72 核心】标准化地址，提取建筑指纹"""
    if not addr: return "未知"
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    addr = addr.replace('一','1').replace('二','2').replace('三','3').replace('四','4').replace('五','5')
    # 提取到 栋/号/座/大厦/村 为止，忽略后续房号
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def calculate_billing_days(row, start_range, end_range):
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

def execute_smart_dispatch_spatial_first(df, active_sitters):
    """【V72 进化】地理空间优先 -> 负载均衡"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    
    # 1. 记录已分配的负载
    sitter_load = {s: 0 for s in active_sitters}
    for s in df['喂猫师']:
        if s in sitter_load: sitter_load[s] += 1
        
    # 2. 对未分配订单进行地址归一化
    df['building_fingerprint'] = df['详细地址'].apply(get_normalized_address)
    
    # 3. 按建筑单元进行“整体捆绑分配”
    unassigned_mask = ~df['喂猫师'].isin(active_sitters)
    if unassigned_mask.any() and active_sitters:
        # 获取所有待分配的建筑包
        building_groups = df[unassigned_mask].groupby('building_fingerprint')
        
        for _, group in building_groups:
            # 找到当前活最少的人
            best_sitter = min(sitter_load, key=sitter_load.get)
            # 核心：将该大楼的所有猫，不论几个，全部锁给同一个人
            df.loc[group.index, '喂猫师'] = best_sitter
            sitter_load[best_sitter] += len(group)
            
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

# --- 4. 视觉与复制工具组件 ---

def copy_to_clipboard_component(text):
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
            alert('简报已复制！');
        }}, function(err) {{
            console.error('无法复制: ', err);
        }});
    }}
    </script>
    """
    components.html(html_code, height=70)

def generate_excel_v72(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name='汇总')
        df.drop_duplicates(subset=['宠物名字', '详细地址'])[['宠物名字', '详细地址', '喂猫师', '备注']].to_excel(writer, index=False, sheet_name='宠物归属明细')
        for s in df['喂猫师'].unique():
            if str(s).strip() and str(s) != 'nan':
                df[df['喂猫师'] == s][['作业日期', '拟定顺序', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name=str(s)[:31])
    return output.getvalue()

# --- 5. UI 与 侧边栏布局 (V44 置顶风格) ---

st.set_page_config(page_title="指挥中心 V72.0", layout="wide")

def set_ui():
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { width: 200px !important; height: 50px !important; font-size: 18px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000; background-color: #FFFFFF !important; margin-bottom: 12px !important; display: block; margin-left: auto; margin-right: auto; border: 3px solid #000 !important; border-radius: 10px !important; }
        .quick-nav div.stButton > button { width: 100px !important; height: 25px !important; font-size: 12px !important; border-radius: 4px !important; box-shadow: 1.5px 1.5px 0px #000; border: 1.5px solid #000 !important; }
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
    
    d_sel = st.date_input("锁定周期", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    st.divider()
    s_filter = st.multiselect("🔍 状态筛选", options=["进行中", "已结束", "待处理"], default=["进行中"])
    active_sitters = ["梦蕊", "依蕊"]
    active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True, key=f"v72_{s}")]
    
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    for p in ["数据中心", "任务进度", "订单信息", "智能看板"]:
        if st.button(p): st.session_state['page'] = p
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()
    with st.expander("🔑 团队授权"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 6. 各频道全量逻辑 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 云端数据同步与维护")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        st.subheader("⚙️ 订单生命周期管理 (App 修改同步)")
        edit_dc = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态']], 
                                 column_config={
                                     "喂猫师": st.column_config.SelectboxColumn("归属人员", options=active_sitters),
                                     "订单状态": st.column_config.SelectboxColumn("生命周期", options=["进行中", "已结束", "待处理"])
                                 }, use_container_width=True)
        if st.button("🚀 提交全部同步至飞书"):
            sc = 0
            for i, row in edit_dc.iterrows():
                if row['订单状态'] != df_raw.iloc[i]['订单状态']: update_feishu_field(df_raw.iloc[i]['_system_id'], "订单状态", row['订单状态']); sc += 1
                if row['喂猫师'] != df_raw.iloc[i]['喂猫师']: update_feishu_field(df_raw.iloc[i]['_system_id'], "喂猫师", row['喂猫师']); sc += 1
            st.success(f"已同步 {sc} 条修改。"); st.session_state.pop('feishu_cache', None); st.rerun()
    st.divider()
    with st.expander("录单入口"):
        with st.form("add_v72"):
            a = st.text_input("地址*"); n = st.text_input("猫名"); sd = st.date_input("始"); ed = st.date_input("终")
            if st.form_submit_button("💾 保存"):
                f = {"详细地址": a.strip(), "宠物名字": n.strip(), "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)}
                requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                st.session_state.pop('feishu_cache', None); st.rerun()

elif st.session_state['page'] == "任务进度":
    st.title("📊 深圳现场状态同步")
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        edit_p = st.data_editor(df_p[['宠物名字', '详细地址', '进度']], column_config={"进度": st.column_config.SelectboxColumn("状态", options=["未开始", "已出发", "服务中", "已完成"])}, use_container_width=True)
        if st.button("🚀 提交反馈"):
            for i, row in edit_p.iterrows():
                if row['进度'] != df_p.iloc[i]['进度']: update_feishu_field(df_p.iloc[i]['_system_id'], "进度", row['进度'])
            st.session_state.pop('feishu_cache', None); st.rerun()

elif st.session_state['page'] == "订单信息":
    st.title("📝 财务对账全景 (100% 全量)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        df_i = df_raw[df_raw['订单状态'].isin(s_filter)] if s_filter else df_raw
        if isinstance(d_sel, tuple) and len(d_sel) == 2:
            df_i['计费天数'] = df_i.apply(lambda r: calculate_billing_days(r, d_sel[0], d_sel[1]), axis=1)
            st.metric("📊 周期内计费总计 (应收)", f"{df_i['计费天数'].sum()} 次")
        for c in ['服务开始日期', '服务结束日期']:
            if c in df_i.columns: df_i[c] = pd.to_datetime(df_i[c]).dt.strftime('%Y-%m-%d')
        st.dataframe(df_i[['宠物名字', '计费天数', '喂猫师', '服务开始日期', '服务结束日期', '投喂频率', '订单状态', '详细地址']], use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度中心 (聚类捆绑版)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty and isinstance(d_sel, tuple) and len(d_sel) == 2:
        df_kb = df_raw[df_raw['订单状态'].isin(s_filter)] if s_filter else df_raw
        if st.button("✨ 1. 拟定方案 (空间聚类优先)"):
            ap = []; dk = execute_smart_dispatch_spatial_first(df_kb, active); days = pd.date_range(d_sel[0], d_sel[1]).tolist()
            for d in days:
                ct = pd.Timestamp(d); d_v = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                if not d_v.empty:
                    d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not d_v.empty:
                        with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, d_v['详细地址']))
                        d_v[['lng', 'lat']] = pd.DataFrame(coords, index=d_v.index, columns=['lng', 'lat'])
                        dv = d_v.copy(); dv['color'] = dv['喂猫师'].apply(lambda n: [0, 123, 255, 180] if n == "梦蕊" else [255, 165, 0, 180])
                        for s in active:
                            stks = dv[dv['喂猫师'] == s].copy()
                            if not stks.empty:
                                res = optimize_route(stks); res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
            st.session_state['fp'] = pd.concat(ap) if ap else None
            st.success("✅ 拟定完成！同楼/同小区单子已捆绑。")

        if st.session_state.get('fp') is not None:
            st.metric("📊 最终派发总量 (计费点对齐)", f"{len(st.session_state['fp'])} 单")
            st.download_button("📥 2. 导出 Excel", data=generate_excel_v72(st.session_state['fp']), file_name="Cat_Dispatch_V72.xlsx")
            c_f1, c_f2 = st.columns(2)
            vd = c_f1.selectbox("📅 查看日期简报", sorted(st.session_state['fp']['作业日期'].unique()))
            vs = c_f2.selectbox("👤 地图筛选人员", ["全部"] + sorted(active))
            v_data = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
            
            brief = f"📢 {vd} 喂猫任务单\n\n"
            for s in active:
                s_tasks = v_data[v_data['喂猫师'] == s].sort_values('拟定顺序')
                if not s_tasks.empty:
                    brief += f"👤 【{s}】负责人：\n"
                    brief += "\n".join([f"  {t['拟定顺序']}. {t['宠物名字']}-{t['详细地址']}" for _, t in s_tasks.iterrows()]) + "\n\n"
            
            copy_to_clipboard_component(brief.replace('\n', '\\n'))
            st.text_area("📄 简报内容预览：", brief, height=180)

            cur_v = v_data[v_data['喂猫师'] == vs] if vs != "全部" else v_data
            map_d = cur_v.dropna(subset=['lng', 'lat'])
            if not map_d.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=map_d['lng'].mean(), latitude=map_d['lat'].mean(), zoom=11),
                    layers=[pdk.Layer("ScatterplotLayer", map_d, get_position='[lng, lat]', get_color='color', get_radius=350)]))
            st.dataframe(cur_v[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)

# 帮助文档等逻辑保持
