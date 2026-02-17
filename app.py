import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import re
import io
import json
import calendar
import streamlit.components.v1 as components

# --- 1. 核心配置与 ID 强力清洗 (锁定您的飞书运营基地) ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "").strip()

# --- 2. 核心调度与路线耗时引擎 ---

def get_travel_estimate_v95(origin, destination, mode_key):
    """【V95 核心】调用高德 Web 服务计算路程与时间"""
    mode_url_map = {
        "Walking": "walking",
        "Riding": "bicycling", # 对应骑行/电动车
        "Transfer": "integrated" # 对应公交/地铁
    }
    api_type = mode_url_map.get(mode_key, "bicycling")
    url = f"https://restapi.amap.com/v3/direction/{api_type}?origin={origin}&destination={destination}&key={AMAP_API_KEY}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1':
            path = r['route']['paths'][0] if api_type != 'integrated' else r['route']['transits'][0]
            dist = int(path.get('distance', 0))
            dur = int(path.get('duration', 0)) // 60 # 转为分钟
            return dist, dur
    except: pass
    return 0, 0

def get_normalized_address_v95(addr):
    """地址指纹：精准锁定大楼，确保同楼不拆单"""
    if not addr: return "未知"
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    addr = addr.replace("龙华区", "").replace("民治街道", "").replace("龙华街道", "")
    addr = addr.replace('一','1').replace('二','2').replace('三','3').replace('四','4').replace('五','5')
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def calculate_billing_days(row, start_range, end_range):
    """精确财务计费：1=每天, 2=隔天"""
    try:
        if pd.isna(row['服务开始日期']) or pd.isna(row['服务结束日期']): return 0
        s_date = pd.to_datetime(row['服务开始日期']).date()
        e_date = pd.to_datetime(row['服务结束日期']).date()
        raw_f = str(row.get('投喂频率', 1)).strip()
        freq = int(float(raw_f if raw_f else 1))
        if freq < 1: freq = 1
        actual_start, actual_end = max(s_date, start_range), min(e_date, end_range)
        if actual_start > actual_end: return 0
        count = 0; curr = actual_start
        while curr <= actual_end:
            if (curr - s_date).days % freq == 0: count += 1
            curr += timedelta(days=1)
        return count
    except: return 0

def optimize_route_v95(df_sitter, mode_key):
    """路径优化并注入路程耗时数据"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    if len(has_coords) <= 1:
        res = pd.concat([has_coords, no_coords])
        res['拟定顺序'] = range(1, len(res) + 1)
        res['next_dist'] = 0; res['next_dur'] = 0
        return res
    
    unvisited = has_coords.to_dict('records')
    curr_node = unvisited.pop(0); optimized = [curr_node]
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_node['lng']-x['lng'])**2 + (curr_node['lat']-x['lat'])**2))
        dist, dur = get_travel_estimate_v95(f"{curr_node['lng']},{curr_node['lat']}", f"{next_node['lng']},{next_node['lat']}", mode_key)
        optimized[-1]['next_dist'] = dist
        optimized[-1]['next_dur'] = dur
        unvisited.remove(next_node); optimized.append(next_node); curr_node = next_node
    
    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    res_df['next_dist'] = res_df['next_dist'].fillna(0)
    res_df['next_dur'] = res_df['next_dur'].fillna(0)
    return res_df

def execute_smart_dispatch_spatial_v95(df, active_sitters):
    """空间聚类捆绑分配"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    sitter_load = {s: 0 for s in active_sitters}
    for s in df['喂猫师']:
        if s in sitter_load: sitter_load[s] += 1
    df['building_fingerprint'] = df['详细地址'].apply(get_normalized_address_v95)
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
        if '订单状态' in df.columns: df['订单状态'] = df['订单状态'].fillna("进行中")
        else: df['订单状态'] = "进行中"
        df['投喂频率'] = pd.to_numeric(df.get('投喂频率'), errors='coerce').fillna(1).replace(0, 1)
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        if '进度' not in df.columns: df['进度'] = "未开始"
        for col in ['宠物名字', '详细地址', '喂猫师', '备注', 'lng', 'lat']:
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

# --- 4. 辅助组件：一键复制与 Excel ---

def copy_to_clipboard_v95(text):
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
            console.error('复制失败: ', err);
        }});
    }}
    </script>
    """
    components.html(html_code, height=70)

def generate_excel_v95(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name='汇总')
        df.drop_duplicates(subset=['宠物名字', '详细地址'])[['宠物名字', '详细地址', '喂猫师', '备注']].to_excel(writer, index=False, sheet_name='明细')
        for s in df['喂猫师'].unique():
            if str(s).strip() and str(s) != 'nan':
                df[df['喂猫师'] == s][['作业日期', '拟定顺序', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name=str(s)[:31])
    return output.getvalue()

# --- 5. UI 视觉布局 (V44 对齐) ---

st.set_page_config(page_title="指挥中心 V95.0", layout="wide")

def set_ui():
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { width: 200px !important; height: 50px !important; font-size: 18px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000; background-color: #FFFFFF !important; margin-bottom: 12px !important; display: block; margin-left: auto; margin-right: auto; border: 3px solid #000 !important; }
        .quick-nav div.stButton > button { width: 100px !important; height: 25px !important; font-size: 11px !important; border-radius: 4px !important; box-shadow: 1.5px 1.5px 0px #000; border: 1.5px solid #000 !important; }
        .stMetric { background: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #ddd; }
        .help-box { background: #f0f7ff; border-left: 5px solid #1890ff; padding: 15px; border-radius: 8px; margin-bottom: 20px;}
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

# --- 6. 侧边栏布局 (旗舰全功能) ---

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
    
    d_sel = st.date_input("调度范围锁定", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    st.divider()
    s_filter = st.multiselect("🔍 状态筛选器", options=["进行中", "已结束", "待处理"], default=["进行中", "待处理"])
    active_sitters = ["梦蕊", "依蕊"]
    active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True, key=f"v95_{s}")]
    
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    for p in ["数据中心", "任务进度", "订单信息", "智能看板", "帮助文档"]:
        if st.button(p): st.session_state['page'] = p
    st.divider()
    with st.expander("🔑 授权验证"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 7. 频道逻辑全量渲染 (严控无缩减) ---

if st.session_state['page'] == "数据中心":
    st.title("📂 云端数据同步与录单中心")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        st.subheader("⚙️ 订单归属与状态管理台")
        edit_dc = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态']], 
                                 column_config={"喂猫师": st.column_config.SelectboxColumn("人员归属", options=active_sitters), "订单状态": st.column_config.SelectboxColumn("当前状态", options=["进行中", "已结束", "待处理"])}, 
                                 use_container_width=True)
        if st.button("🚀 提交同步修改"):
            for i, row in edit_dc.iterrows():
                if row['订单状态'] != df_raw.iloc[i]['订单状态']: update_feishu_field(df_raw.iloc[i]['_system_id'], "订单状态", row['订单状态'])
                if row['喂猫师'] != df_raw.iloc[i]['喂猫师']: update_feishu_field(df_raw.iloc[i]['_system_id'], "喂猫师", row['喂猫师'])
            st.success("同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量导入 Excel"):
            up = st.file_uploader("上传文件", type=["xlsx"])
            if up and st.button("🚀 推送云端"):
                du = pd.read_excel(up); tk = get_feishu_token()
                for i, (_, r) in enumerate(du.iterrows()):
                    f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')).strip(), "投喂频率": int(r.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk}"}, json={"fields": f})
                st.success("批量同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()
    with c2:
        with st.expander("手动录单 (✍️)"):
            with st.form("man_v95"):
                a = st.text_input("详细地址*"); n = st.text_input("猫咪名"); sd = st.date_input("开始日期"); ed = st.date_input("结束日期")
                if st.form_submit_button("💾 保存录单"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                    st.session_state.pop('feishu_cache', None); st.rerun()

elif st.session_state['page'] == "任务进度":
    st.title("📊 现场实时状态反馈")
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        edit_p = st.data_editor(df_p[['宠物名字', '详细地址', '进度']], column_config={"进度": st.column_config.SelectboxColumn("执行状态", options=["未开始", "已出发", "服务中", "已完成"])}, use_container_width=True)
        if st.button("🚀 提交同步反馈"):
            for i, row in edit_p.iterrows():
                if row['进度'] != df_p.iloc[i]['进度']: update_feishu_field(df_p.iloc[i]['_system_id'], "进度", row['进度'])
            st.success("已回写！"); st.session_state.pop('feishu_cache', None)

elif st.session_state['page'] == "订单信息":
    st.title("📝 财务对账全景 (100% 对齐版)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        df_i = df_raw[df_raw['订单状态'].isin(s_filter)] if s_filter else df_raw
        if isinstance(d_sel, tuple) and len(d_sel) == 2:
            df_i['计费天数'] = df_i.apply(lambda r: calculate_billing_days(r, d_sel[0], d_sel[1]), axis=1)
            st.metric("📊 周期内计费总次数 (159单核销)", f"{df_i['计费天数'].sum()} 次")
        for c in ['服务开始日期', '服务结束日期']:
            if c in df_i.columns: df_i[c] = pd.to_datetime(df_i[c]).dt.strftime('%Y-%m-%d')
        st.dataframe(df_i[['宠物名字', '计费天数', '喂猫师', '服务开始日期', '服务结束日期', '投喂频率', '订单状态', '详细地址']], use_container_width=True)

# 智能看板 (高德测速导航版)
elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度指挥大屏 (V95 测速版)")
    df_raw = st.session_state['feishu_cache'].copy()
    
    col_nav1, col_nav2 = st.columns([1, 3])
    with col_nav1:
        nav_mode = st.radio("🚲 出行模式切换", ["步行", "骑行/电动车", "地铁/公交"], index=1)
        mode_map = {"步行": "Walking", "骑行/电动车": "Riding", "地铁/公交": "Transfer"}
    
    if not df_raw.empty and isinstance(d_sel, tuple) and len(d_sel) == 2:
        df_kb = df_raw[df_raw['订单状态'].isin(s_filter)] if s_filter else df_raw
        if st.button("✨ 1. 拟定方案并测算路程耗时"):
            ap = []; dk = execute_smart_dispatch_spatial_v95(df_kb, active); days = pd.date_range(d_sel[0], d_sel[1]).tolist()
            for d in days:
                ct = pd.Timestamp(d); d_v = dk[(dk['服务开始日期'].notna()) & (dk['服务结束日期'].notna())].copy()
                d_v = d_v[(d_v['服务开始日期'] <= ct) & (d_v['服务结束日期'] >= ct)]
                if not d_v.empty:
                    d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not d_v.empty:
                        with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, d_v['详细地址']))
                        d_v[['lng', 'lat']] = pd.DataFrame(coords, index=d_v.index, columns=['lng', 'lat'])
                        dv = d_v.copy()
                        dv['color'] = dv['喂猫师'].apply(lambda n: '#007BFF' if n == "梦蕊" else '#FFA500')
                        for s in active:
                            stks = dv[dv['喂猫师'] == s].copy()
                            if not stks.empty:
                                # 核心：调用测速路径优化
                                res = optimize_route_v95(stks, mode_map[nav_mode])
                                res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
            st.session_state['fp'] = pd.concat(ap) if ap else None
            st.success("✅ 方案拟定完成！耗时数据已注入简报。")

        if st.session_state.get('fp') is not None:
            st.metric("📊 最终派单总量 (财务闭环)", f"{len(st.session_state['fp'])} 单")
            st.download_button("📥 2. 导出全量 Excel", data=generate_excel_v95(st.session_state['fp']), file_name="Cat_Dispatch_V95.xlsx")
            c_f1, c_f2 = st.columns(2)
            vd = c_f1.selectbox("📅 简报日期选择", sorted(st.session_state['fp']['作业日期'].unique()))
            vs = c_f2.selectbox("👤 地图筛选人员", ["全部"] + sorted(active))
            v_data = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
            
            # --- V95 核心：路程耗时增强型简报预览 ---
            brief = f"📢 {vd} 任务简报\n\n"
            for s in active:
                s_tasks = v_data[v_data['喂猫师'] == s].sort_values('拟定顺序')
                if not s_tasks.empty:
                    brief += f"👤 【{s}】负责项目：\n"
                    for i, (_, t) in enumerate(s_tasks.iterrows()):
                        task_line = f"  {t['拟定顺序']}. {t['宠物名字']}-{t['详细地址']}"
                        if t['next_dur'] > 0:
                            task_line += f" ➡️ (下站约{t['next_dist']}米，{t['next_dur']}分钟)"
                        brief += task_line + "\n"
                    brief += "\n"
            
            copy_to_clipboard_v95(brief.replace('\n', '\\n'))
            st.text_area("📄 简报预览 (包含路程耗时)：", brief, height=220)
            
            cur_v = v_data[v_data['喂猫师'] == vs] if vs != "全部" else v_data
            map_d_clean = cur_v.dropna(subset=['lng', 'lat'])[['lng', 'lat', '宠物名字', '详细地址', 'color', '拟定顺序']].sort_values('拟定顺序').to_dict('records')
            
            if map_d_clean:
                markers_json = json.dumps(map_d_clean)
                amap_html = f"""
                <div id="container" style="width:100%; height:600px; border-radius:10px; border:1px solid #ccc;"></div>
                <script type="text/javascript">
                    window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }};
                </script>
                <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_API_KEY}&plugin=AMap.Walking,AMap.Riding,AMap.Transfer"></script>
                <script type="text/javascript">
                    const map = new AMap.Map('container', {{ zoom: 16, center: [{map_d_clean[0]['lng']}, {map_d_clean[0]['lat']}] }});
                    const markers_data = {markers_json};
                    
                    markers_data.forEach(m => {{
                        const marker = new AMap.Marker({{
                            position: [m.lng, m.lat],
                            map: map,
                            content: `<div style="width:24px; height:24px; background:${{m.color}}; border:2px solid white; border-radius:50%; display:flex; align-items:center; justify-content:center; color:white; font-weight:bold; font-size:11px; box-shadow:0 0 5px rgba(0,0,0,0.5);">${{m.拟定顺序}}</div>`
                        }});
                        marker.setLabel({{ direction:'top', offset: new AMap.Pixel(0, -5), content: m.宠物名字 }});
                    }});

                    if (markers_data.length > 1) {{
                        const mode = "{mode_map[nav_mode]}";
                        let router;
                        if (mode === "Walking") router = new AMap.Walking({{ map: map, hideMarkers: true }});
                        else if (mode === "Riding") router = new AMap.Riding({{ map: map, hideMarkers: true }});
                        else router = new AMap.Transfer({{ map: map, city: '深圳市', hideMarkers: true }});

                        for (let i = 0; i < markers_data.length - 1; i++) {{
                            const start = [markers_data[i].lng, markers_data[i].lat];
                            const end = [markers_data[i+1].lng, markers_data[i+1].lat];
                            router.search(start, end, function(status, result) {{}});
                        }}
                    }}
                    map.setFitView();
                </script>
                """
                components.html(amap_html, height=620)
            st.dataframe(cur_v[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)

elif st.session_state['page'] == "帮助文档":
    st.title("📖 V95 指战员手册")
    st.markdown('<div class="help-box">', unsafe_allow_html=True)
    st.subheader("🎯 测速简报使用说明")
    st.markdown("""
    1. **路程耗时**：简报现在自动计算并显示第一家到第二家的路程和预计时间。
    2. **简报预览**：修复了简报内容不可见的问题，现在预览框可直接查阅内容。
    3. **地图连续规划**：地图上增加了连续的蓝色路径规划线，不再只是离散点。
    4. **全量补齐**：数据中心已补齐手动录单所有输入框及保存按钮。
    """)
    st.markdown('</div>', unsafe_allow_html=True)
