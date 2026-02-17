import streamlit as st

# --- 【V133 核心加固：入口状态保险锁】 ---
def init_session_state_v133():
    td = datetime.now().date() if 'datetime' in globals() else None
    keys_defaults = {
        'system_logs': [],
        'commute_stats': {},
        'page': "智能看板",
        'plan_state': "IDLE",
        'feishu_cache': None,
        'r': (None, None)
    }
    for key, val in keys_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

# --- 1. 全球指战指令集导入 ---
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import re
import io
import json
import calendar
from urllib.parse import quote
import streamlit.components.v1 as components

# 执行强制初始化序列
if 'r' not in st.session_state or st.session_state['r'] == (None, None):
    td = datetime.now().date()
    st.session_state['r'] = (td, td + timedelta(days=1))
init_session_state_v133()

# --- 2. 核心配置与双 Key 穿透自检 ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 

# 双核 Key 物理对齐
AMAP_KEY_WS = st.secrets.get("AMAP_KEY_WS", "").strip() 
AMAP_KEY_JS = st.secrets.get("AMAP_KEY_JS", "").strip() 
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "").strip()

def add_log(msg, level="INFO"):
    ts = datetime.now().strftime('%H:%M:%S')
    icon = "ℹ️" if level=="INFO" else "❌"
    entry = f"[{ts}] {icon} {msg}"
    if 'system_logs' in st.session_state:
        st.session_state['system_logs'].append(entry)

# --- 3. 核心底座函数 (坐标、测速与财务) ---

@st.cache_data(show_spinner=False)
def get_coords_v133(address):
    """【大脑 Key】地址转坐标"""
    if not address: return None, "地址为空"
    clean_addr = str(address).strip().replace(" ", "")
    full_addr = clean_addr if clean_addr.startswith("深圳市") else f"深圳市{clean_addr}"
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(full_addr)}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return (float(loc[0]), float(loc[1])), "SUCCESS"
        return None, f"解析失败: {r.get('info', '验证失败')}"
    except Exception as e:
        return None, f"异常: {str(e)}"

def get_travel_estimate_v133(origin, destination, mode_key):
    """【大脑 Key】算路引擎"""
    mode_url_map = {"Walking": "walking", "Riding": "bicycling", "Transfer": "integrated"}
    api_type = mode_url_map.get(mode_key, "bicycling")
    url = f"https://restapi.amap.com/v3/direction/{api_type}?origin={origin}&destination={destination}&key={AMAP_KEY_WS}"
    try:
        time.sleep(0.2) # 保护 QPS 不被熔断
        r = requests.get(url, timeout=10).json()
        if r['status'] == '1':
            path = r['route']['paths'][0] if api_type != 'integrated' else r['route']['transits'][0]
            return int(path.get('distance', 0)), int(path.get('duration', 0)) // 60, "SUCCESS"
        return 0, 0, f"算路报错: {r.get('info')}"
    except Exception as e:
        return 0, 0, f"网络波动: {str(e)}"

def calculate_billing_days_v133(row, start_range, end_range):
    """【159单绝对计费】"""
    try:
        if pd.isna(row['服务开始日期']) or pd.isna(row['服务结束日期']): return 0
        s_date = pd.to_datetime(row['服务开始日期']).date()
        e_date = pd.to_datetime(row['服务结束日期']).date()
        freq = int(float(str(row.get('投喂频率', 1)).strip() or 1))
        a_start, a_end = max(s_date, start_range), min(e_date, end_range)
        if a_start > a_end: return 0
        c = 0; curr = a_start
        while curr <= a_end:
            if (curr - s_date).days % freq == 0: c += 1
            curr += timedelta(days=1)
        return c
    except: return 0

def optimize_route_v133(df_sitter, mode_key, sitter_name, date_str):
    """【物理回写】解决 0 数据顽疾"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    total = len(df_sitter); v_coord = len(has_coords)
    add_log(f"👤 {sitter_name} ({date_str}): 订单池 {total}，有效坐标 {v_coord}")
    if v_coord <= 1:
        res = pd.concat([has_coords, no_coords])
        res['拟定顺序'] = range(1, len(res) + 1)
        st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": 0, "dur": 0}
        return res
    unvisited = has_coords.to_dict('records')
    curr_node = unvisited.pop(0); optimized = [curr_node]
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_node['lng']-x['lng'])**2 + (curr_node['lat']-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node); curr_node = next_node
    t_d, t_t = 0, 0
    for i in range(len(optimized) - 1):
        orig, dest = f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}"
        dist, dur, status = get_travel_estimate_v133(orig, dest, mode_key)
        if status != "SUCCESS": add_log(f"🚩 {sitter_name} {status}", level="ERROR")
        optimized[i]['next_dist'], optimized[i]['next_dur'] = dist, dur
        t_d += dist; t_t += dur
    st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": t_d, "dur": t_t}
    return pd.concat([pd.DataFrame(optimized), no_coords])

def execute_smart_dispatch_spatial_v133(df, active_sitters):
    """【找回 V99 空间调度逻辑】同楼不拆单"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    s_load = {s: 0 for s in active_sitters}
    def get_building(addr):
        if not addr: return "未知"
        a = str(addr).replace("深圳市", "").replace("广东", "").replace(" ","")
        match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', a)
        return match.group(1) if match else a
    df['building_fp'] = df['详细地址'].apply(get_building)
    unassigned = ~df['喂猫师'].isin(active_sitters)
    if unassigned.any() and active_sitters:
        groups = df[unassigned].groupby('building_fp')
        for _, group in groups:
            best = min(s_load, key=s_load.get)
            df.loc[group.index, '喂猫师'] = best
            s_load[best] += len(group)
    return df

# --- 4. 飞书服务与 UI 布局 ---

def fetch_feishu_v133():
    try:
        r_auth = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        token = r_auth.json().get("tenant_access_token")
        if not token: return pd.DataFrame()
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"page_size": 500}, timeout=15).json()
        items = r.get("data", {}).get("items", [])
        if not items: return pd.DataFrame()
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        df['订单状态'] = df.get('订单状态', '进行中').fillna('进行中')
        df['投喂频率'] = pd.to_numeric(df.get('投喂频率'), errors='coerce').fillna(1).replace(0, 1)
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        for col in ['宠物名字', '详细地址', '喂猫师', 'lng', 'lat']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

st.set_page_config(page_title="指挥中心 V133.0", layout="wide")

def set_ui_v133():
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { width: 100% !important; height: 50px !important; font-size: 18px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000; border: 3.2px solid #000 !important; }
        .quick-nav div.stButton > button { width: 100% !important; height: 32px !important; font-size: 11px !important; border: 1.5px solid #000 !important; }
        .stTextArea textarea { font-size: 14px !important; background-color: #f8f9fa !important; border: 1.5px solid #ddd !important; }
        .commute-card { background-color: #000000 !important; border-left: 10px solid #00ff00 !important; padding: 22px !important; border-radius: 12px !important; color: #ffffff !important; margin-bottom: 20px !important; box-shadow: 0 10px 20px rgba(0,0,0,0.5); }
        .commute-card h4 { color: #ffcc00 !important; margin: 0 0 10px 0 !important; font-size: 18px !important; }
        .commute-card p { font-size: 22px !important; font-weight: 900 !important; margin: 5px 0 !important; color: #ffffff !important; }
        .debug-tower { background-color: #1a1a1a; border-left: 10px solid #ff4d4f; padding: 12px; border-radius: 8px; color: #ff4d4f; font-family: monospace; font-size: 13px; margin-bottom: 20px; }
        </style>
        """, unsafe_allow_html=True)

set_ui_v133()

if st.session_state['feishu_cache'] is None:
    st.session_state['feishu_cache'] = fetch_feishu_v133()

# --- 5. 侧边栏布局 ---

with st.sidebar:
    st.subheader("📅 洛阳数字化总调")
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
    d_sel = st.date_input("指战周期锁定", value=st.session_state['r'])
    st.divider()
    active_sitters = ["梦蕊", "依蕊"]
    active = [s for s in active_sitters if st.checkbox(f"{s} (执勤)", value=True, key=f"v133_{s}")]
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    for p in ["数据中心", "智能看板", "帮助文档"]:
        if st.button(p): st.session_state['page'] = p
    st.divider()
    with st.expander("🔑 权限校验"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 6. 数据中心 (整合对账与录单) ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数字化管理中枢 (对账与录单)")
    df_raw = st.session_state['feishu_cache'].copy() if st.session_state['feishu_cache'] is not None else pd.DataFrame()
    if not df_raw.empty:
        st.subheader("📝 财务计费对账 (159单绝对闭环)")
        if isinstance(d_sel, tuple) and len(d_sel) == 2:
            df_raw['计费天数'] = df_raw.apply(lambda r: calculate_billing_days_v133(r, d_sel[0], d_sel[1]), axis=1)
            st.metric("📊 周期内计费总单量", f"{df_raw['计费天数'].sum()} 次")
        st.dataframe(df_raw[['宠物名字', '计费天数', '喂猫师', '服务开始日期', '服务结束日期', '订单状态', '详细地址']], use_container_width=True)
    st.divider()
    if not df_raw.empty:
        st.subheader("⚙️ 飞书云端同步编辑器")
        edit_dc = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态']], 
                                 column_config={"喂猫师": st.column_config.SelectboxColumn("归属", options=active_sitters), "订单状态": st.column_config.SelectboxColumn("状态", options=["进行中", "已结束", "待处理"])}, 
                                 use_container_width=True)
        if st.button("🚀 提交同步并保存"):
            for i, row in edit_dc.iterrows():
                for f in ['订单状态', '喂猫师']:
                    token = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
                    requests.patch(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{df_raw.iloc[i]['_system_id']}", headers={"Authorization": f"Bearer {token}"}, json={"fields": {f: str(row[f])}})
            st.session_state['feishu_cache'] = None; st.rerun()
    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("Excel 批量快速录单"):
            up = st.file_uploader("名单上传", type=["xlsx"])
            if up and st.button("🚀 开始推送云端"):
                du = pd.read_excel(up); tk_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
                for i, (_, r) in enumerate(du.iterrows()):
                    f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')).strip(), "投喂频率": int(r.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk_a}"}, json={"fields": f})
                st.session_state['feishu_cache'] = None; st.rerun()
    with c2:
        with st.expander("手动精准录单 (✍️)"):
            with st.form("man_v133"):
                a = st.text_input("详细地址*"); n = st.text_input("猫咪名"); sd = st.date_input("开始日期"); ed = st.date_input("截止日期")
                if st.form_submit_button("💾 确认录单并保存"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    tk_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk_a}"}, json={"fields": f})
                    st.session_state['feishu_cache'] = None; st.rerun()

# --- 7. 智能看板 ---

elif st.session_state['page'] == "智能看板":
    st.title("🚀 数字化指挥大屏 (V133 旗舰双核版)")
    st.markdown('<div class="debug-tower">🗼 后台通讯塔 (Key 状态与全链路穿透)</div>', unsafe_allow_html=True)
    if st.session_state['system_logs']:
        for log in st.session_state['system_logs'][-12:]:
            st.write(f"`{log}`")
        if st.button("🧹 清空普查历史"): st.session_state['system_logs'] = []; st.rerun()
    else: st.info("📡 指挥链路通畅。")

    df_raw = st.session_state['feishu_cache'].copy() if st.session_state['feishu_cache'] is not None else pd.DataFrame()
    col_nav1, col_nav2 = st.columns([1, 3])
    with col_nav1:
        nav_mode = st.radio("🚲 出行模式", ["步行", "骑行/电动车", "地铁/公交"], index=1)
        mode_map = {"步行": "Walking", "骑行/电动车": "Riding", "地铁/公交": "Transfer"}
    c_btn1, c_btn3, c_spacer = st.columns([1, 1, 5])
    if c_btn1.button("▶️ 开始拟定方案"): 
        st.session_state['plan_state'] = "RUNNING"
        st.session_state['commute_stats'] = {} 
        add_log("📈 启动双核对账流程...")
    if c_btn3.button("⏹️ 重置大屏"): 
        st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        df_kb = df_raw[df_raw['订单状态'].isin(["进行中", "待处理"])] if not df_raw.empty else df_raw
        if not df_kb.empty:
            with st.status("🛸 穿透对账中...", expanded=True) as status:
                dk = execute_smart_dispatch_spatial_v133(df_kb, active)
                days = pd.date_range(d_sel[0], d_sel[1]).tolist()
                ap = []
                for idx, d in enumerate(days):
                    d_str = d.strftime('%Y-%m-%d'); ct = pd.Timestamp(d)
                    d_v = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        if not d_v.empty:
                            with ThreadPoolExecutor(max_workers=5) as ex:
                                results = list(ex.map(get_coords_v133, d_v['详细地址']))
                            coords_list = [r[0] for r in results]
                            for r in results: 
                                if r[1] != "SUCCESS": add_log(r[1], level="ERROR")
                            d_v[['lng', 'lat']] = pd.DataFrame([ [c[0], c[1]] if c else [None, None] for c in coords_list ], index=d_v.index, columns=['lng', 'lat'])
                            for s in active:
                                stks = d_v[d_v['喂猫师'] == s].copy()
                                if not stks.empty:
                                    res = optimize_route_v133(stks, mode_map[nav_mode], s, d_str)
                                    res['作业日期'] = d_str; ap.append(res)
                st.session_state['fp'] = pd.concat(ap) if ap else None
                status.update(label="✅ 普查完成！", state="complete")
                st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        c_v1, c_v2 = st.columns(2)
        vd = c_v1.selectbox("📅 作业日期", sorted(st.session_state['fp']['作业日期'].unique()))
        vs = c_v2.selectbox("👤 视角隔离", ["全部"] + sorted(active))
        day_all = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        v_data = day_all if vs == "全部" else day_all[day_all['喂猫师'] == vs]
        st.subheader(f"⏱️ {vs} 视角·指战实时指标")
        c_m1, c_m2 = st.columns(2)
        show_sitters = active if vs == "全部" else [vs]
        for i, s in enumerate(show_sitters):
            stats_key = f"{vd}_{s}"
            s_data = st.session_state['commute_stats'].get(stats_key, {"dist": 0, "dur": 0})
            t_count = len(day_all[day_all['喂猫师'] == s])
            card_html = f"""<div class="commute-card"><h4>👤 {s} 指标</h4><p>当日单量：{t_count} 单</p><p style="color: #00ff00 !important;">预估耗时：{int(s_data['dur'])} 分钟</p><p style="color: #ffffff !important;">总行程：{s_data['dist']/1000:.1f} km</p></div>"""
            [c_m1, c_m2][i % 2].markdown(card_html, unsafe_allow_html=True)
        
        st.text_area("📄 简报预览 (物理锚定版)：", f"📢 {vd} 简报 ({vs})\n" + "\n".join([f"{int(r['拟定顺序'])}. {r['宠物名字']}-{r['详细地址']} ➡️ ({int(r.get('next_dur', 0))}分)" for _,r in v_data.iterrows()]), height=200)
        
        map_clean = v_data.dropna(subset=['lng', 'lat']).copy()
        map_json = map_clean[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序']].to_dict('records')
        amap_html = f"""
        <div id="map_box" style="width:100%; height:600px; border:3.5px solid #000; border-radius:15px; background:#f0f0f0;">
            <div id="no_coord" style="padding:20px; display:none; color:#ff4d4f; font-weight:bold;">⚠️ 坐标解析成功率为 0%，请检查后台通讯塔。</div>
        </div>
        <script type="text/javascript">
            window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }};
        </script>
        <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_KEY_JS}&plugin=AMap.Walking,AMap.Riding,AMap.Transfer"></script>
        <script type="text/javascript">
            (function() {{
                const data = {json.dumps(map_json)};
                if (data.length === 0) {{ document.getElementById('no_coord').style.display='block'; return; }}
                const colors = {{"梦蕊": "#007BFF", "依蕊": "#FFA500"}};
                const map = new AMap.Map('map_box', {{ zoom: 14, center: [data[0].lng, data[0].lat] }});
                data.forEach(m => {{
                    new AMap.Marker({{ position: [m.lng, m.lat], map: map,
                        content: `<div style="width:28px;height:28px;background:${{colors[m.喂猫师] || '#666'}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:26px;font-size:12px;font-weight:bold;box-shadow:0 0 10px rgba(0,0,0,0.5);">${{m.拟定顺序}}</div>`
                    }}).setLabel({{ direction:'top', offset: new AMap.Pixel(0, -5), content: m.宠物名字 }});
                }});
                function drawChain(idx, sData, mode, map) {{
                    if (idx >= sData.length - 1) {{ setTimeout(()=>map.setFitView(), 500); return; }}
                    if (sData[idx].喂猫师 !== sData[idx+1].喂猫师) {{ drawChain(idx+1, sData, mode, map); return; }}
                    let router; const cfg = {{ map: map, hideMarkers: true, strokeColor: colors[sData[idx].喂猫师], strokeOpacity: 0.95, strokeWeight: 8 }};
                    const mKey = {{"步行": "Walking", "骑行/电动车": "Riding", "地铁/公交": "Transfer"}}["{nav_mode}"];
                    if (mKey === "Walking") router = new AMap.Walking(cfg);
                    else if (mKey === "Riding") router = new AMap.Riding(cfg);
                    else router = new AMap.Transfer({{ ...cfg, city: '深圳市' }});
                    router.search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], function() {{ setTimeout(() => drawChain(idx + 1, sData, mode, map), 450); }});
                }}
                if (data.length > 1) drawChain(0, data, "{nav_mode}", map); else map.setFitView();
            }})();
        </script>"""
        components.html(amap_html, height=620)
        st.dataframe(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址']], use_container_width=True)
