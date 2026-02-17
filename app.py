import streamlit as st
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

# 【V126双核 Key 物理映射】
AMAP_KEY_WS = st.secrets.get("AMAP_KEY_WS", "").strip() 
AMAP_KEY_JS = st.secrets.get("AMAP_KEY_JS", "").strip() 
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "").strip()

# 初始化全局持久日志与保险箱
if 'system_logs' not in st.session_state: st.session_state['system_logs'] = []
if 'commute_stats' not in st.session_state: st.session_state['commute_stats'] = {}

# --- 2. 核心底座函数 (坐标转换、诊断测速与对账) ---

def add_log(msg):
    """【V126 新增】通讯塔增强型日志记录"""
    ts = datetime.now().strftime('%H:%M:%S')
    st.session_state['system_logs'].append(f"[{ts}] {msg}")

@st.cache_data(show_spinner=False)
def get_coords_v126(address):
    """地址转经纬度，带普查标记"""
    if not address: return None, None
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address=深圳市{address}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return float(loc[0]), float(loc[1])
    except: pass
    return None, None

def get_travel_estimate_v126(origin, destination, mode_key):
    """带频率保护的算路引擎"""
    mode_url_map = {"Walking": "walking", "Riding": "bicycling", "Transfer": "integrated"}
    api_type = mode_url_map.get(mode_key, "bicycling")
    url = f"https://restapi.amap.com/v3/direction/{api_type}?origin={origin}&destination={destination}&key={AMAP_KEY_WS}"
    try:
        time.sleep(0.2) # 规避 QPS 熔断
        r = requests.get(url, timeout=10).json()
        if r['status'] == '1':
            path = r['route']['paths'][0] if api_type != 'integrated' else r['route']['transits'][0]
            return int(path.get('distance', 0)), int(path.get('duration', 0)) // 60, "SUCCESS"
        return 0, 0, f"API返回错误: {r.get('info')}"
    except Exception as e:
        return 0, 0, f"网络波动: {str(e)}"

def optimize_route_v126(df_sitter, mode_key, sitter_name, date_str):
    """【V126 物理锚定】路径优化并强制回填普查数据"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    
    add_log(f"👤 {sitter_name} ({date_str}): 待处理 {len(has_coords)} 条有坐标订单")
    
    if len(has_coords) <= 1:
        res = pd.concat([has_coords, no_coords])
        res['拟定顺序'] = range(1, len(res) + 1)
        res['next_dist'], res['next_dur'] = 0, 0
        st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": 0, "dur": 0}
        return res
    
    unvisited = has_coords.to_dict('records')
    curr_node = unvisited.pop(0); optimized = [curr_node]
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_node['lng']-x['lng'])**2 + (curr_node['lat']-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node); curr_node = next_node
    
    total_d, total_t = 0, 0
    for i in range(len(optimized) - 1):
        orig, dest = f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}"
        dist, dur, status = get_travel_estimate_v126(orig, dest, mode_key)
        if status != "SUCCESS": add_log(f"🚩 测速故障: {sitter_name} {status}")
        optimized[i]['next_dist'], optimized[i]['next_dur'] = dist, dur
        total_d += dist; total_t += dur

    # 强制物理锚定保险箱
    st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": total_d, "dur": total_t}
    add_log(f"✅ {sitter_name} 测速完成: {total_d/1000:.1f}km, {total_t}分钟")

    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    for c in ['next_dist', 'next_dur']: res_df[c] = res_df.get(c, 0).fillna(0)
    return res_df

def execute_smart_dispatch_spatial_v126(df, active_sitters):
    """【找回 V99 空间聚类】"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    sitter_load = {s: 0 for s in active_sitters}
    for s in df['喂猫师']:
        if s in sitter_load: sitter_load[s] += 1
    
    def get_building(addr):
        if not addr: return "未知"
        addr = str(addr).replace("深圳市", "").replace("广东", "").replace(" ","")
        match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
        return match.group(1) if match else addr
        
    df['building_fp'] = df['详细地址'].apply(get_building)
    unassigned = ~df['喂猫师'].isin(active_sitters)
    if unassigned.any() and active_sitters:
        groups = df[unassigned].groupby('building_fp')
        for _, group in groups:
            best = min(sitter_load, key=sitter_load.get)
            df.loc[group.index, '喂猫师'] = best
            sitter_load[best] += len(group)
    return df

# --- 3. 飞书 API 服务 (无损版) ---

def get_feishu_token_v126():
    try:
        r = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("tenant_access_token")
    except: return None

def fetch_feishu_data_v126():
    token = get_feishu_token_v126()
    if not token: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"page_size": 500}, timeout=15).json()
        items = r.get("data", {}).get("items", [])
        if not items: return pd.DataFrame()
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        df['订单状态'] = df.get('订单状态', '进行中').fillna('进行中')
        df['投喂频率'] = pd.to_numeric(df.get('投喂频率'), errors='coerce').fillna(1).replace(0, 1)
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: 
                df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        for col in ['宠物名字', '详细地址', '喂猫师', 'lng', 'lat']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_field_v126(record_id, field_name, value):
    token = get_feishu_token_v126()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{str(record_id).strip()}"
    try:
        r = requests.patch(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json={"fields": {field_name: str(value)}}, timeout=10)
        return r.status_code == 200
    except: return False

# --- 4. 视觉方案锁 (不删减) ---

st.set_page_config(page_title="指挥中心 V126.0", layout="wide")

def set_ui_v126():
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { width: 100% !important; height: 55px !important; font-size: 19px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000; border: 3px solid #000 !important; }
        .quick-nav div.stButton > button { width: 100% !important; height: 35px !important; font-size: 11px !important; border: 1.5px solid #000 !important; }
        .stTextArea textarea { font-size: 15px !important; background-color: #eeeeee !important; color: #000000 !important; border: 2px solid #000 !important; font-weight: 500; }
        .commute-card { background-color: #000000 !important; border-left: 12px solid #00ff00 !important; padding: 25px !important; border-radius: 15px !important; color: #ffffff !important; margin-bottom: 25px !important; box-shadow: 0 10px 20px rgba(0,0,0,0.6); }
        .commute-card h4 { color: #ffcc00 !important; margin: 0 0 10px 0 !important; font-size: 22px !important; }
        .commute-card p { font-size: 26px !important; font-weight: 900 !important; margin: 5px 0 !important; color: #ffffff !important; }
        .debug-tower { background-color: #1a1a1a; border-left: 10px solid #ff4d4f; padding: 15px; border-radius: 8px; color: #ff4d4f; font-family: 'Courier New', monospace; font-size: 14px; margin-bottom: 20px; }
        </style>
        """, unsafe_allow_html=True)

set_ui_v126()

# --- 5. 侧边栏布局 ---

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data_v126()
if 'plan_state' not in st.session_state: st.session_state['plan_state'] = "IDLE"

with st.sidebar:
    st.subheader("📅 指挥中枢")
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
    
    d_sel = st.date_input("指战周期", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    st.divider()
    sitters_list = ["梦蕊", "依蕊"]
    active = [s for s in sitters_list if st.checkbox(f"{s} (在岗)", value=True, key=f"v126_{s}")]
    
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    for p in ["数据中心", "智能看板", "帮助文档"]:
        if st.button(p): st.session_state['page'] = p
    st.divider()
    with st.expander("🔑 权限授权"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 6. 数据中心 (整合版) ---

if st.session_state['page'] == "数据中心":
    st.title("📂 洛阳管理中枢 (对账+录单)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        st.subheader("📝 财务级计费核销 (159单闭环)")
        if isinstance(d_sel, tuple) and len(d_sel) == 2:
            def calc_days(row):
                try:
                    s_date = pd.to_datetime(row['服务开始日期']).date()
                    e_date = pd.to_datetime(row['服务结束日期']).date()
                    freq = int(float(str(row.get('投喂频率', 1)).strip() or 1))
                    a_start, a_end = max(s_date, d_sel[0]), min(e_date, d_sel[1])
                    if a_start > a_end: return 0
                    c = 0; curr = a_start
                    while curr <= a_end:
                        if (curr - s_date).days % freq == 0: c += 1
                        curr += timedelta(days=1)
                    return c
                except: return 0
            df_raw['计费天数'] = df_raw.apply(calc_days, axis=1)
            st.metric("📊 周期内计费总单量", f"{df_raw['计费天数'].sum()} 次")
        st.dataframe(df_raw[['宠物名字', '计费天数', '喂猫师', '服务开始日期', '服务结束日期', '订单状态', '详细地址']], use_container_width=True)

    st.divider()
    if not df_raw.empty:
        st.subheader("⚙️ 飞书同步维护")
        edit_dc = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态']], 
                                 column_config={"喂猫师": st.column_config.SelectboxColumn("归属", options=sitters_list), "订单状态": st.column_config.SelectboxColumn("状态", options=["进行中", "已结束", "待处理"])}, 
                                 use_container_width=True)
        if st.button("🚀 确认并同步飞书"):
            for i, row in edit_dc.iterrows():
                for f in ['订单状态', '喂猫师']:
                    if row[f] != df_raw.iloc[i][f]: update_feishu_field_v126(df_raw.iloc[i]['_system_id'], f, row[f])
            st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("Excel 批量导入"):
            up = st.file_uploader("名单上传", type=["xlsx"])
            if up and st.button("🚀 推送云端"):
                du = pd.read_excel(up); tk = get_feishu_token_v126()
                for i, (_, r) in enumerate(du.iterrows()):
                    f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')).strip(), "投喂频率": int(r.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk}"}, json={"fields": f})
                st.session_state.pop('feishu_cache', None); st.rerun()
    with c2:
        with st.expander("手动单条开单 (✍️)"):
            with st.form("man_v126"):
                a = st.text_input("详细地址*"); n = st.text_input("猫咪名字"); sd = st.date_input("开始日期"); ed = st.date_input("截止日期")
                if st.form_submit_button("💾 确认并保存"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token_v126()}"}, json={"fields": f})
                    st.session_state.pop('feishu_cache', None); st.rerun()

# --- 7. 智能看板 (全链路穿透版) ---
elif st.session_state['page'] == "智能看板":
    st.title("🚀 数字化指挥大屏 (V126 穿透诊断版)")
    
    # 【核心新增】穿透普查通讯塔
    st.markdown('<div class="debug-tower">🗼 指控通讯塔 (全链路普查模式)</div>', unsafe_allow_html=True)
    if st.session_state['system_logs']:
        for log in st.session_state['system_logs'][-15:]:
            st.write(f"`{log}`")
        if st.button("🧹 清空普查历史"): st.session_state['system_logs'] = []; st.rerun()
    else:
        st.info("📡 链路通畅。点击“开始拟定”执行全链路对账。")

    df_raw = st.session_state['feishu_cache'].copy()
    col_nav1, col_nav2 = st.columns([1, 3])
    with col_nav1:
        nav_mode = st.radio("🚲 出行模式", ["步行", "骑行/电动车", "地铁/公交"], index=1)
        mode_map = {"步行": "Walking", "骑行/电动车": "Riding", "地铁/公交": "Transfer"}
    
    c_btn1, c_btn3, c_spacer = st.columns([1, 1, 5])
    if c_btn1.button("▶️ 开始拟定指战方案"): 
        st.session_state['plan_state'] = "RUNNING"
        st.session_state['commute_stats'] = {} 
        add_log(f"📈 启动普查: 原始池共 {len(df_raw)} 条记录")

    if c_btn3.button("⏹️ 重置大屏"): 
        st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        df_kb = df_raw[df_raw['订单状态'].isin(["进行中", "待处理"])] if not df_raw.empty else df_raw
        if not df_kb.empty:
            with st.status("🛸 正在执行穿透对账...", expanded=True) as status:
                dk = execute_smart_dispatch_spatial_v126(df_kb, active)
                days = pd.date_range(d_sel[0], d_sel[1]).tolist()
                ap = []
                for idx, d in enumerate(days):
                    d_str = d.strftime('%Y-%m-%d')
                    status.update(label=f"🔄 对账日期: {d_str}", state="running")
                    ct = pd.Timestamp(d)
                    
                    # 强力日期穿透过滤
                    d_v = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                    if d_v.empty:
                        add_log(f"🗓️ {d_str}: 订单服务日期未覆盖当前日期")
                    else:
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        if d_v.empty:
                            add_log(f"🗓️ {d_str}: 投喂频率判定今日无任务")
                        else:
                            add_log(f"🗓️ {d_str}: 锁定待执行任务 {len(d_v)} 条")
                            with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords_v126, d_v['详细地址']))
                            d_v[['lng', 'lat']] = pd.DataFrame(coords, index=d_v.index, columns=['lng', 'lat'])
                            for s in active:
                                stks = d_v[d_v['喂猫师'] == s].copy()
                                if not stks.empty:
                                    res = optimize_route_v126(stks, mode_map[nav_mode], s, d_str)
                                    res['作业日期'] = d_str; ap.append(res)
                                else:
                                    add_log(f"👤 {d_str} {s}: 暂无分配任务")
                st.session_state['fp'] = pd.concat(ap) if ap else None
                status.update(label="✅ 普查完成！", state="complete")
                st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        c_v1, c_v2 = st.columns(2)
        vd = c_v1.selectbox("📅 作业日期选择", sorted(st.session_state['fp']['作业日期'].unique()))
        vs = c_v2.selectbox("👤 视角隔离", ["全部"] + sorted(active))
        
        day_all = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        v_data = day_all if vs == "全部" else day_all[day_all['喂猫师'] == vs]
        
        # --- 黑金态势卡片 (穿透对账显示) ---
        st.subheader(f"⏱️ {vs} 视角·指战实时态势")
        c_m1, c_m2 = st.columns(2)
        show_sitters = active if vs == "全部" else [vs]
        for i, s in enumerate(show_sitters):
            stats_key = f"{vd}_{s}"
            s_data = st.session_state['commute_stats'].get(stats_key, {"dist": 0, "dur": 0})
            t_count = len(day_all[day_all['喂猫师'] == s])
            card_html = f"""<div class="commute-card"><h4>👤 {s} 态势对账</h4><p>当日履约：{t_count} 单</p><p style="color: #00ff00 !important;">预估耗时：{int(s_data['dur'])} 分钟</p><p style="color: #00d4ff !important;">行程距离：{s_data['dist']/1000:.1f} km</p></div>"""
            [c_m1, c_m2][i % 2].markdown(card_html, unsafe_allow_html=True)
        
        st.text_area("📄 每一段路程耗时普查指引：", f"📢 {vd} 任务简报 ({vs})\n" + "\n".join([f"{int(r['拟定顺序'])}. {r['宠物名字']}-{r['详细地址']} ➡️ (约 {int(r['next_dist'])}米, {int(r['next_dur'])}分)" for _,r in v_data.iterrows()]), height=200)

        # --- 【V126 强心跳地图】确保 100% 显示容器 ---
        map_clean = v_data.dropna(subset=['lng', 'lat']).copy()
        map_clean['color'] = map_clean['喂猫师'].apply(lambda n: '#007BFF' if n == "梦蕊" else '#FFA500')
        map_json = map_clean[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序', 'color']].to_dict('records')
        
        amap_html = f"""
        <div id="map_box" style="width:100%; height:600px; border:3.5px solid #000; border-radius:15px; background:#f0f0f0;">
            <div id="no_data" style="padding:20px; display:none; color:#666;">ℹ️ 选定视角内暂无坐标点可供渲染。</div>
        </div>
        <script type="text/javascript">
            window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }};
        </script>
        <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_KEY_JS}&plugin=AMap.Walking,AMap.Riding,AMap.Transfer"></script>
        <script type="text/javascript">
            (function() {{
                const data = {json.dumps(map_json)};
                if (data.length === 0) {{ document.getElementById('no_data').style.display='block'; return; }}
                
                const map = new AMap.Map('map_box', {{ zoom: 15, center: [data[0].lng, data[0].lat] }});
                
                data.forEach(m => {{
                    new AMap.Marker({{
                        position: [m.lng, m.lat], map: map,
                        content: `<div style="width:28px;height:28px;background:${{m.color}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:26px;font-size:12px;font-weight:bold;box-shadow:0 0 10px rgba(0,0,0,0.5);">${{m.拟定顺序}}</div>`
                    }}).setLabel({{ direction:'top', offset: new AMap.Pixel(0, -5), content: m.宠物名字 }});
                }});

                function drawChain(idx, sData, mode, map) {{
                    if (idx >= sData.length - 1) {{ setTimeout(()=>map.setFitView(), 500); return; }}
                    if (sData[idx].喂猫师 !== sData[idx+1].喂猫师) {{ drawChain(idx+1, sData, mode, map); return; }}
                    let router;
                    const cfg = {{ map: map, hideMarkers: true, strokeColor: sData[idx].color, strokeOpacity: 0.95, strokeWeight: 8 }};
                    const mKey = {{"步行": "Walking", "骑行/电动车": "Riding", "地铁/公交": "Transfer"}}["{nav_mode}"];
                    if (mKey === "Walking") router = new AMap.Walking(cfg);
                    else if (mKey === "Riding") router = new AMap.Riding(cfg);
                    else router = new AMap.Transfer({{ ...cfg, city: '深圳市' }});
                    router.search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], function(s, r) {{
                        setTimeout(() => drawChain(idx + 1, sData, mode, map), 450);
                    }});
                }}
                if (data.length > 1) drawChain(0, data, "{nav_mode}", map); else map.setFitView();
            }})();
        </script>"""
        components.html(amap_html, height=620)
        st.dataframe(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址']], use_container_width=True)

elif st.session_state['page'] == "帮助文档":
    st.title("📖 V126 穿透诊断手册")
    st.markdown("""
    1. **全链路穿透**：顶部“通讯塔”现在会显示日期筛选、频率判定、坐标获取的每一个步骤结果。
    2. **地图强心跳**：修复了无坐标点时容器彻底不显示的 Bug。现在会显示空背景并明确提示“无坐标点”。
    3. **耗时对账**：通过物理内存保险箱直接提取公里数，解决 Pandas 索引造成的 0 数据。
    4. **算法回归**：完整保留 V99 空间聚类、159 单对账及录单全表单。
    """)
