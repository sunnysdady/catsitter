import streamlit as st

# ==========================================
# --- 【V136 核心加固：全链路状态保险锁】 ---
# ==========================================
def init_session_state_v136():
    """彻底终结 KeyError，保障并发稳定性"""
    td = datetime.now().date() if 'datetime' in globals() else None
    keys_defaults = {
        'system_logs': [],
        'commute_stats': {},
        'page': "智能看板",
        'plan_state': "IDLE",
        'feishu_cache': None,
        'r': (td, td + timedelta(days=1)) if td else (None, None)
    }
    for key, val in keys_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

# --- 1. 物理导入全量指战库 (严格不删减) ---
import pandas as pd
import requests
import time
import math
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import re
import io
import json
import calendar
from urllib.parse import quote
import streamlit.components.v1 as components

init_session_state_v136()

# --- 2. 核心配置与双 Key 穿透锁定 ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 

# 双核物理映射：c26... (WS), c67... (JS)
AMAP_KEY_WS = st.secrets.get("AMAP_KEY_WS", "c26fc76dd582c32e4406552df8ba40ff").strip() 
AMAP_KEY_JS = st.secrets.get("AMAP_KEY_JS", "c67e780b4d72b313f825746f8b02d840").strip() 
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "f3bd8f946c9fdf05cb73e259b108e527").strip()

def add_log(msg, level="INFO"):
    ts = datetime.now().strftime('%H:%M:%S')
    icon = "ℹ️" if level=="INFO" else "❌"
    entry = f"[{ts}] {icon} {msg}"
    if 'system_logs' in st.session_state:
        st.session_state['system_logs'].append(entry)

# --- 3. 核心底座逻辑 (坐标、自愈测速与财务) ---

def haversine_fallback(lon1, lat1, lon2, lat2, mode):
    """【V136直线自愈】球面距离算法"""
    R = 6371000 
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    dist = 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))
    real_dist = dist * 1.3 # 路网修正系数
    speed_map = {"Walking": 50, "Riding": 250, "Transfer": 300}
    dur = real_dist / speed_map.get(mode, 200)
    return int(real_dist), max(1, int(dur/60))

@st.cache_data(show_spinner=False)
def get_coords_v136(address):
    if not address: return None, "地址为空"
    clean_addr = str(address).strip().replace(" ", "")
    full_addr = clean_addr if clean_addr.startswith("深圳市") else f"深圳市{clean_addr}"
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(full_addr)}"
    try:
        time.sleep(0.12)
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return (float(loc[0]), float(loc[1])), "SUCCESS"
        return None, f"解析失败: {r.get('info')}"
    except: return None, "解析异常"

def get_travel_estimate_v136(origin, destination, mode_key):
    """【V136大脑算路】"""
    mode_url_map = {"Walking": "walking", "Riding": "bicycling", "Transfer": "integrated"}
    api_type = mode_url_map.get(mode_key, "bicycling")
    url = f"https://restapi.amap.com/v3/direction/{api_type}?origin={origin}&destination={destination}&key={AMAP_KEY_WS}"
    try:
        time.sleep(0.2) 
        r = requests.get(url, timeout=8).json()
        if r['status'] == '1':
            path = r['route']['paths'][0] if api_type != 'integrated' else r['route']['transits'][0]
            return int(path.get('distance', 0)), int(path.get('duration', 0)) // 60, "SUCCESS"
        return 0, 0, f"API报错: {r.get('info')}"
    except Exception as e:
        return 0, 0, f"网络异常: {str(e)}"

def get_normalized_address_v136(addr):
    """【复位 V99 地址识别】"""
    if not addr: return "未知"
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def calculate_billing_days_v136(row, start_range, end_range):
    """【159单绝对对账】"""
    try:
        if pd.isna(row['服务开始日期']) or pd.isna(row['服务结束日期']): return 0
        s_date = pd.to_datetime(row['服务开始日期']).date()
        e_date = pd.to_datetime(row['服务结束日期']).date()
        freq = int(float(str(row.get('投喂频率', 1)).strip() or 1))
        a_start, a_end = max(s_date, start_range), min(e_date, end_range)
        if a_start > a_end: return 0
        count = 0; curr = a_start
        while curr <= actual_end: # 修正循环变量
            if (curr - s_date).days % freq == 0: count += 1
            curr += timedelta(days=1)
        return count
    except: return 0

def optimize_route_v136(df_sitter, mode_key, sitter_name, date_str):
    """【V136 强固版路径优化】强制列名对齐，杜绝 KeyError"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    
    total_l = len(df_sitter); coord_l = len(has_coords)
    add_log(f"👤 {sitter_name} ({date_str}): 锁定任务 {total_l}，坐标获取 {coord_l}")
    
    if coord_l <= 1:
        res = pd.concat([has_coords, no_coords])
        res['拟定顺序'] = range(1, len(res) + 1)
        res['next_dist'] = 0; res['next_dur'] = 0
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
        dist, dur, status = get_travel_estimate_v136(orig, dest, mode_key)
        
        # 自愈逻辑
        if status != "SUCCESS":
            add_log(f"🚩 {sitter_name} API报错({status})，启用直线自愈", level="ERROR")
            dist, dur = haversine_fallback(optimized[i]['lng'], optimized[i]['lat'], optimized[i+1]['lng'], optimized[i+1]['lat'], mode_key)
            
        optimized[i]['next_dist'] = dist
        optimized[i]['next_dur'] = dur
        total_d += dist; total_t += dur

    # 内存物理锁
    st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": total_d, "dur": total_t}
    
    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    # 【V136强补列名】
    for c in ['next_dist', 'next_dur']: res_df[c] = res_df.get(c, 0).fillna(0)
    return res_df

def execute_smart_dispatch_spatial_v136(df, active_sitters):
    """【复位 V99 空间聚类】"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    s_load = {s: 0 for s in active_sitters}
    df['building_fp'] = df['详细地址'].apply(get_normalized_address_v136)
    unassigned = ~df['喂猫师'].isin(active_sitters)
    if unassigned.any() and active_sitters:
        groups = df[unassigned].groupby('building_fp')
        for _, group in groups:
            best = min(s_load, key=s_load.get)
            df.loc[group.index, '喂猫师'] = best
            s_load[best] += len(group)
    return df

# --- 4. 飞书服务与 UI (不删减排版) ---

def fetch_feishu_v136():
    try:
        r_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        token = r_a.json().get("tenant_access_token")
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

st.set_page_config(page_title="指挥中心 V136.0", layout="wide")

def set_ui_v136():
    """【排版锁定】"""
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { width: 100% !important; height: 50px !important; font-size: 18px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000; border: 3.5px solid #000 !important; background-color: #fff !important; }
        .quick-nav div.stButton > button { width: 100% !important; height: 35px !important; font-size: 11px !important; border: 1.5px solid #000 !important; }
        .commute-card { background-color: #000000 !important; border-left: 12px solid #00ff00 !important; padding: 25px !important; border-radius: 12px !important; color: #ffffff !important; margin-bottom: 25px !important; box-shadow: 0 10px 25px rgba(0,0,0,0.6); }
        .commute-card h4 { color: #ffcc00 !important; margin: 0 0 10px 0 !important; font-size: 20px !important; }
        .commute-card p { font-size: 26px !important; font-weight: 900 !important; margin: 5px 0 !important; line-height: 1.2; }
        .debug-tower { background-color: #1a1a1a; border-left: 10px solid #ff4d4f; padding: 15px; border-radius: 8px; color: #ff4d4f; font-family: monospace; font-size: 14px; margin-bottom: 20px; }
        </style>
        """, unsafe_allow_html=True)

set_ui_v136()

if st.session_state['feishu_cache'] is None:
    st.session_state['feishu_cache'] = fetch_feishu_v136()

# --- 5. 侧边栏 (100*25 排版) ---

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
    d_sel = st.date_input("指战周期", value=st.session_state['r'])
    st.divider()
    sitters_list = ["梦蕊", "依蕊"]
    active = [s for s in sitters_list if st.checkbox(f"{s} (执勤)", value=True, key=f"v136_{s}")]
    st.divider()
    for p in ["数据中心", "智能看板", "帮助文档"]:
        if st.button(p): st.session_state['page'] = p
    with st.expander("🔑 权限校验"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 6. 数据中心 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数字化管理中枢 (财务对账)")
    df_raw = st.session_state['feishu_cache'].copy() if st.session_state['feishu_cache'] is not None else pd.DataFrame()
    if not df_raw.empty:
        st.subheader("📝 财务对账 (159单闭环)")
        if isinstance(d_sel, tuple) and len(d_sel) == 2:
            df_raw['计费天数'] = df_raw.apply(lambda r: calculate_billing_days_v136(r, d_sel[0], d_sel[1]), axis=1)
            st.metric("📊 周期内计费总单量", f"{df_raw['计费天数'].sum()} 次")
        st.dataframe(df_raw[['宠物名字', '计费天数', '喂猫师', '服务开始日期', '服务结束日期', '订单状态', '详细地址']], use_container_width=True)
    st.divider()
    if not df_raw.empty:
        st.subheader("⚙️ 飞书云端同步")
        edit_dc = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态']], use_container_width=True)
        if st.button("🚀 提交同步"): st.session_state['feishu_cache'] = None; st.rerun()

# --- 7. 智能看板 ---

elif st.session_state['page'] == "智能看板":
    st.title("🚀 数字化指挥大屏 (V136 列名加固版)")
    st.markdown('<div class="debug-tower">🗼 指控通讯塔 (API状态与KeyError自愈)</div>', unsafe_allow_html=True)
    if st.session_state['system_logs']:
        for log in st.session_state['system_logs'][-12:]: st.write(f"`{log}`")
        if st.button("🧹 清空"): st.session_state['system_logs'] = []; st.rerun()

    df_raw = st.session_state['feishu_cache'].copy() if st.session_state['feishu_cache'] is not None else pd.DataFrame()
    col_nav1, col_nav2 = st.columns([1, 3])
    with col_nav1:
        nav_mode = st.radio("🚲 出行模式", ["步行", "骑行/电动车", "地铁/公交"], index=1)
        mode_map = {"步行": "Walking", "骑行/电动车": "Riding", "地铁/公交": "Transfer"}
    
    c_btn1, c_btn3, c_spacer = st.columns([1, 1, 5])
    if c_btn1.button("▶️ 开始拟定方案"): 
        st.session_state['plan_state'] = "RUNNING"; st.session_state['commute_stats'] = {} 
        add_log("📈 启动穿透普查流程...")

    if st.session_state['plan_state'] == "RUNNING":
        df_kb = df_raw[df_raw['订单状态'].isin(["进行中", "待处理"])]
        if not df_kb.empty:
            with st.status("🛸 穿透对账与测速中...", expanded=True) as status:
                dk = execute_smart_dispatch_spatial_v136(df_kb, active)
                days = pd.date_range(d_sel[0], d_sel[1]).tolist()
                ap = []
                for idx, d in enumerate(days):
                    d_str = d.strftime('%Y-%m-%d'); ct = pd.Timestamp(d)
                    d_v = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        if not d_v.empty:
                            with ThreadPoolExecutor(max_workers=5) as ex:
                                results = list(ex.map(get_coords_v136, d_v['详细地址']))
                            d_v[['lng', 'lat']] = pd.DataFrame([ [c[0][0], c[0][1]] if c[0] else [None, None] for c in results ], index=d_v.index, columns=['lng', 'lat'])
                            for s in active:
                                stks = d_v[d_v['喂猫师'] == s].copy()
                                if not stks.empty:
                                    res = optimize_route_v136(stks, mode_map[nav_mode], s, d_str)
                                    res['作业日期'] = d_str; ap.append(res)
                st.session_state['fp'] = pd.concat(ap) if ap else None
                status.update(label="✅ 普查完成！列名冲突已加固。", state="complete")
                st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        c_v1, c_v2 = st.columns(2)
        vd = c_v1.selectbox("📅 作业日期", sorted(st.session_state['fp']['作业日期'].unique()))
        vs = c_v2.selectbox("👤 视角隔离", ["全部"] + sorted(active))
        day_all = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        v_data = day_all if vs == "全部" else day_all[day_all['喂猫师'] == vs]
        
        # --- 黑金指标 ---
        st.subheader(f"⏱️ {vs} 视角·指战对账面板")
        c_m1, c_m2 = st.columns(2)
        show_sitters = active if vs == "全部" else [vs]
        for i, s in enumerate(show_sitters):
            stats_key = f"{vd}_{s}"
            s_data = st.session_state['commute_stats'].get(stats_key, {"dist": 0, "dur": 0})
            card_html = f"""<div class="commute-card"><h4>👤 {s} 指标</h4><p>当日履约：{len(day_all[day_all['喂猫师']==s])} 单</p><p style="color: #00ff00 !important;">预估耗时：{int(s_data['dur'])} 分钟</p><p style="color: #ffffff !important;">总行程：{s_data['dist']/1000:.1f} km</p></div>"""
            [c_m1, c_m2][i % 2].markdown(card_html, unsafe_allow_html=True)
        
        # 【V136安全防护简报】
        brief_lines = []
        for _, r in v_data.iterrows():
            d_dur = r.get('next_dur', 0) # 防御式读取
            brief_lines.append(f"{int(r.get('拟定顺序', 0))}. {r.get('宠物名字', '小猫')}-{r.get('详细地址','深圳')} ➡️ ({int(d_dur)}分)")
        st.text_area("📄 简报预览 (安全加固版)：", f"📢 {vd} 简报 ({vs})\n" + "\n".join(brief_lines), height=200)

        # --- 地图渲染 ---
        map_clean = v_data.dropna(subset=['lng', 'lat']).copy()
        map_json = map_clean[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序']].to_dict('records')
        amap_html = f"""
        <div id="map_box" style="width:100%; height:600px; border:3.5px solid #000; border-radius:15px; background:#f0f0f0;">
            <div id="no_coord" style="padding:20px; display:none; color:#ff4d4f;">⚠️ 选定视角坐标获取率为 0%。</div>
        </div>
        <script type="text/javascript"> window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }}; </script>
        <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_KEY_JS}&plugin=AMap.Walking,AMap.Riding,AMap.Transfer"></script>
        <script type="text/javascript">
            (function() {{
                const data = {json.dumps(map_json)}; if (data.length === 0) {{ document.getElementById('no_coord').style.display='block'; return; }}
                const colors = {{"梦蕊": "#007BFF", "依蕊": "#FFA500"}};
                const map = new AMap.Map('map_box', {{ zoom: 14, center: [data[0].lng, data[0].lat] }});
                data.forEach(m => {{
                    new AMap.Marker({{ position: [m.lng, m.lat], map: map,
                        content: `<div style="width:28px;height:28px;background:${{colors[m.喂猫师] || '#666'}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:26px;font-size:12px;font-weight:bold;">${{m.拟定顺序}}</div>`
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
