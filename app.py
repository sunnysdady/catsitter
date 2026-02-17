import streamlit as st

# ==========================================
# --- 【V145 入口状态锁：视角与排版物理锁定】 ---
# ==========================================
def init_session_state_v145():
    """彻底解决 KeyError 与 IndexError，保障洛阳总部指挥舱平稳运行"""
    td = datetime.now().date() if 'datetime' in globals() else None
    keys_defaults = {
        'system_logs': [],
        'commute_stats': {},
        'page': "智能看板",
        'plan_state': "IDLE", 
        'progress_val': 0.0,
        'feishu_cache': None,
        'r': (td, td + timedelta(days=1)) if td else (None, None),
        'viewport': "管理员视角",
        'departure_point': "深圳市龙华区 潜龙花园 4A 栋",
        'travel_mode': "Riding"
    }
    for k, v in keys_defaults.items():
        if k not in st.session_state: st.session_state[k] = v

# --- 1. 物理导入全量指战作战库 (拒绝静默缩减) ---
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

init_session_state_v145()

# --- 2. 核心配置与双 Key 穿透锁定 ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 

# 双核物理映射
AMAP_KEY_WS = st.secrets.get("AMAP_KEY_WS", "c26fc76dd582c32e4406552df8ba40ff").strip() 
AMAP_KEY_JS = st.secrets.get("AMAP_KEY_JS", "c67e780b4d72b313f825746f8b02d840").strip() 
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "f3bd8f946c9fdf05cb73e259b108e527").strip()

def add_log(msg, level="INFO"):
    """【指调通讯塔：黑匣子级日志记录】"""
    ts = datetime.now().strftime('%H:%M:%S')
    icon = "ℹ️" if level=="INFO" else "🚩"
    entry = f"[{ts}] {icon} {msg}"
    if 'system_logs' in st.session_state:
        st.session_state['system_logs'].append(entry)
    else:
        st.session_state['system_logs'] = [entry]

# --- 3. 核心底座逻辑 (坐标解析、自愈测速、财务核销) ---

def haversine_v145(lon1, lat1, lon2, lat2, mode):
    """【高精直线自愈】球面算法解决龙华短距 1 分钟顽疾"""
    R = 6371000 
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    dist = 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))
    real_dist = dist * 1.35 # 路网修正因子
    speed_map = {"Walking": 66, "Riding": 250, "Transfer": 333}
    return int(real_dist), math.ceil(real_dist / speed_map.get(mode, 200))

@st.cache_data(show_spinner=False)
def get_coords_v145(address):
    """【大脑 Key】地理编码"""
    if not address: return None, "空"
    clean_addr = str(address).strip().replace(" ", "")
    full_addr = clean_addr if clean_addr.startswith("深圳市") else f"深圳市{clean_addr}"
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(full_addr)}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return (float(loc[0]), float(loc[1])), "SUCCESS"
    except: pass
    return None, "解析失败"

def get_travel_estimate_v145(origin, destination, mode_key):
    """【大脑 Key】路网测速引擎"""
    mode_url_map = {"Walking": "walking", "Riding": "bicycling", "Transfer": "integrated"}
    api_type = mode_url_map.get(mode_key, "bicycling")
    url = f"https://restapi.amap.com/v3/direction/{api_type}?origin={origin}&destination={destination}&key={AMAP_KEY_WS}"
    try:
        time.sleep(0.2) 
        r = requests.get(url, timeout=10).json()
        if r['status'] == '1':
            path = r['route']['paths'][0] if api_type != 'integrated' else r['route']['transits'][0]
            return int(path.get('distance', 0)), math.ceil(int(path.get('duration', 0)) / 60), "SUCCESS"
    except: pass
    return 0, 0, "API_FAIL"

def get_normalized_address_v145(addr):
    """【全量复位 V99】精准地址指纹，确保同楼不拆单"""
    if not addr: return "未知"
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    addr = addr.replace("龙华区", "").replace("民治街道", "").replace("龙华街道", "")
    addr = addr.replace('一','1').replace('二','2').replace('三','3').replace('四','4').replace('五','5')
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def calculate_billing_v145(row, start_range, end_range):
    """【159单绝对财务逻辑】"""
    try:
        if pd.isna(row['服务开始日期']) or pd.isna(row['服务结束日期']): return 0
        s_date = pd.to_datetime(row['服务开始日期']).date()
        e_date = pd.to_datetime(row['服务结束日期']).date()
        freq = int(float(str(row.get('投喂频率', 1)).strip() or 1))
        a_start, a_end = max(s_date, start_range), min(e_date, end_range)
        if a_start > a_end: return 0
        count = 0; curr = a_start
        while curr <= a_end:
            if (curr - s_date).days % freq == 0: count += 1
            curr += timedelta(days=1)
        return count
    except: return 0

def optimize_route_v145(df_sitter, mode_key, sitter_name, date_str, start_addr):
    """【出征引擎】包含起点的测速闭环"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    if len(has_coords) == 0:
        st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": 0, "dur": 0}
        return df_sitter
    
    start_pt, _ = get_coords_v145(start_addr)
    unvisited = has_coords.to_dict('records')
    curr_lng, curr_lat = start_pt if start_pt else (unvisited[0]['lng'], unvisited[0]['lat'])
    
    optimized = []
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_lng-x['lng'])**2 + (curr_lat-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node)
        curr_lng, curr_lat = next_node['lng'], next_node['lat']
    
    t_d, t_t = 0, 0
    # 起点出征首段
    if start_pt:
        d0, t0, s0 = get_travel_estimate_v145(f"{start_pt[0]},{start_pt[1]}", f"{optimized[0]['lng']},{optimized[0]['lat']}", mode_key)
        if s0 != "SUCCESS": d0, t0 = haversine_v145(start_pt[0], start_pt[1], optimized[0]['lng'], optimized[0]['lat'], mode_key)
        optimized[0]['prev_dur'] = t0; t_d += d0; t_t += t0

    # 后续任务路段
    for i in range(len(optimized) - 1):
        d, t, s = get_travel_estimate_v145(f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}", mode_key)
        if s != "SUCCESS": d, t = haversine_v145(optimized[i]['lng'], optimized[i]['lat'], optimized[i+1]['lng'], optimized[i+1]['lat'], mode_key)
        optimized[i]['next_dist'], optimized[i]['next_dur'] = d, t
        t_d += d; t_t += t

    st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": t_d, "dur": t_t}
    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

# --- 4. 视觉 UI 旗舰锁：排版重构逻辑 ---

st.set_page_config(page_title="指挥中心 V145.0", layout="wide")

def set_ui_v145():
    st.markdown("""
        <style>
        /* 1. 侧边栏整体背景与文字加固 */
        [data-testid="stSidebar"] { background-color: #fcfcfc !important; border-right: 2px solid #eee; }
        
        /* 2. 航道导航大按钮 (100*50 规格突出) */
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { 
            width: 100% !important; height: 55px !important; 
            font-size: 20px !important; font-weight: 900 !important; 
            box-shadow: 4px 4px 0px #000; border: 3px solid #000 !important; 
            background-color: #fff !important; margin-bottom: 15px !important;
            transition: 0.2s all;
        }
        .main-nav div.stButton > button:hover { transform: translate(-2px, -2px); box-shadow: 6px 6px 0px #000; background-color: #f0f0f0 !important; }
        
        /* 3. 参数配置区微调 */
        .config-header { background: #000; color: #fff; padding: 5px 12px; border-radius: 4px; font-size: 14px; font-weight: bold; margin-bottom: 10px; }
        .quick-nav div.stButton > button { width: 100% !important; height: 38px !important; font-size: 13px !important; border: 1.5px solid #333 !important; }
        
        /* 4. 黑金态势卡片锁定 */
        .commute-card { background-color: #000000 !important; border-left: 12px solid #00ff00 !important; padding: 25px !important; border-radius: 12px !important; color: #ffffff !important; margin-bottom: 25px !important; box-shadow: 0 10px 25px rgba(0,0,0,0.6); }
        .commute-card h4 { color: #ffcc00 !important; margin: 0 0 10px 0 !important; font-size: 20px !important; letter-spacing: 1px; }
        .commute-card p { font-size: 26px !important; font-weight: 900 !important; margin: 5px 0 !important; line-height: 1.1; }
        
        /* 5. 黑匣子日志 */
        .debug-tower { background-color: #1a1a1a; border: 1px solid #444; padding: 12px; border-radius: 8px; color: #00ff00; font-family: 'Courier New', monospace; font-size: 12px; height: 280px; overflow-y: auto; }
        </style>
        """, unsafe_allow_html=True)

set_ui_v145()

# --- 5. 侧边栏重构布局 ---

with st.sidebar:
    # --- 模块 A：身份权限锁 (置顶) ---
    st.markdown('<div class="config-header">🔑 权限与视角准入</div>', unsafe_allow_html=True)
    st.session_state['viewport'] = st.selectbox("确定您的指战身份", ["管理员视角", "梦蕊视角", "依蕊视角"], index=0, label_visibility="collapsed")
    st.divider()

    # --- 模块 B：航道导航 (大按钮突出) ---
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    if st.button("🚀 智能指挥看板"): st.session_state['page'] = "智能看板"
    if st.button("📂 数字化数据中心"): st.session_state['page'] = "数据中心"
    if st.button("📖 指战操作手册"): st.session_state['page'] = "帮助文档"
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()

    # --- 模块 C：战术参数配置 (中心功能区) ---
    st.markdown('<div class="config-header">⚙️ 战术参数配置</div>', unsafe_allow_html=True)
    
    # 日期配置 (2x2 快捷网格)
    td = datetime.now().date()
    cq1, cq2 = st.columns(2)
    with cq1:
        if st.button("📍 今天"): st.session_state['r'] = (td, td + timedelta(days=1))
        if st.button("📍 本月"): st.session_state['r'] = (td.replace(day=1), td.replace(day=calendar.monthrange(td.year, td.month)[1]) + timedelta(days=1))
    with cq2:
        if st.button("📍 明天"): st.session_state['r'] = (td + timedelta(days=1), td + timedelta(days=2))
        if st.button("📍 本周"): st.session_state['r'] = (td - timedelta(days=td.weekday()), td + timedelta(days=(6-td.weekday())+1))
    st.session_state['r'] = st.date_input("指战分析区间", value=st.session_state['r'])

    # 出征配置
    st.markdown("**🚩 出征起点**")
    presets = ["深圳市龙华区 潜龙花园 4A 栋", "乐荟中心", "星河world 二期 c 栋", "手动输入..."]
    sel = st.selectbox("选择或输入", presets, index=0, label_visibility="collapsed")
    if sel == "手动输入...": st.session_state['departure_point'] = st.text_input("详情起点", value="深圳市龙华区")
    else: st.session_state['departure_point'] = sel
    
    # 机动模式
    st.markdown("**🚲 指战机动模式**")
    nav_mode = st.radio("选择出行方式", ["步行", "骑行", "地铁/公交"], index=1, label_visibility="collapsed")
    mode_map = {"步行": "Walking", "骑行": "Riding", "地铁/公交": "Transfer"}
    st.session_state['travel_mode'] = mode_map[nav_mode]
    st.divider()

    # --- 模块 D：在岗调度管理 ---
    st.markdown('<div class="config-header">👥 在岗调度管理</div>', unsafe_allow_html=True)
    sitters_list = ["梦蕊", "依蕊"]
    active = [s for s in sitters_list if st.checkbox(f"{s} (今日执勤)", value=True, key=f"v145_{s}")]
    
    # --- 模块 E：影子通讯塔 (折叠黑匣子) ---
    st.divider()
    with st.expander("📡 指调通讯塔 (影子监控)", expanded=False):
        log_content = "\n".join(st.session_state['system_logs'][-40:])
        st.markdown(f'<div class="debug-tower">{log_content}</div>', unsafe_allow_html=True)
        if st.button("🧹 复位日志系统"): st.session_state['system_logs'] = []; st.rerun()

# --- 6. 数据中心：全量复位与容错逻辑 ---

def fetch_feishu_v145():
    try:
        r_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        token = r_a.json().get("tenant_access_token")
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"page_size": 500}, timeout=15).json()
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in r.get("data", {}).get("items", [])])
        df['订单状态'] = df.get('订单状态', '进行中').fillna('进行中')
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        for col in ['宠物名字', '详细地址', '喂猫师', 'lng', 'lat']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

if st.session_state['feishu_cache'] is None: st.session_state['feishu_cache'] = fetch_feishu_v145()

if st.session_state['page'] == "数据中心":
    st.title("📂 数字化管理中枢 (财务对账)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        df_raw['计费天数'] = 0
        if isinstance(st.session_state['r'], tuple) and len(st.session_state['r']) >= 2:
            df_raw['计费天数'] = df_raw.apply(lambda r: calculate_billing_v145(r, st.session_state['r'][0], st.session_state['r'][1]), axis=1)
            st.metric("📊 周期内累计计费单量", f"{df_raw['计费天数'].sum()} 次")
        safe_cols = [c for c in ['宠物名字', '计费天数', '喂猫师', '订单状态', '详细地址'] if c in df_raw.columns]
        st.dataframe(df_raw[safe_cols], use_container_width=True)
    if st.button("🚀 强制同步云端数据"): st.session_state['feishu_cache'] = None; st.rerun()

# --- 7. 智能看板：三键状态机与视角对账 ---

elif st.session_state['page'] == "智能看板":
    st.title(f"🚀 {st.session_state['viewport']} (V145.0)")
    
    # 三键指挥控制台
    c_btn1, c_btn2, c_btn3, c_spacer = st.columns([1, 1, 1, 4])
    if c_btn1.button("▶️ 启动方案拟定"): st.session_state['plan_state'] = "RUNNING"
    if c_btn2.button("⏸️ 暂停普查"): st.session_state['plan_state'] = "PAUSED"
    if c_btn3.button("⏹️ 取消并清空"): 
        st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        # IndexError 终极防护锁
        if not isinstance(st.session_state['r'], tuple) or len(st.session_state['r']) < 2:
            st.warning("⚠️ 观测周期尚未选定，请在侧边栏点选【起始】与【结束】日期。")
            st.session_state['plan_state'] = "IDLE"; st.stop()

        df_kb = st.session_state['feishu_cache'].copy()
        if not df_kb.empty:
            prog_bar = st.progress(0.0, text="🛸 穿透数据时空中...")
            with st.status("🛸 正在执行出征全链路路网对账...", expanded=True) as status:
                # 复位 V99 空间算法逻辑 (同楼不拆单)
                if '喂猫师' not in df_kb.columns: df_kb['喂猫师'] = ""
                s_load = {s: 0 for s in active}
                df_kb['building_fp'] = df_kb['详细地址'].apply(get_normalized_address_v145)
                unassigned = ~df_kb['喂猫师'].isin(active)
                if unassigned.any() and active:
                    for _, group in df_kb[unassigned].groupby('building_fp'):
                        best = min(s_load, key=s_load.get); df_kb.loc[group.index, '喂猫师'] = best; s_load[best] += len(group)
                
                days = pd.date_range(st.session_state['r'][0], st.session_state['r'][1]).tolist()
                ap = []
                for idx, d in enumerate(days):
                    if st.session_state['plan_state'] == "PAUSED": break
                    prog_bar.progress((idx + 1) / len(days), text=f"🔄 穿透日期: {d.strftime('%Y-%m-%d')}")
                    ct = pd.Timestamp(d); d_v = df_kb[(df_kb['服务开始日期'] <= ct) & (df_kb['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        if not d_v.empty:
                            with ThreadPoolExecutor(max_workers=5) as ex:
                                results = list(ex.map(get_coords_v145, d_v['详细地址']))
                            d_v[['lng', 'lat']] = pd.DataFrame([ [c[0][0], c[0][1]] if c[0] else [None, None] for c in results ], index=d_v.index, columns=['lng', 'lat'])
                            for s in active:
                                stks = d_v[d_v['喂猫师'] == s].copy()
                                if not stks.empty:
                                    res = optimize_route_v145(stks, st.session_state['travel_mode'], s, d.strftime('%Y-%m-%d'), st.session_state['departure_point'])
                                    res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
                st.session_state['fp'] = pd.concat(ap) if ap else None
                status.update(label="✅ 普查完成！出征方案已就位。", state="complete")
                st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        vd = st.selectbox("📅 选择作业日期", sorted(st.session_state['fp']['作业日期'].unique()))
        day_all = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        vs = "全部" if "管理员" in st.session_state['viewport'] else ("梦蕊" if "梦蕊" in st.session_state['viewport'] else "依蕊")
        v_data = day_all if vs == "全部" else day_all[day_all['喂猫师'] == vs]
        
        # --- 黑金指标 (对账单) ---
        c_m1, c_m2 = st.columns(2); show_sitters = active if vs == "全部" else [vs]
        for i, s in enumerate(show_sitters):
            s_data = st.session_state['commute_stats'].get(f"{vd}_{s}", {"dist": 0, "dur": 0})
            card_html = f"""<div class="commute-card"><h4>👤 {s} 态势概览</h4><p style="color:#0f0;">单量：{len(day_all[day_all['喂猫师']==s])} 单</p><p>耗时：{int(s_data['dur'])} 分钟</p><p>路程：{s_data['dist']/1000:.2f} km</p></div>"""
            [c_m1, c_m2][i % 2].markdown(card_html, unsafe_allow_html=True)
        
        # --- 任务简报 (高精取整版) ---
        brief = [f"🚩 出征起点：{st.session_state['departure_point']}"]
        for _, r in v_data.iterrows():
            n_dur = pd.to_numeric(r.get('next_dur', 0), errors='coerce'); n_dist = pd.to_numeric(r.get('next_dist', 0), errors='coerce')
            p_dur = pd.to_numeric(r.get('prev_dur', 0), errors='coerce')
            line = f"{int(r.get('拟定顺序', 0))}. {r.get('宠物名字', '小猫')}-{r.get('详细地址','深圳')}"
            if r['拟定顺序'] == 1 and p_dur > 0: line += f" ⬅️ (起点出发 {int(p_dur)}分)"
            if n_dur > 0: line += f" ➡️ (下站约 {int(n_dist)}米, {int(n_dur)}分)"
            else: line += " 🏁 [终点站]"
            brief.append(line)
        st.text_area("📄 每一段路程对账明细:", "\n".join(brief), height=250)

        # --- 地图接力渲染 ---
        map_clean = v_data.dropna(subset=['lng', 'lat']).copy()
        if not map_clean.empty:
            map_json = map_clean[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序']].to_dict('records')
            amap_html = f"""
            <div id="map_box" style="width:100%; height:600px; border:3.5px solid #000; border-radius:15px; background:#f0f0f0;"></div>
            <script type="text/javascript"> window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }}; </script>
            <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_KEY_JS}&plugin=AMap.Walking,AMap.Riding"></script>
            <script type="text/javascript">
                (function() {{
                    const data = {json.dumps(map_json)}; const colors = {{"梦蕊": "#007BFF", "依蕊": "#FFA500"}};
                    const map = new AMap.Map('map_box', {{ zoom: 14, center: [data[0].lng, data[0].lat] }});
                    data.forEach(m => {{
                        new AMap.Marker({{ position: [m.lng, m.lat], map: map,
                            content: `<div style="width:28px;height:28px;background:${{colors[m.喂猫师]}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:26px;font-size:12px;font-weight:bold;">${{m.拟定顺序}}</div>`
                        }}).setLabel({{ direction:'top', offset: new AMap.Pixel(0, -5), content: m.宠物名字 }});
                    }});
                    function draw(idx, sData, map) {{
                        if (idx >= sData.length - 1) {{ setTimeout(()=>map.setFitView(), 500); return; }}
                        if (sData[idx].喂猫师 !== sData[idx+1].喂猫师) {{ draw(idx+1, sData, map); return; }}
                        let router;
                        if ("{st.session_state['travel_mode']}" === "Walking") router = new AMap.Walking({{ map: map, hideMarkers: true, strokeColor: colors[sData[idx].喂猫师], strokeWeight: 8 }});
                        else router = new AMap.Riding({{ map: map, hideMarkers: true, strokeColor: colors[sData[idx].喂猫师], strokeWeight: 8 }});
                        router.search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], function(s, r) {{
                            setTimeout(() => draw(idx + 1, sData, map), 450);
                        }});
                    }}
                    draw(0, data, map);
                }})();
            </script>"""
            components.html(amap_html, height=620)
