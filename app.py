import streamlit as st

# ==========================================
# --- 【V158 状态与视觉锁：高对比度版】 ---
# ==========================================
def init_session_state_v158():
    """彻底解决信息看不清与功能缺失，保障洛阳总部全链路透明"""
    td = datetime.now().date() if 'datetime' in globals() else None
    defaults = {
        'system_logs': [],
        'commute_stats': {},
        'page': "智能派单看板",
        'plan_state': "IDLE", 
        'feishu_cache': None,
        'r': (td, td + timedelta(days=1)) if td else (None, None),
        'viewport': "管理员模式",
        'admin_sub_view': "全部人员",
        'departure_point': "深圳市龙华区 潜龙花园 4A 栋",
        'travel_mode': "Riding"
    }
    for k, v in defaults.items():
        if k not in st.session_state: st.session_state[k] = v

# --- 1. 物理导入全量指战作战库 (严禁静默缩减) ---
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

# 持久化通信
if 'http_session' not in st.session_state:
    st.session_state.http_session = requests.Session()

init_session_state_v158()

# --- 2. 配置与双 Key 穿透锁定 ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 

AMAP_KEY_WS = st.secrets.get("AMAP_KEY_WS", "c26fc76dd582c32e4406552df8ba40ff").strip() 
AMAP_KEY_JS = st.secrets.get("AMAP_KEY_JS", "c67e780b4d72b313f825746f8b02d840").strip() 
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "f3bd8f946c9fdf05cb73e259b108e527").strip()

def add_log(msg, level="INFO"):
    """【追踪级系统日志】上帝视角补全"""
    ts = datetime.now().strftime('%H:%M:%S')
    icon = "✓" if level=="INFO" else "🚩"
    st.session_state['system_logs'].append(f"[{ts}] {icon} {msg}")

# --- 3. 核心底座逻辑 (回滚稳健层 + 频率精准计算) ---

def haversine_v158(lon1, lat1, lon2, lat2, mode):
    R = 6371000 
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    dist = 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))
    real_dist = dist * 1.35
    speed_map = {"Walking": 66, "Riding": 250, "Transfer": 333}
    return int(real_dist), math.ceil(real_dist / speed_map.get(mode, 200))

@st.cache_data(show_spinner=False, ttl=3600)
def get_coords_v158(address):
    if not address: return None, "空"
    clean_addr = str(address).strip().replace(" ", "")
    full_addr = clean_addr if clean_addr.startswith("深圳市") else f"深圳市{clean_addr}"
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(full_addr)}"
    try:
        r = requests.get(url, timeout=10).json()
        if r.get('status') == '1' and r.get('geocodes'):
            loc = r['geocodes'][0]['location'].split(',')
            return (float(loc[0]), float(loc[1])), "SUCCESS"
    except: pass
    return None, "解析失败"

def get_travel_v158(origin, destination, mode_key):
    m_map = {"Walking": "walking", "Riding": "bicycling", "Transfer": "integrated"}
    api_type = m_map.get(mode_key, "bicycling")
    url = f"https://restapi.amap.com/v3/direction/{api_type}?origin={origin}&destination={destination}&key={AMAP_KEY_WS}"
    try:
        time.sleep(0.12)
        r = requests.get(url, timeout=10).json()
        if r.get('status') == '1':
            path = r['route']['paths'][0] if api_type != 'integrated' else r['route']['transits'][0]
            return int(path.get('distance', 0)), math.ceil(int(path.get('duration', 0)) / 60), "SUCCESS"
    except: pass
    return 0, 0, "ERR"

def get_normalized_v158(addr):
    if not addr: return "未知"
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    addr = addr.replace("龙华区", "").replace("民治街道", "").replace("龙华街道", "")
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def optimize_route_v158(df_sitter, mode_key, sitter_name, date_str, start_addr):
    """【出征引擎】物理回滚稳健版"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    if len(has_coords) == 0:
        st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": 0, "dur": 0}
        return df_sitter
    
    start_pt, _ = get_coords_v158(start_addr)
    unvisited = has_coords.to_dict('records')
    curr_lng, curr_lat = start_pt if start_pt else (unvisited[0]['lng'], unvisited[0]['lat'])
    
    optimized = []
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_lng-x['lng'])**2 + (curr_lat-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node)
        curr_lng, curr_lat = next_node['lng'], next_node['lat']
    
    t_d, t_t = 0, 0
    if start_pt:
        d0, t0, s0 = get_travel_v158(f"{start_pt[0]},{start_pt[1]}", f"{optimized[0]['lng']},{optimized[0]['lat']}", mode_key)
        if s0 != "SUCCESS": d0, t0 = haversine_v158(start_pt[0], start_pt[1], optimized[0]['lng'], optimized[0]['lat'], mode_key)
        optimized[0]['prev_dur'] = t0; t_d += d0; t_t += t0

    for i in range(len(optimized) - 1):
        d, t, s = get_travel_v158(f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}", mode_key)
        if s != "SUCCESS": d, t = haversine_v158(optimized[i]['lng'], optimized[i]['lat'], optimized[i+1]['lng'], optimized[i+1]['lat'], mode_key)
        optimized[i]['next_dist'], optimized[i]['next_dur'] = d, t
        t_d += d; t_t += t

    st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": t_d, "dur": t_t}
    add_log(f"✅ {sitter_name} 测算闭环: {t_d/1000:.2f}km")
    
    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    for c in ['next_dist', 'next_dur', 'prev_dur']: 
        res_df[c] = pd.to_numeric(res_df.get(c, 0), errors='coerce').fillna(0)
    return res_df

# --- 4. 【视觉重构】高对比度深色 UI 引擎 ---

st.set_page_config(page_title="小猫直喂派单旗舰平台", layout="wide", initial_sidebar_state="expanded")

def set_ui_v158():
    st.markdown("""
        <style>
        /* 侧边栏：深色简约风格锁 */
        [data-testid="stSidebar"] { background-color: #1e1e1e !important; border-right: 1px solid #333; }
        .sidebar-header-v158 { font-size: 0.85rem; font-weight: 800; color: #777; margin: 1.2rem 0 0.5rem 0; letter-spacing: 1.2px; }
        [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { color: #ffffff !important; }
        
        /* 灰色圆角矩阵块 */
        .v158-box [data-testid="stVerticalBlock"] div.stButton > button { 
            width: 100% !important; height: 50px !important; font-size: 15px !important; font-weight: 600 !important; 
            border-radius: 12px !important; border: 1px solid #3d3d3d !important;
            background-color: #2d2d2d !important; color: #ffffff !important; margin-bottom: 12px !important;
        }
        .v158-box div.stButton > button:hover { background-color: #444 !important; border-color: #007bff !important; }
        
        /* 统计卡片：高对比度重构 */
        .status-container { display: flex; gap: 15px; margin-bottom: 25px; }
        .card-base { flex: 1; padding: 18px; border-radius: 14px; text-align: center; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        .card-total { background-color: #2d2d2d; border: 1px solid #444; color: #fff; }
        .card-match { background-color: #004085; border: 1px solid #0056b3; color: #fff; }
        .card-map { background-color: #155724; border: 1px solid #1e7e34; color: #fff; }
        .card-val { font-size: 1.8rem; font-weight: 900; margin-bottom: 2px; }
        .card-lab { font-size: 0.85rem; font-weight: 600; opacity: 0.9; }

        /* 行程简报卡片 */
        .metric-v158 { background-color: #ffffff; border: 1px solid #eee; border-left: 8px solid #007bff; padding: 22px; border-radius: 14px; margin-bottom: 20px; box-shadow: 0 5px 15px rgba(0,0,0,0.05); }
        .metric-v158 h4 { color: #888; font-size: 14px; margin: 0 0 6px 0; }
        .metric-v158 p { font-size: 28px; font-weight: 900; color: #111; margin: 0; }
        
        /* 影子日志终端 */
        .terminal-v158 { background-color: #111; color: #00ff00; padding: 12px; border-radius: 10px; font-family: monospace; font-size: 11px; height: 260px; overflow-y: auto; border: 1px solid #333; }
        </style>
        """, unsafe_allow_html=True)

set_ui_v158()

# --- 5. 侧边栏布局：身份确定与功能导航 ---

with st.sidebar:
    st.markdown('<div class="sidebar-header-v158">👤 切换操作角色</div>', unsafe_allow_html=True)
    st.session_state['viewport'] = st.selectbox("Identity", ["管理员模式", "梦蕊模式", "依蕊模式"], label_visibility="collapsed")
    st.divider()

    st.markdown('<div class="sidebar-header-v158">🧭 平台功能导航</div>', unsafe_allow_html=True)
    st.markdown('<div class="v158-box">', unsafe_allow_html=True)
    if st.button("📊 派单对账大屏"): st.session_state['page'] = "智能派单看板"
    if st.button("📂 资料同步管理"): st.session_state['page'] = "订单录入管理"
    if st.button("📖 平台使用手册"): st.session_state['page'] = "手册指南"
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()

    st.markdown('<div class="sidebar-header-v158">⚙️ 核心派单参数</div>', unsafe_allow_html=True)
    td = datetime.now().date(); c1, c2 = st.columns(2)
    with c1:
        if st.button("今天"): st.session_state['r'] = (td, td + timedelta(days=1))
        if st.button("本月"): st.session_state['r'] = (td.replace(day=1), td.replace(day=calendar.monthrange(td.year, td.month)[1]) + timedelta(days=1))
    with c2:
        if st.button("明天"): st.session_state['r'] = (td + timedelta(days=1), td + timedelta(days=2))
        if st.button("本周"): st.session_state['r'] = (td - timedelta(days=td.weekday()), td + timedelta(days=(6-td.weekday())+1))
    st.session_state['r'] = st.date_input("日期范围", value=st.session_state['r'])

    st.markdown("**📍 出征起始点**")
    locs = ["深圳市龙华区 潜龙花园 4A 栋", "乐荟中心", "星河world 二期 c 栋", "自定义输入..."]
    sel_loc = st.selectbox("起点", locs, label_visibility="collapsed")
    if sel_loc == "自定义输入...": st.session_state['departure_point'] = st.text_input("详情起始地址", value="深圳市")
    else: st.session_state['departure_point'] = sel_loc
    st.divider()

    with st.expander("📡 系统影子日志塔", expanded=False):
        logs_txt = "\n".join(st.session_state['system_logs'][-50:])
        st.markdown(f'<div class="terminal-v158">{logs_txt}</div>', unsafe_allow_html=True)
        if st.button("复位历史记录"): st.session_state['system_logs'] = []; st.rerun()

# --- 6. 订单资料中心：满血回归对账 ---

def fetch_data_v158():
    try:
        r_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        tk = r_a.json().get("tenant_access_token")
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
        r = st.session_state.http_session.get(url, headers={"Authorization": f"Bearer {tk}"}, params={"page_size": 500}, timeout=15).json()
        df = pd.DataFrame([dict(i['fields'], _id=i['record_id']) for i in r.get("data", {}).get("items", [])])
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        for col in ['宠物名字', '详细地址', '喂猫师', '订单状态', '投喂频率']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

if st.session_state['feishu_cache'] is None: st.session_state['feishu_cache'] = fetch_data_v158()

if st.session_state['page'] == "订单录入管理":
    st.title("📂 资料同步与 159 对账中心")
    df = st.session_state['feishu_cache'].copy()
    if not df.empty:
        # 159对账核心
        df['累计单量'] = 0
        if isinstance(st.session_state['r'], tuple) and len(st.session_state['r']) >= 2:
            def calc(row):
                try:
                    s, e = pd.to_datetime(row['服务开始日期']).date(), pd.to_datetime(row['服务结束日期']).date()
                    freq, a_s, a_e = int(row.get('投喂频率', 1)), max(s, st.session_state['r'][0]), min(e, st.session_state['r'][1])
                    if a_s > a_e: return 0
                    return sum(1 for d in range((a_e-a_s).days + 1) if (a_s + timedelta(days=d) - s).days % freq == 0)
                except: return 0
            df['累计单量'] = df.apply(calc, axis=1)
            st.metric("分析周期内总派单数", f"{df['累计单量'].sum()} 次")
        
        st.subheader("⚙️ 飞书云端同步编辑")
        edit_df = st.data_editor(df[['宠物名字', '详细地址', '喂猫师', '订单状态', '投喂频率']], use_container_width=True)
        if st.button("🚀 强制同步至飞书"):
            tk = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
            for i, row in edit_df.iterrows():
                requests.patch(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{df.iloc[i]['_id']}", headers={"Authorization": f"Bearer {tk}"}, json={"fields": {"订单状态": str(row['订单状态']), "喂猫师": str(row['喂猫师']), "投喂频率": int(row['投喂频率'])}})
            st.session_state['feishu_cache'] = None; st.rerun()

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量：Excel 快速导入"):
            up = st.file_uploader("文件上传", type=["xlsx"])
            if up and st.button("推送云端名单"):
                du = pd.read_excel(up); tk_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
                for _, r in du.iterrows():
                    f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')), "投喂频率": int(r.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk_a}"}, json={"fields": f})
                st.session_state['feishu_cache'] = None; st.rerun()
    with c2:
        with st.expander("手动：单兵开单录入"):
            with st.form("man_v158"):
                a = st.text_input("详细地址*"); n = st.text_input("宠物名"); sd = st.date_input("起始"); ed = st.date_input("结束"); fq = st.number_input("频率", value=1)
                if st.form_submit_button("💾 保存资料"):
                    tk_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000), "投喂频率": int(fq), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk_a}"}, json={"fields": f})
                    st.session_state['feishu_cache'] = None; st.rerun()

# --- 7. 派单看板：视觉重构与一键复制版 ---

elif st.session_state['page'] == "智能派单看板":
    st.title(f"派单动态态势 · {st.session_state['viewport']}")
    
    # 【核心：统计卡片高对比度重构】
    df_raw = st.session_state['feishu_cache'].copy()
    m_c = 0; hit_c = 0
    if st.session_state.get('fp') is not None:
        m_c = len(st.session_state['fp']); hit_c = len(st.session_state['fp'].dropna(subset=['lng']))
    
    st.markdown(f"""
    <div class="status-container">
        <div class="card-base card-total"><div class="card-val">{len(df_raw)}</div><div class="card-lab">📊 全部客户总数</div></div>
        <div class="card-base card-match"><div class="card-val">{m_c}</div><div class="card-lab">🐱 今日待派单数</div></div>
        <div class="card-base card-map"><div class="card-val">{hit_c}</div><div class="card-lab">📍 地图已点亮数</div></div>
    </div>
    """, unsafe_allow_html=True)

    # 控制台
    c1, c2, c3, c4 = st.columns([1, 1, 1, 4])
    if c1.button("▶ 启动方案分析"): st.session_state['plan_state'] = "RUNNING"
    if c2.button("⏸ 暂停普查"): st.session_state['plan_state'] = "PAUSED"
    if c3.button("↺ 复位重置"): st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        if not isinstance(st.session_state['r'], tuple) or len(st.session_state['r']) < 2:
            st.warning("⚠️ 请点选完整的日期范围。"); st.session_state['plan_state'] = "IDLE"; st.stop()

        prog_bar = st.progress(0.0, text="同步高德路网轴中...")
        with st.status("正在回归执行 V144 同步测速引擎...", expanded=True) as status:
            # 复位 V99 空间聚类 (同楼不拆单)
            sitters = ["梦蕊", "依蕊"]
            df_raw['building_fp'] = df_raw['详细地址'].apply(get_normalized_v158)
            s_load = {s: 0 for s in sitters}
            unassigned = ~df_raw.get('喂猫师', '').isin(sitters)
            if unassigned.any():
                for _, g in df_raw[unassigned].groupby('building_fp'):
                    best = min(s_load, key=s_load.get); df_raw.loc[g.index, '喂猫师'] = best; s_load[best] += len(g)
            
            days = pd.date_range(st.session_state['r'][0], st.session_state['r'][1]).tolist()
            all_plans = []
            for idx, d in enumerate(days):
                if st.session_state['plan_state'] == "PAUSED": break
                prog_bar.progress((idx+1)/len(days), text=f"对账日期: {d.strftime('%Y-%m-%d')}")
                ct = pd.Timestamp(d); d_v = df_raw[(df_raw['服务开始日期'].dt.date <= ct.date()) & (df_raw['服务结束日期'].dt.date >= ct.date())].copy()
                if not d_v.empty:
                    # 上帝视角日志：频率计算追踪
                    def trace_freq(r):
                        diff = (ct.date() - r['服务开始日期'].date()).days
                        res = diff % int(r.get('投喂频率',1)) == 0
                        if res: add_log(f"[{r['宠物名字']}] 匹配成功 (间隔{diff}天，频率{r['投喂频率']})")
                        else: add_log(f"[{r['宠物名字']}] 跳过 (间隔{diff}天，频率{r['投喂频率']})")
                        return res
                    d_v = d_v[d_v.apply(trace_freq, axis=1)]
                    if not d_v.empty:
                        with ThreadPoolExecutor(max_workers=5) as ex:
                            results = list(ex.map(get_coords_v158, d_v['详细地址']))
                        d_v[['lng', 'lat']] = pd.DataFrame([ [c[0][0], c[0][1]] if c[0] else [None, None] for c in results ], index=d_v.index, columns=['lng', 'lat'])
                        for s in sitters:
                            stks = d_v[d_v['喂猫师'] == s].copy()
                            if not stks.empty:
                                all_plans.append(optimize_route_v158(stks, "Riding", s, d.strftime('%Y-%m-%d'), st.session_state['departure_point']).assign(作业日期=d.strftime('%Y-%m-%d')))
            st.session_state['fp'] = pd.concat(all_plans) if all_plans else None
            status.update(label="✅ 计算完成！态势已透明化。", state="complete")
            st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        # 并排视角对账
        cd, cv = st.columns(2)
        with cd: vd = st.selectbox("📅 选择派单服务日期", sorted(st.session_state['fp']['作业日期'].unique()))
        with cv:
            if st.session_state['viewport'] == "管理员模式":
                st.session_state['admin_sub_view'] = st.selectbox("👤 指定路线视角", ["全部人员", "梦蕊", "依蕊"])
            else: st.write(f"视角: **{st.session_state['viewport']}**")
        
        day_all = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        vs_role = "全部" if (st.session_state['viewport'] == "管理员模式" and st.session_state['admin_sub_view'] == "全部人员") else (st.session_state['admin_sub_view'] if st.session_state['viewport'] == "管理员模式" else ("梦蕊" if "梦蕊" in st.session_state['viewport'] else "依蕊"))
        v_data = day_all if vs_role == "全部" else day_all[day_all['喂猫师'] == vs_role]
        
        # 指标卡片
        c1, c2 = st.columns(2); show_names = ["梦蕊", "依蕊"] if vs_role == "全部" else [vs_role]
        for i, sn in enumerate(show_names):
            stt = st.session_state['commute_stats'].get(f"{vd}_{sn}", {"dist": 0, "dur": 0})
            with [c1, c2][i%2]:
                st.markdown(f"""<div class="metric-v158"><h4>{sn} 派单统计</h4><p>单量：{len(day_all[day_all['喂猫师']==sn])} 单</p><p style="color:#007bff;">预计耗时：{int(stt['dur'])} 分钟</p><p>路段里程：{stt['dist']/1000:.2f} km</p></div>""", unsafe_allow_html=True)
        
        # --- 指令：派单简报一键复制与语义校准 ---
        brief = [f"📊 派单简报：今日共有 {len(v_data)} 户需上门，路线如下：", f"🚩 起始出发点：{st.session_state['departure_point']}"]
        for _, r in v_data.iterrows():
            nd, ns, pd_dur = pd.to_numeric(r.get('next_dur', 0), errors='coerce'), pd.to_numeric(r.get('next_dist', 0), errors='coerce'), pd.to_numeric(r.get('prev_dur', 0), errors='coerce')
            seq = int(pd.to_numeric(r.get('拟定顺序', 0), errors='coerce'))
            line = f"{seq}. {r.get('宠物名字', '猫咪')}-{r.get('详细地址','深圳')}"
            if seq == 1 and pd_dur > 0: line += f" (🚗 首段出征耗时 {int(pd_dur)}分)"
            if nd > 0: line += f" ➝ (下站约 {int(ns)}m, {int(nd)}分)"
            else: line += " 🏁 行程终点 (今日任务全部完成)"
            brief.append(line)
        
        brief_final = "\n".join(brief)
        # 一键复制组件
        if st.button("📋 点击一键复制派单指令 (可直接粘贴发微信)"):
            st.write(f'<script>navigator.clipboard.writeText(`{brief_final}`);</script>', unsafe_allow_html=True)
            st.success("指令已存入剪贴板！")
        st.text_area("📄 服务日报详情预览:", brief_final, height=250)

        # 地图强制渲染 (JS 眼睛)
        map_clean = v_data.dropna(subset=['lng', 'lat']).copy()
        if not map_clean.empty:
            map_json = map_clean[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序']].to_dict('records')
            amap_html = f"""
            <div id="map_box" style="width:100%; height:600px; border:1px solid #ddd; border-radius:16px; background:#f8f9fa;"></div>
            <script type="text/javascript"> window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }}; </script>
            <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_KEY_JS}&plugin=AMap.Walking,AMap.Riding"></script>
            <script type="text/javascript">
                (function() {{
                    try {{
                        const data = {json.dumps(map_json)}; const colors = {{"梦蕊": "#007BFF", "依蕊": "#FFA500"}};
                        const map = new AMap.Map('map_box', {{ zoom: 14, center: [data[0].lng, data[0].lat] }});
                        data.forEach(m => {{
                            new AMap.Marker({{ position: [m.lng, m.lat], map: map,
                                content: `<div style="width:28px;height:28px;background:${{colors[m.喂猫师]}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:24px;font-size:12px;font-weight:bold;">${{m.拟定顺序}}</div>`
                            }}).setLabel({{ direction:'top', offset: new AMap.Pixel(0, -5), content: m.宠物名字 }});
                        }});
                        function drawChain(idx, sData, map) {{
                            if (idx >= sData.length - 1) {{ setTimeout(()=>map.setFitView(), 500); return; }}
                            if (sData[idx].喂猫师 !== sData[idx+1].喂猫师) {{ drawChain(idx+1, sData, map); return; }}
                            new AMap.Riding({{ map: map, hideMarkers: true, strokeColor: colors[sData[idx].喂猫师], strokeWeight: 8 }})
                            .search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], ()=>setTimeout(()=>drawChain(idx+1, sData, map), 450));
                        }}
                        drawChain(0, data, map);
                    }} catch(e) {{ console.error(e); }}
                }})();
            </script>"""
            components.html(amap_html, height=620)

# --- 8. 平台手册与频率模型 ---

elif st.session_state['page'] == "手册指南":
    st.title("📖 派单管理平台 (2026 旗舰效率版)")
    st.markdown("""
    ### 1. 投喂频率计算 (对账核心)
    本系统采用“日期偏移偏移量”判定，公式如下：
    - `Δt = (当前日期 - 服务开始日期).days`
    - `判定结果 = (Δt % 投喂频率 == 0)`
    - **实战举例**：如果服务从2月1日开始，频率为 2（隔天喂）：
        - 2月1日：间隔0天，0%2=0 ✅ 需服务
        - 2月2日：间隔1天，1%2=1 ❌ 跳过
        - 2月3日：间隔2天，2%2=0 ✅ 需服务

    ### 2. 派单指令快速分发
    分析完成后，日报框上方有 **[📋 复制派单指令]** 按钮。点击后即可直接在微信粘贴发给依蕊或梦蕊。

    ### 3. 日志追踪说明
    如果您发现某只猫没出现在地图上，请展开侧边栏底部的“影子日志”。它会详细记录该猫是否因为频率不满足而被过滤。
    """)
