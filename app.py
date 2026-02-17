import streamlit as st

# ==========================================
# --- 【V153 入口状态保险锁：全量功能锁定】 ---
# ==========================================
def init_session_state_v153():
    """彻底解决 KeyError 与功能缺失，保障洛阳总部全频道指战"""
    td = datetime.now().date() if 'datetime' in globals() else None
    keys_defaults = {
        'system_logs': [],
        'commute_stats': {},
        'page': "智能派单看板",
        'plan_state': "IDLE", 
        'progress_val': 0.0,
        'feishu_cache': None,
        'r': (td, td + timedelta(days=1)) if td else (None, None),
        'viewport': "管理员模式",
        'admin_sub_view': "全部人员",
        'departure_point': "深圳市龙华区 潜龙花园 4A 栋",
        'travel_mode': "Riding"
    }
    for k, v in keys_defaults.items():
        if k not in st.session_state: st.session_state[k] = v

# --- 1. 物理导入核心库 (严禁静默缩减) ---
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

# 持久化通信链路
if 'http_session' not in st.session_state:
    st.session_state.http_session = requests.Session()

init_session_state_v153()

# --- 2. 凭证与双 Key 穿透锁定 ---
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
    ts = datetime.now().strftime('%H:%M:%S')
    icon = "✓" if level=="INFO" else "🚩"
    entry = f"[{ts}] {icon} {msg}"
    if 'system_logs' in st.session_state:
        st.session_state['system_logs'].append(entry)

# --- 3. 核心计算引擎 (回归 V144 同步逻辑，彻底终结 0 数据) ---

def haversine_v153(lon1, lat1, lon2, lat2, mode):
    """【高精自愈】解决短途 1 分钟精度"""
    R = 6371000 
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    dist = 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))
    real_dist = dist * 1.35
    speed_map = {"Walking": 66, "Riding": 250, "Transfer": 333}
    return int(real_dist), math.ceil(real_dist / speed_map.get(mode, 200))

@st.cache_data(show_spinner=False, ttl=3600)
def get_coords_v153(address):
    """【高性能缓存】地理编码"""
    if not address: return None, "空"
    clean_addr = str(address).strip().replace(" ", "")
    full_addr = clean_addr if clean_addr.startswith("深圳市") else f"深圳市{clean_addr}"
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(full_addr)}"
    try:
        r = st.session_state.http_session.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return (float(loc[0]), float(loc[1])), "SUCCESS"
    except: pass
    return None, "解析失败"

def get_travel_estimate_v153(origin, destination, mode_key):
    """【大脑 Key】稳健测速"""
    mode_url_map = {"Walking": "walking", "Riding": "bicycling", "Transfer": "integrated"}
    api_type = mode_url_map.get(mode_key, "bicycling")
    url = f"https://restapi.amap.com/v3/direction/{api_type}?origin={origin}&destination={destination}&key={AMAP_KEY_WS}"
    try:
        r = st.session_state.http_session.get(url, timeout=8).json()
        if r['status'] == '1':
            path = r['route']['paths'][0] if api_type != 'integrated' else r['route']['transits'][0]
            return int(path.get('distance', 0)), math.ceil(int(path.get('duration', 0)) / 60), "SUCCESS"
    except: pass
    return 0, 0, "ERR"

def get_normalized_address_v153(addr):
    """【复位 V99】地址指纹识别"""
    if not addr: return "未知"
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    addr = addr.replace("龙华区", "").replace("民治街道", "").replace("龙华街道", "")
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def optimize_route_v153(df_sitter, mode_key, sitter_name, date_str, start_addr):
    """【出征引擎】物理锁定公里数与顺序"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    if len(has_coords) == 0:
        st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": 0, "dur": 0}
        return df_sitter
    
    start_pt, _ = get_coords_v153(start_addr)
    unvisited = has_coords.to_dict('records')
    curr_lng, curr_lat = start_pt if start_pt else (unvisited[0]['lng'], unvisited[0]['lat'])
    
    optimized = []
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_lng-x['lng'])**2 + (curr_lat-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node)
        curr_lng, curr_lat = next_node['lng'], next_node['lat']
    
    t_d, t_t = 0, 0
    if start_pt:
        d0, t0, s0 = get_travel_estimate_v153(f"{start_pt[0]},{start_pt[1]}", f"{optimized[0]['lng']},{optimized[0]['lat']}", mode_key)
        if s0 != "SUCCESS": d0, t0 = haversine_v153(start_pt[0], start_pt[1], optimized[0]['lng'], optimized[0]['lat'], mode_key)
        optimized[0]['prev_dur'] = t0; t_d += d0; t_t += t0

    for i in range(len(optimized) - 1):
        d, t, s = get_travel_estimate_v153(f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}", mode_key)
        if s != "SUCCESS": d, t = haversine_v153(optimized[i]['lng'], optimized[i]['lat'], optimized[i+1]['lng'], optimized[i+1]['lat'], mode_key)
        optimized[i]['next_dist'], optimized[i]['next_dur'] = d, t
        t_d += d; t_t += t

    st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": t_d, "dur": t_t}
    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    for c in ['next_dist', 'next_dur', 'prev_dur']: 
        if c not in res_df.columns: res_df[c] = 0
        res_df[c] = res_df[c].fillna(0)
    return res_df

# --- 4. 视觉锁：深色高级版侧边栏 ---

st.set_page_config(page_title="小猫直喂服务派单旗舰平台", layout="wide", initial_sidebar_state="expanded")

def set_ui_v153():
    st.markdown("""
        <style>
        /* 深色极简侧边栏 */
        [data-testid="stSidebar"] { background-color: #1e1e1e !important; border-right: 1px solid #333; }
        .sidebar-header-v153 { font-size: 0.85rem; font-weight: 800; color: #777; margin: 1.2rem 0 0.5rem 0; letter-spacing: 1.2px; }
        [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { color: #ffffff !important; }
        
        /* 灰色圆角矩阵功能块 */
        .box-container-v153 [data-testid="stVerticalBlock"] div.stButton > button { 
            width: 100% !important; height: 52px !important; font-size: 15px !important; font-weight: 600 !important; 
            border-radius: 12px !important; border: 1px solid #3d3d3d !important;
            background-color: #2d2d2d !important; color: #ffffff !important; margin-bottom: 12px !important;
        }
        .box-container-v153 div.stButton > button:hover { background-color: #444 !important; border-color: #007bff !important; }
        
        /* 数据对账卡片 */
        .summary-card-v153 { 
            background-color: #ffffff !important; border: 1px solid #eee; border-left: 8px solid #28a745 !important; padding: 22px !important; 
            border-radius: 14px !important; box-shadow: 0 5px 15px rgba(0,0,0,0.05); margin-bottom: 20px;
        }
        .summary-card-v153 h4 { color: #888 !important; font-size: 14px !important; margin: 0 0 6px 0 !important; }
        .summary-card-v153 p { font-size: 28px !important; font-weight: 900 !important; color: #111 !important; margin: 0 !important; }
        
        /* 终端黑匣子 */
        .terminal-v153 { background-color: #111; color: #00ff00; padding: 12px; border-radius: 10px; font-family: monospace; font-size: 11px; height: 260px; overflow-y: auto; border: 1px solid #333; }
        </style>
        """, unsafe_allow_html=True)

set_ui_v153()

# --- 5. 侧边栏：模块化复位 (视角优先) ---

with st.sidebar:
    # 模块 A：视角锁定 (最顶端)
    st.markdown('<div class="sidebar-header-v153">👤 切换操作视角 (先确定模式)</div>', unsafe_allow_html=True)
    st.session_state['viewport'] = st.selectbox("模式", ["管理员模式", "梦蕊模式", "依蕊模式"], label_visibility="collapsed")
    
    # 指令 1：管理员专属三视角
    if st.session_state['viewport'] == "管理员模式":
        st.markdown("**查看特定路线范围**")
        st.session_state['admin_sub_view'] = st.selectbox("切换子视角", ["全部人员", "梦蕊", "依蕊"], label_visibility="collapsed")
    st.divider()

    # 模块 B：功能导航
    st.markdown('<div class="sidebar-header-v153">🧭 功能频道切换</div>', unsafe_allow_html=True)
    st.markdown('<div class="box-container-v153">', unsafe_allow_html=True)
    if st.button("📊 智能派单看板"): st.session_state['page'] = "智能派单看板"
    if st.button("📂 订单录入管理"): st.session_state['page'] = "订单录入管理"
    if st.button("📖 平台使用手册"): st.session_state['page'] = "平台使用手册"
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()

    # 模块 C：服务参数
    st.markdown('<div class="sidebar-header-v153">⚙️ 派单全局参数</div>', unsafe_allow_html=True)
    td = datetime.now().date(); c1, c2 = st.columns(2)
    with c1:
        if st.button("📍 今天"): st.session_state['r'] = (td, td + timedelta(days=1))
        if st.button("📍 本月"): st.session_state['r'] = (td.replace(day=1), td.replace(day=calendar.monthrange(td.year, td.month)[1]) + timedelta(days=1))
    with c2:
        if st.button("📍 明天"): st.session_state['r'] = (td + timedelta(days=1), td + timedelta(days=2))
        if st.button("📍 本周"): st.session_state['r'] = (td - timedelta(days=td.weekday()), td + timedelta(days=(6-td.weekday())+1))
    st.session_state['r'] = st.date_input("日期区间", value=st.session_state['r'])

    st.markdown("**📍 出征起始点**")
    sel_loc = st.selectbox("起点", ["深圳市龙华区 潜龙花园 4A 栋", "乐荟中心", "星河world 二期 c 栋", "自定义..."], label_visibility="collapsed")
    if sel_loc == "自定义...": st.session_state['departure_point'] = st.text_input("详情起始地址", value="深圳市")
    else: st.session_state['departure_point'] = sel_loc
    st.divider()

    # 模块 D：黑匣子 (底层)
    with st.expander("📡 系统日志影子塔", expanded=False):
        logs = "\n".join(st.session_state['system_logs'][-40:])
        st.markdown(f'<div class="terminal-v153">{logs}</div>', unsafe_allow_html=True)
        if st.button("复位历史记录"): st.session_state['system_logs'] = []; st.rerun()

# --- 6. 订单录入管理：满血版复位 (批量+手动+PATCH) ---

def fetch_data_v153():
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

if st.session_state['feishu_cache'] is None: st.session_state['feishu_cache'] = fetch_data_v153()

if st.session_state['page'] == "订单录入管理":
    st.title("📂 订单录入与飞书同步中心")
    df = st.session_state['feishu_cache'].copy()
    
    # 1. 财务计费对账 (159单绝对闭环)
    if not df.empty:
        df['计费单量'] = 0
        if isinstance(st.session_state['r'], tuple) and len(st.session_state['r']) >= 2:
            def calc(row):
                try:
                    s, e = pd.to_datetime(row['服务开始日期']).date(), pd.to_datetime(row['服务结束日期']).date()
                    freq, a_s, a_e = int(row.get('投喂频率', 1)), max(s, st.session_state['r'][0]), min(e, st.session_state['r'][1])
                    if a_s > a_e: return 0
                    return sum(1 for d in range((a_e-a_s).days + 1) if (a_s + timedelta(days=d) - s).days % freq == 0)
                except: return 0
            df['计费单量'] = df.apply(calc, axis=1)
            st.metric("分析周期内预计总计费数", f"{df['计费单量'].sum()} 次")
        
        st.subheader("⚙️ 飞书云端同步编辑器")
        edit_df = st.data_editor(df[['宠物名字', '详细地址', '喂猫师', '订单状态', '投喂频率']], use_container_width=True)
        if st.button("🚀 强制同步至飞书云端"):
            tk = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
            for i, row in edit_df.iterrows():
                requests.patch(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{df.iloc[i]['_id']}", headers={"Authorization": f"Bearer {tk}"}, json={"fields": {"订单状态": str(row['订单状态']), "喂猫师": str(row['喂猫师']), "投喂频率": int(row['投喂频率'])}})
            st.session_state['feishu_cache'] = None; st.rerun()

    st.divider()
    # 2. 录单模块
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量：Excel 快速导入"):
            up = st.file_uploader("名单上传", type=["xlsx"])
            if up and st.button("开始推送云端"):
                du = pd.read_excel(up); tk_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
                for _, r in du.iterrows():
                    f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')), "投喂频率": int(r.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk_a}"}, json={"fields": f})
                st.session_state['feishu_cache'] = None; st.rerun()
    with c2:
        with st.expander("手动：单兵精准开单"):
            with st.form("man_v153"):
                a = st.text_input("详细地址*"); n = st.text_input("宠物名"); sd = st.date_input("起始日"); ed = st.date_input("结束日"); f_q = st.number_input("投喂频率 (1代表每天)", value=1)
                if st.form_submit_button("💾 保存并同步"):
                    tk_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000), "投喂频率": int(f_q), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk_a}"}, json={"fields": f})
                    st.session_state['feishu_cache'] = None; st.rerun()

# --- 7. 智能派单看板：上帝视角与日报回归 ---

elif st.session_state['page'] == "智能派单看板":
    st.title(f"派单态势 · {st.session_state['viewport']}")
    
    # 状态栏
    df_raw = st.session_state['feishu_cache'].copy()
    m_c = 0; g_c = 0
    if st.session_state.get('fp') is not None:
        m_c = len(st.session_state['fp']); g_c = len(st.session_state['fp'].dropna(subset=['lng']))
    st.markdown(f"""<div style="background:#f1f3f5; padding:15px; border-radius:12px; display:flex; justify-content:space-around; margin-bottom:20px;">
        <div style="text-align:center;"><p style="font-size:0.8rem; color:#666;">飞书库总计</p><p style="font-size:1.1rem; font-weight:800;">{len(df_raw)}</p></div>
        <div style="text-align:center;"><p style="font-size:0.8rem; color:#666;">本日匹配订单</p><p style="font-size:1.1rem; font-weight:800; color:#007bff;">{m_c}</p></div>
        <div style="text-align:center;"><p style="font-size:0.8rem; color:#666;">坐标有效点</p><p style="font-size:1.1rem; font-weight:800; color:#28a745;">{g_c}</p></div>
    </div>""", unsafe_allow_html=True)

    # 三键控制
    c1, c2, c3, c4 = st.columns([1, 1, 1, 4])
    if c1.button("▶ 启动方案"): st.session_state['plan_state'] = "RUNNING"
    if c2.button("⏸ 暂停普查"): st.session_state['plan_state'] = "PAUSED"
    if c3.button("↺ 复位清空"): st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        # IndexError 安全锁
        if not isinstance(st.session_state['r'], tuple) or len(st.session_state['r']) < 2:
            st.warning("⚠️ 请点选完整的起始和结束日期。"); st.session_state['plan_state'] = "IDLE"; st.stop()

        if not df_raw.empty:
            p_bar = st.progress(0.0, text="穿透数据流...")
            with st.status("正在回归执行 V144 同步测速...", expanded=True) as status:
                # V99 空间算法
                sitters = ["梦蕊", "依蕊"]
                df_raw['building_fp'] = df_raw['详细地址'].apply(get_normalized_address_v153)
                s_load = {s: 0 for s in sitters}
                unassigned = ~df_raw.get('喂猫师', '').isin(sitters)
                if unassigned.any():
                    for _, g in df_raw[unassigned].groupby('building_fp'):
                        best = min(s_load, key=s_load.get); df_raw.loc[g.index, '喂猫师'] = best; s_load[best] += len(g)
                
                days = pd.date_range(st.session_state['r'][0], st.session_state['r'][1]).tolist()
                all_plans = []
                for idx, d in enumerate(days):
                    if st.session_state['plan_state'] == "PAUSED": break
                    p_bar.progress((idx+1)/len(days), text=f"计算日期: {d.strftime('%Y-%m-%d')}")
                    ct = pd.Timestamp(d); d_v = df_raw[(df_raw['服务开始日期'] <= ct) & (df_raw['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        # 投喂频率核心公式
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率',1)) == 0, axis=1)]
                        if not d_v.empty:
                            with ThreadPoolExecutor(max_workers=5) as ex:
                                results = list(ex.map(get_coords_v153, d_v['详细地址']))
                            d_v[['lng', 'lat']] = pd.DataFrame([ [c[0][0], c[0][1]] if c[0] else [None, None] for c in results ], index=d_v.index, columns=['lng', 'lat'])
                            for s in sitters:
                                stks = d_v[d_v['喂猫师'] == s].copy()
                                if not stks.empty:
                                    all_plans.append(optimize_route_v153(stks, "Riding", s, d.strftime('%Y-%m-%d'), st.session_state['departure_point']).assign(作业日期=d.strftime('%Y-%m-%d')))
                st.session_state['fp'] = pd.concat(all_plans) if all_plans else None
                status.update(label="✅ 派单计算完成！地图已复位。", state="complete")
                st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        vd = st.selectbox("选择服务日期", sorted(st.session_state['fp']['作业日期'].unique()))
        day_all = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        
        # 指令 1：管理员视角子过滤
        if st.session_state['viewport'] == "管理员模式":
            sub_v = st.session_state['admin_sub_view']
            vs_role = "全部" if sub_v == "全部人员" else sub_v
        else:
            vs_role = "梦蕊" if "梦蕊" in st.session_state['viewport'] else "依蕊"
        
        v_data = day_all if vs_role == "全部" else day_all[day_all['喂猫师'] == vs_role]
        
        # 态势卡片
        c1, c2 = st.columns(2); show_names = ["梦蕊", "依蕊"] if vs_role == "全部" else [vs_role]
        for i, sn in enumerate(show_names):
            stt = st.session_state['commute_stats'].get(f"{vd}_{sn}", {"dist": 0, "dur": 0})
            with [c1, c2][i%2]:
                st.markdown(f"""<div class="summary-card-v153"><h4>{sn} 派单统计</h4><p>单量：{len(day_all[day_all['喂猫师']==sn])} 单</p><p style="color:#007bff;">总耗时：{int(stt['dur'])} 分钟</p><p>路程：{stt['dist']/1000:.2f} km</p></div>""", unsafe_allow_html=True)
        
        # 指令 2：当日日报功能回归
        brief = [f"起始出发点：{st.session_state['departure_point']}"]
        for _, r in v_data.iterrows():
            nd, ns, pd_dur = pd.to_numeric(r.get('next_dur', 0), errors='coerce'), pd.to_numeric(r.get('next_dist', 0), errors='coerce'), pd.to_numeric(r.get('prev_dur', 0), errors='coerce')
            seq = int(pd.to_numeric(r.get('拟定顺序', 0), errors='coerce'))
            line = f"{seq}. {r.get('宠物名字', '猫咪')}-{r.get('详细地址','深圳')}"
            if seq == 1 and pd_dur > 0: line += f" (🚗 首段出征耗时 {int(pd_dur)}分)"
            if nd > 0: line += f" ➝ (下站 {int(ns)}m, {int(nd)}分)"
            else: line += " (🏁 终点派送完毕)"
            brief.append(line)
        st.text_area("📋 今日派单日报指引 (含出征耗时):", "\n".join(brief), height=250)

        # 地图逻辑 (JS 强制优先渲染)
        map_clean = v_data.dropna(subset=['lng', 'lat']).copy()
        if not map_clean.empty:
            map_json = map_clean[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序']].to_dict('records')
            amap_html = f"""
            <div id="map_box" style="width:100%; height:600px; border:1px solid #ddd; border-radius:16px; background:#f8f9fa;"></div>
            <script type="text/javascript"> window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }}; </script>
            <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_KEY_JS}&plugin=AMap.Walking,AMap.Riding"></script>
            <script type="text/javascript">
                (function() {{
                    const data = {json.dumps(map_json)}; const colors = {{"梦蕊": "#007BFF", "依蕊": "#FFA500"}};
                    const map = new AMap.Map('map_box', {{ zoom: 14, center: [data[0].lng, data[0].lat] }});
                    data.forEach(m => {{
                        new AMap.Marker({{ position: [m.lng, m.lat], map: map,
                            content: `<div style="width:26px;height:28px;background:${{colors[m.喂猫师]}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:24px;font-size:12px;font-weight:bold;">${{m.拟定顺序}}</div>`
                        }}).setLabel({{ direction:'top', offset: new AMap.Pixel(0, -5), content: m.宠物名字 }});
                    }});
                    function draw(idx, sData, map) {{
                        if (idx >= sData.length - 1) {{ setTimeout(()=>map.setFitView(), 500); return; }}
                        if (sData[idx].喂猫师 !== sData[idx+1].喂猫师) {{ draw(idx+1, sData, map); return; }}
                        new AMap.Riding({{ map: map, hideMarkers: true, strokeColor: colors[sData[idx].喂猫师], strokeWeight: 8 }})
                        .search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], ()=>setTimeout(()=>draw(idx+1, sData, map), 450));
                    }}
                    draw(0, data, map);
                }})();
            </script>"""
            components.html(amap_html, height=620)

# --- 8. 平台使用手册：逻辑补全 ---

elif st.session_state['page'] == "平台使用手册":
    st.title("📖 小猫直喂派单平台手册 (2026版)")
    st.markdown("""
    ### 1. 核心投喂间隔逻辑
    本平台采用“日期偏移量取模”算法：
    * **公式**：`当日是否投喂 = (当前日期 - 服务开始日期) % 投喂频率 == 0`
    * **举例**：开始日期是2月1日，频率是3天/次。
        - 2月1日：(0 % 3) = 0 ✅ 服务
        - 2月2日：(1 % 3) = 1 ❌ 跳过
        - 2月3日：(2 % 3) = 2 ❌ 跳过
        - 2月4日：(3 % 3) = 0 ✅ 服务

    ### 2. 派单计算流程
    1. **权限确定**：在侧边栏最顶端选择您的身份。
    2. **参数设定**：选定日期范围和“出征起点”。
    3. **启动方案**：系统会先执行飞书数据拉取 -> 执行 V99 空间调度(同楼不拆单) -> 执行路径最优排序。
    4. **结果对账**：通过顶部的仪表盘和日报，确认公里数与预计耗时。

    ### 3. 数据维护
    * 在“订单录入管理”中，您可以直接修改喂猫师归属或订单状态。
    * 修改后必须点击“强制同步至飞书云端”才会永久生效。
    """)
