import streamlit as st

# ==========================================
# --- 【V148 入口状态与容错锁：彻底终结 KeyError】 ---
# ==========================================
def init_session_state_v148():
    """
    强制入口初始化，视角优先锁定。
    物理隔离 KeyError 与 IndexError 冲突点
    """
    td = datetime.now().date() if 'datetime' in globals() else None
    keys_defaults = {
        'system_logs': [],
        'commute_stats': {},
        'page': "实时派单看板",
        'plan_state': "IDLE", 
        'progress_val': 0.0,
        'feishu_cache': None,
        'r': (td, td + timedelta(days=1)) if td else (None, None),
        'viewport': "管理员模式",
        'departure_point': "深圳市龙华区 潜龙花园 4A 栋",
        'travel_mode': "Riding"
    }
    for k, v in keys_defaults.items():
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

# 性能优化：通信链路持久化
if 'http_session' not in st.session_state:
    st.session_state.http_session = requests.Session()

init_session_state_v148()

# --- 2. 核心配置与双 Key 穿透锁定 ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 

# 双核驱动：c26... (算路), c67... (地图)
AMAP_KEY_WS = st.secrets.get("AMAP_KEY_WS", "c26fc76dd582c32e4406552df8ba40ff").strip() 
AMAP_KEY_JS = st.secrets.get("AMAP_KEY_JS", "c67e780b4d72b313f825746f8b02d840").strip() 
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "f3bd8f946c9fdf05cb73e259b108e527").strip()

def add_log(msg, level="INFO"):
    """黑匣子日志回显"""
    ts = datetime.now().strftime('%H:%M:%S')
    icon = "✓" if level=="INFO" else "!"
    entry = f"[{ts}] {icon} {msg}"
    if 'system_logs' in st.session_state:
        st.session_state['system_logs'].append(entry)
    else:
        st.session_state['system_logs'] = [entry]

# --- 3. 核心底座逻辑 (坐标、测速与分配) ---

def haversine_v148(lon1, lat1, lon2, lat2, mode):
    """【精度自愈】解决 1 分钟精度顽疾"""
    R = 6371000 
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    dist = 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))
    real_dist = dist * 1.35 # 路网修正系数
    speed_map = {"Walking": 66, "Riding": 250, "Transfer": 333} # 米/分
    return int(real_dist), math.ceil(real_dist / speed_map.get(mode, 200))

@st.cache_data(show_spinner=False, ttl=3600)
def get_coords_v148(address):
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

def get_travel_estimate_v148(origin, destination, mode_key):
    """【算路大脑】"""
    mode_url_map = {"Walking": "walking", "Riding": "bicycling", "Transfer": "integrated"}
    api_type = mode_url_map.get(mode_key, "bicycling")
    url = f"https://restapi.amap.com/v3/direction/{api_type}?origin={origin}&destination={destination}&key={AMAP_KEY_WS}"
    try:
        r = st.session_state.http_session.get(url, timeout=8).json()
        if r['status'] == '1':
            path = r['route']['paths'][0] if api_type != 'integrated' else r['route']['transits'][0]
            return int(path.get('distance', 0)), math.ceil(int(path.get('duration', 0)) / 60), "SUCCESS"
    except: pass
    return 0, 0, "API_ERR"

def get_normalized_address_v148(addr):
    """【复位 V99】高精地址指纹，确保同楼不拆单"""
    if not addr: return "未知"
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    addr = addr.replace("龙华区", "").replace("民治街道", "").replace("龙华街道", "")
    addr = addr.replace('一','1').replace('二','2').replace('三','3').replace('四','4').replace('五','5')
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def calculate_billing_v148(row, start_range, end_range):
    """【159单绝对财务逻辑】"""
    try:
        if pd.isna(row['服务开始日期']) or pd.isna(row['服务结束日期']): return 0
        s_date = pd.to_datetime(row['服务开始日期']).date()
        e_date = pd.to_datetime(row['服务结束日期']).date()
        freq = int(float(str(row.get('投喂频率', 1)).strip() or 1))
        a_start = max(s_date, start_range); a_end = min(e_date, end_range)
        if a_start > a_end: return 0
        count = 0; curr = a_start
        while curr <= a_end:
            if (curr - s_date).days % freq == 0: count += 1
            curr += timedelta(days=1)
        return count
    except: return 0

def optimize_route_v148(df_sitter, mode_key, sitter_name, date_str, start_addr):
    """【出征引擎】包含起点的测速闭环"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    
    if len(has_coords) == 0:
        st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": 0, "dur": 0}
        return df_sitter
    
    start_pt, _ = get_coords_v148(start_addr)
    unvisited = has_coords.to_dict('records')
    curr_lng, curr_lat = start_pt if start_pt else (unvisited[0]['lng'], unvisited[0]['lat'])
    
    optimized = []
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_lng-x['lng'])**2 + (curr_lat-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node)
        curr_lng, curr_lat = next_node['lng'], next_node['lat']
    
    t_d, t_t = 0, 0
    # A. 起点出征首段
    if start_pt:
        d0, t0, s0 = get_travel_estimate_v148(f"{start_pt[0]},{start_pt[1]}", f"{optimized[0]['lng']},{optimized[0]['lat']}", mode_key)
        if s0 != "SUCCESS": d0, t0 = haversine_v148(start_pt[0], start_pt[1], optimized[0]['lng'], optimized[0]['lat'], mode_key)
        optimized[0]['prev_dur'] = t0; t_d += d0; t_t += t0

    # B. 后续站点测速
    for i in range(len(optimized) - 1):
        d, t, s = get_travel_estimate_v148(f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}", mode_key)
        if s != "SUCCESS": d, t = haversine_v148(optimized[i]['lng'], optimized[i]['lat'], optimized[i+1]['lng'], optimized[i+1]['lat'], mode_key)
        optimized[i]['next_dist'], optimized[i]['next_dur'] = d, t
        t_d += d; t_t += t

    st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": t_d, "dur": t_t}
    add_log(f"✅ {sitter_name} 路径计算完毕: {t_d/1000:.2f}km, {t_t}分")
    
    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    # 【V148强补列名防护】
    for c in ['next_dist', 'next_dur', 'prev_dur']: 
        if c not in res_df.columns: res_df[c] = 0
        res_df[c] = res_df[c].fillna(0)
    return res_df

# --- 4. 【核心重构】极简深色 UI 引擎 (视觉锁) ---

st.set_page_config(page_title="小猫直喂派单平台", layout="wide", initial_sidebar_state="expanded")

def set_ui_v148():
    st.markdown("""
        <style>
        /* 侧边栏：深色系沉浸式 */
        [data-testid="stSidebar"] { 
            background-color: #1e1e1e !important; 
            border-right: 1px solid #333; 
        }
        .sb-header { 
            font-size: 0.85rem; font-weight: 800; color: #777; 
            margin: 1.2rem 0 0.5rem 0; text-transform: uppercase; letter-spacing: 1.5px;
        }
        [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { 
            color: #ffffff !important; 
        }
        
        /* 导航与功能区：深灰色圆角背景框 */
        .box-btn [data-testid="stVerticalBlock"] div.stButton > button { 
            width: 100% !important; height: 52px !important; 
            font-size: 15px !important; font-weight: 600 !important; 
            border-radius: 12px !important; border: 1px solid #3d3d3d !important;
            background-color: #2d2d2d !important; color: #ffffff !important; 
            margin-bottom: 10px !important; transition: 0.25s all ease;
        }
        .box-btn div.stButton > button:hover { 
            background-color: #404040 !important; border-color: #007bff !important; transform: translateY(-1px);
        }
        
        /* 表单控件圆角化 */
        div[data-baseweb="select"], div[data-baseweb="input"], .stDateInput, .stRadio {
            background-color: #2d2d2d !important; border-radius: 12px !important; border: 1px solid #333 !important;
        }

        /* 主页面：专业指标卡片 */
        .metric-card { 
            background-color: #ffffff !important; border: 1px solid #eef; 
            border-left: 8px solid #28a745 !important; padding: 22px !important; 
            border-radius: 14px !important; box-shadow: 0 5px 15px rgba(0,0,0,0.04); 
        }
        .metric-card h4 { color: #888 !important; font-size: 14px !important; margin: 0 0 6px 0 !important; font-weight: 600; }
        .metric-card p { font-size: 28px !important; font-weight: 900 !important; color: #1a1a1a !important; margin: 0 !important; }
        
        /* 系统日志黑匣子 */
        .log-terminal { 
            background-color: #111; color: #00ff00; padding: 12px; 
            border-radius: 10px; font-family: 'Courier New', monospace; font-size: 11px; 
            height: 240px; overflow-y: auto; border: 1px solid #333; line-height: 1.5;
        }
        </style>
        """, unsafe_allow_html=True)

set_ui_v148()

# --- 5. 侧边栏布局：视角锁定与模块对齐 ---

with st.sidebar:
    # 模块 1：视角切换 (置顶)
    st.markdown('<div class="sb-header">👤 视角角色锁定</div>', unsafe_allow_html=True)
    st.session_state['viewport'] = st.selectbox("模式", ["管理员模式", "梦蕊模式", "依蕊模式"], label_visibility="collapsed")
    st.divider()

    # 模块 2：导航频道 (大按钮)
    st.markdown('<div class="sb-header">🧭 功能主导航</div>', unsafe_allow_html=True)
    st.markdown('<div class="box-btn">', unsafe_allow_html=True)
    if st.button("📊 派单看板中心"): st.session_state['page'] = "派单看板"
    if st.button("📂 资料对账中心"): st.session_state['page'] = "数据中心"
    if st.button("📖 操作使用指南"): st.session_state['page'] = "帮助文档"
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()

    # 模块 3：参数配置 (2x2 网格)
    st.markdown('<div class="sb-header">⚙️ 派单服务参数</div>', unsafe_allow_html=True)
    td = datetime.now().date(); c1, c2 = st.columns(2)
    with c1:
        if st.button("📍 今天"): st.session_state['r'] = (td, td + timedelta(days=1))
        if st.button("📍 本月"): st.session_state['r'] = (td.replace(day=1), td.replace(day=calendar.monthrange(td.year, td.month)[1]) + timedelta(days=1))
    with c2:
        if st.button("📍 明天"): st.session_state['r'] = (td + timedelta(days=1), td + timedelta(days=2))
        if st.button("📍 本周"): st.session_state['r'] = (td - timedelta(days=td.weekday()), td + timedelta(days=(6-td.weekday())+1))
    st.session_state['r'] = st.date_input("分析周期", value=st.session_state['r'])

    st.markdown("**📍 出征起始点**")
    loc_opts = ["深圳市龙华区 潜龙花园 4A 栋", "乐荟中心", "星河world 二期 c 栋", "自定义输入..."]
    sel_loc = st.selectbox("起点", loc_opts, label_visibility="collapsed")
    if sel_loc == "自定义输入...": st.session_state['departure_point'] = st.text_input("请输入", value="深圳市")
    else: st.session_state['departure_point'] = sel_loc
    
    st.markdown("**🚲 交通工具选择**")
    m_sel = st.radio("模式", ["步行", "电动车/骑行", "公交/地铁"], index=1, label_visibility="collapsed")
    st.session_state['travel_mode'] = {"步行": "Walking", "电动车/骑行": "Riding", "公交/地铁": "Transfer"}[m_sel]

    # 模块 4：自检黑匣子 (折叠化)
    st.markdown('<div class="sb-header">📡 系统自检日志</div>', unsafe_allow_html=True)
    with st.expander("展开影子通讯塔", expanded=False):
        logs_txt = "\n".join(st.session_state['system_logs'][-35:])
        st.markdown(f'<div class="log-terminal">{logs_txt}</div>', unsafe_allow_html=True)
        if st.button("复位日志记录"): st.session_state['system_logs'] = []; st.rerun()

# --- 6. 数据管理中心：全量对账与容错 ---

@st.cache_resource(ttl=7200)
def get_feishu_token_v148():
    try:
        r = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10).json()
        return r.get("tenant_access_token")
    except: return None

def fetch_data_v148():
    tk = get_feishu_token_v148()
    if not tk: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    try:
        r = st.session_state.http_session.get(url, headers={"Authorization": f"Bearer {tk}"}, params={"page_size": 500}, timeout=15).json()
        df = pd.DataFrame([dict(i['fields'], _id=i['record_id']) for i in r.get("data", {}).get("items", [])])
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        for col in ['宠物名字', '详细地址', '喂猫师']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

if st.session_state['feishu_cache'] is None: st.session_state['feishu_cache'] = fetch_data_v148()

if st.session_state['page'] == "数据中心":
    st.title("📂 客户资料与服务计费中心")
    df = st.session_state['feishu_cache'].copy()
    if not df.empty:
        # 159单绝对对账
        df['计费天数'] = 0
        if isinstance(st.session_state['r'], tuple) and len(st.session_state['r']) >= 2:
            df['计费天数'] = df.apply(lambda r: calculate_billing_v148(r, st.session_state['r'][0], st.session_state['r'][1]), axis=1)
            st.metric("分析周期内累计单量", f"{df['计费天数'].sum()} 次服务")
        
        # IndexError/KeyError 安全列选择
        wanted = ['宠物名字', '计费天数', '喂猫师', '订单状态', '详细地址']
        st.dataframe(df[[c for c in wanted if c in df.columns]], use_container_width=True)
    if st.button("同步云端数据"): st.session_state['feishu_cache'] = None; st.rerun()

# --- 7. 派单看板：三键状态机与 KeyError 绝杀防护 ---

elif st.session_state['page'] == "派单看板":
    st.title(f"派单大屏 · {st.session_state['viewport']}")
    
    # 三键指挥控制台
    c1, c2, c3, c4 = st.columns([1, 1, 1, 4])
    if c1.button("▶ 开始派单"): st.session_state['plan_state'] = "RUNNING"
    if c2.button("⏸ 暂停计算"): st.session_state['plan_state'] = "PAUSED"
    if c3.button("↺ 复位重置"): 
        st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        # IndexError 防护：确保日期范围选满
        if not isinstance(st.session_state['r'], tuple) or len(st.session_state['r']) < 2:
            st.warning("⚠️ 请在左侧分析周期中点选【起始】和【结束】日期。")
            st.session_state['plan_state'] = "IDLE"; st.stop()

        df_raw = st.session_state['feishu_cache'].copy()
        if not df_raw.empty:
            prog_bar = st.progress(0.0, text="同步高德路网数据中...")
            with st.status("正在进行空间聚类与路径测速...", expanded=True) as status:
                # 复位 V99 空间算法
                s_sitters = ["梦蕊", "依蕊"]
                df_raw['building_fp'] = df_raw['详细地址'].apply(get_normalized_address_v148)
                s_load = {s: 0 for s in s_sitters}
                unassigned = ~df_raw.get('喂猫师', '').isin(s_sitters)
                if unassigned.any():
                    for _, g in df_raw[unassigned].groupby('building_fp'):
                        best = min(s_load, key=s_load.get); df_raw.loc[g.index, '喂猫师'] = best; s_load[best] += len(g)
                
                days = pd.date_range(st.session_state['r'][0], st.session_state['r'][1]).tolist()
                all_plans = []
                for idx, d in enumerate(days):
                    if st.session_state['plan_state'] == "PAUSED": break
                    prog_bar.progress((idx+1)/len(days), text=f"分析日期: {d.strftime('%Y-%m-%d')}")
                    ct = pd.Timestamp(d); d_v = df_raw[(df_raw['服务开始日期'] <= ct) & (df_raw['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率',1)) == 0, axis=1)]
                        if not d_v.empty:
                            with ThreadPoolExecutor(max_workers=5) as ex:
                                results = list(ex.map(get_coords_v148, d_v['详细地址']))
                            d_v[['lng', 'lat']] = pd.DataFrame([ [c[0][0], c[0][1]] if c[0] else [None, None] for c in results ], index=d_v.index, columns=['lng', 'lat'])
                            for s in s_sitters:
                                stks = d_v[d_v['喂猫师'] == s].copy()
                                if not stks.empty:
                                    res = optimize_route_v148(stks, st.session_state['travel_mode'], s, d.strftime('%Y-%m-%d'), st.session_state['departure_point'])
                                    res['作业日期'] = d.strftime('%Y-%m-%d'); all_plans.append(res)
                st.session_state['fp'] = pd.concat(all_plans) if all_plans else None
                status.update(label="✅ 派单路径计算完成！数据已入库。", state="complete")
                st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        vd = st.selectbox("选择派单日期", sorted(st.session_state['fp']['作业日期'].unique()))
        day_all = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        vs_role = "全部" if "管理员" in st.session_state['viewport'] else ("梦蕊" if "梦蕊" in st.session_state['viewport'] else "依蕊")
        v_data = day_all if vs_role == "全部" else day_all[day_all['喂猫师'] == vs_role]
        
        # 态势指标卡片
        c1, c2 = st.columns(2); show_names = ["梦蕊", "依蕊"] if vs_role == "全部" else [vs_role]
        for i, s_n in enumerate(show_names):
            s_stats = st.session_state['commute_stats'].get(f"{vd}_{s_n}", {"dist": 0, "dur": 0})
            with [c1, c2][i%2]:
                st.markdown(f"""<div class="metric-card"><h4>{s_n} 派单指标</h4><p>服务单量：{len(day_all[day_all['喂猫师']==s_n])} 单</p><p style="color:#007bff;">预计耗时：{int(s_stats['dur'])} 分钟</p><p>路段总程：{s_stats['dist']/1000:.2f} km</p></div>""", unsafe_allow_html=True)
        
        # --- 派单简报：ValueError 与 KeyError 终极绝杀版 ---
        brief_lines = [f"起始地点：{st.session_state['departure_point']}"]
        for _, r in v_data.iterrows():
            # 物理纠偏：防御式读取
            n_dur = pd.to_numeric(r.get('next_dur', 0), errors='coerce'); n_dist = pd.to_numeric(r.get('next_dist', 0), errors='coerce')
            p_dur = pd.to_numeric(r.get('prev_dur', 0), errors='coerce'); seq = pd.to_numeric(r.get('拟定顺序', 0), errors='coerce')
            
            line = f"{int(seq)}. {r.get('宠物名字', '猫咪')}-{r.get('详细地址', '深圳')}"
            if int(seq) == 1 and p_dur > 0: line += f" (首段耗时 {int(p_dur)}分)"
            if n_dur > 0: line += f" ➝ (下站 {int(n_dist)}m, {int(n_dur)}分)"
            else: line += " (🏁 本单服务完毕)"
            brief_lines.append(line)
        st.text_area("服务行程简报 (含起点耗时):", "\n".join(brief_lines), height=250)

        # 地图接力渲染
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
                            content: `<div style="width:28px;height:28px;background:${{colors[m.喂猫师]}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:24px;font-size:12px;font-weight:bold;">${{m.拟定顺序}}</div>`
                        }}).setLabel({{ direction:'top', offset: new AMap.Pixel(0, -5), content: m.宠物名字 }});
                    }});
                    function drawChain(idx, sData, map) {{
                        if (idx >= sData.length - 1) {{ setTimeout(()=>map.setFitView(), 500); return; }}
                        if (sData[idx].喂猫师 !== sData[idx+1].喂猫师) {{ drawChain(idx+1, sData, map); return; }}
                        let router; const cfg = {{ map: map, hideMarkers: true, strokeColor: colors[sData[idx].喂猫师], strokeWeight: 8 }};
                        if ("{st.session_state['travel_mode']}" === "Walking") router = new AMap.Walking(cfg);
                        else router = new AMap.Riding(cfg);
                        router.search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], ()=>setTimeout(()=>drawChain(idx+1, sData, map), 450));
                    }}
                    drawChain(0, data, map);
                }})();
            </script>"""
            components.html(amap_html, height=620)
