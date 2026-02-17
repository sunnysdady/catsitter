import streamlit as st

# ==========================================
# --- 【V146 入口状态管理：性能与容错】 ---
# ==========================================
def init_session_state_v146():
    """
    初始化系统状态。
    移除了浮夸描述，保留核心稳定性锁
    """
    td = datetime.now().date() if 'datetime' in globals() else None
    defaults = {
        'system_logs': [],
        'commute_stats': {},
        'page': "派单看板",
        'plan_state': "IDLE",  # IDLE, RUNNING, PAUSED
        'progress_val': 0.0,
        'feishu_cache': None,
        'r': (td, td + timedelta(days=1)) if td else (None, None),
        'viewport': "管理员模式",
        'departure_point': "深圳市龙华区 潜龙花园 4A 栋",
        'travel_mode': "Riding"
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

# --- 1. 物理导入核心库 (严禁静默缩减功能) ---
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

# --- 性能优化：创建持久会话 ---
if 'http_session' not in st.session_state:
    st.session_state.http_session = requests.Session()

init_session_state_v146()

# --- 2. 凭证配置与 API 隔离 ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 

# 高德双核驱动：大脑(WS)算路，眼睛(JS)地图
AMAP_KEY_WS = st.secrets.get("AMAP_KEY_WS", "c26fc76dd582c32e4406552df8ba40ff").strip() 
AMAP_KEY_JS = st.secrets.get("AMAP_KEY_JS", "c67e780b4d72b313f825746f8b02d840").strip() 
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "f3bd8f946c9fdf05cb73e259b108e527").strip()

def add_log(msg, level="INFO"):
    """系统运行日志回显"""
    ts = datetime.now().strftime('%H:%M:%S')
    icon = "✓" if level=="INFO" else "!"
    entry = f"[{ts}] {icon} {msg}"
    if 'system_logs' in st.session_state:
        st.session_state['system_logs'].append(entry)

# --- 3. 核心计算底座 (坐标、测速、分配) ---

def haversine_v146(lon1, lat1, lon2, lat2, mode):
    """【精度自愈】球面直线算法，确保 0 数据不出现"""
    R = 6371000 
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    dist = 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))
    real_dist = dist * 1.35 # 基础路网系数
    speed_map = {"Walking": 66, "Riding": 250, "Transfer": 333}
    return int(real_dist), math.ceil(real_dist / speed_map.get(mode, 200))

@st.cache_data(show_spinner=False, ttl=3600)
def get_coords_v146(address):
    """【大脑 Key】地理编码，带长效缓存"""
    if not address: return None, "Empty"
    clean_addr = str(address).strip().replace(" ", "")
    full_addr = clean_addr if clean_addr.startswith("深圳市") else f"深圳市{clean_addr}"
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(full_addr)}"
    try:
        r = st.session_state.http_session.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return (float(loc[0]), float(loc[1])), "SUCCESS"
    except: pass
    return None, "Fail"

def get_travel_estimate_v146(origin, destination, mode_key):
    """【大脑 Key】路网测速"""
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

def get_normalized_address_v146(addr):
    """【复位 V99】地址指纹识别逻辑，确保同楼不拆单"""
    if not addr: return "未知"
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    addr = addr.replace("龙华区", "").replace("民治街道", "").replace("龙华街道", "")
    addr = addr.replace('一','1').replace('二','2').replace('三','3').replace('四','4').replace('五','5')
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def optimize_route_v146(df_sitter, mode_key, sitter_name, date_str, start_addr):
    """【派单算法】计算从出发点到终点的最优路径"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    if len(has_coords) == 0:
        st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": 0, "dur": 0}
        return df_sitter
    
    start_pt, _ = get_coords_v146(start_addr)
    unvisited = has_coords.to_dict('records')
    curr_lng, curr_lat = start_pt if start_pt else (unvisited[0]['lng'], unvisited[0]['lat'])
    
    optimized = []
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_lng-x['lng'])**2 + (curr_lat-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node)
        curr_lng, curr_lat = next_node['lng'], next_node['lat']
    
    total_d, total_t = 0, 0
    # 起点至第一单
    if start_pt:
        d0, t0, s0 = get_travel_estimate_v146(f"{start_pt[0]},{start_pt[1]}", f"{optimized[0]['lng']},{optimized[0]['lat']}", mode_key)
        if s0 != "SUCCESS": d0, t0 = haversine_v146(start_pt[0], start_pt[1], optimized[0]['lng'], optimized[0]['lat'], mode_key)
        optimized[0]['prev_dur'] = t0; total_d += d0; total_t += t0

    # 站点间续航
    for i in range(len(optimized) - 1):
        d, t, s = get_travel_estimate_v146(f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}", mode_key)
        if s != "SUCCESS": d, t = haversine_v146(optimized[i]['lng'], optimized[i]['lat'], optimized[i+1]['lng'], optimized[i+1]['lat'], mode_key)
        optimized[i]['next_dist'], optimized[i]['next_dur'] = d, t
        total_d += d; total_t += t

    st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": total_d, "dur": total_t}
    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

# --- 4. 视觉 UI 引擎 (专业、无浮夸) ---

st.set_page_config(page_title="小猫直喂服务派单平台", layout="wide", initial_sidebar_state="expanded")

def set_ui_v146():
    st.markdown("""
        <style>
        /* 侧边栏专业化 */
        [data-testid="stSidebar"] { background-color: #f8f9fa !important; border-right: 1px solid #ddd; }
        .sidebar-title { font-size: 1.1rem; font-weight: 700; color: #333; margin-bottom: 0.5rem; border-left: 4px solid #007bff; padding-left: 10px; }
        
        /* 导航按钮 */
        .nav-block [data-testid="stVerticalBlock"] div.stButton > button { 
            width: 100% !important; height: 48px !important; 
            font-size: 16px !important; font-weight: 600 !important; 
            border: 1.5px solid #000 !important; border-radius: 8px !important;
            background-color: #fff !important; color: #000 !important; margin-bottom: 10px !important;
        }
        .nav-block div.stButton > button:hover { background-color: #f1f3f5 !important; border-color: #007bff !important; }
        
        /* 数据指标卡片 */
        .info-card { background-color: #ffffff !important; border: 1px solid #e0e0e0 !important; border-left: 8px solid #28a745 !important; padding: 20px !important; border-radius: 8px !important; margin-bottom: 20px !important; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }
        .info-card h4 { color: #555 !important; margin-top: 0 !important; font-size: 16px !important; }
        .info-card p { font-size: 24px !important; font-weight: 800 !important; color: #333 !important; margin: 5px 0 !important; }
        
        /* 日志折叠 */
        .log-box { background-color: #212529; color: #a5d6a7; padding: 10px; border-radius: 6px; font-family: 'Courier New', monospace; font-size: 12px; height: 200px; overflow-y: auto; }
        </style>
        """, unsafe_allow_html=True)

set_ui_v146()

# --- 5. 侧边栏布局重构 ---

with st.sidebar:
    # 模块 1：模式确定
    st.markdown('<div class="sidebar-title">模式切换</div>', unsafe_allow_html=True)
    st.session_state['viewport'] = st.selectbox("当前操作角色", ["管理员模式", "梦蕊模式", "依蕊模式"], label_visibility="collapsed")
    st.divider()

    # 模块 2：导航中心
    st.markdown('<div class="sidebar-title">功能导航</div>', unsafe_allow_html=True)
    st.markdown('<div class="nav-block">', unsafe_allow_html=True)
    if st.button("📊 派单看板中心"): st.session_state['page'] = "派单看板"
    if st.button("📂 客户资料管理"): st.session_state['page'] = "数据管理"
    if st.button("❓ 使用指南"): st.session_state['page'] = "帮助"
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()

    # 模块 3：服务参数配置
    st.markdown('<div class="sidebar-title">服务参数</div>', unsafe_allow_html=True)
    # 日期快捷键
    td = datetime.now().date(); c1, c2 = st.columns(2)
    with c1:
        if st.button("今天"): st.session_state['r'] = (td, td + timedelta(days=1))
        if st.button("本月"): st.session_state['r'] = (td.replace(day=1), td.replace(day=calendar.monthrange(td.year, td.month)[1]) + timedelta(days=1))
    with c2:
        if st.button("明天"): st.session_state['r'] = (td + timedelta(days=1), td + timedelta(days=2))
        if st.button("本周"): st.session_state['r'] = (td - timedelta(days=td.weekday()), td + timedelta(days=(6-td.weekday())+1))
    st.session_state['r'] = st.date_input("派单分析周期", value=st.session_state['r'])

    # 地点模式
    st.markdown("**出发点设置**")
    addrs = ["深圳市龙华区 潜龙花园 4A 栋", "乐荟中心", "星河world 二期 c 栋", "自定义输入..."]
    sel_addr = st.selectbox("选择起始地址", addrs, label_visibility="collapsed")
    if sel_addr == "自定义输入...": st.session_state['departure_point'] = st.text_input("请输入详细地址", value="深圳市")
    else: st.session_state['departure_point'] = sel_addr
    
    st.markdown("**机动出行方式**")
    mode_sel = st.radio("交通工具", ["步行", "电动车/骑行", "地铁/公交"], index=1, label_visibility="collapsed")
    st.session_state['travel_mode'] = {"步行": "Walking", "电动车/骑行": "Riding", "地铁/公交": "Transfer"}[mode_sel]
    st.divider()

    # 模块 4：影子日志 (折叠)
    with st.expander("🛠️ 系统运行日志", expanded=False):
        logs = "\n".join(st.session_state['system_logs'][-30:])
        st.markdown(f'<div class="log-box">{logs}</div>', unsafe_allow_html=True)
        if st.button("清理日志"): st.session_state['system_logs'] = []; st.rerun()

# --- 6. 数据服务中心 ---

@st.cache_resource(ttl=7200)
def get_feishu_session():
    """高性能令牌管理器"""
    try:
        r = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10).json()
        return r.get("tenant_access_token")
    except: return None

def fetch_data_v146():
    token = get_feishu_session()
    if not token: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    try:
        r = st.session_state.http_session.get(url, headers={"Authorization": f"Bearer {token}"}, params={"page_size": 500}, timeout=15).json()
        df = pd.DataFrame([dict(i['fields'], _id=i['record_id']) for i in r.get("data", {}).get("items", [])])
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        return df
    except: return pd.DataFrame()

if st.session_state['feishu_cache'] is None: st.session_state['feishu_cache'] = fetch_data_v146()

if st.session_state['page'] == "数据管理":
    st.title("📂 客户资料与服务对账中心")
    df = st.session_state['feishu_cache'].copy()
    if not df.empty:
        # 财务计费核心逻辑
        st.subheader("服务计费汇总 (159单标准)")
        if isinstance(st.session_state['r'], tuple) and len(st.session_state['r']) >= 2:
            def calc(row):
                try:
                    s, e = pd.to_datetime(row['服务开始日期']).date(), pd.to_datetime(row['服务结束日期']).date()
                    freq = int(row.get('投喂频率', 1))
                    a_s, a_e = max(s, st.session_state['r'][0]), min(e, st.session_state['r'][1])
                    if a_s > a_e: return 0
                    return sum(1 for d in range((a_e-a_s).days + 1) if (a_s + timedelta(days=d) - s).days % freq == 0)
                except: return 0
            df['累计单量'] = df.apply(calc, axis=1)
            st.metric("分析周期内总派单数", f"{df['累计单量'].sum()} 次")
        st.dataframe(df[['宠物名字', '喂猫师', '详细地址', '订单状态']], use_container_width=True)
    if st.button("同步云端数据"): st.session_state['feishu_cache'] = None; st.rerun()

# --- 7. 派单看板：性能加速版 ---

elif st.session_state['page'] == "派单看板":
    st.title(f"猫咪派单平台 · {st.session_state['viewport']}")
    
    # 三键简洁控制
    c1, c2, c3, c4 = st.columns([1, 1, 1, 4])
    if c1.button("▶ 开始派单"): st.session_state['plan_state'] = "RUNNING"
    if c2.button("⏸ 暂停计算"): st.session_state['plan_state'] = "PAUSED"
    if c3.button("↺ 重置平台"): 
        st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        # IndexError 安全锁
        if not isinstance(st.session_state['r'], tuple) or len(st.session_state['r']) < 2:
            st.warning("请在侧边栏选择完整的【起始日期】和【结束日期】。")
            st.session_state['plan_state'] = "IDLE"; st.stop()

        df_raw = st.session_state['feishu_cache'].copy()
        if not df_raw.empty:
            prog = st.progress(0.0, text="正在同步路网数据...")
            with st.status("正在进行空间聚类与动态路径规划...", expanded=True) as status:
                # 复位 V99 空间聚类逻辑 (同楼不拆单)
                active_sitters = ["梦蕊", "依蕊"]
                df_raw['building_fp'] = df_raw['详细地址'].apply(get_normalized_address_v146)
                s_load = {s: 0 for s in active_sitters}
                unassigned = ~df_raw.get('喂猫师', '').isin(active_sitters)
                if unassigned.any():
                    for _, g in df_raw[unassigned].groupby('building_fp'):
                        best = min(s_load, key=s_load.get); df_raw.loc[g.index, '喂猫师'] = best; s_load[best] += len(g)
                
                # 时间轴穿透
                days = pd.date_range(st.session_state['r'][0], st.session_state['r'][1]).tolist()
                all_plans = []
                for idx, d in enumerate(days):
                    if st.session_state['plan_state'] == "PAUSED": break
                    prog.progress((idx+1)/len(days), text=f"分析日期: {d.strftime('%Y-%m-%d')}")
                    ct = pd.Timestamp(d); d_v = df_raw[(df_raw['服务开始日期'] <= ct) & (df_raw['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        d_v = d_v[d_v.apply(lambda r: (ct-r['服务开始日期']).days % int(r.get('投喂频率',1)) == 0, axis=1)]
                        if not d_v.empty:
                            with ThreadPoolExecutor(max_workers=5) as ex:
                                coords = list(ex.map(get_coords_v146, d_v['详细地址']))
                            d_v[['lng', 'lat']] = pd.DataFrame([ [c[0][0], c[0][1]] if c[0] else [None, None] for c in coords ], index=d_v.index, columns=['lng', 'lat'])
                            for s in active_sitters:
                                stks = d_v[d_v['喂猫师'] == s].copy()
                                if not stks.empty:
                                    res = optimize_route_v146(stks, st.session_state['travel_mode'], s, d.strftime('%Y-%m-%d'), st.session_state['departure_point'])
                                    res['作业日期'] = d.strftime('%Y-%m-%d'); all_plans.append(res)
                st.session_state['fp'] = pd.concat(all_plans) if all_plans else None
                status.update(label="派单路径计算完成！", state="complete")
                st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        vd = st.selectbox("选择服务日期", sorted(st.session_state['fp']['作业日期'].unique()))
        day_all = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        vs = "全部" if "管理员" in st.session_state['viewport'] else ("梦蕊" if "梦蕊" in st.session_state['viewport'] else "依蕊")
        v_data = day_all if vs == "全部" else day_all[day_all['喂猫师'] == vs]
        
        # 精简态势卡片
        c1, c2 = st.columns(2); show_s = ["梦蕊", "依蕊"] if vs == "全部" else [vs]
        for i, s in enumerate(show_s):
            stats = st.session_state['commute_stats'].get(f"{vd}_{s}", {"dist": 0, "dur": 0})
            with [c1, c2][i%2]:
                st.markdown(f"""<div class="info-card"><h4>{s} 派单统计</h4><p>单量：{len(day_all[day_all['喂猫师']==s])} 单</p><p style="color:#28a745;">耗时：{int(stats['dur'])} 分钟</p><p>路程：{stats['dist']/1000:.2f} km</p></div>""", unsafe_allow_html=True)
        
        # 派单简报 (纠偏版)
        brief = [f"起始地点：{st.session_state['departure_point']}"]
        for _, r in v_data.iterrows():
            # 强制纠偏 ValueError
            n_dur = pd.to_numeric(r.get('next_dur', 0), errors='coerce'); n_dist = pd.to_numeric(r.get('next_dist', 0), errors='coerce')
            p_dur = pd.to_numeric(r.get('prev_dur', 0), errors='coerce')
            line = f"{int(r.get('拟定顺序', 0))}. {r.get('宠物名字', '小猫')}-{r.get('详细地址','深圳')}"
            if r['拟定顺序'] == 1 and p_dur > 0: line += f" (首段耗时 {int(p_dur)}分)"
            if n_dur > 0: line += f" ➝ (下站 {int(n_dist)}m, {int(n_dur)}分)"
            else: line += " (🏁 终点)"
            brief.append(line)
        st.text_area("服务行程简报 (含出征耗时):", "\n".join(brief), height=250)

        # 高效接力渲染地图
        map_clean = v_data.dropna(subset=['lng', 'lat']).copy()
        if not map_clean.empty:
            map_json = map_clean[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序']].to_dict('records')
            amap_html = f"""
            <div id="map_box" style="width:100%; height:600px; border:1px solid #ddd; border-radius:12px; background:#f8f9fa;"></div>
            <script type="text/javascript"> window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }}; </script>
            <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_KEY_JS}&plugin=AMap.Walking,AMap.Riding"></script>
            <script type="text/javascript">
                (function() {{
                    const data = {json.dumps(map_json)}; const colors = {{"梦蕊": "#007BFF", "依蕊": "#FFA500"}};
                    const map = new AMap.Map('map_box', {{ zoom: 14, center: [data[0].lng, data[0].lat] }});
                    data.forEach(m => {{
                        new AMap.Marker({{ position: [m.lng, m.lat], map: map,
                            content: `<div style="width:26px;height:26px;background:${{colors[m.喂猫师]}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:24px;font-size:12px;font-weight:bold;">${{m.拟定顺序}}</div>`
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
