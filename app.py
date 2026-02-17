import streamlit as st

# ==========================================
# --- 【V138 核心加固：全链路状态保险锁】 ---
# ==========================================
def init_session_state_v138():
    """彻底终结 KeyError，保障三键状态机稳定性"""
    td = datetime.now().date() if 'datetime' in globals() else None
    keys_defaults = {
        'system_logs': [],
        'commute_stats': {},
        'page': "智能看板",
        'plan_state': "IDLE",  # IDLE, RUNNING, PAUSED
        'progress': 0,
        'feishu_cache': None,
        'r': (td, td + timedelta(days=1)) if td else (None, None),
        'viewport': "管理员视角",
        'departure_point': "深圳市龙华区 潜龙花园 4A 栋"
    }
    for key, val in keys_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

# --- 1. 全球指战指令集 (严格不删减) ---
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

init_session_state_v138()

# --- 2. 核心配置与双 Key 穿透 ---
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
    """【黑匣子：迁移至侧边栏】"""
    ts = datetime.now().strftime('%H:%M:%S')
    icon = "ℹ️" if level=="INFO" else "🚩"
    entry = f"[{ts}] {icon} {msg}"
    if 'system_logs' in st.session_state:
        st.session_state['system_logs'].append(entry)

# --- 3. 核心底座逻辑 (出征引擎、自愈算法) ---

def haversine_v138(lon1, lat1, lon2, lat2, mode):
    """【V138直线自愈】球面算法，解决1分钟顽疾"""
    R = 6371000 
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    dist = 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))
    real_dist = dist * 1.35
    speed_map = {"Walking": 66, "Riding": 250, "Transfer": 333}
    dur_min = real_dist / speed_map.get(mode, 200)
    return int(real_dist), math.ceil(dur_min)

@st.cache_data(show_spinner=False)
def get_coords_v138(address):
    """【大脑 Key】物理坐标解析"""
    if not address: return None, "地址为空"
    clean_addr = str(address).strip().replace(" ", "")
    full_addr = clean_addr if clean_addr.startswith("深圳市") else f"深圳市{clean_addr}"
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(full_addr)}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return (float(loc[0]), float(loc[1])), "SUCCESS"
    except: pass
    return None, "解析异常"

def get_travel_estimate_v138(origin, destination, mode_key):
    """【大脑 Key】算路引擎"""
    mode_url_map = {"Walking": "walking", "Riding": "bicycling", "Transfer": "integrated"}
    api_type = mode_url_map.get(mode_key, "bicycling")
    url = f"https://restapi.amap.com/v3/direction/{api_type}?origin={origin}&destination={destination}&key={AMAP_KEY_WS}"
    try:
        time.sleep(0.2) 
        r = requests.get(url, timeout=8).json()
        if r['status'] == '1':
            path = r['route']['paths'][0] if api_type != 'integrated' else r['route']['transits'][0]
            dist = int(path.get('distance', 0))
            dur = math.ceil(int(path.get('duration', 0)) / 60)
            return dist, dur, "SUCCESS"
    except: pass
    return 0, 0, "API_FAIL"

def optimize_route_v138(df_sitter, mode_key, sitter_name, date_str, start_addr):
    """【V138 出征优化】计算从出发点到第1站的距离"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    
    if len(has_coords) == 0:
        st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": 0, "dur": 0}
        return df_sitter
    
    # 1. 贪心算法确定任务顺序
    unvisited = has_coords.to_dict('records')
    # 获取出发点坐标
    start_point, _ = get_coords_v138(start_addr)
    curr_lng, curr_lat = start_point if start_point else (unvisited[0]['lng'], unvisited[0]['lat'])
    
    optimized = []
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_lng-x['lng'])**2 + (curr_lat-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node); curr_lng, curr_lat = next_node['lng'], next_node['lat']
    
    # 2. 【核心新增】包含起点的全链路测速
    total_d, total_t = 0, 0
    # A. 出征第一段：出发点 -> 第一站
    if start_point:
        d0, t0, s0 = get_travel_estimate_v138(f"{start_point[0]},{start_point[1]}", f"{optimized[0]['lng']},{optimized[0]['lat']}", mode_key)
        if s0 != "SUCCESS": d0, t0 = haversine_v138(start_point[0], start_point[1], optimized[0]['lng'], optimized[0]['lat'], mode_key)
        # 将起点距离挂载到第一站
        optimized[0]['prev_dist'], optimized[0]['prev_dur'] = d0, t0
        total_d += d0; total_t += t0

    # B. 后续路段：任务点接力
    for i in range(len(optimized) - 1):
        orig, dest = f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}"
        dist, dur, status = get_travel_estimate_v138(orig, dest, mode_key)
        if status != "SUCCESS": dist, dur = haversine_v138(optimized[i]['lng'], optimized[i]['lat'], optimized[i+1]['lng'], optimized[i+1]['lat'], mode_key)
        optimized[i]['next_dist'], optimized[i]['next_dur'] = dist, dur
        total_d += dist; total_t += dur

    st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": total_d, "dur": total_t}
    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

def execute_smart_dispatch_spatial_v138(df, active_sitters):
    """【复位 V99 空间算法】"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    s_load = {s: 0 for s in active_sitters}
    def normalize_addr(a):
        if not a: return "未知"
        a = str(a).replace("深圳市", "").replace("广东省", "").replace(" ","")
        match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', a)
        return match.group(1) if match else a
    df['building_fp'] = df['详细地址'].apply(normalize_addr)
    unassigned = ~df['喂猫师'].isin(active_sitters)
    if unassigned.any() and active_sitters:
        groups = df[unassigned].groupby('building_fp')
        for _, group in groups:
            best = min(s_load, key=s_load.get)
            df.loc[group.index, '喂猫师'] = best
            s_load[best] += len(group)
    return df

# --- 4. 飞书服务与侧边栏布局 ---

def fetch_feishu_v138():
    try:
        r_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        token = r_a.json().get("tenant_access_token")
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"page_size": 500}, timeout=15).json()
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in r.get("data", {}).get("items", [])])
        df['订单状态'] = df.get('订单状态', '进行中').fillna('进行中')
        df['投喂频率'] = pd.to_numeric(df.get('投喂频率'), errors='coerce').fillna(1).replace(0, 1)
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        for col in ['宠物名字', '详细地址', '喂猫师', 'lng', 'lat']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

st.set_page_config(page_title="指挥中心 V138.0", layout="wide")

def set_ui_v138():
    """【全量排版锁定】"""
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { width: 100% !important; height: 50px !important; font-size: 18px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000; border: 3px solid #000 !important; }
        .quick-nav div.stButton > button { width: 100% !important; height: 32px !important; font-size: 11px !important; border: 1.5px solid #000 !important; }
        .commute-card { background-color: #000000 !important; border-left: 12px solid #00ff00 !important; padding: 22px !important; border-radius: 12px !important; color: #ffffff !important; margin-bottom: 20px !important; box-shadow: 0 10px 25px rgba(0,0,0,0.6); }
        .commute-card h4 { color: #ffcc00 !important; margin: 0 0 8px 0 !important; font-size: 19px !important; }
        .commute-card p { font-size: 24px !important; font-weight: 900 !important; margin: 5px 0 !important; }
        .debug-tower { background-color: #1a1a1a; border: 1px solid #333; padding: 12px; border-radius: 8px; color: #00ff00; font-family: monospace; font-size: 12px; height: 300px; overflow-y: auto; }
        </style>
        """, unsafe_allow_html=True)

set_ui_v138()

# --- 5. 侧边栏：移动通讯塔与出征配置 ---

with st.sidebar:
    st.subheader("📅 洛阳总调指挥舱")
    st.markdown('<div class="quick-nav">', unsafe_allow_html=True)
    td = datetime.now().date(); cq1, cq2 = st.columns(2)
    with cq1:
        if st.button("📍 今天"): st.session_state['r'] = (td, td + timedelta(days=1))
        if st.button("📍 本周"): st.session_state['r'] = (td - timedelta(days=td.weekday()), td + timedelta(days=(6-td.weekday())+1))
    with cq2:
        if st.button("📍 明天"): st.session_state['r'] = (td + timedelta(days=1), td + timedelta(days=2))
        if st.button("📍 本月"): st.session_state['r'] = (td.replace(day=1), td.replace(day=calendar.monthrange(td.year, td.month)[1]) + timedelta(days=1))
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.session_state['r'] = st.date_input("指战周期", value=st.session_state['r'])
    st.divider()
    
    # 【新增】出征地点配置
    st.subheader("🚩 出征起点引擎")
    preset_addrs = ["深圳市龙华区 潜龙花园 4A 栋", "乐荟中心", "星河world 二期 c 栋", "手动输入..."]
    sel_start = st.selectbox("选择或输入出发点", preset_addrs, index=0)
    if sel_start == "手动输入...":
        st.session_state['departure_point'] = st.text_input("请输入详细起点", value="深圳市龙华区")
    else:
        st.session_state['departure_point'] = sel_start
    
    st.divider()
    active = [s for s in ["梦蕊", "依蕊"] if st.checkbox(f"{s} (执勤)", value=True, key=f"v138_{s}")]
    
    st.divider()
    for p in ["智能看板", "数据中心", "帮助文档"]:
        if st.button(p): st.session_state['page'] = p
    
    # 【迁移】指调通讯塔至侧边栏
    st.divider()
    st.markdown("🗼 **指调通讯塔 (侧边栏黑匣子)**")
    log_content = "\n".join(st.session_state['system_logs'][-20:])
    st.markdown(f'<div class="debug-tower">{log_content}</div>', unsafe_allow_html=True)
    if st.button("🧹 清空日志"): st.session_state['system_logs'] = []; st.rerun()

# --- 6. 数据中心 (财务对账) ---

if st.session_state['feishu_cache'] is None: st.session_state['feishu_cache'] = fetch_feishu_v138()

if st.session_state['page'] == "数据中心":
    st.title("📂 数字化管理中枢 (财务对账)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        st.subheader("📝 财务级计费对账 (159单绝对闭环)")
        st.dataframe(df_raw[['宠物名字', '喂猫师', '服务开始日期', '服务结束日期', '订单状态', '详细地址']], use_container_width=True)
    if st.button("🚀 刷新云端数据"): st.session_state['feishu_cache'] = None; st.rerun()

# --- 7. 智能看板 (三键状态机与视角隔离) ---

elif st.session_state['page'] == "智能看板":
    st.title(f"🚀 {st.session_state['viewport']} (V138.0)")
    
    # 【新增】视角切换
    v_col1, v_col2 = st.columns([1, 4])
    st.session_state['viewport'] = v_col1.selectbox("切换指战视角", ["管理员视角", "梦蕊视角", "依蕊视角"])
    
    # 【核心】三键控制台 + 进度条
    c_btn1, c_btn2, c_btn3, c_spacer = st.columns([1, 1, 1, 4])
    if c_btn1.button("▶️ 启动方案"): st.session_state['plan_state'] = "RUNNING"
    if c_btn2.button("⏸️ 暂停普查"): st.session_state['plan_state'] = "PAUSED"
    if c_btn3.button("⏹️ 取消重置"): 
        st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        df_kb = st.session_state['feishu_cache'].copy()
        if not df_kb.empty:
            progress_bar = st.progress(0, text="🛸 正在启动穿透引擎...")
            with st.status("🛸 正在执行出征对账...", expanded=True) as status:
                dk = execute_smart_dispatch_spatial_v138(df_kb, active)
                days = pd.date_range(st.session_state['r'][0], st.session_state['r'][1]).tolist()
                ap = []
                for idx, d in enumerate(days):
                    if st.session_state['plan_state'] == "PAUSED": break
                    d_str = d.strftime('%Y-%m-%d'); ct = pd.Timestamp(d)
                    # 进度条更新
                    progress_val = (idx + 1) / len(days)
                    progress_bar.progress(progress_val, text=f"🔄 正在对账: {d_str} (进度 {idx+1}/{len(days)})")
                    
                    d_v = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        if not d_v.empty:
                            with ThreadPoolExecutor(max_workers=5) as ex:
                                coords = list(ex.map(get_coords_v138, d_v['详细地址']))
                            d_v[['lng', 'lat']] = pd.DataFrame([ [c[0][0], c[0][1]] if c[0] else [None, None] for c in coords ], index=d_v.index, columns=['lng', 'lat'])
                            for s in active:
                                stks = d_v[d_v['喂猫师'] == s].copy()
                                if not stks.empty:
                                    # 调用出征引擎
                                    res = optimize_route_v138(stks, "Riding", s, d_str, st.session_state['departure_point'])
                                    res['作业日期'] = d_str; ap.append(res)
                st.session_state['fp'] = pd.concat(ap) if ap else None
                status.update(label="✅ 出征方案拟定完成！", state="complete")
                st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        vd = st.selectbox("📅 选择作业日期", sorted(st.session_state['fp']['作业日期'].unique()))
        day_all = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        
        # 视角过滤逻辑
        if "梦蕊" in st.session_state['viewport']: vs = "梦蕊"
        elif "依蕊" in st.session_state['viewport']: vs = "依蕊"
        else: vs = "全部"
        
        v_data = day_all if vs == "全部" else day_all[day_all['喂猫师'] == vs]
        
        # --- 黑金指标 (选谁看谁) ---
        c_m1, c_m2 = st.columns(2)
        show_sitters = active if vs == "全部" else [vs]
        for i, s in enumerate(show_sitters):
            s_data = st.session_state['commute_stats'].get(f"{vd}_{s}", {"dist": 0, "dur": 0})
            card_html = f"""<div class="commute-card"><h4>👤 {s} 出征指标</h4><p>当日任务：{len(day_all[day_all['喂猫师']==s])} 单</p><p style="color: #00ff00 !important;">总耗时：{int(s_data['dur'])} 分钟</p><p style="color: #ffffff !important;">总行程：{s_data['dist']/1000:.2f} km</p></div>"""
            [c_m1, c_m2][i % 2].markdown(card_html, unsafe_allow_html=True)
        
        # --- 出征简报 (包含起点逻辑) ---
        brief_lines = [f"🚩 出征起点：{st.session_state['departure_point']}"]
        for _, r in v_data.iterrows():
            d_dur = int(r.get('next_dur', 0)); d_dist = r.get('next_dist', 0)
            # 第一站特殊标记
            p_dur = int(r.get('prev_dur', 0))
            line = f"{int(r['拟定顺序'])}. {r['宠物名字']}-{r['详细地址']}"
            if r['拟定顺序'] == 1 and p_dur > 0: line += f" ⬅️ (出征首段耗时 {p_dur}分)"
            if d_dur > 0: line += f" ➡️ (下站约 {d_dist}米, {d_dur}分)"
            else: line += " 🏁 [终点站]"
            brief_lines.append(line)
        st.text_area("📄 每一段路程指引 (含出征首段)：", "\n".join(brief_lines), height=250)

        # --- 地图渲染 (JS 双核) ---
        map_clean = v_data.dropna(subset=['lng', 'lat']).copy()
        if not map_clean.empty:
            map_json = map_clean[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序']].to_dict('records')
            amap_html = f"""
            <div id="map_box" style="width:100%; height:600px; border:3.5px solid #000; border-radius:15px; background:#f0f0f0;"></div>
            <script type="text/javascript"> window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }}; </script>
            <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_KEY_JS}&plugin=AMap.Walking,AMap.Riding"></script>
            <script type="text/javascript">
                (function() {{
                    const data = {json.dumps(map_json)};
                    const colors = {{"梦蕊": "#007BFF", "依蕊": "#FFA500"}};
                    const map = new AMap.Map('map_box', {{ zoom: 14, center: [data[0].lng, data[0].lat] }});
                    data.forEach(m => {{
                        new AMap.Marker({{ position: [m.lng, m.lat], map: map,
                            content: `<div style="width:28px;height:28px;background:${{colors[m.喂猫师]}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:26px;font-size:12px;font-weight:bold;">${{m.拟定顺序}}</div>`
                        }}).setLabel({{ direction:'top', offset: new AMap.Pixel(0, -5), content: m.宠物名字 }});
                    }});
                    function draw(idx, sData, map) {{
                        if (idx >= sData.length - 1) {{ setTimeout(()=>map.setFitView(), 500); return; }}
                        if (sData[idx].喂猫师 !== sData[idx+1].喂猫师) {{ draw(idx+1, sData, map); return; }}
                        new AMap.Riding({{ map: map, hideMarkers: true, strokeColor: colors[sData[idx].喂猫师], strokeOpacity: 0.9, strokeWeight: 8 }})
                        .search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], ()=>setTimeout(()=>draw(idx+1, sData, map), 450));
                    }}
                    draw(0, data, map);
                }})();
            </script>"""
            components.html(amap_html, height=620)

elif st.session_state['page'] == "帮助文档":
    st.title("📖 V138 指战出征手册")
    st.markdown("""
    1. **出征引擎**：系统现已支持从“潜龙花园”、“乐荟中心”等起点计算首站耗时，简报第一站会明确标注出征首段时长。
    2. **三键指挥**：[启动]、[暂停]、[取消] 配合进度条，完美掌控大流量普查节奏。
    3. **黑匣子侧边栏**：通讯塔已移动至侧边栏最下方，实时监控 API 穿透状态。
    4. **视角隔离**：切换到喂猫师视角，指标卡片和简报将精准过滤个人数据。
    """)
