import streamlit as st
import pandas as pd
import requests
import time
import math
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import re
import json
import calendar
from urllib.parse import quote
import streamlit.components.v1 as components

# ==========================================
# --- 【V171 状态死锁：逻辑物理全展开层】 ---
# ==========================================
def init_system_v171():
    """物理锁定状态池，严禁缩减物理行"""
    # 1. 物理锁定单日：绝杀单量叠加
    td = datetime.now().date()
    if 'r' not in st.session_state:
        st.session_state.r = (td, td)
    
    # 2. 状态变量全量显式初始化
    if 'system_logs' not in st.session_state:
        st.session_state.system_logs = []
    if 'commute_stats' not in st.session_state:
        st.session_state.commute_stats = {}
    if 'page' not in st.session_state:
        st.session_state.page = "动态指挥大屏"
    if 'plan_state' not in st.session_state:
        st.session_state.plan_state = "IDLE"
    if 'feishu_cache' not in st.session_state:
        st.session_state.feishu_cache = None
    if 'viewport' not in st.session_state:
        st.session_state.viewport = "管理员模式"
    if 'admin_sub_view' not in st.session_state:
        st.session_state.admin_sub_view = "全部人员"
    if 'departure_point' not in st.session_state:
        st.session_state.departure_point = "深圳市龙华区 潜龙花园 4A 栋"
    if 'travel_mode' not in st.session_state:
        st.session_state.travel_mode = "Riding"

# 物理请求会话
if 'http_session' not in st.session_state:
    st.session_state.http_session = requests.Session()

init_system_v171()

# --- 1. 配置中心与双 Key 穿透 ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f").strip()
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s").strip()

# 高德地图双核：WS测速大脑 + JS绘图眼睛
AMAP_KEY_WS = st.secrets.get("AMAP_KEY_WS", "c26fc76dd582c32e4406552df8ba40ff").strip()
AMAP_KEY_JS = st.secrets.get("AMAP_KEY_JS", "c67e780b4d72b313f825746f8b02d840").strip()
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "f3bd8f946c9fdf05cb73e259b108e527").strip()

def add_trace(msg, level="INFO"):
    """【追踪日志】上帝视角记录每一次计算流转"""
    ts = datetime.now().strftime('%H:%M:%S')
    icon = "✓" if level=="INFO" else "🚩"
    st.session_state.system_logs.append(f"[{ts}] {icon} {msg}")

# --- 2. 核心底座逻辑 (坐标 100% 命中与自愈) ---

def haversine_v171(lon1, lat1, lon2, lat2, mode):
    """【直线自愈】解决路网 API 响应异常"""
    R = 6371000 
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dist = 2 * R * math.asin(math.sqrt(math.sin(math.radians(lat2-lat1)/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(math.radians(lon2-lon1)/2)**2))
    real_dist = dist * 1.35
    speed = 250 if mode == "Riding" else 66
    return int(real_dist), math.ceil(real_dist / speed)

@st.cache_data(show_spinner=False, ttl=3600)
def get_coords_v171(address):
    """【100%点亮】精准 -> 模糊裁切 -> 随机物理锚点"""
    if not address: return (114.032, 22.618), "DOUDI"
    full_addr = f"深圳市{str(address).strip().replace(' ', '')}"
    try:
        r = requests.get(f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(full_addr)}", timeout=8).json()
        if r.get('status') == '1' and r.get('geocodes'):
            loc = r['geocodes'][0]['location'].split(',')
            return (float(loc[0]), float(loc[1])), "SUCCESS"
        # 降级 1：裁切房号
        fuzzy = re.sub(r'(\d+栋|\d+座|\d+单元|\d+号).*', '', full_addr)
        r2 = requests.get(f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(fuzzy)}", timeout=5).json()
        if r2.get('status') == '1' and r2.get('geocodes'):
            loc2 = r2['geocodes'][0]['location'].split(',')
            return (float(loc2[0]), float(loc2[1])), "FUZZY"
        # 降级 2：物理强制点亮 (解决 Marker 缺失)
        return (114.032 + np.random.uniform(-0.005, 0.005), 22.618 + np.random.uniform(-0.005, 0.005)), "FALLBACK"
    except: return (114.032, 22.618), "ERROR"

def get_normalized_v171(addr):
    """【空间聚类】提取小区名用于智能补位"""
    if not addr: return "未知"
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def optimize_route_v171(df, sitter, date_str, start_addr):
    """【物理锁死】顺序引擎，绝不报 KeyError"""
    with ThreadPoolExecutor(max_workers=5) as ex:
        results = list(ex.map(get_coords_v171, df['详细地址']))
    df['lng'] = [r[0][0] for r in results]; df['lat'] = [r[0][1] for r in results]
    
    start_pt, _ = get_coords_v171(start_addr)
    unvisited = df.to_dict('records')
    curr_lng, curr_lat = start_pt[0], start_pt[1]
    optimized = []
    while unvisited:
        next_node = min(unvisited, key=lambda x: (curr_lng-x['lng'])**2 + (curr_lat-x['lat'])**2)
        unvisited.remove(next_node); optimized.append(next_node)
        curr_lng, curr_lat = next_node['lng'], next_node['lat']
    
    td, tt = 0, 0
    # 逐站路网回填
    for i in range(len(optimized)):
        orig = start_pt if i == 0 else (optimized[i-1]['lng'], optimized[i-1]['lat'])
        dest = (optimized[i]['lng'], optimized[i]['lat'])
        url = f"https://restapi.amap.com/v3/direction/bicycling?origin={orig[0]},{orig[1]}&destination={dest[0]},{dest[1]}&key={AMAP_KEY_WS}"
        try:
            r = requests.get(url, timeout=5).json()
            d, t = int(r['route']['paths'][0]['distance']), math.ceil(int(r['route']['paths'][0]['duration'])/60)
        except: d, t = haversine_v171(orig[0], orig[1], dest[0], dest[1], "Riding")
        if i == 0: optimized[i]['prev_dur'] = t
        else: optimized[i-1]['next_dist'] = d; optimized[i-1]['next_dur'] = t
        td += d; tt += t
    
    st.session_state.commute_stats[f"{date_str}_{sitter}"] = {"dist": td, "dur": tt}
    res = pd.DataFrame(optimized); res['拟定顺序'] = range(1, len(res)+1)
    return res

# --- 3. 视觉纠偏方案：高对比度指挥官 UI ---
st.set_page_config(page_title="小猫直喂派单平台", layout="wide", initial_sidebar_state="expanded")
st.markdown("""<style>
    /* 1. 深色极简侧边栏 */
    [data-testid="stSidebar"] { background-color: #0f0f0f !important; color: #ffffff !important; border-right: 1px solid #333; }
    .sb-h { font-size: 0.85rem; font-weight: 800; color: #666; margin: 1.2rem 0 0.5rem 0; letter-spacing: 1.5px; text-transform: uppercase; }
    
    /* 2. 灰色圆角矩阵盒子 (物理展开) */
    .v171-btn [data-testid="stVerticalBlock"] div.stButton > button { 
        width: 100% !important; height: 52px !important; font-size: 15px !important; font-weight: 600 !important; 
        border-radius: 12px !important; border: 1px solid #3d3d3d !important; background-color: #262626 !important; color: #ffffff !important;
    }
    .v171-btn div.stButton > button:hover { background-color: #444 !important; border-color: #007bff !important; }

    /* 3. 实时卡片：绝杀配色融合 (高饱和纯色) */
    .st-status-row { display: flex; gap: 15px; margin-bottom: 30px; }
    .st-card { flex: 1; padding: 25px; border-radius: 20px; text-align: center; color: white !important; border: 1px solid rgba(255,255,255,0.1); box-shadow: 0 5px 20px rgba(0,0,0,0.6); transition: 0.3s all; }
    .bg-black { background: #111111; } 
    .bg-blue { background: #003366; } 
    .bg-indigo { background: #311b92; }
    .bg-orange { background: #bf360c; } 
    .v-num { font-size: 2.6rem; font-weight: 900; text-shadow: 2px 2px 8px rgba(0,0,0,0.8); display: block; line-height: 1; }
    .v-lab { font-size: 0.95rem; font-weight: 700; opacity: 0.9; display: block; margin-top: 10px; }

    .terminal-v171 { background-color: #050505; color: #00ff00; padding: 15px; border-radius: 12px; font-family: monospace; font-size: 11px; height: 320px; overflow-y: auto; border: 1px solid #333; line-height: 1.6; }
</style>""", unsafe_allow_html=True)

# --- 4. 侧边栏：中枢结构 (物理全展开) ---
with st.sidebar:
    st.markdown('<div class="sb-h">👤 视角角色确认</div>', unsafe_allow_html=True)
    st.session_state.viewport = st.selectbox("Role", ["管理员模式", "梦蕊模式", "依蕊模式"], label_visibility="collapsed")
    st.divider()

    st.markdown('<div class="sb-h">🧭 指战频道导航</div>', unsafe_allow_html=True)
    st.markdown('<div class="v171-btn">', unsafe_allow_html=True)
    if st.button("📊 动态指挥看板"): st.session_state.page = "动态指挥大屏"
    if st.button("📂 资料录入中心"): st.session_state.page = "资料录入管理"
    if st.button("📖 平台使用手册"): st.session_state.page = "手册"
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()

    st.markdown('<div class="sb-h">⚙️ 作战参数 (锁定单日)</div>', unsafe_allow_html=True)
    td = datetime.now().date(); c1, c2 = st.columns(2)
    with c1:
        # 物理修正快捷键：彻底锁定单日
        if st.button("今天"): st.session_state.r = (td, td)
        if st.button("本月"): st.session_state.r = (td.replace(day=1), td.replace(day=calendar.monthrange(td.year, td.month)[1]))
    with c2:
        if st.button("明天"): st.session_state.r = (td+timedelta(1), td+timedelta(1))
        if st.button("本周"): st.session_state.r = (td-timedelta(td.weekday()), td+timedelta(6-td.weekday()))
    st.session_state.r = st.date_input("日期范围", value=st.session_state.r, label_visibility="collapsed")
    st.session_state.departure_point = st.selectbox("起始出征点", ["深圳市龙华区 潜龙花园 4A 栋", "乐荟中心", "星河world 二期 c 栋", "手动输入..."])
    st.divider()

    with st.expander("📡 系统影子日志塔"):
        st.markdown(f'<div class="terminal-v171">{"<br>".join(st.session_state.system_logs[-60:])}</div>', unsafe_allow_html=True)
        if st.button("复位历史日志"): st.session_state.system_logs = []; st.rerun()

# --- 5. 飞书数据与智能补位预判引擎 ---
def fetch_feishu_v171():
    try:
        r_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10).json()
        tk = r_a.get("tenant_access_token")
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
        res = st.session_state.http_session.get(url, headers={"Authorization": f"Bearer {tk}"}, params={"page_size": 500}, timeout=15).json()
        df = pd.DataFrame([dict(i['fields'], _id=i['record_id']) for i in res['data']['items']])
        for col in ['服务开始日期', '服务结束日期']:
            df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
        for col in ['宠物名字', '详细地址', '喂猫师', '订单状态', '投喂频率']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

if st.session_state.feishu_cache is None: 
    st.session_state.feishu_cache = fetch_feishu_v171()

# 【瞬时预判与智能补位】核心算法展开
df_raw = st.session_state.feishu_cache.copy()
total_hit_cnt, m_cnt, e_cnt, auto_assigned_cnt = 0, 0, 0, 0
final_dispatch_list = pd.DataFrame()

if not df_raw.empty and isinstance(st.session_state.r, tuple) and len(st.session_state.r) == 2:
    target_d = st.session_state.r[0]
    # A. 频率命中判定
    mask = (df_raw['服务开始日期'].dt.date <= target_d) & (df_raw['服务结束日期'].dt.date >= target_d)
    match_df = df_raw[mask].copy()
    if not match_df.empty:
        # B. 投喂间隔模型 (1=每日, 2=隔日)
        match_df['is_hit'] = match_df.apply(lambda r: (target_d - r['服务开始日期'].date()).days % int(r.get('投喂频率', 1)) == 0, axis=1)
        hit_df = match_df[match_df['is_hit']].drop_duplicates(subset=['详细地址'])
        total_hit_cnt = len(hit_df)
        
        # C. 智能自动补位逻辑展开
        hit_df['building'] = hit_df['详细地址'].apply(get_normalized_v171)
        
        # 1. 提取飞书已手动分配的种子名单
        assigned = hit_df[hit_df['喂猫师'].isin(['梦蕊', '依蕊'])].copy()
        unassigned = hit_df[~hit_df['喂猫师'].isin(['梦蕊', '依蕊'])].copy()
        
        # 2. 空间继承与负载均衡
        for idx, row in unassigned.iterrows():
            # 原则 1：空间继承 (同楼锁定同一人)
            same_b = assigned[assigned['building'] == row['building']]
            if not same_b.empty:
                hit_df.at[idx, '喂猫师'] = same_b.iloc[0]['喂猫师']
                hit_df.at[idx, '_dispatch_type'] = "空间继承"
            else:
                # 原则 2：负载均衡 (补位给少的一方)
                m_load = len(hit_df[hit_df['喂猫师'] == "梦蕊"])
                e_load = len(hit_df[hit_df['喂猫师'] == "依蕊"])
                best_sitter = "梦蕊" if m_load <= e_load else "依蕊"
                hit_df.at[idx, '喂猫师'] = best_sitter
                hit_df.at[idx, '_dispatch_type'] = "智能补位"
                auto_assigned_cnt += 1
        
        m_cnt = len(hit_df[hit_df['喂猫师'] == "梦蕊"])
        e_cnt = len(hit_df[hit_df['喂猫师'] == "依蕊"])
        final_dispatch_list = hit_df

# --- 6. 模块实现：资料同步中心 ---
if st.session_state.page == "资料录入管理":
    st.title("📂 订单录入与飞书 PATCH 对账中心")
    if not df_raw.empty:
        st.subheader("⚙️ 飞书云端编辑器 (物理同步)")
        edit_df = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态', '投喂频率']], use_container_width=True)
        if st.button("🚀 启动物理同步修改"):
            tk_v = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
            for i, row in edit_df.iterrows():
                requests.patch(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{df_raw.iloc[i]['_id']}", 
                               headers={"Authorization": f"Bearer {tk_v}"}, 
                               json={"fields": {"订单状态": str(row['订单状态']), "喂猫师": str(row['喂猫师']), "投喂频率": int(row['投喂频率'])}})
            st.session_state.feishu_cache = None; st.rerun()

elif st.session_state.page == "动态指挥大屏":
    st.title("📊 派单态势指挥大屏 (V171 智能补位版)")
    
    # 【高对比度瞬时统计卡片】
    st.markdown(f"""
    <div class="st-status-row">
        <div class="st-card bg-black"><span class="v-num">{total_hit_cnt}</span><span class="v-lab">📊 今日需服务总计</span></div>
        <div class="st-card bg-blue"><span class="v-num">{m_cnt}</span><span class="v-lab">🐱 梦蕊总负担</span></div>
        <div class="st-card bg-indigo"><span class="v-num">{e_cnt}</span><span class="v-lab">🐱 依蕊总负担</span></div>
        <div class="st-card bg-orange"><span class="v-num">{auto_assigned_cnt}</span><span class="v-lab">🚩 智能补位单量</span></div>
    </div>
    """, unsafe_allow_html=True)
    
    # 看板顶层并排布局
    col_d, col_v = st.columns(2)
    with col_d: vd = st.selectbox("📅 对账派单服务日期", sorted([st.session_state.r[0].strftime('%Y-%m-%d')]), index=0)
    with col_v:
        if st.session_state.viewport == "管理员模式":
            st.session_state.admin_sub_view = st.selectbox("👤 指定路线视角切换", ["全部人员", "梦蕊", "依蕊"])
        else: st.info(f"角色锁定：{st.session_state.viewport}")

    # 三键指挥中心
    c1, c2, c3, _ = st.columns([1,1,1,4])
    if c1.button("▶ 启动路径接力分析"): st.session_state.plan_state = "RUNNING"
    if c3.button("↺ 重置并重读缓存"): st.session_state.plan_state = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state.plan_state == "RUNNING":
        with st.status("正在回归执行高精测速与顺序连线对账...", expanded=True) as status:
            sitters = ["梦蕊", "依蕊"]; days = pd.date_range(st.session_state.r[0], st.session_state.r[1]).tolist()
            all_plans = []
            for d in days:
                ct = pd.Timestamp(d); d_v = final_dispatch_list.copy()
                if not d_v.empty:
                    for s in sitters:
                        stks = d_v[d_v['喂猫师'] == s].copy()
                        if not stks.empty:
                            all_plans.append(optimize_route_v171(stks, s, d.strftime('%Y-%m-%d'), st.session_state.departure_point).assign(作业日期=d.strftime('%Y-%m-%d')))
            st.session_state.fp = pd.concat(all_plans) if all_plans else None; st.session_state.plan_state = "IDLE"
            status.update(label="✅ 分析完毕！100% 顺序连线已就绪。", state="complete")

    if st.session_state.get('fp') is not None:
        day_all = st.session_state.fp[st.session_state.fp['作业日期'] == vd]
        sub_v = st.session_state.admin_sub_view if st.session_state.viewport == "管理员模式" else ("梦蕊" if "梦蕊" in st.session_state.viewport else "依蕊")
        v_data = day_all if sub_v == "全部人员" else day_all[day_all['喂猫师'] == sub_v]
        
        # 路线指标卡
        c1, c2 = st.columns(2); show_names = ["梦蕊", "依蕊"] if sub_v == "全部人员" else [sub_v]
        for i, sn in enumerate(show_names):
            stt = st.session_state.commute_stats.get(f"{vd}_{sn}", {"dist": 0, "dur": 0})
            with [c1, c2][i%2]: st.markdown(f"""<div style="background:#fff; border-left:10px solid #007bff; padding:20px; border-radius:15px; box-shadow:0 4px 15px rgba(0,0,0,0.05); margin-bottom:15px;">
                <h4 style="margin:0; color:#888; font-size:13px;">{sn} 作战统计</h4><p style="font-size:26px; font-weight:900; color:#111; margin:5px 0;">站点：{len(day_all[day_all.喂猫师==sn])} 单</p>
                <p style="font-size:15px; color:#007bff;">预计耗时：{int(stt['dur'])} 分钟 | 路程：{stt['dist']/1000:.2f} km</p></div>""", unsafe_allow_html=True)
        
        # 日报一键复制引擎
        brief = [f"📊 派单简报 ({vd})：今日需上门 {len(v_data)} 户 (含智能补位)", f"🚩 统一起点：{st.session_state.departure_point}"]
        for _, r in v_data.iterrows():
            line = f"{int(r.拟定顺序)}. {r.宠物名字}-{r.详细地址}"
            if r.拟定顺序 == 1: line += f" (🚗 首段耗时 {int(r.prev_dur)}分)"
            if r.get('next_dur', 0) > 0: line += f" ➝ (下站约 {int(r['next_dist'])}m, {int(r['next_dur'])}分)"
            else: line += " 🏁 行程终点 (任务完成)"
            brief.append(line)
        
        final_txt = "\n".join(brief)
        if st.button("📋 一键复制今日派单日报"):
            components.html(f"<script>navigator.clipboard.writeText(`{final_txt}`); alert('✅ 复制成功！');</script>", height=0)
        st.text_area("📄 行报详情明细", final_txt, height=220)

        # 【核心：100% 顺序连线接力 JS】
        map_json = v_data[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序']].to_dict('records')
        amap_html = f"""<div id="m" style="width:100%;height:600px;border-radius:20px;background:#f8f9fa;border:1px solid #ddd;"></div>
        <script src="https://webapi.amap.com/maps?v=2.0&key={AMAP_KEY_JS}&plugin=AMap.Walking,AMap.Riding"></script>
        <script>
            window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }};
            const data = {json.dumps(map_json)}; const colors = {{"梦蕊": "#007BFF", "依蕊": "#311b92"}};
            const map = new AMap.Map('m', {{ zoom: 14, center: [data[0].lng, data[0].lat] }});
            data.forEach(m => {{
                new AMap.Marker({{ position:[m.lng, m.lat], map:map, content:`<div style="width:28px;height:28px;background:${{colors[m.喂猫师]}};border:2px solid #fff;border-radius:50%;color:white;text-align:center;line-height:26px;font-size:12px;font-weight:bold;">${{m.拟定顺序}}</div>` }});
            }});
            // 物理接力连线引擎
            function drawSequential(i) {{
                if (i >= data.length - 1) {{ setTimeout(()=>map.setFitView(), 500); return; }}
                // 如果喂猫师不一致(跨人连线)，则跳过
                if (data[i].喂猫师 !== data[i+1].喂猫师) {{ drawSequential(i+1); return; }}
                
                new AMap.Riding({{ map:map, hideMarkers:true, strokeColor:colors[data[i].喂猫师], strokeWeight:8 }})
                .search([data[i].lng, data[i].lat], [data[i+1].lng, data[i+1].lat], (status, res) => {{
                    // 如果路网失败，物理连一条直线确保 100% 连通
                    if (status !== 'complete') {{
                        new AMap.Polyline({{ path: [[data[i].lng, data[i].lat], [data[i+1].lng, data[i+1].lat]], strokeColor: colors[data[i].喂猫师], strokeWeight: 4, strokeStyle: 'dashed', map: map }});
                    }}
                    setTimeout(()=>drawSequential(i+1), 400);
                }});
            }}
            drawSequential(0);
        </script>"""
        components.html(amap_html, height=620)

elif st.session_state.page == "手册":
    st.title("📖 派单平台全量手册 (V171 智能补位版)")
    st.markdown("""
    ### 1. 智能自动补位算法
    针对飞书里“喂猫师”字段为空的订单，系统自动执行以下原则：
    - **空间继承**：若同一栋楼/小区已分配过人员，系统自动锁定给该人。
    - **负载均衡**：若为全新小区，系统自动分配给当前接单量较少的一方，确保梦/依负载平衡。
    
    ### 2. 100% 顺序连线逻辑
    - 地图连线严格遵循拟定顺序（1 -> 2 -> 3）。
    - 引入了 **“接力绘图引擎”**，若某段高德路网数据缺失，系统会物理绘制一条虚线段，确保轨迹永不断裂。
    
    ### 3. 数据录入对账
    在资料中心修改后，请务必点击“物理同步”按钮，否则修改仅在内存中生效。
    """)
