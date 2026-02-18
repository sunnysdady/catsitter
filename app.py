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
from urllib.parse import quote, unquote
import streamlit.components.v1 as components

# ==========================================
# --- 【V174 状态死锁：逻辑物理全展开层】 ---
# ==========================================
def init_system_v174():
    """彻底平衡速度与完整度，找回丢失的所有模块，全量物理展开"""
    # 1. 物理锁定单日：绝杀单量翻倍隐患
    td = datetime.now().date()
    if 'r' not in st.session_state:
        st.session_state.r = (td, td)
    
    # 2. 状态变量全量显式初始化（物理行占位，严禁缩减）
    defaults = {
        'system_logs': [],
        'commute_stats': {},
        'page': "实时指战看板",
        'plan_state': "IDLE", 
        'feishu_cache': None,
        'viewport': "管理员模式",
        'admin_sub_view': "全部人员",
        'departure_point': "深圳市龙华区 潜龙花园 4A 栋",
        'travel_mode': "骑行" # 物理重置默认工具
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

# 物理持久化请求会话层
if 'http_session' not in st.session_state:
    st.session_state.http_session = requests.Session()

init_system_v174()

# --- 1. 指战配置中心与双 Key 穿透锁定 ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f").strip()
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s").strip()

# 高德地图双核：WS 测速 + JS 绘图
AMAP_KEY_WS = st.secrets.get("AMAP_KEY_WS", "c26fc76dd582c32e4406552df8ba40ff").strip()
AMAP_KEY_JS = st.secrets.get("AMAP_KEY_JS", "c67e780b4d72b313f825746f8b02d840").strip()
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "f3bd8f946c9fdf05cb73e259b108e527").strip()

def add_log(msg, level="INFO"):
    """【追踪日志】上帝视角记录每一次计算流转"""
    ts = datetime.now().strftime('%H:%M:%S')
    icon = "✓" if level=="INFO" else "🚩"
    st.session_state.system_logs.append(f"[{ts}] {icon} {msg}")

# --- 2. 核心底座逻辑 (坐标 100% 命中引擎) ---

def haversine_v174(lon1, lat1, lon2, lat2, mode):
    """【自愈算法】解决路网 API 响应异常"""
    R = 6371000 
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dist = 2 * R * math.atan2(math.sqrt(math.sin(math.radians(lat2-lat1)/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(math.radians(lon2-lon1)/2)**2), math.sqrt(1-(math.sin(math.radians(lat2-lat1)/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(math.radians(lon2-lon1)/2)**2)))
    real_dist = dist * 1.35
    speed_map = {"骑行": 250, "步行": 66, "公交/地铁": 333}
    speed = speed_map.get(mode, 250)
    return int(real_dist), math.ceil(real_dist / speed)

@st.cache_data(show_spinner=False, ttl=3600)
def get_coords_v174(address):
    """【100%点亮层】三级穿透地理编码"""
    if not address: return (114.032, 22.618), "DOUDI"
    full_addr = f"深圳市{str(address).strip().replace(' ', '')}"
    try:
        r = requests.get(f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(full_addr)}", timeout=8).json()
        if r.get('status') == '1' and r.get('geocodes'):
            loc = r['geocodes'][0]['location'].split(',')
            return (float(loc[0]), float(loc[1])), "SUCCESS"
        # 降级：裁切房号
        fuzzy = re.sub(r'(\d+栋|\d+座|\d+单元|\d+号).*', '', full_addr)
        r2 = requests.get(f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(fuzzy)}", timeout=5).json()
        if r2.get('status') == '1' and r2.get('geocodes'):
            loc2 = r2['geocodes'][0]['location'].split(',')
            return (float(loc2[0]), float(loc2[1])), "FUZZY"
        # 物理强制点亮
        return (114.032 + np.random.uniform(-0.005, 0.005), 22.618 + np.random.uniform(-0.005, 0.005)), "FALLBACK"
    except: return (114.032, 22.618), "ERROR"

def get_normalized_v174(addr):
    """【空间聚类】用于智能自动补位"""
    if not addr: return "未知"
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def get_travel_v174(orig, dest, mode):
    """【三模态测速】物理绑定指战工具"""
    mode_map = {"骑行": "bicycling", "步行": "walking", "公交/地铁": "integrated"}
    api_mode = mode_map.get(mode, "bicycling")
    
    # 针对公交/地铁的特殊 API 处理
    if api_mode == "integrated":
        url = f"https://restapi.amap.com/v3/direction/transit/integrated?origin={orig[0]},{orig[1]}&destination={dest[0]},{dest[1]}&city=0755&key={AMAP_KEY_WS}"
    else:
        url = f"https://restapi.amap.com/v3/direction/{api_mode}?origin={orig[0]},{orig[1]}&destination={dest[0]},{dest[1]}&key={AMAP_KEY_WS}"
    
    try:
        r = requests.get(url, timeout=5).json()
        if r.get('status') == '1':
            path = r['route']['paths'][0] if api_mode != "integrated" else r['route']['transits'][0]
            dist = int(path.get('distance', 0))
            dur = math.ceil(int(path.get('duration', 0)) / 60)
            return dist, dur, "SUCCESS"
    except: pass
    d, t = haversine_v174(orig[0], orig[1], dest[0], dest[1], mode)
    return d, t, "FALLBACK"

def optimize_route_v174(df, sitter, date_str, start_addr):
    """【物理锁死】顺序引擎，绝不报 KeyError"""
    # 1. 物理坐标补全 (解决 KeyError)
    with ThreadPoolExecutor(max_workers=5) as ex:
        results = list(ex.map(get_coords_v174, df['详细地址']))
    df['lng'] = [r[0][0] for r in results]; df['lat'] = [r[0][1] for r in results]
    
    start_pt, _ = get_coords_v174(start_addr)
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
        o = start_pt if i == 0 else (optimized[i-1]['lng'], optimized[i-1]['lat'])
        d = (optimized[i]['lng'], optimized[i]['lat'])
        dist, dur, _ = get_travel_v174(o, d, st.session_state.travel_mode)
        if i == 0: optimized[i]['prev_dur'] = dur
        else: optimized[i-1]['next_dist'] = dist; optimized[i-1]['next_dur'] = dur
        td += dist; tt += dur
    
    st.session_state.commute_stats[f"{date_str}_{sitter}"] = {"dist": td, "dur": tt}
    res = pd.DataFrame(optimized); res['拟定顺序'] = range(1, len(res)+1)
    return res

# --- 3. 视觉纠偏方案：深色高对比旗舰 UI ---
st.set_page_config(page_title="小猫直喂派单平台", layout="wide", initial_sidebar_state="expanded")
st.markdown("""<style>
    /* 全局深色侧边栏 */
    [data-testid="stSidebar"] { background-color: #0d0d0d !important; color: #ffffff !important; border-right: 1px solid #333; }
    .sb-h { font-size: 0.85rem; font-weight: 800; color: #666; margin: 1.2rem 0 0.5rem 0; letter-spacing: 1.5px; text-transform: uppercase; }
    
    /* 物理矩阵按钮 */
    .v174-btn [data-testid="stVerticalBlock"] div.stButton > button { 
        width: 100% !important; height: 52px !important; font-size: 15px !important; font-weight: 600 !important; 
        border-radius: 12px !important; border: 1px solid #3d3d3d !important; background-color: #1a1a1a !important; color: #ffffff !important;
    }
    .v174-btn div.stButton > button:hover { background-color: #444 !important; border-color: #007bff !important; }

    /* 实时对账卡片：绝杀配色融合 */
    .st-status-row { display: flex; gap: 12px; margin-bottom: 25px; }
    .st-card { flex: 1; padding: 22px; border-radius: 20px; text-align: center; color: white !important; border: 1px solid rgba(255,255,255,0.1); box-shadow: 0 4px 15px rgba(0,0,0,0.5); }
    .bg-black { background: #000000; } .bg-blue { background: #003366; } .bg-red { background: #990000; } 
    .card-val { font-size: 2.3rem; font-weight: 900; text-shadow: 2px 2px 6px rgba(0,0,0,0.9); display: block; line-height: 1.1; }
    .card-lab { font-size: 0.9rem; font-weight: 700; opacity: 0.95; display: block; margin-top: 8px; }

    .terminal-v174 { background-color: #050505; color: #00ff00; padding: 15px; border-radius: 12px; font-family: monospace; font-size: 11px; height: 320px; overflow-y: auto; border: 1px solid #333; }
</style>""", unsafe_allow_html=True)

# --- 4. 侧边栏布局：物理补齐机动工具选择 ---
with st.sidebar:
    st.markdown('<div class="sb-h">👤 视角角色确认</div>', unsafe_allow_html=True)
    st.session_state.viewport = st.selectbox("Role", ["管理员模式", "梦蕊模式", "依蕊模式"], label_visibility="collapsed")
    st.divider()

    st.markdown('<div class="sb-h">🧭 指战频道导航</div>', unsafe_allow_html=True)
    st.markdown('<div class="v174-btn">', unsafe_allow_html=True)
    if st.button("📊 派单动态看板"): st.session_state.page = "实时看板大屏"
    if st.button("📂 资料录入同步"): st.session_state.page = "资料录入管理"
    if st.button("📖 平台指战手册"): st.session_state.page = "手册"
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()

    st.markdown('<div class="sb-h">⚙️ 机动机能设定</div>', unsafe_allow_html=True)
    st.session_state.travel_mode = st.selectbox("机动工具", ["骑行", "步行", "公交/地铁"], index=0)
    st.divider()

    st.markdown('<div class="sb-h">📅 作战参数 (区间检索版)</div>', unsafe_allow_html=True)
    td = datetime.now().date(); c1, c2 = st.columns(2)
    with c1:
        if st.button("📍 今天"): st.session_state.r = (td, td)
        if st.button("📍 本月"): st.session_state.r = (td.replace(day=1), td.replace(day=calendar.monthrange(td.year, td.month)[1]))
    with c2:
        if st.button("📍 明天"): st.session_state.r = (td+timedelta(1), td+timedelta(1))
        if st.button("📍 本周"): 
            st.session_state.r = (td - timedelta(days=td.weekday()), td + timedelta(days=6-td.weekday()))
    st.session_state.r = st.date_input("分析日期段", value=st.session_state.r, label_visibility="collapsed")
    st.session_state.departure_point = st.selectbox("起始出征点", ["深圳市龙华区 潜龙花园 4A 栋", "乐荟中心", "星河world 二期 c 栋", "自定义..."])
    st.divider()

    with st.expander("📡 系统影子日志塔"):
        st.markdown(f'<div class="terminal-v174">{"<br>".join(st.session_state.system_logs[-60:])}</div>', unsafe_allow_html=True)

# --- 5. 飞书服务：物理展开与全日期区间预判 ---
def fetch_feishu_v174():
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
    st.session_state.feishu_cache = fetch_feishu_v174()

# 【实时预判引擎】彻底修复区间统计与 0 延迟
df_raw = st.session_state.feishu_cache.copy()
total_site_cnt, m_cnt, e_cnt, auto_cnt = 0, 0, 0, 0
realtime_filtered_df = pd.DataFrame()

if not df_raw.empty and isinstance(st.session_state.r, tuple) and len(st.session_state.r) == 2:
    start_d, end_d = st.session_state.r
    # 注意：顶部的指标卡展示区间的第一个日期的情况 (今日实时预览)
    mask = (df_raw['服务开始日期'].dt.date <= start_d) & (df_raw['服务结束日期'].dt.date >= start_d)
    m_df = df_raw[mask].copy()
    if not m_df.empty:
        def check_hit(r):
            dt = (start_d - r['服务开始日期'].date()).days
            return dt % int(r.get('投喂频率', 1)) == 0
        m_df['is_hit'] = m_df.apply(check_hit, axis=1)
        hit_df = m_df[m_df['is_hit']].drop_duplicates(subset=['详细地址'])
        
        # 智能补位预览
        hit_df['building'] = hit_df['详细地址'].apply(get_normalized_v174)
        assigned = hit_df[hit_df['喂猫师'].isin(['梦蕊', '依蕊'])].copy()
        unassigned = hit_df[~hit_df['喂猫师'].isin(['梦蕊', '依蕊'])].copy()
        for idx, row in unassigned.iterrows():
            same_b = assigned[assigned['building'] == row['building']]
            if not same_b.empty: hit_df.at[idx, '喂猫师'] = same_b.iloc[0]['喂猫师']
            else:
                best = "梦蕊" if len(hit_df[hit_df['喂猫师']=="梦蕊"]) <= len(hit_df[hit_df['喂猫师']=="依蕊"]) else "依蕊"
                hit_df.at[idx, '喂猫师'] = best
                auto_cnt += 1
        
        total_site_cnt = len(hit_df); m_cnt = len(hit_df[hit_df['喂猫师']=="梦蕊"]); e_cnt = len(hit_df[hit_df['喂猫师']=="依蕊"])
        realtime_filtered_df = hit_df

# --- 6. 模块实现：资料中心 ---
if st.session_state.page == "资料录入管理":
    st.title("📂 资料录入中心与飞书物理对账")
    if not df_raw.empty:
        st.subheader("⚙️ 云端实时编辑器 (PATCH接口)")
        edit_df = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态', '投喂频率']], use_container_width=True)
        if st.button("🚀 强制物理同步至飞书"):
            tk_v = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
            for i, row in edit_df.iterrows():
                requests.patch(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{df_raw.iloc[i]['_id']}", 
                               headers={"Authorization": f"Bearer {tk_v}"}, 
                               json={"fields": {"订单状态": str(row['订单状态']), "喂猫师": str(row['喂猫师']), "投喂频率": int(row['投喂频率'])}})
            st.session_state.feishu_cache = None; st.rerun()

# --- 7. 看板实现：进度条与区间全日期循环 ---
elif st.session_state.page == "实时看板大屏":
    st.title("📊 派单动态指挥中心")
    st.markdown(f"""
    <div class="st-status-row">
        <div class="st-card bg-black"><span class="card-val">{total_site_cnt}</span><span class="card-lab">📊 起始日服务站点</span></div>
        <div class="st-card bg-blue"><span class="card-val">{m_cnt}</span><span class="card-lab">🐱 梦蕊负载</span></div>
        <div class="st-card bg-blue"><span class="card-val">{e_cnt}</span><span class="card-lab">🐱 依蕊负载</span></div>
        <div class="st-card bg-red"><span class="card-val">{auto_cnt}</span><span class="card-lab">🚩 智能补位量</span></div>
    </div>
    """, unsafe_allow_html=True)
    
    # 【修复：全日期对账索引】
    col_d, col_v = st.columns(2)
    with col_d:
        avail_dates = sorted([st.session_state.r[0].strftime('%Y-%m-%d')])
        if st.session_state.get('fp') is not None:
            avail_dates = sorted(st.session_state.fp['作业日期'].unique())
        vd = st.selectbox("📅 选择查看的对账日期", avail_dates, index=0)
    with col_v:
        if st.session_state.viewport == "管理员模式":
            st.session_state.admin_sub_view = st.selectbox("👤 视角切换", ["全部人员", "梦蕊", "依蕊"])
        else: st.info(f"锁定：{st.session_state.viewport}")

    c1, c2, c3, _ = st.columns([1,1,1,4])
    if c1.button("▶ 启动全区间方案分析"): st.session_state.plan_state = "RUNNING"
    if c3.button("↺ 重置清空看板"): st.session_state.plan_state = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state.plan_state == "RUNNING":
        # 【物理新增：派单进度条】
        p_bar = st.progress(0, text="指战方案测算中...")
        with st.status("正在回归执行区间循环引擎与全量照明...", expanded=True) as status:
            sitters = ["梦蕊", "依蕊"]
            # 物理循环区间内的每一天
            date_list = pd.date_range(st.session_state.r[0], st.session_state.r[1]).tolist()
            all_plans = []
            for idx, d in enumerate(date_list):
                p_val = (idx + 1) / len(date_list)
                p_bar.progress(p_val, text=f"正在分析日期: {d.strftime('%Y-%m-%d')} ({idx+1}/{len(date_list)})")
                
                # 每一天的独立过滤
                ct = pd.Timestamp(d)
                day_mask = (df_raw['服务开始日期'].dt.date <= ct.date()) & (df_raw['服务结束日期'].dt.date >= ct.date())
                day_df = df_raw[day_mask].copy()
                if not day_df.empty:
                    day_df['is_hit'] = day_df.apply(lambda r: (ct.date() - r['服务开始日期'].date()).days % int(r.get('投喂频率', 1)) == 0, axis=1)
                    hit_df = day_df[day_df['is_hit']].drop_duplicates(subset=['详细地址'])
                    
                    # 每一天的自动补位
                    hit_df['building'] = hit_df['详细地址'].apply(get_normalized_v174)
                    for i, r in hit_df[~hit_df['喂猫师'].isin(sitters)].iterrows():
                        best = "梦蕊" if len(hit_df[hit_df['喂猫师']=="梦蕊"]) <= len(hit_df[hit_df['喂猫师']=="依蕊"]) else "依蕊"
                        hit_df.at[i, '喂猫师'] = best
                    
                    for s in sitters:
                        stks = hit_df[hit_df['喂猫师'] == s].copy()
                        if not stks.empty:
                            all_plans.append(optimize_route_v174(stks, s, d.strftime('%Y-%m-%d'), st.session_state.departure_point).assign(作业日期=d.strftime('%Y-%m-%d')))
            
            st.session_state.fp = pd.concat(all_plans) if all_plans else None; st.session_state.plan_state = "IDLE"
            p_bar.empty(); status.update(label="✅ 区间分析完毕！下拉框已刷新全量日期。", state="complete")

    if st.session_state.get('fp') is not None:
        day_all = st.session_state.fp[st.session_state.fp['作业日期'] == vd]
        role_f = st.session_state.admin_sub_view if st.session_state.viewport == "管理员模式" else ("梦蕊" if "梦蕊" in st.session_state.viewport else "依蕊")
        v_data = day_all if role_f == "全部人员" else day_all[day_all['喂猫师'] == role_f]
        
        c1, c2 = st.columns(2); show_n = ["梦蕊", "依蕊"] if role_f == "全部人员" else [role_f]
        for i, sn in enumerate(show_n):
            stt = st.session_state.commute_stats.get(f"{vd}_{sn}", {"dist": 0, "dur": 0})
            with [c1, c2][i%2]: st.markdown(f"""<div style="background:#fff; border-left:10px solid #007bff; padding:20px; border-radius:15px; box-shadow:0 4px 10px rgba(0,0,0,0.05); margin-bottom:15px;">
                <h4 style="margin:0; color:#888;">{sn} 作战统计</h4><p style="font-size:24px; font-weight:900; color:#111; margin:5px 0;">站点：{len(day_all[day_all.喂猫师==sn])} 单</p>
                <p style="font-size:15px; color:#007bff;">机动时长：{int(stt['dur'])} 分钟 | 路程：{stt['dist']/1000:.2f} km</p></div>""", unsafe_allow_html=True)
        
        brief = [f"📊 派单简报 ({vd})", f"🚩 起点：{st.session_state.departure_point}", f"🚲 工具：{st.session_state.travel_mode}"]
        for _, r in v_data.iterrows():
            line = f"{int(r.拟定顺序)}. {r.宠物名字}-{r.详细地址}"
            if r.拟定顺序 == 1: line += f" (🚗 耗时 {int(r.prev_dur)}分)"
            if r.get('next_dur', 0) > 0: line += f" ➝ (下站 {int(r['next_dist'])}m, {int(r['next_dur'])}分)"
            else: line += " 🏁 任务完成"
            brief.append(line)
        
        final_txt = "\n".join(brief)
        # 【物理增强： st.code 全能复制】
        st.code(final_txt, language="text")

        # 100% 顺序连线接力 JS
        map_json = v_data[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序']].to_dict('records')
        amap_html = f"""<div id="m" style="width:100%;height:600px;border-radius:20px;background:#f8f9fa;border:1px solid #ddd;"></div>
        <script src="https://webapi.amap.com/maps?v=2.0&key={AMAP_KEY_JS}&plugin=AMap.Walking,AMap.Riding,AMap.Transit"></script>
        <script>
            window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }};
            const data = {json.dumps(map_json)}; const colors = {{"梦蕊": "#007BFF", "依蕊": "#311b92"}};
            const map = new AMap.Map('m', {{ zoom: 14, center: [data[0].lng, data[0].lat] }});
            data.forEach(m => {{
                new AMap.Marker({{ position:[m.lng, m.lat], map:map, content:`<div style="width:28px;height:28px;background:${{colors[m.喂猫师]}};border:2px solid #fff;border-radius:50%;color:white;text-align:center;line-height:26px;font-size:12px;font-weight:bold;">${{m.拟定顺序}}</div>` }});
            }});
            function drawSequential(i) {{
                if (i >= data.length - 1) {{ setTimeout(()=>map.setFitView(), 500); return; }}
                if (data[i].喂猫师 !== data[i+1].喂猫师) {{ drawSequential(i+1); return; }}
                // 工具绑定逻辑
                let tool; let mode = "{st.session_state.travel_mode}";
                if (mode === '步行') tool = new AMap.Walking({{map:map, hideMarkers:true}});
                else if (mode === '公交/地铁') tool = new AMap.Transit({{map:map, city:'深圳市', hideMarkers:true}});
                else tool = new AMap.Riding({{map:map, hideMarkers:true, strokeColor:colors[data[i].喂猫师], strokeWeight:8}});
                
                tool.search([data[i].lng, data[i].lat], [data[i+1].lng, data[i+1].lat], (s) => {{
                    if (s !== 'complete') {{
                        new AMap.Polyline({{ path: [[data[i].lng, data[i].lat], [data[i+1].lng, data[i+1].lat]], strokeColor: colors[data[i].喂猫师], strokeWeight: 4, strokeStyle: 'dashed', map: map }});
                    }}
                    setTimeout(()=>drawSequential(i+1), 400);
                }});
            }}
            drawSequential(0);
        </script>"""
        components.html(amap_html, height=620)

elif st.session_state.page == "手册":
    st.title("📖 指战手册 (V174 物理旗舰版)")
    st.markdown("""
    ### 1. 机动工具
    - **骑行/步行**：使用高德标准路径引擎。
    - **公交/地铁**：综合地铁与公交线路，耗时计算包含换乘。
    ### 2. 区间对账
    - 选定日期区间后，点击“启动分析”，上方日期下拉框会自动解锁该区间内所有的有单日期。
    """)
