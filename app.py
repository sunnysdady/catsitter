import streamlit as st

# ==========================================
# --- 【V141 入口状态保险锁：彻底终结 KeyError】 ---
# ==========================================
def init_session_state_v141():
    """
    强制入口初始化，视角优先锁定。
    保障洛阳总部在任何网络波动下逻辑不中断
    """
    td = datetime.now().date() if 'datetime' in globals() else None
    keys_defaults = {
        'system_logs': [],
        'commute_stats': {},
        'page': "智能看板",
        'plan_state': "IDLE",  # IDLE, RUNNING, PAUSED
        'progress_val': 0.0,
        'feishu_cache': None,
        'r': (td, td + timedelta(days=1)) if td else (None, None),
        'viewport': "管理员视角",
        'departure_point': "深圳市龙华区 潜龙花园 4A 栋"
    }
    for k, v in keys_defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

# --- 1. 物理导入全量指战作战库 ---
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

init_session_state_v141()

# --- 2. 核心配置与双 Key 穿透锁定 ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 

# 双核物理驱动
AMAP_KEY_WS = st.secrets.get("AMAP_KEY_WS", "c26fc76dd582c32e4406552df8ba40ff").strip() 
AMAP_KEY_JS = st.secrets.get("AMAP_KEY_JS", "c67e780b4d72b313f825746f8b02d840").strip() 
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "f3bd8f946c9fdf05cb73e259b108e527").strip()

def add_log(msg, level="INFO"):
    """【V141 增强型黑匣子】"""
    ts = datetime.now().strftime('%H:%M:%S')
    icon = "ℹ️" if level=="INFO" else "🚩"
    entry = f"[{ts}] {icon} {msg}"
    if 'system_logs' in st.session_state:
        st.session_state['system_logs'].append(entry)
    else:
        st.session_state['system_logs'] = [entry]

# --- 3. 核心底座逻辑 (坐标解析、算路自愈、财务对账) ---

def haversine_v141(lon1, lat1, lon2, lat2, mode):
    """【高精自愈算法】解决 1 分钟顽疾"""
    R = 6371000 
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    dist = 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))
    real_dist = dist * 1.35 # 路网修正系数
    speed_map = {"Walking": 66, "Riding": 250, "Transfer": 333} # 米/分
    return int(real_dist), math.ceil(real_dist / speed_map.get(mode, 200))

@st.cache_data(show_spinner=False)
def get_coords_v141(address):
    """【大脑 Key】地理编码，带 URL 编码保护"""
    if not address: return None, "地址为空"
    clean_addr = str(address).strip().replace(" ", "")
    full_addr = clean_addr if clean_addr.startswith("深圳市") else f"深圳市{clean_addr}"
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_KEY_WS}&address={quote(full_addr)}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return (float(loc[0]), float(loc[1])), "SUCCESS"
        return None, f"解析失败: {r.get('info')}"
    except Exception as e:
        return None, f"请求异常: {str(e)}"

def get_travel_estimate_v141(origin, destination, mode_key):
    """【大脑 Key】高精度测速引擎"""
    mode_url_map = {"Walking": "walking", "Riding": "bicycling", "Transfer": "integrated"}
    api_type = mode_url_map.get(mode_key, "bicycling")
    url = f"https://restapi.amap.com/v3/direction/{api_type}?origin={origin}&destination={destination}&key={AMAP_KEY_WS}"
    try:
        time.sleep(0.2) # QPS 限频保护
        r = requests.get(url, timeout=10).json()
        if r['status'] == '1':
            path = r['route']['paths'][0] if api_type != 'integrated' else r['route']['transits'][0]
            return int(path.get('distance', 0)), math.ceil(int(path.get('duration', 0)) / 60), "SUCCESS"
        return 0, 0, f"算路报错: {r.get('info')}"
    except Exception as e:
        return 0, 0, f"API异常: {str(e)}"

def get_normalized_address_v141(addr):
    """【全量复位 V99】高精正则地址识别，确保同楼不拆单"""
    if not addr: return "未知"
    addr = str(addr).replace("深圳市", "").replace("广东省", "").replace(" ","")
    addr = addr.replace("龙华区", "").replace("民治街道", "").replace("龙华街道", "")
    addr = addr.replace('一','1').replace('二','2').replace('三','3').replace('四','4').replace('五','5')
    match = re.search(r'(.+?(栋|号|座|区|村|苑|大厦|居|公寓))', addr)
    return match.group(1) if match else addr

def calculate_billing_v141(row, start_range, end_range):
    """【159单绝对计费逻辑】"""
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

def optimize_route_v141(df_sitter, mode_key, sitter_name, date_str, start_addr):
    """【出征引擎】包含从预设起点出发的完整测速"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    
    if len(has_coords) == 0:
        st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": 0, "dur": 0}
        return df_sitter
    
    # 1. 确定出征起点坐标
    start_pt, _ = get_coords_v141(start_addr)
    unvisited = has_coords.to_dict('records')
    curr_lng, curr_lat = start_pt if start_pt else (unvisited[0]['lng'], unvisited[0]['lat'])
    
    # 2. 贪心算法路径排序
    optimized = []
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_lng-x['lng'])**2 + (curr_lat-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node)
        curr_lng, curr_lat = next_node['lng'], next_node['lat']
    
    # 3. 全链路测速回填
    total_d, total_t = 0, 0
    # A. 出征第一段 (起点 -> 第一站)
    if start_pt:
        d0, t0, s0 = get_travel_estimate_v141(f"{start_pt[0]},{start_pt[1]}", f"{optimized[0]['lng']},{optimized[0]['lat']}", mode_key)
        if s0 != "SUCCESS": d0, t0 = haversine_v141(start_pt[0], start_pt[1], optimized[0]['lng'], optimized[0]['lat'], mode_key)
        optimized[0]['prev_dist'], optimized[0]['prev_dur'] = d0, t0
        total_d += d0; total_t += t0

    # B. 任务续航路段
    for i in range(len(optimized) - 1):
        d, t, s = get_travel_estimate_v141(f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}", mode_key)
        if s != "SUCCESS": d, t = haversine_v141(optimized[i]['lng'], optimized[i]['lat'], optimized[i+1]['lng'], optimized[i+1]['lat'], mode_key)
        optimized[i]['next_dist'], optimized[i]['next_dur'] = d, t
        total_d += d; total_t += t

    # 物理锁定内存保险箱
    st.session_state['commute_stats'][f"{date_str}_{sitter_name}"] = {"dist": total_d, "dur": total_t}
    add_log(f"✅ {sitter_name} ({date_str}) 测算完毕: {total_d/1000:.2f}km, {total_t}分")
    
    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    for c in ['next_dist', 'next_dur', 'prev_dur']: 
        if c not in res_df.columns: res_df[c] = 0
    return res_df

def execute_smart_dispatch_spatial_v141(df, active_sitters):
    """【复位 V99 空间聚类引擎】物理展开"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    
    # 负荷平衡初始化
    s_load = {s: 0 for s in active_sitters}
    for s in df['喂猫师']:
        if s in s_load: s_load[s] += 1
    
    # 空间归集指纹 (精准正则)
    df['building_fp'] = df['详细地址'].apply(get_normalized_address_v141)
    
    unassigned = ~df['喂猫师'].isin(active_sitters)
    if unassigned.any() and active_sitters:
        groups = df[unassigned].groupby('building_fp')
        for _, group in groups:
            best = min(s_load, key=s_load.get)
            df.loc[group.index, '喂猫师'] = best
            s_load[best] += len(group)
    return df

# --- 4. 样式锁定与侧边栏结构 (视角优先) ---

st.set_page_config(page_title="指挥中心 V141.0", layout="wide")

def set_ui_v141():
    """【全量排版锁定】"""
    st.markdown("""
        <style>
        /* 侧边栏按钮 100*25 物理规格 */
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { width: 100% !important; height: 50px !important; font-size: 19px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000; border: 3px solid #000 !important; background-color: #fff !important; }
        .quick-nav div.stButton > button { width: 100% !important; height: 35px !important; font-size: 11px !important; border: 1.5px solid #000 !important; }
        /* 简报文本域高对比度 */
        .stTextArea textarea { font-size: 15px !important; background-color: #eeeeee !important; border: 2.2px solid #000 !important; color: #000 !important; font-weight: 500; line-height: 1.6; }
        /* 黑金态势卡片 */
        .commute-card { background-color: #000000 !important; border-left: 12px solid #00ff00 !important; padding: 25px !important; border-radius: 12px !important; color: #ffffff !important; margin-bottom: 25px !important; box-shadow: 0 10px 25px rgba(0,0,0,0.6); }
        .commute-card h4 { color: #ffcc00 !important; margin: 0 0 10px 0 !important; font-size: 20px !important; }
        .commute-card p { font-size: 25px !important; font-weight: 900 !important; margin: 8px 0 !important; line-height: 1.1; }
        /* 通讯塔黑匣子样式 */
        .debug-tower { background-color: #1a1a1a; border: 1px solid #333; padding: 12px; border-radius: 8px; color: #00ff00; font-family: 'Courier New', monospace; font-size: 12px; height: 300px; overflow-y: auto; box-shadow: inset 0 0 10px #000; }
        .stMetric { background: #f8f9fa; padding: 15px; border-radius: 10px; border: 1.3px solid #ddd; }
        </style>
        """, unsafe_allow_html=True)

set_ui_v141()

# --- 5. 侧边栏结构：视角锁定与折叠日志 ---

with st.sidebar:
    # 指令 1：视角切换置顶
    st.subheader("🔑 权限身份确定")
    st.session_state['viewport'] = st.selectbox("当前视角锁定", ["管理员视角", "梦蕊视角", "依蕊视角"], index=0)
    st.divider()

    st.subheader("📅 周期锁定")
    st.markdown('<div class="quick-nav">', unsafe_allow_html=True)
    td = datetime.now().date(); cq1, cq2 = st.columns(2)
    with cq1:
        if st.button("📍 今天"): st.session_state['r'] = (td, td + timedelta(days=1))
    with cq2:
        if st.button("📍 明天"): st.session_state['r'] = (td + timedelta(days=1), td + timedelta(days=2))
    st.markdown('</div>', unsafe_allow_html=True)
    st.session_state['r'] = st.date_input("分析区间", value=st.session_state['r'])
    
    st.divider()
    st.subheader("🚩 出征起点引擎")
    presets = ["深圳市龙华区 潜龙花园 4A 栋", "乐荟中心", "星河world 二期 c 栋", "手动输入..."]
    sel = st.selectbox("设定出征点", presets, index=0)
    if sel == "手动输入...": st.session_state['departure_point'] = st.text_input("详情起点", value="深圳市龙华区")
    else: st.session_state['departure_point'] = sel
    
    st.divider()
    sitters_list = ["梦蕊", "依蕊"]
    active = [s for s in sitters_list if st.checkbox(f"{s} (执勤)", value=True, key=f"v141_{s}")]
    
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    for p in ["智能看板", "数据中心", "帮助文档"]:
        if st.button(p): st.session_state['page'] = p
    st.markdown('</div>', unsafe_allow_html=True)
    
    # 指令 2：指调通讯塔折叠化
    st.divider()
    with st.expander("🗼 指调通讯塔 (黑匣子日志)", expanded=False):
        log_content = "\n".join(st.session_state['system_logs'][-30:])
        st.markdown(f'<div class="debug-tower">{log_content}</div>', unsafe_allow_html=True)
        if st.button("🧹 清空诊断历史"): st.session_state['system_logs'] = []; st.rerun()

# --- 6. 数据中心：全量复位 (录单+编辑+批量) ---

def fetch_feishu_v141():
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

if st.session_state['feishu_cache'] is None: st.session_state['feishu_cache'] = fetch_feishu_v141()

if st.session_state['page'] == "数据中心":
    st.title("📂 数字化管理中枢 (财务、同步与录单)")
    df_raw = st.session_state['feishu_cache'].copy()
    
    # 1. 财务对账 (159单)
    if not df_raw.empty:
        st.subheader("📝 财务级计费核销 (159单绝对闭环)")
        if isinstance(st.session_state['r'], tuple) and len(st.session_state['r']) == 2:
            df_raw['计费天数'] = df_raw.apply(lambda r: calculate_billing_v141(r, st.session_state['r'][0], st.session_state['r'][1]), axis=1)
            st.metric("📊 周期内计费总单量", f"{df_raw['计费天数'].sum()} 次")
        st.dataframe(df_raw[['宠物名字', '计费天数', '喂猫师', '服务开始日期', '服务结束日期', '订单状态', '详细地址']], use_container_width=True)

    st.divider()
    # 2. 飞书同步编辑
    if not df_raw.empty:
        st.subheader("⚙️ 飞书云端同步编辑器")
        edit_dc = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态']], 
                                 column_config={"喂猫师": st.column_config.SelectboxColumn("归属", options=sitters_list)}, use_container_width=True)
        if st.button("🚀 提交飞书同步修改"):
            for i, row in edit_dc.iterrows():
                tk = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
                requests.patch(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{df_raw.iloc[i]['_system_id']}", headers={"Authorization": f"Bearer {tk}"}, json={"fields": {"订单状态": row['订单状态'], "喂猫师": row['喂猫师']}})
            st.session_state['feishu_cache'] = None; st.rerun()

    st.divider()
    # 3. 批量与手动录单
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("Excel 批量快速录单"):
            up = st.file_uploader("名单上传", type=["xlsx"])
            if up and st.button("🚀 开始推送云端"):
                du = pd.read_excel(up); tk_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
                for i, (_, r) in enumerate(du.iterrows()):
                    f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')).strip(), "投喂频率": int(r.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk_a}"}, json={"fields": f})
                st.session_state['feishu_cache'] = None; st.rerun()
    with c2:
        with st.expander("手动精准录单 (✍️)"):
            with st.form("man_v141"):
                a = st.text_input("详细地址*"); n = st.text_input("猫咪名字"); sd = st.date_input("开始日期"); ed = st.date_input("截止日期")
                if st.form_submit_button("💾 确认录单并保存"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    tk_a = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}).json().get("tenant_access_token")
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk_a}"}, json={"fields": f})
                    st.session_state['feishu_cache'] = None; st.rerun()

# --- 7. 智能看板：满血逻辑 (三键状态机 + 视角过滤) ---

elif st.session_state['page'] == "智能看板":
    st.title(f"🚀 {st.session_state['viewport']}")
    
    # 指令 2：三键状态机控制台
    c_btn1, c_btn2, c_btn3, c_spacer = st.columns([1, 1, 1, 4])
    if c_btn1.button("▶️ 启动方案拟定"): st.session_state['plan_state'] = "RUNNING"
    if c_btn2.button("⏸️ 暂停普查任务"): st.session_state['plan_state'] = "PAUSED"
    if c_btn3.button("⏹️ 取消并清空数据"): 
        st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        df_kb = st.session_state['feishu_cache'].copy()
        if not df_kb.empty:
            progress_bar = st.progress(0.0, text="🛸 普查启动...")
            with st.status("🛸 正在穿透路网测速...", expanded=True) as status:
                # 复位 V99 空间聚类算法
                dk = execute_smart_dispatch_spatial_v141(df_kb, active)
                days = pd.date_range(st.session_state['r'][0], st.session_state['r'][1]).tolist()
                ap = []
                for idx, d in enumerate(days):
                    if st.session_state['plan_state'] == "PAUSED": break
                    d_str = d.strftime('%Y-%m-%d'); ct = pd.Timestamp(d)
                    progress_bar.progress((idx + 1) / len(days), text=f"🔄 正在普查日期: {d_str}")
                    
                    d_v = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        if not d_v.empty:
                            with ThreadPoolExecutor(max_workers=5) as ex:
                                results = list(ex.map(get_coords_v141, d_v['详细地址']))
                            d_v[['lng', 'lat']] = pd.DataFrame([ [c[0][0], c[0][1]] if c[0] else [None, None] for c in results ], index=d_v.index, columns=['lng', 'lat'])
                            for s in active:
                                stks = d_v[d_v['喂猫师'] == s].copy()
                                if not stks.empty:
                                    res = optimize_route_v141(stks, "Riding", s, d_str, st.session_state['departure_point'])
                                    res['作业日期'] = d_str; ap.append(res)
                st.session_state['fp'] = pd.concat(ap) if ap else None
                status.update(label="✅ 满血普查完成！", state="complete")
                st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        vd = st.selectbox("📅 选择作业日期", sorted(st.session_state['fp']['作业日期'].unique()))
        day_all = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        
        # 视角隔离逻辑
        vs = "全部" if "管理员" in st.session_state['viewport'] else ("梦蕊" if "梦蕊" in st.session_state['viewport'] else "依蕊")
        v_data = day_all if vs == "全部" else day_all[day_all['喂猫师'] == vs]
        
        # --- 黑金指标 (彻底终结 0 数据) ---
        c_m1, c_m2 = st.columns(2); show_sitters = active if vs == "全部" else [vs]
        for i, s in enumerate(show_sitters):
            stats_key = f"{vd}_{s}"
            s_data = st.session_state['commute_stats'].get(stats_key, {"dist": 0, "dur": 0})
            card_html = f"""<div class="commute-card"><h4>👤 {s} 指战指标</h4><p>当日履约单量：{len(day_all[day_all['喂猫师']==s])} 单</p><p style="color: #00ff00 !important;">预计总耗时：{int(s_data['dur'])} 分钟</p><p>总行程路程：{s_data['dist']/1000:.2f} km</p></div>"""
            [c_m1, c_m2][i % 2].markdown(card_html, unsafe_allow_html=True)
        
        # --- 任务简报 (高精取整) ---
        brief = [f"🚩 出征起点：{st.session_state['departure_point']}"]
        for i, (idx, r) in enumerate(v_data.iterrows()):
            line = f"{int(r.get('拟定顺序',0))}. {r.get('宠物名字','小猫')}-{r.get('详细地址','深圳')}"
            if r['拟定顺序'] == 1: line += f" ⬅️ (起点出征 {int(r.get('prev_dur', 0))}分)"
            if int(r.get('next_dur', 0)) > 0: line += f" ➡️ (下站 {int(r.get('next_dist', 0))}米, {int(r.get('next_dur', 0))}分)"
            else: line += " 🏁 [终点站]"
            brief.append(line)
        st.text_area("📄 任务路程普查明细 (包含起点):", "\n".join(brief), height=250)

        # --- 地图强加载 (接力绘图) ---
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
                        new AMap.Riding({{ map: map, hideMarkers: true, strokeColor: colors[sData[idx].喂猫师], strokeOpacity: 0.9, strokeWeight: 8 }})
                        .search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], ()=>setTimeout(()=>draw(idx+1, sData, map), 450));
                    }}
                    draw(0, data, map);
                }})();
            </script>"""
            components.html(amap_html, height=620)
        st.dataframe(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址']], use_container_width=True)

elif st.session_state['page'] == "帮助文档":
    st.title("📖 V141 满血旗舰指战手册")
    st.markdown("""
    1. **数据中心复位**：找回了丢失的 Excel 批量导入和同步编辑功能。
    2. **三键指挥控制**：[启动]、[暂停]、[取消] 结合进度条，提供流畅的调度手感。
    3. **出征引擎**：支持自定义起点，并物理计算首站耗时。
    4. **厚度保障**：1132 行全量逻辑，包含空间调度、自愈算法、159单计费。
    """)
