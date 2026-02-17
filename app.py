import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import re
import io
import json
import calendar
import streamlit.components.v1 as components

# --- 1. 核心配置与 ID 强力清洗 ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()
AMAP_JS_CODE = st.secrets.get("AMAP_JS_CODE", "").strip()

# --- 2. 核心底座函数 (坐标与耗时) ---

@st.cache_data(show_spinner=False)
def get_coords(address):
    if not address: return None, None
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_API_KEY}&address=深圳市{address}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return float(loc[0]), float(loc[1])
    except: pass
    return None, None

def get_travel_estimate_v106(origin, destination, mode_key):
    """高德 Web 服务计算路程与时间"""
    mode_url_map = {"步行": "walking", "骑行/电动车": "bicycling", "地铁/公交": "integrated"}
    api_type = mode_url_map.get(mode_key, "bicycling")
    url = f"https://restapi.amap.com/v3/direction/{api_type}?origin={origin}&destination={destination}&key={AMAP_API_KEY}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1':
            path = r['route']['paths'][0] if api_type != 'integrated' else r['route']['transits'][0]
            return int(path.get('distance', 0)), int(path.get('duration', 0)) // 60
    except: pass
    return 0, 0

def optimize_route_v106(df_sitter, mode_key):
    """路径优化引擎：强制注入耗时数据"""
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    if len(has_coords) <= 1:
        res = pd.concat([has_coords, no_coords])
        res['拟定顺序'] = range(1, len(res) + 1)
        res['next_dist'], res['next_dur'] = 0, 0
        return res
    
    unvisited = has_coords.to_dict('records')
    curr_node = unvisited.pop(0); optimized = [curr_node]
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_node['lng']-x['lng'])**2 + (curr_node['lat']-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node); curr_node = next_node
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_travel_estimate_v106, f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}", mode_key): i for i in range(len(optimized)-1)}
        for future in as_completed(futures):
            idx = futures[future]
            dist, dur = future.result()
            optimized[idx]['next_dist'], optimized[idx]['next_dur'] = dist, dur

    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    for c in ['next_dist', 'next_dur']:
        res_df[c] = res_df.get(c, 0).fillna(0)
    return res_df

# --- 3. 飞书 API 服务 ---

def get_feishu_token():
    try:
        r = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("tenant_access_token")
    except: return None

def fetch_feishu_data():
    token = get_feishu_token()
    if not token: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"page_size": 500}, timeout=15).json()
        items = r.get("data", {}).get("items", [])
        if not items: return pd.DataFrame()
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        df['订单状态'] = df.get('订单状态', '进行中').fillna('进行中')
        df['投喂频率'] = pd.to_numeric(df.get('投喂频率'), errors='coerce').fillna(1).replace(0, 1)
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        return df
    except: return pd.DataFrame()

def update_feishu_field(record_id, field_name, value):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{str(record_id).strip()}"
    try:
        r = requests.patch(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json={"fields": {field_name: str(value)}}, timeout=10)
        return r.status_code == 200
    except: return False

# --- 4. 辅助组件 ---

def copy_to_clipboard_v106(text):
    html_code = f"""
    <div style="margin-bottom: 20px;">
        <button onclick="navigator.clipboard.writeText(`{text}`).then(()=>alert('简报复制成功'))" style="
            width: 220px; height: 50px; background-color: #000; color: white;
            border-radius: 12px; font-weight: 800; cursor: pointer; border: none;
            box-shadow: 4px 4px 0px #000; font-size: 16px;">
            📋 一键复制简报
        </button>
    </div>
    """
    components.html(html_code, height=70)

# --- 5. UI 视觉方案 (V44 对齐) ---

st.set_page_config(page_title="指挥中心 V106.0", layout="wide")

def set_ui():
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { width: 100% !important; height: 50px !important; font-size: 18px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000; background-color: #FFFFFF !important; margin-bottom: 12px !important; border: 3px solid #000 !important; }
        .quick-nav div.stButton > button { width: 100% !important; height: 30px !important; font-size: 12px !important; border: 1.5px solid #000 !important; }
        /* 简报文本域高对比度修复 */
        .stTextArea textarea { font-size: 15px !important; background-color: #eeeeee !important; color: #000000 !important; font-weight: 500 !important; border: 2px solid #000 !important; }
        .stMetric { background: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #ddd; }
        </style>
        """, unsafe_allow_html=True)

set_ui()

# --- 6. 侧边栏 ---

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()
if 'plan_state' not in st.session_state: st.session_state['plan_state'] = "IDLE"

with st.sidebar:
    st.subheader("📅 快捷调度 (100*25)")
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
    
    d_sel = st.date_input("日期范围锁定", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    s_filter = st.multiselect("状态", options=["进行中", "已结束", "待处理"], default=["进行中", "待处理"])
    active_sitters = ["梦蕊", "依蕊"]
    active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True, key=f"v106_{s}")]
    
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    for p in ["数据中心", "任务进度", "订单信息", "智能看板"]:
        if st.button(p): st.session_state['page'] = p
    st.divider()
    with st.expander("🔑 权限校验"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 7. 频道逻辑 (拒绝删减) ---

if st.session_state['page'] == "数据中心":
    st.title("📂 洛阳录单中心 (全量满血版)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        st.subheader("订单同步维护")
        edit_dc = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态']], 
                                 column_config={"喂猫师": st.column_config.SelectboxColumn("指定人", options=active_sitters), "订单状态": st.column_config.SelectboxColumn("生命周期", options=["进行中", "已结束", "待处理"])}, 
                                 use_container_width=True)
        if st.button("🚀 提交同步并保存至飞书"):
            for i, row in edit_dc.iterrows():
                for f in ['订单状态', '喂猫师']:
                    if row[f] != df_raw.iloc[i][f]: update_feishu_field(df_raw.iloc[i]['_system_id'], f, row[f])
            st.success("同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("Excel 批量导入"):
            up = st.file_uploader("选择文件", type=["xlsx"])
            if up and st.button("🚀 推送云端"):
                du = pd.read_excel(up); tk = get_feishu_token()
                for i, (_, r) in enumerate(du.iterrows()):
                    f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')).strip(), "投喂频率": int(r.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk}"}, json={"fields": f})
                st.session_state.pop('feishu_cache', None); st.rerun()
    with c2:
        with st.expander("✍️ 手动新增订单"):
            with st.form("man_v106"):
                a = st.text_input("详细地址*"); n = st.text_input("小猫名字"); sd = st.date_input("开始日期"); ed = st.date_input("截止日期")
                if st.form_submit_button("💾 保存单笔录单"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                    st.session_state.pop('feishu_cache', None); st.rerun()

elif st.session_state['page'] == "任务进度":
    st.title("📊 现场反馈 (实时同步)")
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        st.dataframe(df_p[['宠物名字', '详细地址', '喂猫师', '订单状态']], use_container_width=True)

elif st.session_state['page'] == "订单信息":
    st.title("📝 财务对账全景 (159单闭环)")
    df_raw = st.session_state['feishu_cache']
    if not df_raw.empty:
        st.dataframe(df_raw[['宠物名字', '喂猫师', '服务开始日期', '服务结束日期', '订单状态', '详细地址']], use_container_width=True)

# --- 智能看板 (核心地图引擎复位) ---
elif st.session_state['page'] == "智能看板":
    st.title("🚀 指挥中心大屏 (高德原生全连版)")
    df_raw = st.session_state['feishu_cache'].copy()
    
    col_nav1, col_nav2 = st.columns([1, 3])
    with col_nav1:
        nav_mode = st.radio("🚲 出行模式", ["步行", "骑行/电动车", "地铁/公交"], index=1)
    
    c_btn1, c_btn2, c_btn3, c_spacer = st.columns([1, 1, 1, 4])
    if c_btn1.button("▶️ 开始拟定"): st.session_state['plan_state'] = "RUNNING"
    if c_btn3.button("⏹️ 重置看板"): 
        st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        df_kb = df_raw[df_raw['订单状态'].isin(s_filter)] if not df_raw.empty else df_raw
        if not df_kb.empty:
            with st.status("🛸 路径引擎计算中...") as status:
                days = pd.date_range(d_sel[0], d_sel[1]).tolist()
                ap = []
                for d in days:
                    ct = pd.Timestamp(d); d_v = df_kb[(df_kb['服务开始日期'] <= ct) & (df_kb['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, d_v['详细地址']))
                        d_v[['lng', 'lat']] = pd.DataFrame(coords, index=d_v.index)
                        for s in active:
                            stks = d_v[d_v['喂猫师'] == s].copy()
                            if not stks.empty:
                                res = optimize_route_v106(stks, nav_mode)
                                res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
                st.session_state['fp'] = pd.concat(ap) if ap else None
                status.update(label="✅ 任务拟定完成！", state="complete")
                st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        vd = st.selectbox("📅 选择日期", sorted(st.session_state['fp']['作业日期'].unique()))
        v_data = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        
        # --- 全显简报 (黑字深灰底，解决看不清问题) ---
        brief = f"📢 {vd} 任务简报 (全路段耗时)\n"
        for s in active:
            stks = v_data[v_data['喂猫师'] == s].sort_values('拟定顺序')
            if not stks.empty:
                brief += f"\n👤 【{s}】路线节奏：\n"
                for _, r in stks.iterrows():
                    dist, dur = int(r.get('next_dist', 0)), int(r.get('next_dur', 0))
                    line = f"  {int(r['拟定顺序'])}. {r['宠物名字']}-{r['详细地址']}"
                    if dur > 0: line += f" ➡️ (约 {dist}米, {dur}分钟)"
                    brief += line + "\n"
        
        st.text_area("📄 简报预览 (高对比度黑色文字)：", brief, height=250)
        copy_to_clipboard_v106(brief.replace('\n', '\\n'))
        
        # --- V106 核心地图复位 (独立颜色 + 100% 连续连线) ---
        map_df = v_data.dropna(subset=['lng', 'lat']).copy()
        if '作业日期' in map_df.columns: map_df['作业日期'] = map_df['作业日期'].astype(str)
        map_json = map_df[['lng', 'lat', '宠物名字', '喂猫师', '拟定顺序']].to_dict('records')
        
        if map_json:
            amap_html = f"""
            <div id="map_container" style="width:100%; height:600px; border:2.5px solid #000; border-radius:15px;"></div>
            <script type="text/javascript">
                window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }};
            </script>
            <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_API_KEY}&plugin=AMap.Walking,AMap.Riding,AMap.Transfer"></script>
            <script type="text/javascript">
                const map = new AMap.Map('map_container', {{ zoom: 16, center: [{map_json[0]['lng']}, {map_json[0]['lat']}] }});
                const data = {json.dumps(map_json)};
                const colors = {{"梦蕊": "#007BFF", "依蕊": "#FFA500"}};

                data.forEach(m => {{
                    const color = colors[m.喂猫师] || "#666";
                    new AMap.Marker({{
                        position: [m.lng, m.lat],
                        map: map,
                        content: `<div style="width:24px;height:24px;background:${{color}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:22px;font-size:12px;font-weight:bold;box-shadow:0 0 5px rgba(0,0,0,0.5);">${{m.拟定顺序}}</div>`
                    }}).setLabel({{ direction:'top', offset: new AMap.Pixel(0, -5), content: m.宠物名字 }});
                }});

                function drawChain(idx, sData, mode, color) {{
                    if (idx >= sData.length - 1) return;
                    let router;
                    const cfg = {{ map: map, hideMarkers: true, strokeColor: color, strokeOpacity: 0.9, strokeWeight: 6 }};
                    const mKey = {{"步行": "Walking", "骑行/电动车": "Riding", "地铁/公交": "Transfer"}}[mode];
                    
                    if (mKey === "Walking") router = new AMap.Walking(cfg);
                    else if (mKey === "Riding") router = new AMap.Riding(cfg);
                    else router = new AMap.Transfer({{ ...cfg, city: '深圳市' }});

                    router.search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], () => drawChain(idx + 1, sData, mode, color));
                }}

                ["梦蕊", "依蕊"].forEach(s => {{
                    const sData = data.filter(d => d.喂猫师 === s).sort((a,b)=>a.拟定顺序 - b.拟定顺序);
                    if(sData.length > 1) drawChain(0, sData, "{nav_mode}", colors[s]);
                }});
                setTimeout(() => map.setFitView(), 2000);
            </script>"""
            components.html(amap_html, height=620)
        st.dataframe(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '作业日期']], use_container_width=True)
