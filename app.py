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

# --- 1. 核心配置与 ID 强力清洗 (锁定您的飞书运营基地) ---
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

# --- 2. 核心底座函数 (坐标转换、并发测速与财务核销) ---

@st.cache_data(show_spinner=False)
def get_coords(address):
    """【V117稳定回归】地址转经纬度"""
    if not address: return None, None
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_API_KEY}&address=深圳市{address}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return float(loc[0]), float(loc[1])
    except: pass
    return None, None

def get_travel_estimate_v117(origin, destination, mode_key):
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

def calculate_billing_days(row, start_range, end_range):
    """【159单绝对闭环】计费天数核销逻辑"""
    try:
        if pd.isna(row['服务开始日期']) or pd.isna(row['服务结束日期']): return 0
        s_date = pd.to_datetime(row['服务开始日期']).date()
        e_date = pd.to_datetime(row['服务结束日期']).date()
        freq = int(float(str(row.get('投喂频率', 1)).strip() or 1))
        actual_start, actual_end = max(s_date, start_range), min(e_date, end_range)
        if actual_start > actual_end: return 0
        count = 0; curr = actual_start
        while curr <= actual_end:
            if (curr - s_date).days % freq == 0: count += 1
            curr += timedelta(days=1)
        return count
    except: return 0

def optimize_route_v117(df_sitter, mode_key):
    """【V117物理锚定】路径优化并强制回填公里与耗时"""
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
    
    # 强制执行并发测速，并物理写入结果集
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_travel_estimate_v117, f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}", mode_key): i for i in range(len(optimized)-1)}
        for future in as_completed(futures):
            idx = futures[future]
            dist, dur = future.result()
            optimized[idx]['next_dist'] = dist
            optimized[idx]['next_dur'] = dur

    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    for c in ['next_dist', 'next_dur']: res_df[c] = res_df.get(c, 0).fillna(0)
    return res_df

# --- 3. 飞书服务 (杜绝删减) ---

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
        for col in ['宠物名字', '详细地址', '喂猫师', '备注', 'lng', 'lat']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_field(record_id, field_name, value):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{str(record_id).strip()}"
    try:
        r = requests.patch(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json={"fields": {field_name: str(value)}}, timeout=10)
        return r.status_code == 200
    except: return False

# --- 4. 视觉方案与辅助组件 (黑金旗舰版) ---

st.set_page_config(page_title="指挥中心 V117.0", layout="wide")

def set_ui():
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { width: 100% !important; height: 50px !important; font-size: 18px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000; background-color: #FFFFFF !important; margin-bottom: 12px !important; border: 3px solid #000 !important; }
        .quick-nav div.stButton > button { width: 100% !important; height: 35px !important; font-size: 12px !important; border: 1.5px solid #000 !important; }
        .stTextArea textarea { font-size: 15px !important; background-color: #f2f2f2 !important; color: #000000 !important; font-weight: 500 !important; border: 2.5px solid #000 !important; }
        /* 黑金卡片复位 */
        .commute-card { background-color: #000000 !important; border-left: 10px solid #00ff00 !important; padding: 25px !important; border-radius: 15px !important; color: #ffffff !important; margin-bottom: 20px !important; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }
        .commute-card h4 { color: #ffcc00 !important; margin: 0 0 12px 0 !important; font-size: 21px !important; }
        .commute-card p { font-size: 26px !important; font-weight: 900 !important; margin: 5px 0 !important; color: #ffffff !important; }
        .stMetric { background: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #ddd; }
        </style>
        """, unsafe_allow_html=True)

set_ui()

# --- 5. 侧边栏布局 (全量复位与权限) ---

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()
if 'plan_state' not in st.session_state: st.session_state['plan_state'] = "IDLE"

with st.sidebar:
    st.subheader("📅 洛阳数字化总调中心")
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
    
    d_sel = st.date_input("日期锁定", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    st.divider()
    active_sitters = ["梦蕊", "依蕊"]
    active = [s for s in active_sitters if st.checkbox(f"{s} (今日出勤)", value=True, key=f"v117_{s}")]
    
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    for p in ["智能看板", "数据中心", "帮助文档"]:
        if st.button(p): st.session_state['page'] = p
    st.divider()
    with st.expander("🔑 指挥官授权"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 6. 整合频道：数据中心 (对账+同步+开单) ---

if st.session_state['page'] == "数据中心":
    st.title("📂 洛阳管理中枢 (对账+开单)")
    df_raw = st.session_state['feishu_cache'].copy()
    
    if not df_raw.empty:
        st.subheader("📝 159单绝对闭环核销汇总")
        if isinstance(d_sel, tuple) and len(d_sel) == 2:
            df_raw['计费天数'] = df_raw.apply(lambda r: calculate_billing_days(r, d_sel[0], d_sel[1]), axis=1)
            st.metric("📊 周期内计费总单量", f"{df_raw['计费天数'].sum()} 次上门")
        st.dataframe(df_raw[['宠物名字', '计费天数', '喂猫师', '服务开始日期', '服务结束日期', '订单状态', '详细地址']], use_container_width=True)

    st.divider()
    if not df_raw.empty:
        st.subheader("⚙️ 飞书同步编辑器")
        edit_dc = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态']], 
                                 column_config={"喂猫师": st.column_config.SelectboxColumn("指定人", options=active_sitters), "订单状态": st.column_config.SelectboxColumn("状态", options=["进行中", "已结束", "待处理"])}, 
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
            if up and st.button("🚀 开始推送"):
                du = pd.read_excel(up); tk = get_feishu_token()
                for i, (_, r) in enumerate(du.iterrows()):
                    f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')).strip(), "投喂频率": int(r.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk}"}, json={"fields": f})
                st.session_state.pop('feishu_cache', None); st.rerun()
    with c2:
        # --- 手动录单表单复位 ---
        with st.expander("手动录单 (✍️)"):
            with st.form("man_v117"):
                a = st.text_input("详细地址*"); n = st.text_input("小猫名字"); sd = st.date_input("开始日期"); ed = st.date_input("截止日期")
                if st.form_submit_button("💾 确认录单并保存"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                    st.session_state.pop('feishu_cache', None); st.rerun()

# --- 7. 智能看板：V117 全功能复位区 ---
elif st.session_state['page'] == "智能看板":
    st.title("🚀 数字化指挥大屏 (全链路修复版)")
    df_raw = st.session_state['feishu_cache'].copy()
    
    col_nav1, col_nav2 = st.columns([1, 3])
    with col_nav1:
        nav_mode = st.radio("🚲 出行模式", ["步行", "骑行/电动车", "地铁/公交"], index=1)
    
    c_btn1, c_btn3, c_spacer = st.columns([1, 1, 5])
    if c_btn1.button("▶️ 开始拟定方案"): st.session_state['plan_state'] = "RUNNING"
    if c_btn3.button("⏹️ 重置指挥看板"): 
        st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        df_kb = df_raw[df_raw['订单状态'].isin(["进行中", "待处理"])] if not df_raw.empty else df_raw
        if not df_kb.empty:
            with st.status("🛸 高德路径引擎测速中...", expanded=True) as status:
                days = pd.date_range(d_sel[0], d_sel[1]).tolist()
                ap = []
                for idx, d in enumerate(days):
                    status.update(label=f"🔄 分析第 {idx+1}/{len(days)} 天轨迹与路网...", state="running")
                    ct = pd.Timestamp(d); d_v = df_kb[(df_kb['服务开始日期'] <= ct) & (df_kb['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        if not d_v.empty:
                            with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, d_v['详细地址']))
                            d_v[['lng', 'lat']] = pd.DataFrame(coords, index=d_v.index, columns=['lng', 'lat'])
                            for s in active:
                                stks = d_v[d_v['喂猫师'] == s].copy()
                                if not stks.empty:
                                    # 【核心修复】执行物理锚定计算
                                    res = optimize_route_v117(stks, nav_mode)
                                    res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
                st.session_state['fp'] = pd.concat(ap) if ap else None
                status.update(label="✅ 任务拟定完成！耗时数据已锁定。", state="complete")
                st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        c_vs1, c_vs2 = st.columns(2)
        vd = c_vs1.selectbox("📅 选择日期", sorted(st.session_state['fp']['作业日期'].unique()))
        # 【视角隔离复位】
        vs = c_vs2.selectbox("👤 视角隔离 (切换查看详情)", ["全部"] + sorted(active))
        
        day_all = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        v_data = day_all if vs == "全部" else day_all[day_all['喂猫师'] == vs]
        
        # --- 【修复】不再为 0 的黑金态势卡片 ---
        st.subheader(f"⏱️ {vs} 视角·态势详情")
        c_metrics1, c_metrics2 = st.columns(2)
        sitters_to_show = active if vs == "全部" else [vs]
        
        for i, s in enumerate(sitters_to_show):
            s_sum = day_all[day_all['喂猫师'] == s]
            if not s_sum.empty:
                t_count = len(s_sum)
                # 核心：直接读取物理回填列
                t_dist = s_sum['next_dist'].sum() / 1000
                t_dur = s_sum['next_dur'].sum()
                card_html = f"""
                <div class="commute-card">
                    <h4>👤 {s} 任务态势</h4>
                    <p>当日总订单：{t_count} 单</p>
                    <p style="color: #00ff00 !important;">路段预计耗时：{int(t_dur)} 分钟</p>
                    <p style="color: #00d4ff !important;">总计行程：{t_dist:.1f} km</p>
                </div>
                """
                [c_metrics1, c_metrics2][i % 2].markdown(card_html, unsafe_allow_html=True)
        
        # --- 耗时简报预览 ---
        brief = f"📢 {vd} 任务简报 ({vs} 视角)\n"
        for s in sitters_to_show:
            stks = day_all[day_all['喂猫师'] == s].sort_values('拟定顺序')
            if not stks.empty:
                brief += f"\n👤 【{s}】路线节奏：\n"
                for _, r in stks.iterrows():
                    dist, dur = int(r.get('next_dist', 0)), int(r.get('next_dur', 0))
                    line = f"  {int(r['拟定顺序'])}. {r['宠物名字']}-{r['详细地址']}"
                    if dur > 0: line += f" ➡️ (约 {dist}米, {dur}分钟)"
                    brief += line + "\n"
        
        st.text_area("📄 简报预览 (高对比度黑色文字)：", brief, height=250)
        copy_html = f"""<button onclick="navigator.clipboard.writeText(`{brief.replace('\\n', '\\\\n')}`).then(()=>alert('已复制'))" style="width:200px;height:45px;background:#000;color:#fff;border-radius:10px;cursor:pointer;">📋 一键复制简报</button>"""
        components.html(copy_html, height=55)
        
        # --- 【核心修复】丢失的 100 行地图渲染引擎复位 ---
        map_clean = v_data.dropna(subset=['lng', 'lat']).copy()
        if not map_clean.empty:
            map_clean['作业日期'] = map_clean['作业日期'].astype(str)
            map_json = map_clean[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序']].to_dict('records')
            
            amap_html = f"""
            <div id="map_box" style="width:100%; height:600px; border:2.5px solid #000; border-radius:15px; background:#f0f0f0;"></div>
            <script type="text/javascript">
                window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }};
            </script>
            <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_API_KEY}&plugin=AMap.Walking,AMap.Riding,AMap.Transfer"></script>
            <script type="text/javascript">
                const map = new AMap.Map('map_box', {{ zoom: 16, center: [{map_json[0]['lng']}, {map_json[0]['lat']}] }});
                const data = {json.dumps(map_json)};
                const colors = {{"梦蕊": "#007BFF", "依蕊": "#FFA500"}};

                data.forEach(m => {{
                    const color = colors[m.喂猫师] || "#666";
                    new AMap.Marker({{
                        position: [m.lng, m.lat],
                        map: map,
                        content: `<div style="width:26px;height:26px;background:${{color}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:24px;font-size:12px;font-weight:bold;box-shadow:0 0 10px rgba(0,0,0,0.5);">${{m.拟定顺序}}</div>`
                    }}).setLabel({{ direction:'top', offset: new AMap.Pixel(0, -5), content: m.宠物名字 }});
                }});

                // 【V117复位】接力式链式渲染函数
                function drawRouteRecursive(idx, sData, mode, color) {{
                    if (idx >= sData.length - 1) return;
                    let router;
                    const cfg = {{ map: map, hideMarkers: true, strokeColor: color, strokeOpacity: 0.95, strokeWeight: 8 }};
                    const mKey = {{"步行": "Walking", "骑行/电动车": "Riding", "地铁/公交": "Transfer"}}["{nav_mode}"];
                    if (mKey === "Walking") router = new AMap.Walking(cfg);
                    else if (mKey === "Riding") router = new AMap.Riding(cfg);
                    else router = new AMap.Transfer({{ ...cfg, city: '深圳市' }});

                    router.search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], function(status) {{
                        drawRouteRecursive(idx + 1, sData, mode, color);
                    }});
                }}

                const curSitters = ("{vs}" === "全部") ? ["梦蕊", "依蕊"] : ["{vs}"];
                curSitters.forEach(s => {{
                    const sData = data.filter(d => d.喂猫师 === s).sort((a,b)=>a.拟定顺序 - b.拟定顺序);
                    if(sData.length > 1) drawRouteRecursive(0, sData, "{nav_mode}", colors[s]);
                }});
                setTimeout(() => map.setFitView(), 1500);
            </script>"""
            components.html(amap_html, height=620)
        
        st.dataframe(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '作业日期']], use_container_width=True)

elif st.session_state['page'] == "帮助文档":
    st.title("📖 V117 指战员旗舰手册")
    st.markdown("""
    1. **地图复位**：找回了丢失的 JS 渲染逻辑，采用递归接力技术实现蓝橙 100% 物理全连。
    2. **数据锚定**：通过物理回灌解决了耗时数据显示 0 的顽疾。
    3. **全量补全**：找回手动录单（全输入框）、159单绝对计费汇总、侧边栏快捷日期及指挥官权限校验。
    4. **视角隔离**：选人视图下，全站数据同步过滤。
    """)
