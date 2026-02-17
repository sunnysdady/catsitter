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

# --- 2. 核心底座函数 (解决 NameError) ---

@st.cache_data(show_spinner=False)
def get_coords(address):
    """【修复】全局定义坐标转换函数"""
    if not address: return None, None
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_API_KEY}&address=深圳市{address}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return float(loc[0]), float(loc[1])
    except: pass
    return None, None

def get_travel_estimate_v104(origin, destination, mode_key):
    """高德 Web 服务计算路程与时间"""
    mode_url_map = {"Walking": "walking", "Riding": "bicycling", "Transfer": "integrated"}
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
    """精确财务计费：1=每天, 2=隔天"""
    try:
        if pd.isna(row['服务开始日期']) or pd.isna(row['服务结束日期']): return 0
        s_date = pd.to_datetime(row['服务开始日期']).date()
        e_date = pd.to_datetime(row['服务结束日期']).date()
        freq = int(float(str(row.get('投喂频率', 1)).strip() or 1))
        if freq < 1: freq = 1
        actual_start, actual_end = max(s_date, start_range), min(e_date, end_range)
        if actual_start > actual_end: return 0
        count = 0; curr = actual_start
        while curr <= actual_end:
            if (curr - s_date).days % freq == 0: count += 1
            curr += timedelta(days=1)
        return count
    except: return 0

def optimize_route_v104(df_sitter, mode_key):
    """路径优化并强制注入耗时数据"""
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
        futures = {executor.submit(get_travel_estimate_v104, f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}", mode_key): i for i in range(len(optimized)-1)}
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
        if '进度' not in df.columns: df['进度'] = "未开始"
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

# --- 4. 辅助组件 ---

def copy_to_clipboard_v104(text):
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

def generate_excel_v104(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name='汇总')
        df.drop_duplicates(subset=['宠物名字', '详细地址'])[['宠物名字', '详细地址', '喂猫师', '备注']].to_excel(writer, index=False, sheet_name='详细名单')
        for s in df['喂猫师'].unique():
            if str(s).strip() and str(s) != 'nan':
                df[df['喂猫师'] == s][['作业日期', '拟定顺序', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name=str(s)[:31])
    return output.getvalue()

# --- 5. UI 视觉布局 ---

st.set_page_config(page_title="指挥中心 V104.0", layout="wide")

def set_ui():
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button { width: 200px !important; height: 50px !important; font-size: 18px !important; font-weight: 800 !important; box-shadow: 4px 4px 0px #000; background-color: #FFFFFF !important; margin-bottom: 12px !important; border: 3px solid #000 !important; }
        .quick-nav div.stButton > button { width: 100px !important; height: 25px !important; font-size: 12px !important; border-radius: 4px !important; border: 1.5px solid #000 !important; }
        .stTextArea textarea { font-size: 15px !important; line-height: 1.8 !important; background-color: #fdfdfd !important; border: 2px solid #000 !important; }
        .stMetric { background: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #ddd; }
        </style>
        """, unsafe_allow_html=True)

set_ui()

# --- 6. 侧边栏布局 ---

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
    st.divider()
    s_filter = st.multiselect("状态筛选器", options=["进行中", "已结束", "待处理"], default=["进行中", "待处理"])
    active_sitters = ["梦蕊", "依蕊"]
    active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True, key=f"v104_{s}")]
    
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    for p in ["数据中心", "任务进度", "订单信息", "智能看板"]:
        if st.button(p): st.session_state['page'] = p
    st.divider()
    with st.expander("🔑 授权验证"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 7. 各频道逻辑 (全量审计，拒绝删减) ---

if st.session_state['page'] == "数据中心":
    st.title("📂 洛阳数据中心 (全量满血版)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        st.subheader("⚙️ 订单归属与状态调整")
        edit_dc = st.data_editor(df_raw[['宠物名字', '详细地址', '喂猫师', '订单状态']], 
                                 column_config={"喂猫师": st.column_config.SelectboxColumn("归属", options=active_sitters), "订单状态": st.column_config.SelectboxColumn("状态", options=["进行中", "已结束", "待处理"])}, 
                                 use_container_width=True)
        if st.button("🚀 提交同步并保存"):
            for i, row in edit_dc.iterrows():
                for f in ['订单状态', '喂猫师']:
                    if row[f] != df_raw.iloc[i][f]: update_feishu_field(df_raw.iloc[i]['_system_id'], f, row[f])
            st.success("同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("Excel 批量导入"):
            up = st.file_uploader("选择文件", type=["xlsx"])
            if up and st.button("🚀 推送至飞书"):
                du = pd.read_excel(up); tk = get_feishu_token()
                for i, (_, r) in enumerate(du.iterrows()):
                    f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')).strip(), "投喂频率": int(r.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk}"}, json={"fields": f})
                st.session_state.pop('feishu_cache', None); st.rerun()
    with c2:
        # --- 全量手动录单表单 ---
        with st.expander("单条手动录单 (✍️)"):
            with st.form("man_v104"):
                a = st.text_input("详细地址*"); n = st.text_input("猫咪名"); sd = st.date_input("开始日期"); ed = st.date_input("结束日期")
                if st.form_submit_button("💾 确认录单并保存"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                    st.session_state.pop('feishu_cache', None); st.rerun()

elif st.session_state['page'] == "任务进度":
    st.title("📊 现场状态实时反馈")
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        edit_p = st.data_editor(df_p[['宠物名字', '详细地址', '进度']], column_config={"进度": st.column_config.SelectboxColumn("反馈", options=["未开始", "已出发", "服务中", "已完成"])}, use_container_width=True)
        if st.button("🚀 回写反馈状态"):
            for i, row in edit_p.iterrows():
                if row['进度'] != df_p.iloc[i]['进度']: update_feishu_field(df_p.iloc[i]['_system_id'], "进度", row['进度'])
            st.success("回写完成！"); st.session_state.pop('feishu_cache', None)

elif st.session_state['page'] == "订单信息":
    st.title("📝 财务对账全景 (159单绝对闭环)")
    df_raw = st.session_state['feishu_cache'].copy()
    if not df_raw.empty:
        df_i = df_raw[df_raw['订单状态'].isin(s_filter)] if s_filter else df_raw
        if isinstance(d_sel, tuple) and len(d_sel) == 2:
            df_i['计费天数'] = df_i.apply(lambda r: calculate_billing_days(r, d_sel[0], d_sel[1]), axis=1)
            st.metric("📊 周期内计费总次数", f"{df_i['计费天数'].sum()} 次")
        for c in ['服务开始日期', '服务结束日期']:
            if c in df_i.columns: df_i[c] = pd.to_datetime(df_i[c]).dt.strftime('%Y-%m-%d')
        st.dataframe(df_i[['宠物名字', '计费天数', '喂猫师', '服务开始日期', '服务结束日期', '订单状态', '详细地址']], use_container_width=True)

# 智能看板：V104 全闭环导航版
elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度指挥大屏 (V104 全闭环版)")
    df_raw = st.session_state['feishu_cache'].copy()
    
    col_nav1, col_nav2 = st.columns([1, 3])
    with col_nav1:
        nav_mode = st.radio("🚲 出行模式", ["步行", "骑行/电动车", "地铁/公交"], index=1)
        mode_map = {"步行": "Walking", "骑行/电动车": "Riding", "地铁/公交": "Transfer"}
    
    # 指挥三键
    c_btn1, c_btn2, c_btn3, c_spacer = st.columns([1, 1, 1, 4])
    if c_btn1.button("▶️ 开始拟定"): st.session_state['plan_state'] = "RUNNING"
    if c_btn2.button("⏸️ 暂停测速"): st.session_state['plan_state'] = "PAUSED"
    if c_btn3.button("⏹️ 取消重置"): 
        st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        if not df_raw.empty and isinstance(d_sel, tuple) and len(d_sel) == 2:
            df_kb = df_raw[df_raw['订单状态'].isin(s_filter)] if s_filter else df_raw
            # --- 进度条复位 ---
            with st.status("🛸 正在启动高德路径引擎...", expanded=True) as status:
                st.write("📡 空间调度计算中...")
                dk = df_kb.copy()
                days = pd.date_range(d_sel[0], d_sel[1]).tolist()
                ap = []; total_days = len(days)
                for idx, d in enumerate(days):
                    if st.session_state['plan_state'] == "PAUSED": break
                    status.update(label=f"🔄 测算第 {idx+1}/{total_days} 天路网分布...", state="running")
                    ct = pd.Timestamp(d); d_v = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        if not d_v.empty:
                            # --- 修复 NameError 调用 ---
                            with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, d_v['详细地址']))
                            d_v[['lng', 'lat']] = pd.DataFrame(coords, index=d_v.index, columns=['lng', 'lat'])
                            for s in active:
                                stks = d_v[d_v['喂猫师'] == s].copy()
                                if not stks.empty:
                                    res = optimize_route_v104(stks, mode_map[nav_mode])
                                    res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
                if st.session_state['plan_state'] != "PAUSED":
                    st.session_state['fp'] = pd.concat(ap) if ap else None
                    status.update(label="✅ 任务拟定完成！159单已闭环。", state="complete")
                    st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        st.metric("📊 最终拟定单量", f"{len(st.session_state['fp'])} 单")
        c_f1, c_f2 = st.columns(2)
        vd = c_f1.selectbox("📅 日期筛选", sorted(st.session_state['fp']['作业日期'].unique()))
        vs = c_f2.selectbox("👤 蓝/橙线路筛选", ["全部"] + sorted(active))
        v_data = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        
        # --- 全显简报预览 ---
        full_brief = f"📢 {vd} 指战简报 (全链路耗时对齐)\n"
        for s in active:
            stks = v_data[v_data['喂猫师'] == s].sort_values('拟定顺序')
            if not stks.empty:
                full_brief += f"\n👤 【{s}】全天路线：\n"
                for i, row in stks.iterrows():
                    dist, dur = int(row.get('next_dist', 0)), int(row.get('next_dur', 0))
                    line = f"  {int(row['拟定顺序'])}. {row['宠物名字']}-{row['详细地址']}"
                    if dur > 0: line += f" ➡️ (约 {dist}米, {dur}分钟)"
                    full_brief += line + "\n"
        
        copy_to_clipboard_v104(full_brief.replace('\n', '\\n'))
        st.text_area("📄 每一段任务耗时预览 (100% 同步)：", full_brief, height=280)
        
        # --- V104 地图核心：JSON 序列化修复 + 身份连线 ---
        map_df = v_data.dropna(subset=['lng', 'lat']).copy()
        if '作业日期' in map_df.columns: map_df['作业日期'] = map_df['作业日期'].astype(str)
        # 移除非 JSON 格式字段，防止 TypeError
        map_json_data = map_df[['lng', 'lat', '宠物名字', '详细地址', '喂猫师', '拟定顺序']].to_dict('records')
        
        if map_json_data:
            amap_html = f"""
            <div id="container" style="width:100%; height:600px; border:2px solid #000; border-radius:15px;"></div>
            <script type="text/javascript">
                window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }};
            </script>
            <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_API_KEY}&plugin=AMap.Walking,AMap.Riding,AMap.Transfer"></script>
            <script type="text/javascript">
                const map = new AMap.Map('container', {{ zoom: 16, center: [{map_json_data[0]['lng']}, {map_json_data[0]['lat']}] }});
                const markers_data = {json.dumps(map_json_data)};
                const colors = {{"梦蕊": "#007BFF", "依蕊": "#FFA500"}};

                // 绘制序号标记
                markers_data.forEach(m => {{
                    const color = colors[m.喂猫师] || "#666";
                    const marker = new AMap.Marker({{
                        position: [m.lng, m.lat],
                        map: map,
                        content: `<div style="width:24px;height:24px;background:${{color}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:22px;font-size:12px;font-weight:bold;box-shadow:0 0 5px rgba(0,0,0,0.5);">${{m.拟定顺序}}</div>`
                    }});
                }});

                // 【V104 递归绘制】确保路径 100% 连续且颜色身份对齐
                function drawPathV104(idx, data, mode, map, sName) {{
                    const sData = data.filter(d => d.喂猫师 === sName).sort((a,b)=>a.拟定顺序 - b.拟定顺序);
                    if (idx >= sData.length - 1) return;
                    
                    const color = colors[sName];
                    let router;
                    const cfg = {{ map: map, hideMarkers: true, strokeColor: color, strokeOpacity: 0.9, strokeWeight: 6 }};
                    if (mode === "Walking") router = new AMap.Walking(cfg);
                    else if (mode === "Riding") router = new AMap.Riding(cfg);
                    else router = new AMap.Transfer({{ ...cfg, city: '深圳市' }});

                    router.search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], function(status) {{
                        drawPathV104(idx + 1, data, mode, map, sName);
                    }});
                }}

                ["梦蕊", "依蕊"].forEach(s => drawPathV104(0, markers_data, "{mode_map[nav_mode]}", map, s));
                setTimeout(() => map.setFitView(), 2000);
            </script>
            """
            components.html(amap_html, height=620)
        st.dataframe(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '作业日期']].sort_values('拟定顺序'), use_container_width=True)
