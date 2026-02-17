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

# --- 1. 核心配置与 ID 清洗 ---
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

# --- 2. 调度与测速核心 ---
def get_travel_estimate_v102(origin, destination, mode_key):
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

def optimize_route_v102(df_sitter, mode_key):
    has_coords = df_sitter.dropna(subset=['lng', 'lat']).copy()
    no_coords = df_sitter[df_sitter['lng'].isna()].copy()
    if len(has_coords) <= 1:
        res = pd.concat([has_coords, no_coords])
        res['拟定顺序'] = range(1, len(res) + 1)
        res['next_dist'], res['next_dur'] = 0, 0
        return res
    
    # 贪心排序
    unvisited = has_coords.to_dict('records')
    curr_node = unvisited.pop(0); optimized = [curr_node]
    while unvisited:
        next_node = min(unvisited, key=lambda x: np.sqrt((curr_node['lng']-x['lng'])**2 + (curr_node['lat']-x['lat'])**2))
        unvisited.remove(next_node); optimized.append(next_node); curr_node = next_node
    
    # 并发测速并物理回填
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_travel_estimate_v102, f"{optimized[i]['lng']},{optimized[i]['lat']}", f"{optimized[i+1]['lng']},{optimized[i+1]['lat']}", mode_key): i for i in range(len(optimized)-1)}
        for future in as_completed(futures):
            idx = futures[future]
            dist, dur = future.result()
            optimized[idx]['next_dist'], optimized[idx]['next_dur'] = dist, dur

    res_df = pd.concat([pd.DataFrame(optimized), no_coords])
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    res_df['next_dist'] = res_df.get('next_dist', 0).fillna(0)
    res_df['next_dur'] = res_df.get('next_dur', 0).fillna(0)
    return res_df

# --- 3. 飞书服务 (保留全量功能) ---
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
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items]) if items else pd.DataFrame()
        if not df.empty:
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
def copy_to_clipboard_v102(text):
    html = f"""<button onclick="navigator.clipboard.writeText('{text}').then(()=>alert('复制成功'))" style="width:200px;height:45px;background:#000;color:#fff;border-radius:10px;font-weight:bold;cursor:pointer;">📋 一键复制简报</button>"""
    components.html(html, height=55)

# --- 5. UI 布局 ---
st.set_page_config(page_title="指挥中心 V102.0", layout="wide")
st.markdown("""<style>.main-nav button { width:100%; height:50px; font-weight:800; border:2px solid #000; margin-bottom:10px; } .stTextArea textarea { font-size:14px; background:#f9f9f9; border:1px solid #000; }</style>""", unsafe_allow_html=True)

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()
if 'plan_state' not in st.session_state: st.session_state['plan_state'] = "IDLE"

with st.sidebar:
    st.subheader("📅 调度控制")
    td = datetime.now().date()
    d_sel = st.date_input("日期范围", value=(td, td + timedelta(days=1)))
    s_filter = st.multiselect("状态", options=["进行中", "已结束", "待处理"], default=["进行中", "待处理"])
    active = [s for s in ["梦蕊", "依蕊"] if st.checkbox(f"{s} (出勤)", value=True)]
    st.divider()
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    for p in ["数据中心", "订单信息", "智能看板"]:
        if st.button(p): st.session_state['page'] = p
    st.markdown('</div>', unsafe_allow_html=True)

# --- 7. 频道逻辑 ---
if st.session_state['page'] == "数据中心":
    st.title("📂 录单中心 (洛阳总部)")
    df = st.session_state['feishu_cache'].copy()
    if not df.empty:
        st.subheader("订单状态同步")
        edit = st.data_editor(df[['宠物名字', '详细地址', '喂猫师', '订单状态']], use_container_width=True)
        if st.button("🚀 同步修改"):
            for i, row in edit.iterrows():
                for f in ['订单状态', '喂猫师']:
                    if row[f] != df.iloc[i][f]: update_feishu_field(df.iloc[i]['_system_id'], f, row[f])
            st.session_state.pop('feishu_cache', None); st.rerun()
    st.divider()
    with st.form("manual_entry"):
        st.subheader("✍️ 手动新增订单")
        c1, c2 = st.columns(2)
        addr = c1.text_input("地址*"); name = c2.text_input("猫名")
        sd = c1.date_input("开始"); ed = c2.date_input("结束")
        if st.form_submit_button("💾 保存录单"):
            f = {"详细地址": addr, "宠物名字": name, "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000), "订单状态": "进行中"}
            requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
            st.session_state.pop('feishu_cache', None); st.rerun()

elif st.session_state['page'] == "订单信息":
    st.title("📝 财务对账 (159单闭环)")
    df = st.session_state['feishu_cache']
    if not df.empty:
        st.dataframe(df[['宠物名字', '喂猫师', '服务开始日期', '服务结束日期', '订单状态', '详细地址']], use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度看板 (颜色/连线/耗时修复版)")
    nav_mode = st.radio("出行模式", ["步行", "骑行/电动车", "地铁/公交"], horizontal=True)
    m_key = {"步行": "Walking", "骑行/电动车": "Riding", "地铁/公交": "Transfer"}[nav_mode]
    
    c1, c2, c3 = st.columns(3)
    if c1.button("▶️ 开始拟定"): st.session_state['plan_state'] = "RUNNING"
    if c2.button("⏹️ 重置"): st.session_state['plan_state'] = "IDLE"; st.session_state.pop('fp', None); st.rerun()

    if st.session_state['plan_state'] == "RUNNING":
        df = st.session_state['feishu_cache']
        df = df[df['订单状态'].isin(s_filter)] if not df.empty else df
        if not df.empty:
            with st.status("🛸 路径引擎计算中...") as status:
                days = pd.date_range(d_sel[0], d_sel[1]).tolist()
                ap = []
                for d in days:
                    ct = pd.Timestamp(d); d_v = df[(df['服务开始日期'] <= ct) & (df['服务结束日期'] >= ct)].copy()
                    if not d_v.empty:
                        d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                        with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, d_v['详细地址']))
                        d_v[['lng', 'lat']] = pd.DataFrame(coords, index=d_v.index)
                        for s in active:
                            stks = d_v[d_v['喂猫师'] == s].copy()
                            if not stks.empty:
                                res = optimize_route_v102(stks, m_key)
                                res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
                st.session_state['fp'] = pd.concat(ap) if ap else None
                status.update(label="✅ 计算完成", state="complete")
            st.session_state['plan_state'] = "IDLE"

    if st.session_state.get('fp') is not None:
        vd = st.selectbox("日期", sorted(st.session_state['fp']['作业日期'].unique()))
        v_data = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
        
        # 简报生成 (耗时对齐)
        brief = f"📢 {vd} 任务简报\n"
        for s in active:
            stks = v_data[v_data['喂猫师'] == s].sort_values('拟定顺序')
            if not stks.empty:
                brief += f"\n👤 【{s}】:\n"
                for _, r in stks.iterrows():
                    line = f"  {int(r['拟定顺序'])}. {r['宠物名字']}-{r['详细地址']}"
                    if r.get('next_dur', 0) > 0: line += f" ➡️ ({int(r['next_dist'])}米, {int(r['next_dur'])}分)"
                    brief += line + "\n"
        
        st.text_area("📄 简报预览", brief, height=200)
        copy_to_clipboard_v102(brief.replace('\n', '\\n'))
        
        # 地图逻辑 (独立颜色/连续绘制)
        map_json = v_data.dropna(subset=['lng', 'lat']).to_dict('records')
        if map_json:
            amap_html = f"""
            <div id="container" style="width:100%; height:600px; border:2px solid #000;"></div>
            <script type="text/javascript">
                window._AMapSecurityConfig = {{ securityJsCode: "{AMAP_JS_CODE}" }};
            </script>
            <script type="text/javascript" src="https://webapi.amap.com/maps?v=2.0&key={AMAP_API_KEY}&plugin=AMap.Walking,AMap.Riding,AMap.Transfer"></script>
            <script type="text/javascript">
                const map = new AMap.Map('container', {{ zoom: 16, center: [{map_json[0]['lng']}, {map_json[0]['lat']}] }});
                const data = {json.dumps(map_json)};
                const sitters = ["梦蕊", "依蕊"];
                const colors = {{"梦蕊": "#007BFF", "依蕊": "#FFA500"}};

                sitters.forEach(s => {{
                    const sData = data.filter(d => d.喂猫师 === s).sort((a,b) => a.拟定顺序 - b.拟定顺序);
                    if(sData.length === 0) return;
                    
                    sData.forEach(m => {{
                        const marker = new AMap.Marker({{
                            position: [m.lng, m.lat],
                            map: map,
                            content: `<div style="width:24px;height:24px;background:${{colors[s]}};border:2px solid #fff;border-radius:50%;color:#fff;text-align:center;line-height:20px;font-size:12px;font-weight:bold;">${{m.拟定顺序}}</div>`
                        }});
                    }});

                    function drawSequential(idx) {{
                        if (idx >= sData.length - 1) return;
                        let router;
                        const cfg = {{ map: map, hideMarkers: true, strokeColor: colors[s], strokeOpacity: 0.9, strokeWeight: 6 }};
                        if("{m_key}" === "Walking") router = new AMap.Walking(cfg);
                        else if("{m_key}" === "Riding") router = new AMap.Riding(cfg);
                        else router = new AMap.Transfer({{ ...cfg, city: '深圳市' }});
                        
                        router.search([sData[idx].lng, sData[idx].lat], [sData[idx+1].lng, sData[idx+1].lat], (status) => {{
                            drawSequential(idx + 1);
                        }});
                    }}
                    drawSequential(0);
                }});
                setTimeout(() => map.setFitView(), 2000);
            </script>"""
            components.html(amap_html, height=620)
