import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import re
import io
import calendar

# --- 1. 核心配置与 ID 清洗 ---
def clean_id(raw_id):
    if not raw_id: return ""
    # 提取 bas/tbl/rec 开头的 20-30 位 ID
    match = re.search(r'(bas|tbl|rec)[a-zA-Z0-9]+', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "")) 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 调度大脑逻辑 ---

def get_distance(p1, p2):
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    if len(df_sitter) <= 1:
        df_sitter['拟定顺序'] = range(1, len(df_sitter) + 1)
        return df_sitter
    unvisited = df_sitter.to_dict('records')
    current_node = unvisited.pop(0)
    optimized_list = [current_node]
    while unvisited:
        next_node = min(unvisited, key=lambda x: get_distance(
            (current_node['lng'], current_node['lat']), (x['lng'], x['lat'])
        ))
        unvisited.remove(next_node)
        optimized_list.append(next_node)
        current_node = next_node
    res_df = pd.DataFrame(optimized_list)
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

def execute_smart_dispatch(df, active_sitters):
    """归属记忆引擎"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    cat_to_sitter_map = {f"{row['宠物名字']}_{row['详细地址']}": str(row['喂猫师']).strip() 
                         for _, row in df.iterrows() if str(row.get('喂猫师', '')).strip() not in ["", "nan"]}
    sitter_load = {s: 0 for s in active_sitters}
    for s in df['喂猫师']:
        if s in sitter_load: sitter_load[s] += 1
    for i, row in df.iterrows():
        if str(row.get('喂猫师', '')).strip() not in ["", "nan"]: continue
        key = f"{row['宠物名字']}_{row['详细地址']}"
        if key in cat_to_sitter_map:
            df.at[i, '喂猫师'] = cat_to_sitter_map[key]
        elif active_sitters:
            best = min(sitter_load, key=sitter_load.get)
            df.at[i, '喂猫师'] = best
            cat_to_sitter_map[key] = best
            sitter_load[best] += 1
    return df

# --- 3. 飞书 API 交互 (带 404 诊断) ---

def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("tenant_access_token")
    except: return None

def fetch_feishu_data():
    token = get_feishu_token()
    if not token or not APP_TOKEN or not TABLE_ID: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, params={"page_size": 500}, timeout=15).json()
        items = r.get("data", {}).get("items", [])
        if not items: return pd.DataFrame()
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        for c in ['服务开始日期', '服务结束日期']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        if '进度' not in df.columns: df['进度'] = "未开始"
        for col in ['宠物名字', '详细地址', '喂猫师', '备注', 'lng', 'lat', '投喂频率']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_diagnostic(record_id, field_name, value):
    """【V49 核心】诊断级同步：显示真实 URL"""
    token = get_feishu_token()
    clean_rid = str(record_id).strip()
    if not APP_TOKEN or not TABLE_ID or not clean_rid:
        return False, "ID 缺失，请检查 Secrets 配置"
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{clean_rid}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fields": {field_name: str(value)}}
    
    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            return True, "Success"
        else:
            # 这里的详细报错会显示在页面上
            return False, f"HTTP {r.status_code} | Path: .../records/{clean_rid} | Msg: {r.text}"
    except Exception as e:
        return False, f"Exception: {str(e)}"

# --- 4. 视觉方案 (200*50 与 100*25) ---

def set_ui():
    st.markdown("""
        <style>
        /* A. 主导航 (200*50) */
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button {
            width: 200px !important; height: 50px !important;
            border: 3px solid #000 !important; border-radius: 10px !important;
            font-size: 18px !important; font-weight: 800 !important;
            box-shadow: 4px 4px 0px #000; background-color: #FFFFFF !important;
            margin-bottom: 12px !important; display: block; margin-left: auto; margin-right: auto;
        }
        /* B. 快捷调度 (100*25) */
        .quick-nav div.stButton > button {
            width: 100px !important; height: 25px !important;
            font-size: 11px !important; padding: 0px !important;
            border: 1.5px solid #000 !important; border-radius: 4px !important;
            box-shadow: 1.5px 1.5px 0px #000; margin: 2px !important;
        }
        .stMetric { background: white; padding: 10px; border-radius: 5px; border: 1px solid #eee; }
        .diagnostic-box { background: #fff1f0; border: 1px solid #ffa39e; padding: 15px; border-radius: 5px; color: #cf1322; font-family: monospace; }
        </style>
        """, unsafe_allow_html=True)

def generate_excel_multisheet(df):
    output = io.BytesIO()
    full_df = df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values(['作业日期', '喂猫师', '拟定顺序'])
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        full_df.to_excel(writer, index=False, sheet_name='汇总')
        for s in df['喂猫师'].unique():
            if str(s).strip() and str(s) != 'nan':
                df[df['喂猫师'] == s][['作业日期', '拟定顺序', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name=str(s)[:31])
    return output.getvalue()

@st.cache_data(show_spinner=False)
def get_coords(address):
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_API_KEY}&address=深圳市{address}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            loc = r['geocodes'][0]['location'].split(',')
            return float(loc[0]), float(loc[1])
    except: pass
    return None, None

# --- 5. 侧边栏布局重构 (V49 置顶指挥舱) ---

st.set_page_config(page_title="指挥中心 V49.0", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()

with st.sidebar:
    # 1. 【置顶】调度配置舱
    st.subheader("📅 快捷范围 (100*25)")
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
    
    d_sel = st.date_input("调度区间", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    sitters = ["梦蕊", "依蕊"]
    active = [s for s in sitters if st.checkbox(f"{s} (出勤)", value=True, key=f"active_{s}")]
    
    st.divider()

    # 2. 【居中】功能频道按钮 (200*50)
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("📊 任务进度"): st.session_state['page'] = "任务进度"
    if st.button("📝 订单信息"): st.session_state['page'] = "订单信息"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    st.markdown('</div>', unsafe_allow_html=True)

    st.divider()

    # 3. 【沉底】帮助与授权
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    if st.button("📖 帮助文档"): st.session_state['page'] = "帮助文档"
    st.markdown('</div>', unsafe_allow_html=True)
    with st.expander("🔑 团队授权"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 6. 逻辑频道渲染 ---

if st.session_state['page'] == "帮助文档":
    st.title("📖 V49 诊断指引")
    st.markdown("""
    ### 🛑 遇到 404 怎么办？
    1. **检查 ID**：请核对你的 `FEISHU_TABLE_ID` 是否是以 `tbl...` 开头的字符串，而不是工作表的名称。
    2. **手动方案**：如果同步失败，请点击智能看板的“导出 Excel”，然后手动将人员名单粘贴回飞书。
    3. **环境自检**：
    """)
    st.code(f"APP_TOKEN: {APP_TOKEN[:5]}... | TABLE_ID: {TABLE_ID[:5]}...")

elif st.session_state['page'] == "任务进度":
    st.title("📊 任务进度闭环")
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        edit = st.data_editor(df_p[['宠物名字', '详细地址', '喂猫师', '进度']], 
                              column_config={"进度": st.column_config.SelectboxColumn("状态", options=["未开始", "已出发", "服务中", "已完成"], required=True)}, 
                              use_container_width=True)
        if st.button("🚀 提交全部更新"):
            sc = 0; err_log = ""
            for i, row in edit.iterrows():
                if row['进度'] != df_p.iloc[i]['进度']:
                    ok, msg = update_feishu_diagnostic(df_p.iloc[i]['_system_id'], "进度", row['进度'])
                    if ok: sc += 1
                    else: err_log = msg
            if sc: st.success(f"已同步 {sc} 条。")
            if err_log: st.error(f"报错详情：{err_log}")
            st.session_state.pop('feishu_cache', None)

elif st.session_state['page'] == "数据中心":
    st.title("📂 云端快照同步")
    up = st.file_uploader("导入新订单", type=["xlsx"])
    if up and st.button("🚀 推送云端"):
        du = pd.read_excel(up); pb = st.progress(0); tk = get_feishu_token()
        for i, (_, r) in enumerate(du.iterrows()):
            f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')).strip(), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000)}
            requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk}"}, json={"fields": f})
            pb.progress((i + 1) / len(du))
        st.success("批量同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()
    st.button("🔄 刷新预览", on_click=lambda: st.session_state.pop('feishu_cache', None))
    dp = st.session_state['feishu_cache'].copy()
    if not dp.empty:
        disp = dp.drop(columns=['lng', 'lat', '_system_id'], errors='ignore')
        for c in ['服务开始日期', '服务结束日期']:
            if c in disp.columns: disp[c] = pd.to_datetime(disp[c]).dt.strftime('%Y-%m-%d')
        st.dataframe(disp, use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度拟定与归属锁定")
    if not st.session_state['feishu_cache'].empty and isinstance(d_sel, tuple) and len(d_sel) == 2:
        if st.button("✨ 1. 拟定调度方案"):
            ap = []; ae = []; dk = st.session_state['feishu_cache'].copy()
            days = pd.date_range(d_sel[0], d_sel[1]).tolist()
            dk = execute_smart_dispatch(dk, active)
            pb = st.progress(0)
            for i, d in enumerate(days):
                ct = pd.Timestamp(d); d_df = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                if not d_df.empty:
                    d_df = d_df[d_df.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not d_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, d_df['详细地址']))
                        d_df[['lng', 'lat']] = pd.DataFrame(coords, index=d_df.index)
                        em = d_df['lng'].isna()
                        if em.any(): ae.append(d_df[em].copy())
                        dv = d_df.dropna(subset=['lng', 'lat']).copy()
                        if not dv.empty:
                            dv['color'] = dv['喂猫师'].apply(lambda n: [0, 123, 255, 180] if n == "梦蕊" else ([255, 165, 0, 180] if n == "依蕊" else [128, 128, 128, 180]))
                            for s in active:
                                stks = dv[dv['喂猫师'] == s].copy()
                                if not stks.empty:
                                    res = optimize_route(stks); res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
                pb.progress((i + 1) / len(days))
            st.session_state['fp'] = pd.concat(ap) if ap else None
            st.session_state['fe'] = pd.concat(ae) if ae else None
            st.success("✅ 方案拟定完成！")

        if st.session_state.get('fp') is not None:
            # 强力锁定：带诊断反馈
            if st.button("🔒 2. 强力锁定人员归属 (同步至云端)"):
                with st.spinner("正在强力同步归属关系..."):
                    lc = 0; diag_msg = ""
                    unique_plan = st.session_state['fp'].drop_duplicates(subset=['宠物名字', '详细地址'])
                    for _, row in unique_plan.iterrows():
                        ok, msg = update_feishu_diagnostic(row['_system_id'], "喂猫师", row['喂猫师'])
                        if ok: lc += 1
                        else: diag_msg = msg
                    if lc > 0: st.success(f"同步成功！已锁定 {lc} 条记录。")
                    if diag_msg: st.error(f"同步异常报告：{diag_msg}")
                    st.session_state.pop('feishu_cache', None)
            
            st.download_button("📥 3. 导出多 Sheet Excel", data=generate_excel_multisheet(st.session_state['fp']), file_name="Dispatch.xlsx")
            res_f = st.session_state['fp']
            vd = st.selectbox("📅 选择日期", sorted(res_f['作业日期'].unique()))
            v_data = res_f[res_f['作业日期'] == vd]
            st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color='color', get_radius=350, pickable=True)]))
            st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
