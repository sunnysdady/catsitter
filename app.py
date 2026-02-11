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

# --- 1. 核心配置与 ID 强力清洗 (锁定精准对位) ---
def clean_id(raw_id):
    if not raw_id: return ""
    match = re.search(r'[a-zA-Z0-9]{15,}', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
# 自动使用您提供的最新 URL 精准 ID
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "MdvxbpyUHaFkWksl4B6cPlfpn2f")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "tbl6Ziz0dO1evH7s")) 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 调度大脑核心逻辑 (V44 经典算法) ---

def get_distance(p1, p2):
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """优化路径顺序：1 -> 2 -> 3"""
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
    """负载均衡逻辑：优先继承飞书已填归属"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    sitter_load = {s: 0 for s in active_sitters}
    for s in df['喂猫师']:
        if s in sitter_load: sitter_load[s] += 1
    for i, row in df.iterrows():
        if str(row.get('喂猫师', '')).strip() not in ["", "nan"]: continue
        if active_sitters:
            best = min(sitter_load, key=sitter_load.get)
            df.at[i, '喂猫师'] = best
            sitter_load[best] += 1
    return df

# --- 3. 飞书 API 交互逻辑 ---

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
        if '进度' not in df.columns: df['进度'] = "待处理"
        for col in ['宠物名字', '详细地址', '喂猫师', '备注', 'lng', 'lat', '投喂频率']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_status(record_id, status_val):
    """状态回写：将进度同步至飞书云端"""
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{str(record_id).strip()}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fields": {"进度": str(status_val)}}
    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=10)
        return r.status_code == 200
    except: return False

# --- 4. V60 强化版多 Sheet 导出工具 ---

def generate_excel_v60(df):
    """导出包含人员分表及宠物归属明细页"""
    output = io.BytesIO()
    full_df = df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values(['作业日期', '喂猫师', '拟定顺序'])
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Sheet 1: 全量汇总
        full_df.to_excel(writer, index=False, sheet_name='汇总')
        # Sheet 2: 宠物归属明细 (关键新增)
        mapping_df = df.drop_duplicates(subset=['宠物名字', '详细地址'])[['宠物名字', '详细地址', '喂猫师', '备注']]
        mapping_df.to_excel(writer, index=False, sheet_name='宠物归属明细')
        # 喂猫师个人分表
        for s in df['喂猫师'].unique():
            if str(s).strip() and str(s) != 'nan':
                df[df['喂猫师'] == s][['作业日期', '拟定顺序', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name=str(s)[:31])
    return output.getvalue()

# --- 5. UI 视觉方案 (200*50 与 100*25) ---

def set_ui():
    st.markdown("""
        <style>
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button {
            width: 200px !important; height: 50px !important;
            border: 3px solid #000 !important; border-radius: 10px !important;
            font-size: 18px !important; font-weight: 800 !important;
            box-shadow: 4px 4px 0px #000; background-color: #FFFFFF !important;
            margin-bottom: 12px !important; display: block; margin-left: auto; margin-right: auto;
        }
        .quick-nav div.stButton > button {
            width: 100px !important; height: 25px !important;
            font-size: 11px !important; padding: 0px !important;
            border: 1.5px solid #000 !important; border-radius: 4px !important;
            box-shadow: 1.5px 1.5px 0px #000; margin: 2px !important;
        }
        .stMetric { background: white; padding: 10px; border-radius: 5px; border: 1px solid #eee; }
        .help-box { background: #f0f7ff; border-left: 5px solid #1890ff; padding: 15px; border-radius: 5px; }
        </style>
        """, unsafe_allow_html=True)

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

# --- 6. 侧边栏布局 (V44 对齐：指挥舱置顶) ---

st.set_page_config(page_title="指挥中心 V60.0", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()

with st.sidebar:
    # A. 【置顶】快速选择区域 (100*25)
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
    
    d_sel = st.date_input("选择周期范围", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    sitters = ["梦蕊", "依蕊"]
    active = [s for s in sitters if st.checkbox(f"{s} (出勤)", value=True, key=f"v60_{s}")]
    
    st.divider()
    # B. 【居中】功能菜单 (200*50)
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("📊 任务进度"): st.session_state['page'] = "任务进度"
    if st.button("📝 订单信息"): st.session_state['page'] = "订单信息"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    if st.button("📖 帮助文档"): st.session_state['page'] = "帮助文档"
    st.markdown('</div>', unsafe_allow_html=True)
    st.divider()
    # C. 【沉底】文档与授权
    with st.expander("🔑 团队授权"):
        if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()

# --- 7. 各频道逻辑渲染 (全量复活版) ---

# 频道 1: 数据中心
if st.session_state['page'] == "数据中心":
    st.title("📂 数据同步入口")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量录单 (Excel)"):
            up = st.file_uploader("上传文件", type=["xlsx"])
            if up and st.button("🚀 推送至云端"):
                du = pd.read_excel(up); pb = st.progress(0); tk = get_feishu_token()
                for i, (_, r) in enumerate(du.iterrows()):
                    f = {"详细地址": str(r['详细地址']).strip(), "宠物名字": str(r.get('宠物名字', '小猫')).strip(), "投喂频率": int(r.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(r['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(r['服务结束日期']), datetime.min.time()).timestamp()*1000)}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tk}"}, json={"fields": f})
                    pb.progress((i + 1) / len(du))
                st.success("批量同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()
    with c2:
        with st.expander("单条手动录单"):
            with st.form("man_reg"):
                a = st.text_input("地址*"); n = st.text_input("名"); sd = st.date_input("开始"); ed = st.date_input("结束")
                if st.form_submit_button("💾 保存"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "投喂频率": 1, "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                    st.success("录入成功！"); st.session_state.pop('feishu_cache', None); st.rerun()
    st.button("🔄 刷新快照预览", on_click=lambda: st.session_state.pop('feishu_cache', None))
    dp = st.session_state['feishu_cache'].copy()
    if not dp.empty:
        disp = dp.drop(columns=['lng', 'lat', '_system_id'], errors='ignore')
        for c in ['服务开始日期', '服务结束日期']:
            if c in disp.columns: disp[c] = pd.to_datetime(disp[c]).dt.strftime('%Y-%m-%d')
        st.dataframe(disp, use_container_width=True)

# 频道 2: 任务进度
elif st.session_state['page'] == "任务进度":
    st.title("📊 执行进度实时上云")
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        total = len(df_p); done = len(df_p[df_p['进度'] == '已完成'])
        st.columns(3)[0].metric("总单量", total); st.columns(3)[1].metric("已完工", done); st.columns(3)[2].metric("完工率", f"{int(done/total*100) if total > 0 else 0}%")
        edit = st.data_editor(df_p[['宠物名字', '详细地址', '喂猫师', '进度']], column_config={"进度": st.column_config.SelectboxColumn("状态", options=["未开始", "已出发", "服务中", "已完成"], required=True)}, use_container_width=True)
        if st.button("🚀 提交全部更新至飞书"):
            sc = 0
            for i, row in edit.iterrows():
                if row['进度'] != df_p.iloc[i]['进度']:
                    if update_feishu_status(df_p.iloc[i]['_system_id'], row['进度']): sc += 1
            st.success(f"已同步 {sc} 条记录。"); st.session_state.pop('feishu_cache', None)

# 频道 3: 订单信息
elif st.session_state['page'] == "订单信息":
    st.title("📝 订单全局图与热力分布")
    df_i = st.session_state['feishu_cache'].copy()
    if not df_i.empty:
        s = st.text_input("🔍 秒搜宠物归属", placeholder="输入小猫名...")
        if s: df_i = df_i[df_i['宠物名字'].str.contains(s, na=False)]
        with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, df_i['详细地址']))
        df_i[['lng', 'lat']] = pd.DataFrame(coords, index=df_i.index, columns=['lng', 'lat'])
        dm = df_i.dropna(subset=['lng', 'lat'])
        if not dm.empty:
            st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=dm['lng'].mean(), latitude=dm['lat'].mean(), zoom=10),
                layers=[pdk.Layer("HeatmapLayer", dm, get_position='[lng, lat]', radius_pixels=60, intensity=1)]))
        st.dataframe(df_i[['宠物名字', '详细地址', '喂猫师', '备注']], use_container_width=True)

# 频道 4: 智能看板 (含归属导出强化)
elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度指挥大屏")
    if not st.session_state['feishu_cache'].empty and isinstance(d_sel, tuple) and len(d_sel) == 2:
        if st.button("✨ 1. 拟定方案并审计频率"):
            ap = []; dk = st.session_state['feishu_cache'].copy()
            days = pd.date_range(d_sel[0], d_sel[1]).tolist()
            dk = execute_smart_dispatch(dk, active)
            for d in days:
                ct = pd.Timestamp(d); d_v = dk[(dk['服务开始日期'] <= ct) & (dk['服务结束日期'] >= ct)].copy()
                if not d_v.empty:
                    # 频率修正：1每天，2隔天
                    d_v = d_v[d_v.apply(lambda r: (ct - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not d_v.empty:
                        with ThreadPoolExecutor(max_workers=5) as ex: coords = list(ex.map(get_coords, d_v['详细地址']))
                        d_v[['lng', 'lat']] = pd.DataFrame(coords, index=d_v.index, columns=['lng', 'lat'])
                        dv = d_v.dropna(subset=['lng', 'lat']).copy()
                        if not dv.empty:
                            dv['color'] = dv['喂猫师'].apply(lambda n: [0, 123, 255, 180] if n == "梦蕊" else ([255, 165, 0, 180] if n == "依蕊" else [128, 128, 128, 180]))
                            for s in active:
                                stks = dv[dv['喂猫师'] == s].copy()
                                if not stks.empty:
                                    res = optimize_route(stks); res['作业日期'] = d.strftime('%Y-%m-%d'); ap.append(res)
            st.session_state['fp'] = pd.concat(ap) if ap else None
            st.success("✅ 方案拟定完成！坐标引擎已稳健修复。")

        if st.session_state.get('fp') is not None:
            # 强化版导出按钮
            st.download_button("📥 2. 导出 Excel (含归属明细)", data=generate_excel_v60(st.session_state['fp']), file_name="Cat_Dispatch_V60.xlsx")
            c_f1, c_f2 = st.columns(2)
            vd = c_f1.selectbox("📅 查看日期", sorted(st.session_state['fp']['作业日期'].unique()))
            # 找回筛选功能
            vs = c_f2.selectbox("👤 筛选人员", ["全部"] + sorted(st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]['喂猫师'].unique().tolist()))
            v_data = st.session_state['fp'][st.session_state['fp']['作业日期'] == vd]
            if vs != "全部": v_data = v_data[v_data['喂猫师'] == vs]
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                    layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color='color', get_radius=350, pickable=True)]))
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)

# 频道 5: 帮助文档
elif st.session_state['page'] == "帮助文档":
    st.title("📖 全功能操作指引")
    st.markdown('<div class="help-box">', unsafe_allow_html=True)
    st.subheader("💡 核心更新 (V60.0)")
    st.markdown("""
    * **归属明细页**：导出 Excel 的第二个 Sheet 即为【宠物归属明细】。
    * **频率公式**：1=每天，2=隔一天。例如：开始日期为1号，频率2，则系统只排1号、3号、5号单。
    * **全量复活**：找回了数据中心、人员筛选等全部 V44.0 时代的功能，代码行数已补齐至全长版本。
    """)
    st.markdown('</div>', unsafe_allow_html=True)
