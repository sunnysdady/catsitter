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
    match = re.search(r'(bas|tbl|rec)[a-zA-Z0-9]+', str(raw_id))
    return match.group(0).strip() if match else str(raw_id).strip()

APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = clean_id(st.secrets.get("FEISHU_APP_TOKEN", "")) 
TABLE_ID = clean_id(st.secrets.get("FEISHU_TABLE_ID", "")) 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 调度大脑核心逻辑 ---

def get_distance(p1, p2):
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """锁定所有字段并优化作业顺序"""
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
    """一只猫固定一人逻辑"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    cat_to_sitter_map = {}
    for _, row in df.iterrows():
        s_val = str(row.get('喂猫师', '')).strip()
        if s_val and s_val not in ["nan", ""]:
            cat_to_sitter_map[f"{row['宠物名字']}_{row['详细地址']}"] = s_val
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

# --- 3. 飞书 API 交互逻辑 ---

def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("tenant_access_token")
    except: return None

def fetch_feishu_data():
    token = get_feishu_token()
    if not token: return pd.DataFrame()
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

def update_feishu_status(record_id, status_val):
    """进度回写：实时上云"""
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{str(record_id).strip()}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fields": {"进度": status_val}}
    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=10)
        return r.status_code == 200
    except: return False

# --- 4. 视觉方案与 UI 精修 ---

def set_ui():
    st.markdown("""
        <style>
        /* A. 主频道按钮 (200*50) */
        .main-nav [data-testid="stVerticalBlock"] div.stButton > button {
            width: 200px !important; height: 50px !important;
            border: 3px solid #000 !important; border-radius: 10px !important;
            font-size: 18px !important; font-weight: 800 !important;
            box-shadow: 4px 4px 0px #000; background-color: #FFFFFF !important;
            margin-bottom: 12px !important; display: block; margin-left: auto; margin-right: auto;
        }
        /* B. 快捷按钮 (100*25) */
        .quick-nav div.stButton > button {
            width: 100px !important; height: 25px !important;
            font-size: 12px !important; padding: 0px !important;
            border: 1.5px solid #000 !important; border-radius: 4px !important;
            box-shadow: 1.5px 1.5px 0px #000; margin: 2px !important;
        }
        .stMetric { background: white; padding: 10px; border-radius: 5px; border: 1px solid #eee; }
        .stDataFrame { font-size: 14px !important; }
        </style>
        """, unsafe_allow_html=True)

def generate_excel_multisheet(df):
    """Excel 多 Sheet 导出"""
    output = io.BytesIO()
    full_df = df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values(['作业日期', '喂猫师', '拟定顺序'])
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        full_df.to_excel(writer, index=False, sheet_name='汇总')
        for s in df['喂猫师'].unique():
            s_name = str(s).strip()
            if s_name and s_name != 'nan':
                df[df['喂猫师'] == s][['作业日期', '拟定顺序', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False, sheet_name=s_name[:31])
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

# --- 5. 页面控制与侧边栏布局重构 ---

st.set_page_config(page_title="指挥中心 V43.0", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()

# 侧边栏布局
with st.sidebar:
    # --- 1. 置顶：快捷范围与调度配置 ---
    st.subheader("📅 快捷调度范围 (100*25)")
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
    
    date_sel = st.date_input("调度区间", value=st.session_state.get('r', (td, td + timedelta(days=1))))
    sitters = ["梦蕊", "依蕊"]
    active = [s for s in sitters if st.checkbox(f"{s} (出勤)", value=True)]
    
    st.divider()

    # --- 2. 居中：功能主频道 (200*50) ---
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("📊 任务进度"): st.session_state['page'] = "任务进度"
    if st.button("📝 订单信息"): st.session_state['page'] = "订单信息"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    st.markdown('</div>', unsafe_allow_html=True)

    st.divider()

    # --- 3. 沉底：帮助与授权 ---
    st.markdown('<div class="main-nav">', unsafe_allow_html=True)
    if st.button("📖 帮助文档"): st.session_state['page'] = "帮助文档"
    st.markdown('</div>', unsafe_allow_html=True)
    
    with st.expander("🔑 团队授权", expanded=False):
        auth_val = st.text_input("暗号", type="password", value="xiaomaozhiwei666")
        if auth_val != "xiaomaozhiwei666": st.stop()

# --- 6. 模块频道渲染 ---

# A. 帮助文档 (底部逻辑)
if st.session_state['page'] == "帮助文档":
    st.title("📖 指挥中心操作指引")
    st.markdown("""
    ### 📌 调度核心逻辑
    1. **置顶区域**：左上角快捷键用于快速锁定日期区间，勾选梦蕊/依蕊可动态改变看板生成内容。
    2. **进度反馈**：在“任务进度”频道更新状态，身在洛阳即可实时掌握深圳履约情况。
    3. **拦截机制**：系统自动拦截并报告模糊地址，点击“智能看板”后若出现红框，请务必查看错误清单。
    """)

# B. 任务进度
elif st.session_state['page'] == "任务进度":
    st.title("📊 任务进度实时闭环")
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        total = len(df_p); done = len(df_p[df_p['进度'] == '已完成'])
        c1, c2, c3 = st.columns(3)
        c1.metric("今日总单", total); c2.metric("已完成", done); c3.metric("完工率", f"{int(done/total*100) if total > 0 else 0}%")
        st.divider()
        edited = st.data_editor(df_p[['宠物名字', '详细地址', '喂猫师', '进度']], 
                                column_config={"进度": st.column_config.SelectboxColumn("状态", options=["未开始", "已出发", "服务中", "已完成"], required=True)}, 
                                use_container_width=True)
        if st.button("🚀 提交状态至飞书"):
            suc = 0
            for i, row in edited.iterrows():
                if row['进度'] != df_p.iloc[i]['进度']:
                    if update_feishu_status(df_p.iloc[i]['_system_id'], row['进度']): suc += 1
            st.success(f"同步成功！已更新 {suc} 条记录。"); st.session_state.pop('feishu_cache', None)

# C. 订单信息
elif st.session_state['page'] == "订单信息":
    st.title("📝 订单全景分析")
    df_i = st.session_state['feishu_cache'].copy()
    if not df_i.empty:
        s = st.text_input("🔍 宠物检索", placeholder="输入名字...")
        if s: df_i = df_i[df_i['宠物名字'].str.contains(s, na=False)]
        with ThreadPoolExecutor(max_workers=15) as ex: coords = list(ex.map(get_coords, df_i['详细地址']))
        df_i[['lng', 'lat']] = pd.DataFrame(coords, index=df_i.index)
        dm = df_i.dropna(subset=['lng', 'lat'])
        if not dm.empty:
            st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=dm['lng'].mean(), latitude=dm['lat'].mean(), zoom=10),
                layers=[pdk.Layer("HeatmapLayer", dm, get_position='[lng, lat]', radius_pixels=60, intensity=1)]))
        st.dataframe(df_i[['宠物名字', '详细地址', '喂猫师', '备注']], use_container_width=True)

# D. 数据中心
elif st.session_state['page'] == "数据中心":
    st.title("📂 云端数据快照")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量录入 (Excel)"):
            up = st.file_uploader("文件", type=["xlsx"])
            if up and st.button("🚀 推送云端"):
                df_up = pd.read_excel(up); p_bar = st.progress(0); tok = get_feishu_token()
                for i, (_, row) in enumerate(df_up.iterrows()):
                    f = {"详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(), "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tok}"}, json={"fields": f})
                    p_bar.progress((i + 1) / len(df_up))
                st.success("批量同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()
    with c2:
        with st.expander("单条录入 (✍️)"):
            with st.form("manual"):
                a = st.text_input("地址*"); n = st.text_input("名"); sd = st.date_input("开始"); ed = st.date_input("结束")
                if st.form_submit_button("💾 保存"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "投喂频率": 1, "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                    st.success("录入完成！"); st.session_state.pop('feishu_cache', None); st.rerun()
    st.divider(); st.button("🔄 刷新预览", on_click=lambda: st.session_state.pop('feishu_cache', None))
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        disp = df_p.drop(columns=['lng', 'lat', '_system_id'], errors='ignore')
        for c in ['服务开始日期', '服务结束日期']:
            if c in disp.columns: disp[c] = pd.to_datetime(disp[c]).dt.strftime('%Y-%m-%d')
        st.dataframe(disp, use_container_width=True)

# E. 智能看板
elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能调度中心")
    if not st.session_state['feishu_cache'].empty and isinstance(date_sel, tuple) and len(date_sel) == 2:
        if st.button("✨ 拟定最优调度方案"):
            all_p = []; all_e = []; df_kb = st.session_state['feishu_cache'].copy()
            days = pd.date_range(date_sel[0], date_sel[1]).tolist()
            df_kb = execute_smart_dispatch(df_kb, active)
            p_bar = st.progress(0)
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d); d_df = df_kb[(df_kb['服务开始日期'] <= cur_ts) & (df_kb['服务结束日期'] >= cur_ts)].copy()
                if not d_df.empty:
                    d_df = d_df[d_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not d_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, d_df['详细地址']))
                        d_df[['lng', 'lat']] = pd.DataFrame(coords, index=d_df.index)
                        err_mask = d_df['lng'].isna()
                        if err_mask.any():
                            eb = d_df[err_mask].copy(); eb['作业日期'] = d.strftime('%Y-%m-%d'); all_e.append(eb)
                        dv = d_df.dropna(subset=['lng', 'lat']).copy()
                        if not dv.empty:
                            dv['color'] = dv['喂猫师'].apply(lambda n: [0, 123, 255, 180] if n == "梦蕊" else ([255, 165, 0, 180] if n == "依蕊" else [128, 128, 128, 180]))
                            day_res = []
                            for s in active:
                                stks = dv[dv['喂猫师'] == s].copy()
                                if not stks.empty: day_res.append(optimize_route(stks))
                            if day_res:
                                cd = pd.concat(day_res); cd['作业日期'] = d.strftime('%Y-%m-%d'); all_p.append(cd)
                p_bar.progress((i + 1) / len(days))
            st.session_state['f_p'] = pd.concat(all_p) if all_p else None
            st.session_state['f_e'] = pd.concat(all_e) if all_e else None
            st.success("✅ 方案拟定完成！地图已自动对焦。")

        if st.session_state.get('f_e') is not None:
            st.warning(f"⚠️ 拦截到 {len(st.session_state['f_e'])} 条错误地址任务。")
            with st.expander("📍 错误地址拦截报告"):
                st.dataframe(st.session_state['f_e'][['作业日期', '宠物名字', '详细地址', '备注']], use_container_width=True)
                err_io = io.BytesIO(); st.session_state['f_e'].to_excel(err_io, index=False)
                st.download_button("📥 导出错误清单", data=err_io.getvalue(), file_name="Errors.xlsx")

        if st.session_state.get('f_p') is not None:
            st.download_button("📥 导出全量 Excel", data=generate_excel_multisheet(st.session_state['f_p']), file_name="Dispatch.xlsx")
            c_f1, c_f2 = st.columns(2)
            v_day = c_f1.selectbox("📅 查看日期", sorted(st.session_state['f_p']['作业日期'].unique()))
            v_sit = c_f2.selectbox("👤 筛选人员", ["全部"] + sorted(st.session_state['f_p'][st.session_state['f_p']['作业日期'] == v_day]['喂猫师'].unique().tolist()))
            v_data = st.session_state['f_p'][st.session_state['f_p']['作业日期'] == v_day]
            if v_sit != "全部": v_data = v_data[v_data['喂猫师'] == v_sit]
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                    layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color='color', get_radius=350, pickable=True)]))
                st.markdown("🔵 **梦蕊** | 🟠 **依蕊**")
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                if st.button("📋 生成微信简报"):
                    sum_txt = f"📢 任务清单 ({v_day})\n\n"
                    for s in (active if v_sit == "全部" else [v_sit]):
                        s_tasks = v_data[v_data['喂猫师'] == s].sort_values('拟定顺序')
                        if not s_tasks.empty:
                            sum_txt += f"👤 喂猫师：{s}\n"
                            for _, t in s_tasks.iterrows(): sum_txt += f"   {t['拟定顺序']}. {t['宠物名字']} - {t['详细地址']}\n"
                            sum_txt += "\n"
                    st.text_area("复制简报：", sum_txt, height=200)
