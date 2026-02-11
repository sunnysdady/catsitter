import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import re
import io
from fpdf import FPDF

# --- 1. 核心配置与参数清洗 ---
def clean_id(raw_id):
    if not raw_id: return ""
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
    """锁定 ID 并优化作业顺序"""
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
    """一猫一人固定逻辑"""
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

# --- 3. 飞书 API 交互 ---

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
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], unit='ms', errors='coerce')
        for col in ['宠物名字', '详细地址', '喂猫师', '备注', 'lng', 'lat']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_final(record_id, sitter_name):
    token = get_feishu_token()
    clean_rid = str(record_id).strip()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{clean_rid}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fields": {"喂猫师": str(sitter_name)}}
    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=10)
        return r.status_code == 200
    except: return False

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

# --- 4. 导出工具模块 (PDF & Excel) ---

def generate_pdf(df, target_date):
    """生成 PDF 派工单"""
    pdf = FPDF()
    pdf.add_page()
    # 使用标准系统字体（PDF导出中文需处理，此处简化为导出英文/数字，若需中文需加载ttf字体）
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, txt=f"Cat Sitter Dispatch List ({target_date})", ln=True, align="C")
    pdf.set_font("Helvetica", size=10)
    pdf.ln(10)
    
    # 表头
    pdf.cell(15, 10, "Order", 1); pdf.cell(25, 10, "Sitter", 1); pdf.cell(30, 10, "Pet", 1); pdf.cell(120, 10, "Address", 1); pdf.ln()
    
    for _, row in df.sort_values(['喂猫师', '拟定顺序']).iterrows():
        # 处理可能的特殊字符
        pdf.cell(15, 10, str(row['拟定顺序']), 1)
        pdf.cell(25, 10, str(row['喂猫师']), 1)
        pdf.cell(30, 10, str(row['宠物名字']), 1)
        pdf.cell(120, 10, str(row['详细地址'])[:50], 1)
        pdf.ln()
    return pdf.output()

def generate_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].to_excel(writer, index=False)
    return output.getvalue()

# --- 5. UI 视觉适配 (30px) ---

def set_ui():
    st.markdown("""
        <style>
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important; height: 100px !important;
            border: 4px solid #000 !important; border-radius: 15px !important;
            font-size: 30px !important; font-weight: 900 !important;
            box-shadow: 6px 6px 0px #000;
            background-color: #FFFFFF !important;
        }
        .stDataFrame { font-size: 16px !important; }
        </style>
        """, unsafe_allow_html=True)

# --- 6. 页面控制 ---

st.set_page_config(page_title="指挥中心 V28.0", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"

# --- 7. 模块渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (全量管理)")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("文件", type=["xlsx"])
            if up_file and st.button("🚀 录入云端"):
                df_up = pd.read_excel(up_file); p_bar = st.progress(0); tok = get_feishu_token()
                for i, (_, row) in enumerate(df_up.iterrows()):
                    f = {"详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(), "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tok}"}, json={"fields": f})
                    p_bar.progress((i + 1) / len(df_up))
                st.success("批量同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()
    with c2:
        with st.expander("✍️ 单条手动录入"):
            with st.form("manual"):
                a = st.text_input("地址*"); n = st.text_input("宠物名"); sd = st.date_input("开始"); ed = st.date_input("结束")
                if st.form_submit_button("保存至云端"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "投喂频率": 1, "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                    st.success("录入完成！"); st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    if st.button("🔄 刷新预览数据"):
        st.session_state.pop('feishu_cache', None); st.session_state['feishu_cache'] = fetch_feishu_data(); st.rerun()
    
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        # --- 格式化：显示标准日期 ---
        disp = df_p.drop(columns=['lng', 'lat', '_system_id'], errors='ignore')
        for c in ['服务开始日期', '服务结束日期']:
            if c in disp.columns: disp[c] = pd.to_datetime(disp[c]).dt.strftime('%Y-%m-%d')
        st.dataframe(disp, use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度看板 (V28.0)")
    df_kb = st.session_state['feishu_cache'].copy()
    
    sitters = ["梦蕊", "依蕊"]
    current_active = [s for s in sitters if st.sidebar.checkbox(f"{s} (出勤)", value=True)]
    date_range = st.sidebar.date_input("📅 调度范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))

    if not df_kb.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        if st.button("✨ 1. 拟定排单方案"):
            all_plans = []
            days = pd.date_range(date_range[0], date_range[1]).tolist()
            df_kb = execute_smart_dispatch(df_kb, current_active)
            p_bar = st.progress(0)
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d); d_df = df_kb[(df_kb['服务开始日期'] <= cur_ts) & (df_kb['服务结束日期'] >= cur_ts)].copy()
                if not d_df.empty:
                    d_df = d_df[d_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not d_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, d_df['详细地址']))
                        d_df[['lng', 'lat']] = pd.DataFrame(coords, index=d_df.index); d_df = d_df.dropna(subset=['lng', 'lat'])
                        d_res = []
                        for s in current_active:
                            s_tasks = d_df[d_df['喂猫师'] == s].copy()
                            if not s_tasks.empty: d_res.append(optimize_route(s_tasks))
                        if d_res:
                            cd = pd.concat(d_res); cd['作业日期'] = d.strftime('%Y-%m-%d'); all_plans.append(cd)
                p_bar.progress((i + 1) / len(days))
            st.session_state['final_plan_v28'] = pd.concat(all_plans) if all_plans else None
            st.success("✅ 方案拟定完成！")

        if st.session_state.get('final_plan_v28') is not None:
            res_f = st.session_state['final_plan_v28']
            
            # --- 功能区：导出文档 ---
            col_ex1, col_ex2 = st.columns(2)
            with col_ex1:
                st.download_button("📥 下载 Excel 排单表", data=generate_excel(res_f), file_name="Dispatch.xlsx")
            with col_ex2:
                # PDF 导出 (Unicode支持需加载中文字体，此处默认)
                st.download_button("📥 下载 PDF 派工单", data=generate_pdf(res_f, "All Periods"), file_name="Dispatch.pdf")

            c_f1, c_f2 = st.columns(2)
            v_day = c_f1.selectbox("📅 查看日期", sorted(res_f['作业日期'].unique()))
            v_sitters = ["全部"] + sorted(res_f[res_f['作业日期'] == v_day]['喂猫师'].unique().tolist())
            v_sit = c_f2.selectbox("👤 筛选喂猫师", v_sitters)
            
            v_data = res_f[res_f['作业日期'] == v_day]
            if v_sit != "全部": v_data = v_data[v_data['喂猫师'] == v_sit]
            
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11), layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=350)]))
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                
                # --- 找回：微信简报 ---
                if st.button("📋 生成微信排班简报"):
                    summary = f"📢 派单清单 ({v_day})\n\n"
                    for s in (current_active if v_sit == "全部" else [v_sit]):
                        s_tasks = v_data[v_data['喂猫师'] == s].sort_values('拟定顺序')
                        if not s_tasks.empty:
                            summary += f"👤 喂猫师：{s}\n"
                            for _, t in s_tasks.iterrows(): summary += f"   {t['拟定顺序']}. {t['宠物名字']} - {t['详细地址']}\n"
                            summary += "\n"
                    st.text_area("复制发给深圳团队：", summary, height=200)

                if st.button("✅ 2. 确认并同步飞书"):
                    suc = 0; sync_p = st.progress(0)
                    for i, (_, row) in enumerate(res_f.iterrows()):
                        if row.get('_system_id') and row.get('喂猫师'):
                            if update_feishu_final(row['_system_id'], row['喂猫师']): suc += 1
                        sync_p.progress((i + 1) / len(res_f))
                    st.success(f"🎉 同步完成！回写 {suc} 条记录。")
                    st.session_state.pop('feishu_cache', None)
