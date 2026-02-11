import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import re
import io

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

# --- 2. 调度大脑逻辑 ---

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

# --- 3. 飞书 API 读取逻辑 ---

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
        for col in ['宠物名字', '详细地址', '喂猫师', '备注', 'lng', 'lat', '投喂频率']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

# --- 4. 视觉与导出工具 (30px) ---

def generate_excel_multisheet(df):
    output = io.BytesIO()
    full_df = df[['作业日期', '拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values(['作业日期', '喂猫师', '拟定顺序'])
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        full_df.to_excel(writer, index=False, sheet_name='全量汇总')
        sitters = df['喂猫师'].unique()
        for sitter in sitters:
            s_name = str(sitter).strip()
            if s_name and s_name != 'nan':
                s_df = df[df['喂猫师'] == sitter][['作业日期', '拟定顺序', '宠物名字', '详细地址', '备注']].sort_values(['作业日期', '拟定顺序'])
                s_df.to_excel(writer, index=False, sheet_name=s_name[:31])
    return output.getvalue()

def set_ui():
    st.markdown("""
        <style>
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important; height: 100px !important;
            border: 4px solid #000 !important; border-radius: 15px !important;
            font-size: 30px !important; font-weight: 900 !important;
            box-shadow: 6px 6px 0px #000;
            background-color: #FFFFFF !important; color: #000000 !important;
        }
        .stDataFrame { font-size: 16px !important; }
        .info-card { background: #f8f9fa; border-left: 5px solid #000; padding: 20px; border-radius: 10px; margin-bottom: 10px; }
        .stMetric { background: white; padding: 10px; border-radius: 5px; border: 1px solid #eee; }
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

# --- 5. 流程控制 ---

st.set_page_config(page_title="指挥中心 V37.0", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("📝 订单信息"): st.session_state['page'] = "订单信息"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"

# --- 6. 频道 A: 订单信息 (搜索与热力图) ---

if st.session_state['page'] == "订单信息":
    st.title("📝 订单全景分析 (搜索与热力)")
    df_info = st.session_state['feishu_cache'].copy()
    
    if not df_info.empty:
        # 优化建议：宠物快捷搜索
        search_cat = st.text_input("🔍 快速搜索宠物名 (查看归属师)", placeholder="输入小猫名字...")
        if search_cat:
            df_info = df_info[df_info['宠物名字'].str.contains(search_cat, na=False)]
            
        st.subheader("🌐 深圳业务热力分布")
        with ThreadPoolExecutor(max_workers=15) as ex:
            coords = list(ex.map(get_coords, df_info['详细地址']))
        df_info[['lng', 'lat']] = pd.DataFrame(coords, index=df_info.index)
        df_map = df_info.dropna(subset=['lng', 'lat'])
        
        if not df_map.empty:
            st.pydeck_chart(pdk.Deck(
                map_style=pdk.map_styles.LIGHT,
                initial_view_state=pdk.ViewState(longitude=df_map['lng'].mean(), latitude=df_map['lat'].mean(), zoom=10),
                layers=[pdk.Layer("HeatmapLayer", df_map, get_position='[lng, lat]', radius_pixels=60, intensity=1)]
            ))
        
        st.divider()
        st.dataframe(df_info[['宠物名字', '详细地址', '喂猫师', '备注']], use_container_width=True)

# --- 7. 频道 B: 数据中心 ---

elif st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (云端同步)")
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("文件", type=["xlsx"])
            if up_file and st.button("🚀 录入飞书"):
                df_up = pd.read_excel(up_file); p_bar = st.progress(0); tok = get_feishu_token()
                for i, (_, row) in enumerate(df_up.iterrows()):
                    f = {"详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(), "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tok}"}, json={"fields": f})
                    p_bar.progress((i + 1) / len(df_up))
                st.success("批量成功！"); st.session_state.pop('feishu_cache', None); st.rerun()
    with c2:
        with st.expander("✍️ 单条手动录入"):
            with st.form("manual"):
                a = st.text_input("地址*"); n = st.text_input("名字"); sd = st.date_input("开始日期"); ed = st.date_input("结束日期")
                if st.form_submit_button("💾 保存"):
                    f = {"详细地址": a.strip(), "宠物名字": n.strip(), "投喂频率": 1, "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {get_feishu_token()}"}, json={"fields": f})
                    st.success("录入完成！"); st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    if st.button("🔄 刷新预览"):
        st.session_state.pop('feishu_cache', None); st.session_state['feishu_cache'] = fetch_feishu_data(); st.rerun()
    
    df_p = st.session_state['feishu_cache'].copy()
    if not df_p.empty:
        disp = df_p.drop(columns=['lng', 'lat', '_system_id'], errors='ignore')
        for c in ['服务开始日期', '服务结束日期']:
            if c in disp.columns: disp[c] = pd.to_datetime(disp[c]).dt.strftime('%Y-%m-%d')
        st.dataframe(disp, use_container_width=True)

# --- 8. 频道 C: 智能看板 (色彩辨识与对焦) ---

elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能调度中心 (V37.0)")
    df_kb = st.session_state['feishu_cache'].copy()
    
    # 侧边栏：优化建议：日期快速切换
    with st.sidebar:
        st.divider(); st.subheader("📅 快速调度")
        if st.button("📍 今天"): st.session_state['d_picker'] = datetime.now().date()
        if st.button("📍 明天"): st.session_state['d_picker'] = (datetime.now() + timedelta(days=1)).date()
        date_range = st.date_input("调度范围", value=st.session_state.get('d_picker', datetime.now().date()))
        sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in sitters if st.checkbox(f"{s} (出勤)", value=True)]

    if not df_kb.empty:
        if st.button("✨ 拟定最优方案 (不写回云端)"):
            all_plans = []
            days = [date_range] if isinstance(date_range, datetime.date) else pd.date_range(date_range[0], date_range[1]).tolist()
            df_kb = execute_smart_dispatch(df_kb, current_active)
            p_bar = st.progress(0)
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d); d_df = df_kb[(df_kb['服务开始日期'] <= cur_ts) & (df_kb['服务结束日期'] >= cur_ts)].copy()
                if not d_df.empty:
                    d_df = d_df[d_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not d_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, d_df['详细地址']))
                        d_df[['lng', 'lat']] = pd.DataFrame(coords, index=d_df.index); d_df = d_df.dropna(subset=['lng', 'lat'])
                        
                        # 喂猫师不同色彩标记
                        def get_color(n): return [0, 123, 255, 180] if n == "梦蕊" else ([255, 165, 0, 180] if n == "依蕊" else [128, 128, 128, 180])
                        d_df['color'] = d_df['喂猫师'].apply(get_color)
                        
                        d_res = []
                        for s in current_active:
                            s_tasks = d_df[d_df['喂猫师'] == s].copy()
                            if not s_tasks.empty: d_res.append(optimize_route(s_tasks))
                        if d_res:
                            cd = pd.concat(d_res); cd['作业日期'] = d.strftime('%Y-%m-%d'); all_plans.append(cd)
                p_bar.progress((i + 1) / len(days))
            st.session_state['final_plan_v37'] = pd.concat(all_plans) if all_plans else None
            st.success("✅ 方案拟定完成！")

        if st.session_state.get('final_plan_v37') is not None:
            res_f = st.session_state['final_plan_v37']
            st.download_button("📥 导出多 Sheet Excel", data=generate_excel_multisheet(res_f), file_name="Dispatch.xlsx")
            
            # 优化建议：负载均衡饼图/柱状图
            load_stat = res_f.groupby('喂猫师').size()
            st.bar_chart(load_stat)

            c_f1, c_f2 = st.columns(2)
            v_day = c_f1.selectbox("📅 日期", sorted(res_f['作业日期'].unique()))
            v_sit = c_f2.selectbox("👤 喂猫师", ["全部"] + sorted(res_f[res_f['作业日期'] == v_day]['喂猫师'].unique().tolist()))
            
            v_data = res_f[res_f['作业日期'] == v_day]
            if v_sit != "全部": v_data = v_data[v_data['喂猫师'] == v_sit]
            
            if not v_data.empty:
                # 自动对焦地图 (Auto-Focus)
                st.pydeck_chart(pdk.Deck(
                    map_style=pdk.map_styles.LIGHT,
                    initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                    layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color='color', get_radius=350, pickable=True)]
                ))
                st.markdown("🔵 **梦蕊** | 🟠 **依蕊**")
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                
                if st.button("📋 生成微信简报"):
                    sum_txt = f"📢 任务清单 ({v_day})\n\n"
                    for s in (current_active if v_sit == "全部" else [v_sit]):
                        s_tasks = v_data[v_data['喂猫师'] == s].sort_values('拟定顺序')
                        if not s_tasks.empty:
                            sum_txt += f"👤 喂猫师：{s}\n"
                            for _, t in s_tasks.iterrows(): sum_txt += f"   {t['拟定顺序']}. {t['宠物名字']} - {t['详细地址']}\n"
                            sum_txt += "\n"
                    st.text_area("复制发给团队：", sum_txt, height=200)
