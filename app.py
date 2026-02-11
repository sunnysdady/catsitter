import streamlit as st
import pandas as pd
import requests
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# --- 1. 核心连接配置 (Secrets 自动读取) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 核心算法大脑：路径优化、分配与预警 ---

def get_distance(p1, p2):
    """计算物理直线距离"""
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """最近邻路径算法：按物理距离重新排列『喂猫师』的作业顺序"""
    if len(df_sitter) <= 1:
        df_sitter['拟定顺序'] = range(1, len(df_sitter) + 1)
        return df_sitter
    
    unvisited = df_sitter.to_dict('records')
    current_node = unvisited.pop(0)
    optimized_list = [current_node]
    
    while unvisited:
        next_node = min(unvisited, key=lambda x: get_distance(
            (current_node['lng'], current_node['lat']), 
            (x['lng'], x['lat'])
        ))
        unvisited.remove(next_node)
        optimized_list.append(next_node)
        current_node = next_node
        
    res_df = pd.DataFrame(optimized_list)
    res_df['拟定顺序'] = range(1, len(res_df) + 1)
    return res_df

def execute_smart_dispatch(df, active_sitters):
    """
    三级分配逻辑：
    1. 人工指定优先 (Excel已填数据)
    2. 一只猫固定一人 (同猫+同地址绑定)
    3. 负载均衡 (分给今日接单最少的出勤人员)
    """
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    df['详细地址'] = df['详细地址'].fillna("未知地址")

    # 建立【宠物名字+地址 -> 喂猫师】映射，实现固定一人策略
    cat_to_sitter_map = {}
    for _, row in df[df['喂猫师'] != ""].iterrows():
        key = f"{row['宠物名字']}_{row['详细地址']}"
        cat_to_sitter_map[key] = row['喂猫师']

    # 统计负载
    sitter_load = {s: 0 for s in active_sitters}
    for sitter in df['喂猫师']:
        if sitter in sitter_load: sitter_load[sitter] += 1

    # 执行分配
    for i, row in df.iterrows():
        if row['喂猫师'] != "": continue
        cat_key = f"{row['宠物名字']}_{row['详细地址']}"
        if cat_key in cat_to_sitter_map:
            df.at[i, '喂猫师'] = cat_to_sitter_map[cat_key]
        else:
            if active_sitters:
                best_sitter = min(sitter_load, key=sitter_load.get)
                df.at[i, '喂猫师'] = best_sitter
                cat_to_sitter_map[cat_key] = best_sitter
                sitter_load[best_sitter] += 1
            else:
                df.at[i, '喂猫师'] = "待人工分配"
    return df

def detect_duplicates(df):
    """新增：重复订单与地址预警逻辑"""
    if df.empty: return []
    # 检查【宠物名字+地址】完全重复的行
    dups = df[df.duplicated(subset=['宠物名字', '详细地址'], keep=False)]
    warnings = []
    for _, row in dups.iterrows():
        warnings.append(f"⚠️ 预警：宠物 [{row['宠物名字']}] 在地址 [{row['详细地址']}] 存在重复订单，请核实！")
    return warnings

# --- 3. 飞书 API 交互逻辑 (完整版) ---

def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        res = r.json()
        return res.get("tenant_access_token") if res.get("code") == 0 else None
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
        required_cols = ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '喂猫师', '投喂频率']
        for col in required_cols:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def add_feishu_record(fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        r = requests.post(url, headers=headers, json={"fields": fields}, timeout=10)
        return r.json().get("code") == 0
    except: return False

def update_feishu_record(record_id, fields):
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    clean_fields = {k: ("" if pd.isna(v) else v) for k, v in fields.items()}
    try:
        r = requests.patch(url, headers=headers, json={"fields": clean_fields}, timeout=10)
        return r.json().get("code") == 0
    except: return False

# --- 4. UI 视觉重构 (30px 巨幕适配) ---

def set_ui():
    st.markdown("""
        <style>
        html, body, [data-testid="stAppViewContainer"] { background-color: #FFFFFF !important; color: #000000 !important; }
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important; height: 100px !important;
            background-color: #FFFFFF !important; color: #000000 !important;
            border: 4px solid #000000 !important; border-radius: 15px !important;
            font-size: 30px !important; font-weight: 900 !important;
            margin-bottom: 20px !important;
            box-shadow: 5px 5px 0px #000;
        }
        [data-testid="stSidebar"] div.stButton > button:hover { background-color: #000000 !important; color: #FFFFFF !important; }
        .stDataFrame { font-size: 16px !important; }
        .warning-box { background-color: #fff1f0; border: 1px solid #ff4d4f; padding: 15px; border-radius: 10px; margin-bottom: 20px; }
        </style>
        """, unsafe_allow_html=True)

@st.cache_data(show_spinner=False)
def get_coords(address):
    url = f"https://restapi.amap.com/v3/geocode/geo?key={AMAP_API_KEY}&address=深圳市{address}"
    try:
        r = requests.get(url, timeout=5).json()
        if r['status'] == '1' and r['geocodes']:
            lng, lat = r['geocodes'][0]['location'].split(',')
            return float(lng), float(lat)
    except: return None, None

# --- 5. 页面路由 ---

st.set_page_config(page_title="小猫直喂指挥中心", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "数据中心"

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("密码暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    
    if st.session_state['page'] == "智能看板":
        st.divider(); st.subheader("⚙️ 调度配置")
        active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 调度范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))

if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

# --- 6. 模块渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (云端录入与地址校验)")
    
    # 重复订单预警展示
    warn_list = detect_duplicates(st.session_state['feishu_cache'])
    if warn_list:
        with st.container():
            st.markdown('<div class="warning-box">', unsafe_allow_html=True)
            for w in warn_list: st.error(w)
            st.markdown('</div>', unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("选择文件", type=["xlsx"])
            if up_file and st.button("🚀 启动批量录入"):
                df_up = pd.read_excel(up_file); p_bar = st.progress(0)
                for i, (_, row) in enumerate(df_up.iterrows()):
                    s_ts = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                    e_ts = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                    payload = {"详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(), "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": s_ts, "服务结束日期": e_ts, "喂猫师": str(row.get('喂猫师', '')).strip(), "备注": str(row.get('备注', ''))}
                    add_feishu_record(payload); p_bar.progress((i + 1) / len(df_up))
                st.success("批量同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()

    with c2:
        with st.expander("单条手动录入"):
            with st.form("manual"):
                addr = st.text_input("地址*"); cat = st.text_input("名字"); sd = st.date_input("开始"); ed = st.date_input("结束")
                if st.form_submit_button("保存"):
                    payload = {"详细地址": addr.strip(), "宠物名字": cat.strip(), "投喂频率": 1, "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)}
                    if add_feishu_record(payload): st.success("录入成功！"); st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    if st.button("🔄 强制刷新预览云端数据"):
        st.session_state.pop('feishu_cache', None); st.session_state['feishu_cache'] = fetch_feishu_data(); st.rerun()
    
    st.dataframe(st.session_state['feishu_cache'].drop(columns=['_system_id'], errors='ignore'), use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能调度看板 (地址预警版)")
    df = st.session_state['feishu_cache'].copy()
    
    if not df.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        for col in ['服务开始日期', '服务结束日期']: df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
        
        if st.button("✨ 拟定方案 (接入路径算法 + 地址校验)"):
            all_plans = []
            days = pd.date_range(start_d, end_d).tolist(); p_bar = st.progress(0)
            
            # 执行分配大脑
            df = execute_smart_dispatch(df, current_active)
            
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d)
                day_df = df[(df['服务开始日期'] <= cur_ts) & (df['服务结束日期'] >= cur_ts)].copy()
                if not day_df.empty:
                    day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not day_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, day_df['详细地址']))
                        day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                        day_df = day_df.dropna(subset=['lng', 'lat'])
                        
                        # 分人优化
                        day_sit_plans = []
                        for s in current_active:
                            s_tasks = day_df[day_df['喂猫师'] == s].copy()
                            if not s_tasks.empty: day_sit_plans.append(optimize_route(s_tasks))
                        if day_sit_plans:
                            res_day = pd.concat(day_sit_plans)
                            res_day['作业日期'] = d.strftime('%Y-%m-%d')
                            all_plans.append(res_day)
                p_bar.progress((i + 1) / len(days))
            
            if all_plans:
                st.session_state['final_plan'] = pd.concat(all_plans)
                st.success("✅ 全周期调度已拟定，已按路径排序。")

        if 'final_plan' in st.session_state:
            res = st.session_state['final_plan']
            c1, c2 = st.columns(2)
            with c1: v_day = st.selectbox("📅 查看日期", sorted(res['作业日期'].unique()))
            with c2: v_sit = st.selectbox("👤 筛选喂猫师", ["全部"] + sorted(res['喂猫师'].unique().tolist()))
            
            v_data = res[res['作业日期'] == view_day]
            if v_sit != "全部": v_data = v_data[v_data['喂猫师'] == v_sit]
            
            # 渲染表格，仅展示必要列
            display_cols = ['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']
            actual_cols = [c for c in display_cols if c in v_data.columns]
            if not v_data.empty:
                st.data_editor(v_data[actual_cols].sort_values('拟定顺序'), use_container_width=True)
                
                if st.button("✅ 确认同步喂猫师数据至飞书"):
                    sync_bar = st.progress(0); total = len(res)
                    for i, (_, row) in enumerate(res.iterrows()):
                        update_feishu_record(row['_system_id'], {"喂猫师": row['喂猫师']})
                        sync_bar.progress((i + 1) / total)
                    st.success("🎉 飞书文档更新成功！")
                    st.session_state.pop('feishu_cache', None)
