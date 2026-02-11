import streamlit as st
import pandas as pd
import requests
import io
from datetime import datetime, timedelta

# --- 1. 核心连接配置 (Secrets 自动读取) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 

# --- 2. 调度核心大脑：三级派单逻辑 ---

def execute_smart_dispatch(df, active_sitters):
    """
    三级分配规则：
    1. 人工指定优先：Excel/云端已填写的『喂猫师』绝对保留。
    2. 一只猫固定一人：通过『宠物名字 + 详细地址』锁定历史喂猫师。
    3. 负载均衡：新客户自动分配给当前接单最少的出勤人员。
    """
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    df['详细地址'] = df['详细地址'].fillna("未知地址")
    df['宠物名字'] = df['宠物名字'].fillna("未知小猫")

    # 建立【宠物+地址 -> 喂猫师】绑定字典
    cat_to_sitter_map = {}
    
    # 第一遍扫描：记录飞书文档中现有的绑定关系
    for _, row in df[df['喂猫师'] != ""].iterrows():
        key = f"{row['宠物名字']}_{row['详细地址']}"
        cat_to_sitter_map[key] = row['喂猫师']

    # 统计出勤人员负载
    sitter_load = {s: 0 for s in active_sitters}
    for sitter in df['喂猫师']:
        if sitter in sitter_load: sitter_load[sitter] += 1

    # 第二遍扫描：执行分配
    for i, row in df.iterrows():
        # 优先级 A：已有人工指定，跳过
        if row['喂猫师'] != "": continue
        
        cat_key = f"{row['宠物名字']}_{row['详细地址']}"
        
        # 优先级 B：老客户绑定 (固定一人)
        if cat_key in cat_to_sitter_map:
            df.at[i, '喂猫师'] = cat_to_sitter_map[cat_key]
        else:
            # 优先级 C：系统自动分配 (负载均衡)
            if active_sitters:
                best_sitter = min(sitter_load, key=sitter_load.get)
                df.at[i, '喂猫师'] = best_sitter
                cat_to_sitter_map[cat_key] = best_sitter # 记录新绑定
                sitter_load[best_sitter] += 1
            else:
                df.at[i, '喂猫师'] = "无人出勤"
    return df

# --- 3. 飞书 API 交互逻辑 (完整保留) ---

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
        # 确保关键列存在，防止页面报错
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '喂猫师', '投喂频率', '备注']:
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
        /* 侧边栏按钮：巨幕 30px 极致黑框适配 */
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important; height: 100px !important;
            background-color: #FFFFFF !important; color: #000000 !important;
            border: 4px solid #000000 !important; border-radius: 15px !important;
            font-size: 30px !important; font-weight: 900 !important;
            margin-bottom: 20px !important;
            box-shadow: 6px 6px 0px #000;
        }
        [data-testid="stSidebar"] div.stButton > button:hover { background-color: #000 !important; color: #FFF !important; }
        .stDataFrame { font-size: 18px !important; }
        h1 { border-bottom: 3px solid #000; padding-bottom: 10px; }
        </style>
        """, unsafe_allow_html=True)

# --- 5. 页面路由中心 ---

st.set_page_config(page_title="小猫直喂调度系统 V4.0", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    
    if st.session_state['page'] == "智能看板":
        st.divider(); st.subheader("⚙️ 调度配置")
        active_sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in active_sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 调度范围", value=(datetime.now(), datetime.now() + timedelta(days=2)))

# --- 6. 功能模块渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (云端录入与导入)")
    
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("选择文件", type=["xlsx"])
            if up_file and st.button("🚀 启动数据录入"):
                df_up = pd.read_excel(up_file); p_bar = st.progress(0)
                for i, (_, row) in enumerate(df_up.iterrows()):
                    s_ts = int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000)
                    e_ts = int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000)
                    payload = {
                        "详细地址": str(row['详细地址']).strip(),
                        "宠物名字": str(row.get('宠物名字', '小猫')).strip(),
                        "投喂频率": int(row.get('投喂频率', 1)),
                        "服务开始日期": s_ts, "服务结束日期": e_ts,
                        "喂猫师": str(row.get('喂猫师', '')).strip(),
                        "备注": str(row.get('备注', ''))
                    }
                    add_feishu_record(payload); p_bar.progress((i + 1) / len(df_up))
                st.success("批量同步成功！"); st.session_state.pop('feishu_cache', None); st.rerun()

    with c2:
        with st.expander("单条手动录入"):
            with st.form("single_form"):
                addr = st.text_input("详细地址*"); cat = st.text_input("宠物名字"); sd = st.date_input("开始"); ed = st.date_input("结束")
                if st.form_submit_button("保存至云端"):
                    payload = {
                        "详细地址": addr.strip(), "宠物名字": cat.strip(), "投喂频率": 1,
                        "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000),
                        "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)
                    }
                    if add_feishu_record(payload):
                        st.success("录入成功！"); st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    if st.button("🔄 强制刷新预览云端数据"):
        st.session_state.pop('feishu_cache', None); st.session_state['feishu_cache'] = fetch_feishu_data(); st.rerun()
    
    st.subheader("📊 云端数据预览 (实时快照)")
    df_preview = st.session_state['feishu_cache'].copy()
    if not df_preview.empty:
        for c in ['服务开始日期', '服务结束日期']:
            df_preview[c] = pd.to_datetime(df_preview[c], unit='ms', errors='coerce').dt.strftime('%Y-%m-%d')
        st.dataframe(df_preview.drop(columns=['_system_id'], errors='ignore'), use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能调度看板 (稳健版)")
    df_raw = st.session_state['feishu_cache'].copy()
    
    if not df_raw.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        # 预转换日期格式
        for col in ['服务开始日期', '服务结束日期']:
            df_raw[col] = pd.to_datetime(df_raw[col], unit='ms', errors='coerce')
        
        if st.button("✨ 拟定全周期分配方案"):
            all_day_plans = []
            days = pd.date_range(start_d, end_d).tolist(); p_bar = st.progress(0)
            
            # 运行核心派单算法
            df_assigned = execute_smart_dispatch(df_raw, current_active)
            
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d)
                day_df = df_assigned[(df_assigned['服务开始日期'] <= cur_ts) & (df_assigned['服务结束日期'] >= cur_ts)].copy()
                if not day_df.empty:
                    # 频率过滤
                    day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not day_df.empty:
                        day_df['作业日期'] = d.strftime('%Y-%m-%d')
                        all_day_plans.append(day_df)
                p_bar.progress((i + 1) / len(days))
            
            if all_day_plans:
                st.session_state['final_plan_v4'] = pd.concat(all_day_plans)
                st.success("✅ 调度拟定完成！已执行一只猫固定一人逻辑。")

        if st.session_state.get('final_plan_v4') is not None:
            res_df = st.session_state['final_plan_v4']
            
            c1, c2 = st.columns(2)
            # 采用安全的选择器，防止 NameError
            v_day = c1.selectbox("📅 1. 选择查看日期", sorted(res_df['作业日期'].unique()))
            sitters_found = ["全部"] + sorted(res_df[res_df['作业日期'] == v_day]['喂猫师'].unique().tolist())
            v_sit = c2.selectbox("👤 2. 筛选喂猫师", sitters_found)
            
            # 过滤数据并展示
            display_data = res_df[res_df['作业日期'] == v_day]
            if v_sit != "全部": display_data = display_data[display_data['喂猫师'] == v_sit]
            
            if not display_data.empty:
                st.subheader(f"📋 {v_day} - {v_sit} 任务清单")
                st.data_editor(display_data[['喂猫师', '宠物名字', '详细地址', '备注']], use_container_width=True)
                
                if st.button("✅ 确认同步喂猫师数据至飞书"):
                    sync_p = st.progress(0); total = len(res_df)
                    for i, (_, row) in enumerate(res_df.iterrows()):
                        # 【核心同步】将计算出的“喂猫师”字段回写
                        update_feishu_record(row['_system_id'], {"喂猫师": row['喂猫师']})
                        sync_p.progress((i + 1) / total)
                    st.success("🎉 同步完成！飞书文档已成功更新。")
                    st.session_state.pop('feishu_cache', None)
        else:
            st.info("💡 请先点击上方按钮生成调度方案。")
