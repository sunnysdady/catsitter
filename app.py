import streamlit as st
import pandas as pd
import requests
import io
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# --- 1. 核心配置与授权 (Secrets 自动读取) ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 核心算法大脑：路径优化与分配逻辑 ---

def get_distance(p1, p2):
    """计算简易物理距离"""
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """最近邻算法：优化作业路径"""
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
    """三级派单规则：人工指定 > 一只猫固定一人 > 负载均衡"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    
    # 建立【宠物名字+详细地址】唯一键，锁定一只猫固定一人
    cat_to_sitter_map = {}
    for _, row in df[df['喂猫师'] != ""].iterrows():
        cat_to_sitter_map[f"{row['宠物名字']}_{row['详细地址']}"] = row['喂猫师']
    
    sitter_load = {s: 0 for s in active_sitters}
    for s in df['喂猫师']:
        if s in sitter_load: sitter_load[s] += 1
        
    for i, row in df.iterrows():
        if row['喂猫师'] != "": continue
        cat_key = f"{row['宠物名字']}_{row['详细地址']}"
        if cat_key in cat_to_sitter_map:
            df.at[i, '喂猫师'] = cat_to_sitter_map[cat_key]
        elif active_sitters:
            best = min(sitter_load, key=sitter_load.get)
            df.at[i, '喂猫师'] = best
            cat_to_sitter_map[cat_key] = best
            sitter_load[best] += 1
    return df

# --- 3. 飞书 API 交互层 ---

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
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        # 补全字段防止 KeyError
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '喂猫师', '投喂频率', '备注', 'lng', 'lat']:
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
    payload = {"fields": {k: ("" if pd.isna(v) else v) for k, v in fields.items()}}
    try:
        res = requests.patch(url, headers=headers, json=payload, timeout=10).json()
        return res.get("code") == 0
    except: return False

# --- 4. 视觉风格与地理工具 (30px 巨幕) ---

def set_ui():
    st.markdown("""
        <style>
        /* 侧边栏按钮：巨幕 30px 极致黑框粗体 */
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important; height: 100px !important;
            border: 4px solid #000 !important; border-radius: 15px !important;
            font-size: 30px !important; font-weight: 900 !important;
            box-shadow: 6px 6px 0px #000;
            background-color: #FFFFFF !important; color: #000000 !important;
        }
        [data-testid="stSidebar"] div.stButton > button:hover {
            background-color: #000 !important; color: #FFF !important;
        }
        .stDataFrame { font-size: 16px !important; }
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

# --- 5. 页面路由 ---

st.set_page_config(page_title="指挥中心 V8.0", layout="wide")
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
        st.divider(); sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 调度范围", value=(datetime.now(), datetime.now() + timedelta(days=1)))

# --- 6. 核心业务模块 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (导入、录入与预览)")
    
    col_in1, col_in2 = st.columns(2)
    with col_in1:
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
                        "服务开始日期": s_ts, "服务结束日期": e_ts, "备注": str(row.get('备注', ''))
                    }
                    add_feishu_record(payload); p_bar.progress((i + 1) / len(df_up))
                st.success("批量录入成功！"); st.session_state.pop('feishu_cache', None); st.rerun()

    with col_in2:
        with st.expander("单条手动录入"):
            with st.form("manual_entry"):
                addr = st.text_input("详细地址*"); cat = st.text_input("宠物名字"); sd = st.date_input("开始"); ed = st.date_input("结束")
                if st.form_submit_button("保存至飞书"):
                    payload = {
                        "详细地址": addr.strip(), "宠物名字": cat.strip(), "投喂频率": 1,
                        "服务开始日期": int(datetime.combine(sd, datetime.min.time()).timestamp()*1000),
                        "服务结束日期": int(datetime.combine(ed, datetime.min.time()).timestamp()*1000)
                    }
                    if add_feishu_record(payload):
                        st.success("录入完成！"); st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    if st.button("🔄 刷新预览云端数据"):
        st.session_state.pop('feishu_cache', None); st.session_state['feishu_cache'] = fetch_feishu_data(); st.rerun()
    st.dataframe(st.session_state['feishu_cache'].drop(columns=['_system_id'], errors='ignore'), use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能调度看板 (地图回归版)")
    df_kb = st.session_state['feishu_cache'].copy()
    
    if not df_kb.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        for c in ['服务开始日期', '服务结束日期']: df_kb[c] = pd.to_datetime(df_kb[c], unit='ms', errors='coerce')
        
        if st.button("✨ 拟定方案 (含路径优化与分配逻辑)"):
            all_plans = []
            days = pd.date_range(date_range[0], date_range[1]).tolist()
            # 锁定规则：一只猫固定一人
            df_kb = execute_smart_dispatch(df_kb, current_active)
            
            p_bar = st.progress(0)
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d)
                day_df = df_kb[(df_kb['服务开始日期'] <= cur_ts) & (df_kb['服务结束日期'] >= cur_ts)].copy()
                if not day_df.empty:
                    day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not day_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex:
                            coords = list(ex.map(get_coords, day_df['详细地址']))
                        day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                        day_df = day_df.dropna(subset=['lng', 'lat'])
                        
                        day_res = []
                        for s in current_active:
                            s_tasks = day_df[day_df['喂猫师'] == s].copy()
                            if not s_tasks.empty: day_res.append(optimize_route(s_tasks))
                        if day_res:
                            res_day = pd.concat(day_res)
                            res_day['作业日期'] = d.strftime('%Y-%m-%d')
                            all_plans.append(res_day)
                p_bar.progress((i + 1) / len(days))
            st.session_state['final_plan_v8'] = pd.concat(all_plans) if all_plans else None
            st.success("✅ 方案拟定完成！")

        if st.session_state.get('final_plan_v8') is not None:
            res_final = st.session_state['final_plan_v8']
            col_v1, col_v2 = st.columns(2)
            v_day = col_v1.selectbox("📅 选择日期", sorted(res_final['作业日期'].unique()))
            v_sit = col_v2.selectbox("👤 筛选喂猫师", ["全部"] + sorted(res_final['喂猫师'].unique().tolist()))
            
            v_data = res_final[res_final['作业日期'] == v_day]
            if v_sit != "全部": v_data = v_data[v_data['喂猫师'] == v_sit]
            
            if not v_data.empty:
                # 地图展示
                st.pydeck_chart(pdk.Deck(
                    map_style=pdk.map_styles.LIGHT,
                    initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                    layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]
                ))
                # 任务表
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                
                c_sync1, c_sync2 = st.columns(2)
                with c_sync1:
                    if st.button("📋 生成今日微信简报"):
                        summary = f"📢 清单 ({v_day})\n\n"
                        for s in (current_active if v_sit == "全部" else [v_sit]):
                            s_tasks = v_data[v_data['喂猫师'] == s].sort_values('拟定顺序')
                            if not s_tasks.empty:
                                summary += f"👤 喂猫师：{s}\n"
                                for _, t in s_tasks.iterrows():
                                    summary += f"   {t['拟定顺序']}. {t['宠物名字']} - {t['详细地址']}\n"
                                summary += "\n"
                        st.text_area("复制发到微信：", summary, height=200)

                with c_sync2:
                    if st.button("✅ 确认并同步喂猫师数据至飞书"):
                        sync_p = st.progress(0); total = len(res_final)
                        for i, (_, row) in enumerate(res_final.iterrows()):
                            # 回写计算出的“喂猫师”字段
                            update_feishu_record(row['_system_id'], {"喂猫师": row['喂猫师']})
                            sync_p.progress((i + 1) / total)
                        st.success("🎉 云端更新完成！已填充飞书『喂猫师』列。")
                        st.session_state.pop('feishu_cache', None)
