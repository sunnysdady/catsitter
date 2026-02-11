import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# --- 1. 核心配置与授权 ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 调度逻辑：一猫一人固定派单 ---
def execute_smart_dispatch(df, active_sitters):
    """三级派单规则：人工指定 > 一只猫固定一人 > 负载均衡"""
    if '喂猫师' not in df.columns: df['喂猫师'] = ""
    df['喂猫师'] = df['喂猫师'].fillna("")
    
    # 建立【宠物名字+详细地址】唯一键映射
    cat_to_sitter_map = {}
    # 扫描现有数据：只要这只猫曾经有喂猫师，就锁定
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

# --- 3. 飞书 API 底层交互 ---
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
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '喂猫师', '投喂频率', 'lng', 'lat']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_single_record(record_id, sitter_name):
    """
    核心：向飞书回写喂猫师数据
    """
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # 强制封装为飞书要求的 fields 结构
    payload = {"fields": {"喂猫师": sitter_name}}
    try:
        res = requests.patch(url, headers=headers, json=payload, timeout=10).json()
        return res.get("code") == 0
    except: return False

# --- 4. UI 视觉重构 (30px) ---
def set_ui():
    st.markdown("""
        <style>
        [data-testid="stSidebar"] div.stButton > button {
            width: 100% !important; height: 100px !important;
            border: 4px solid #000 !important; border-radius: 15px !important;
            font-size: 30px !important; font-weight: 900 !important;
            box-shadow: 6px 6px 0px #000;
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

# --- 5. 流程中心 ---
st.set_page_config(page_title="指挥中心 V6.0", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state:
    st.session_state['feishu_cache'] = fetch_feishu_data()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    if st.session_state['page'] == "智能看板":
        st.divider(); sitters = ["梦蕊", "依蕊"]
        current_active = [s for s in sitters if st.checkbox(f"{s} (出勤)", value=True)]
        date_range = st.date_input("📅 日期范围", value=(datetime.now(), datetime.now() + timedelta(days=1)))

# --- 6. 逻辑渲染 ---
if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (云端预览)")
    if st.button("🔄 刷新云端数据"):
        st.session_state.pop('feishu_cache', None); st.session_state['feishu_cache'] = fetch_feishu_data(); st.rerun()
    st.dataframe(st.session_state['feishu_cache'].drop(columns=['_system_id'], errors='ignore'), use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 智能看板 (一只猫固定一人版)")
    df_kb = st.session_state['feishu_cache'].copy()
    
    if not df_kb.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        for c in ['服务开始日期', '服务结束日期']: df_kb[c] = pd.to_datetime(df_kb[c], unit='ms', errors='coerce')
        
        if st.button("✨ 拟定派单方案 (含地图视图)"):
            all_plans = []
            days = pd.date_range(date_range[0], date_range[1]).tolist()
            # 运行核心分配：锁定一只猫固定一人
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
                        day_df['作业日期'] = d.strftime('%Y-%m-%d')
                        all_plans.append(day_df)
                p_bar.progress((i + 1) / len(days))
            st.session_state['final_plan_v6'] = pd.concat(all_plans) if all_plans else None
            st.success("✅ 方案拟定完成！")

        if st.session_state.get('final_plan_v6') is not None:
            res_final = st.session_state['final_plan_v6']
            c1, c2 = st.columns(2)
            v_day = c1.selectbox("📅 选择日期", sorted(res_final['作业日期'].unique()))
            v_sit = c2.selectbox("👤 筛选喂猫师", ["全部"] + sorted(res_final['喂猫师'].unique().tolist()))
            
            v_data = res_final[res_final['作业日期'] == v_day]
            if v_sit != "全部": v_data = v_data[v_data['喂猫师'] == v_sit]
            
            if not v_data.empty:
                # 地图展示
                st.pydeck_chart(pdk.Deck(
                    map_style=pdk.map_styles.LIGHT,
                    initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11),
                    layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]
                ))
                # 任务表格
                st.data_editor(v_data[['喂猫师', '宠物名字', '详细地址', '备注']], use_container_width=True)
                
                # --- 同步逻辑：核心回写区 ---
                if st.button("✅ 确认同步喂猫师数据至飞书"):
                    success_count = 0
                    fail_count = 0
                    sync_p = st.progress(0)
                    total = len(res_final)
                    
                    for i, (_, row) in enumerate(res_final.iterrows()):
                        if update_feishu_single_record(row['_system_id'], row['喂猫师']):
                            success_count += 1
                        else:
                            fail_count += 1
                        sync_p.progress((i + 1) / total)
                    
                    st.success(f"🎉 同步完成！成功：{success_count} 条，失败：{fail_count} 条。")
                    if fail_count > 0:
                        st.warning("提示：部分同步失败。请检查飞书『喂猫师』字段是否为『文本』类型，或是否有权限修改该文档。")
                    st.session_state.pop('feishu_cache', None)
