import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import json

# --- 1. 核心连接配置 ---
APP_ID = st.secrets.get("FEISHU_APP_ID", "").strip()
APP_SECRET = st.secrets.get("FEISHU_APP_SECRET", "").strip()
APP_TOKEN = st.secrets.get("FEISHU_APP_TOKEN", "").strip() 
TABLE_ID = st.secrets.get("FEISHU_TABLE_ID", "").strip() 
AMAP_API_KEY = st.secrets.get("AMAP_KEY", "").strip()

# --- 2. 核心大脑：分配与路径优化 ---

def get_distance(p1, p2):
    return np.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def optimize_route(df_sitter):
    """最近邻路径优化"""
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
    """一猫一人固定派单逻辑"""
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

# --- 3. 飞书 API 交互逻辑 (增加诊断埋点) ---

def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=10)
        return r.json().get("tenant_access_token")
    except Exception as e:
        st.error(f"Token 获取异常: {str(e)}")
        return None

def fetch_feishu_data():
    token = get_feishu_token()
    if not token: return pd.DataFrame()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, params={"page_size": 500}, timeout=15).json()
        items = r.get("data", {}).get("items", [])
        if not items: return pd.DataFrame()
        # 严格保留 record_id 到 _system_id
        df = pd.DataFrame([dict(i['fields'], _system_id=i['record_id']) for i in items])
        # 强制字段对齐
        for col in ['宠物名字', '服务开始日期', '服务结束日期', '详细地址', '喂猫师', '备注', 'lng', 'lat']:
            if col not in df.columns: df[col] = ""
        return df
    except: return pd.DataFrame()

def update_feishu_record_with_log(record_id, sitter_name):
    """
    回写函数：带日志审计
    """
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{record_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fields": {"喂猫师": str(sitter_name)}}
    
    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=10)
        res_json = r.json()
        if res_json.get("code") == 0:
            return True, "成功"
        else:
            return False, f"错误码 {res_json.get('code')}: {res_json.get('msg')}"
    except Exception as e:
        return False, f"请求异常: {str(e)}"

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

# --- 4. 视觉风格与 UI (30px 巨幕) ---

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
        [data-testid="stSidebar"] div.stButton > button:hover { background-color: #000 !important; color: #FFF !important; }
        .diag-box { background: #fafafa; border: 1px solid #ddd; padding: 10px; font-family: monospace; font-size: 12px; max-height: 200px; overflow-y: auto; }
        </style>
        """, unsafe_allow_html=True)

# --- 5. 流程中心 ---

st.set_page_config(page_title="指挥中心 V14.0", layout="wide")
set_ui()

if 'page' not in st.session_state: st.session_state['page'] = "智能看板"
if 'feishu_cache' not in st.session_state: st.session_state['feishu_cache'] = fetch_feishu_data()

with st.sidebar:
    st.header("🔑 团队授权")
    if st.text_input("暗号", type="password", value="xiaomaozhiwei666") != "xiaomaozhiwei666": st.stop()
    st.divider()
    if st.button("📂 数据中心"): st.session_state['page'] = "数据中心"
    if st.button("🚀 智能看板"): st.session_state['page'] = "智能看板"
    
    # 诊断工具箱
    st.divider()
    st.subheader("🛠️ 诊断工具")
    if st.button("🔎 执行系统自检"):
        st.write("1. 令牌检测...")
        tok = get_feishu_token()
        if tok: st.success("令牌有效")
        st.write("2. 数据结构检测...")
        df_test = st.session_state['feishu_cache']
        if '_system_id' in df_test.columns: st.success("ID 字段正常")
        if '喂猫师' in df_test.columns: st.success("业务字段正常")
        else: st.error("飞书文档缺少『喂猫师』字段！")

# --- 6. 模块渲染 ---

if st.session_state['page'] == "数据中心":
    st.title("📂 数据中心 (云端全量预览)")
    
    col_in1, col_in2 = st.columns(2)
    with col_in1:
        with st.expander("批量导入 Excel"):
            up_file = st.file_uploader("文件", type=["xlsx"])
            if up_file and st.button("🚀 录入云端"):
                df_up = pd.read_excel(up_file); p_bar = st.progress(0); tok = get_feishu_token()
                for i, (_, row) in enumerate(df_up.iterrows()):
                    payload = {"详细地址": str(row['详细地址']).strip(), "宠物名字": str(row.get('宠物名字', '小猫')).strip(), "投喂频率": int(row.get('投喂频率', 1)), "服务开始日期": int(datetime.combine(pd.to_datetime(row['服务开始日期']), datetime.min.time()).timestamp()*1000), "服务结束日期": int(datetime.combine(pd.to_datetime(row['服务结束日期']), datetime.min.time()).timestamp()*1000), "备注": str(row.get('备注', ''))}
                    requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records", headers={"Authorization": f"Bearer {tok}"}, json={"fields": payload})
                    p_bar.progress((i + 1) / len(df_up))
                st.success("批量完成！"); st.session_state.pop('feishu_cache', None); st.rerun()

    st.divider()
    if st.button("🔄 强制刷新云端快照"):
        st.session_state.pop('feishu_cache', None); st.session_state['feishu_cache'] = fetch_feishu_data(); st.rerun()
    
    # 预览排除 lng/lat
    df_preview = st.session_state['feishu_cache'].copy()
    if not df_preview.empty:
        disp = df_preview.drop(columns=['lng', 'lat', '_system_id'], errors='ignore')
        st.dataframe(disp, use_container_width=True)

elif st.session_state['page'] == "智能看板":
    st.title("🚀 调度看板 (诊断同步版)")
    df_kb = st.session_state['feishu_cache'].copy()
    
    active_sitters = ["梦蕊", "依蕊"]
    c_s1, c_s2 = st.sidebar.columns(2)
    current_active = [s for s in active_sitters if st.sidebar.checkbox(f"{s} (出勤)", value=True)]
    date_range = st.sidebar.date_input("📅 范围", value=(datetime.now(), datetime.now() + timedelta(days=1)))

    if not df_kb.empty and isinstance(date_range, tuple) and len(date_range) == 2:
        for c in ['服务开始日期', '服务结束日期']: df_kb[c] = pd.to_datetime(df_kb[c], unit='ms', errors='coerce')
        
        if st.button("✨ 拟定最优方案"):
            all_plans = []
            days = pd.date_range(date_range[0], date_range[1]).tolist()
            df_kb = execute_smart_dispatch(df_kb, current_active)
            p_bar = st.progress(0)
            for i, d in enumerate(days):
                cur_ts = pd.Timestamp(d)
                day_df = df_kb[(df_kb['服务开始日期'] <= cur_ts) & (df_kb['服务结束日期'] >= cur_ts)].copy()
                if not day_df.empty:
                    day_df = day_df[day_df.apply(lambda r: (cur_ts - r['服务开始日期']).days % int(r.get('投喂频率', 1)) == 0, axis=1)]
                    if not day_df.empty:
                        with ThreadPoolExecutor(max_workers=10) as ex: coords = list(ex.map(get_coords, day_df['详细地址']))
                        day_df[['lng', 'lat']] = pd.DataFrame(coords, index=day_df.index)
                        day_df = day_df.dropna(subset=['lng', 'lat'])
                        day_res = []
                        for s in current_active:
                            s_tasks = day_df[day_df['喂猫师'] == s].copy()
                            if not s_tasks.empty: day_res.append(optimize_route(s_tasks))
                        if day_res:
                            concat_day = pd.concat(day_res)
                            concat_day['作业日期'] = d.strftime('%Y-%m-%d')
                            all_plans.append(concat_day)
                p_bar.progress((i + 1) / len(days))
            st.session_state['final_plan_v14'] = pd.concat(all_plans) if all_plans else None
            st.success("✅ 方案拟定完成！")

        if st.session_state.get('final_plan_v14') is not None:
            res_f = st.session_state['final_plan_v14']
            v_day = st.selectbox("📅 查看日期", sorted(res_f['作业日期'].unique()))
            v_data = res_f[res_f['作业日期'] == v_day]
            
            if not v_data.empty:
                st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=pdk.ViewState(longitude=v_data['lng'].mean(), latitude=v_data['lat'].mean(), zoom=11), layers=[pdk.Layer("ScatterplotLayer", v_data, get_position='[lng, lat]', get_color=[0, 123, 255, 160], get_radius=300)]))
                st.data_editor(v_data[['拟定顺序', '喂猫师', '宠物名字', '详细地址', '备注']].sort_values('拟定顺序'), use_container_width=True)
                
                # 同步回写按钮
                if st.button("✅ 确认并同步喂猫师数据至飞书"):
                    logs = []
                    suc = 0; tot = len(res_f); sync_p = st.progress(0)
                    for i, (_, row) in enumerate(res_f.iterrows()):
                        if row.get('_system_id') and row.get('喂猫师'):
                            ok, msg = update_feishu_record_with_log(row['_system_id'], row['喂猫师'])
                            if ok: suc += 1
                            else: logs.append(f"ID:{row['_system_id']} 失败: {msg}")
                        sync_p.progress((i + 1) / tot)
                    
                    st.success(f"🎉 同步完成！回写 {suc} 条记录。")
                    if logs:
                        st.error("部分记录同步失败，详见下方诊断日志：")
                        st.markdown(f'<div class="diag-box">{"<br>".join(logs)}</div>', unsafe_allow_html=True)
                    st.session_state.pop('feishu_cache', None)
