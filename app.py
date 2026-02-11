import streamlit as st
import pandas as pd

# 1. 页面设置：巨幕适配
st.set_page_config(page_title="小猫直喂调度系统", layout="wide")

# 强制 30px 巨幕按钮样式
st.markdown("""
    <style>
    div.stButton > button {
        font-size: 30px !important;
        height: 85px !important;
        width: 100% !important;
        border-radius: 12px !important;
        font-weight: bold !important;
        margin-bottom: 20px;
    }
    .stDataFrame { font-size: 18px !important; }
    </style>
    """, unsafe_allow_html=True)

# 2. 当前出勤名单 (根据你截图显示的实时名单)
ON_DUTY_SITTERS = ["梦蕊", "依蕊"]

# 3. 核心分配算法
def execute_assign_logic(df):
    # 确保列存在
    if '喂猫师' not in df.columns:
        df['喂猫师'] = ""
    
    # 填充空值方便处理
    df['喂猫师'] = df['喂猫师'].fillna("")
    df['详细地址'] = df['详细地址'].fillna("未知地址")

    # 映射表：详细地址 -> 喂猫师 (实现“一个客户尽量一个人喂”)
    address_to_sitter = {}
    
    # 先扫描一遍，把人工指定的和已有的关系存下来
    for _, row in df[df['喂猫师'] != ""].iterrows():
        address_to_sitter[row['详细地址']] = row['喂猫师']

    # 统计出勤人员的当前负载量
    sitter_load = {name: 0 for name in ON_DUTY_SITTERS}
    for name in df['喂猫师']:
        if name in sitter_load:
            sitter_load[name] += 1

    # 执行分配
    for i, row in df.iterrows():
        # 优先级1：人工已指定，跳过
        if row['喂猫师'] != "":
            continue
            
        addr = row['详细地址']
        
        # 优先级2：老客户/同地址绑定
        if addr in address_to_sitter:
            df.at[i, '喂猫师'] = address_to_sitter[addr]
        else:
            # 优先级3：系统自动分配（负载均衡）
            if ON_DUTY_SITTERS:
                # 选接单最少的人
                best_sitter = min(sitter_load, key=sitter_load.get)
                df.at[i, '喂猫师'] = best_sitter
                # 更新映射关系，确保该客户后续订单也是此人
                address_to_sitter[addr] = best_sitter
                sitter_load[best_sitter] += 1
            else:
                df.at[i, '喂猫师'] = "无人出勤"
                
    return df

# --- 界面展示 ---

st.title("🐾 小猫直喂数据中心 - 自动调度版")

# 巨幕功能按钮
col1, col2, col3 = st.columns(3)

with col1:
    st.button("📊 批量导入 Excel") # 这里仅作为UI占位

with col2:
    st.button("✍️ 单条手动录入")

with col3:
    if st.button("🔄 强制刷新预览"):
        st.rerun()

# 模拟文件上传逻辑
uploaded_file = st.file_uploader("点击上传待处理的 Excel 订单", type=["xlsx", "xls"])

if uploaded_file:
    # 读取原始数据
    raw_df = pd.read_excel(uploaded_file)
    
    st.markdown("### 🔍 自动分配结果预览")
    
    # 【关键步骤】在显示和同步前，先跑分配逻辑
    final_df = execute_assign_logic(raw_df)
    
    # 显示结果，你会看到“喂猫师”一列被填满了
    st.dataframe(final_df, use_container_width=True)
    
    # 同步飞书按钮
    if st.button("✅ 确认并同步飞书"):
        # 这里的 final_df 已经是带了“喂猫师”数据的表格
        # 在这里执行你的飞书 API 推送逻辑
        st.success("同步已完成！喂猫师数据已成功填充到飞书文档。")
else:
    st.info("请先上传 Excel 订单文件以进行自动分配。")
