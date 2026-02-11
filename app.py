from flask import Flask, render_template_string, request, jsonify
import pandas as pd

app = Flask(__name__)

# --- 模拟数据库和配置 ---
# 假设当前出勤的喂猫师名单（从你截图左侧获取）
ON_DUTY_SITTERS = ["梦蕊", "依蕊"]

# 模拟数据存储（实际应用中可以存入数据库或飞书）
current_data = []

def auto_assign_logic(df):
    """
    核心分配算法
    """
    # 1. 确保“喂猫师”列存在
    if '喂猫师' not in df.columns:
        df['喂猫师'] = ""
    
    # 2. 建立 地址 -> 喂猫师 的映射表（处理“一个客户尽量一个人喂”）
    # 先看表格里有没有已经人工指定的
    customer_mapping = df[df['喂猫师'].notna() & (df['喂猫师'] != "")].set_index('详细地址')['喂猫师'].to_dict()

    # 3. 统计当前每个出勤人员的接单量，用于负载均衡
    sitter_load = {name: 0 for name in ON_DUTY_SITTERS}
    # 统计已有分配的人头数
    for name in df['喂猫师']:
        if name in sitter_load:
            sitter_load[name] += 1

    # 4. 开始循环分配
    for index, row in df.iterrows():
        addr = row['详细地址']
        current_sitter = row['喂猫师']

        # 如果已经有指定喂猫师（第一优先级），跳过
        if pd.notna(current_sitter) and current_sitter != "":
            continue
        
        # 如果该地址之前已经分配过人（第二优先级：客户绑定）
        if addr in customer_mapping:
            df.at[index, '喂猫师'] = customer_mapping[addr]
        else:
            # 第三优先级：系统自动分配（从出勤名单选最闲的人）
            if ON_DUTY_SITTERS:
                # 找负载最小的人
                best_sitter = min(sitter_load, key=sitter_load.get)
                df.at[index, '喂猫师'] = best_sitter
                # 更新映射表和负载，保证该客户下一单也是他
                customer_mapping[addr] = best_sitter
                sitter_load[best_sitter] += 1
            else:
                df.at[index, '喂猫师'] = "无人出勤"

    return df

# --- 网页 HTML 模板 (适配 30px 巨幕) ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        :root { --base-font: 30px; --bg: #001529; }
        body { background: var(--bg); color: white; font-family: sans-serif; display: flex; flex-direction: column; align-items: center; padding: 40px; }
        .btn-group { display: flex; gap: 30px; margin-bottom: 50px; }
        .btn { 
            height: 85px; padding: 0 50px; font-size: var(--base-font); font-weight: bold; 
            color: white; border: none; border-radius: 15px; cursor: pointer; transition: 0.3s;
        }
        .btn-blue { background: #1890ff; }
        .btn-green { background: #52c41a; }
        .btn-orange { background: #faad14; }
        .btn:hover { transform: translateY(-5px); filter: brightness(1.2); }
        
        table { width: 100%; border-collapse: collapse; font-size: 24px; background: rgba(255,255,255,0.05); }
        th, td { border: 1px solid #303030; padding: 20px; text-align: left; }
        th { background: #141414; color: #1890ff; }
        .highlight { color: #52c41a; font-weight: bold; }
    </style>
</head>
<body>
    <div class="btn-group">
        <button class="btn btn-blue" onclick="location.reload()">📊 批量导入 Excel</button>
        <button class="btn btn-green">➕ 单条手动录入</button>
        <button class="btn btn-orange" onclick="refreshData()">🔄 强制刷新预览</button>
    </div>

    <div id="table-container" style="width: 90%;">
        <h2>预览数据 (已自动分配喂猫师)</h2>
        <table>
            <thead>
                <tr>
                    <th>宠物名字</th><th>详细地址</th><th>喂猫师 (系统分配)</th><th>备注</th>
                </tr>
            </thead>
            <tbody id="data-body">
                </tbody>
        </table>
    </div>

    <script>
        async function refreshData() {
            const res = await fetch('/api/get_data');
            const data = await res.json();
            const body = document.getElementById('data-body');
            body.innerHTML = '';
            data.forEach(row => {
                body.innerHTML += `<tr>
                    <td>${row.宠物名字}</td>
                    <td>${row.详细地址}</td>
                    <td class="highlight">${row.喂猫师 || '未分配'}</td>
                    <td>${row.备注 || ''}</td>
                </tr>`;
            });
        }
        // 页面加载时自动刷一次
        window.onload = refreshData;
    </script>
</body>
</html>
"""

# --- 路由逻辑 ---

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/get_data')
def get_data():
    # 模拟从 Excel 导入的原始数据
    raw_data = [
        {"宠物名字": "小胖猫测试", "详细地址": "南山智园D2栋", "喂猫师": "指定张三", "备注": "人工指定"},
        {"宠物名字": "贝贝", "详细地址": "龙华丰路鑫茂公寓", "喂猫师": "", "备注": "系统分配"},
        {"宠物名字": "贴贴猫", "详细地址": "南山智园D2栋", "喂猫师": "", "备注": "应随小胖猫分给张三"},
        {"宠物名字": "大锤", "详细地址": "民治民康路", "喂猫师": "", "备注": "负载均衡分配"},
    ]
    df = pd.DataFrame(raw_data)
    
    # 执行分配算法
    df_assigned = auto_assign_logic(df)
    
    return jsonify(df_assigned.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
