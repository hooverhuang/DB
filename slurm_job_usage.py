import pandas as pd
import pymysql

# MySQL 連線設定
mysql_config = {
    'host': '10.2.240.101',
    'user': 'admin',
    'password': 'admin',
    'database': 'usagereport'
}

# 解析 AllocTRES 欄位
def parse_alloctres(tres, key):
    if pd.isna(tres):
        return 0
    for part in tres.split(','):
        if part.startswith(key + '='):
            value = part.split('=')[1]
            if key == 'mem':
                # 支援 T/G/M 單位，並處理小數點
                if value.endswith('T'):
                    return int(float(value[:-1]) * 1024 * 1024)
                elif value.endswith('G'):
                    return int(float(value[:-1]) * 1024)
                elif value.endswith('M'):
                    return int(float(value[:-1]))
                else:
                    return int(float(value))
            else:
                return int(float(value))
    return 0

# 讀取 csv（用 | 分隔，欄位順序請依 sacct 匯出設定）
df = pd.read_csv('user_job_usage.csv', sep='|', names=['user','jobid','cpu','reqmem','nnodes','alloctres'])

# 只保留主 job（JobID 純數字）
df = df[df['jobid'].astype(str).str.match(r'^\d+$')]

# 解析 memory、gpu、node
df['memory'] = df['alloctres'].apply(lambda x: parse_alloctres(x, 'mem'))
df['gpu'] = df['alloctres'].apply(lambda x: parse_alloctres(x, 'gres/gpu'))
df['node'] = df['alloctres'].apply(lambda x: parse_alloctres(x, 'node'))

# 只保留需要的欄位
df2 = df[['user', 'jobid', 'cpu', 'memory', 'gpu', 'node']]

# 寫入 MySQL
conn = pymysql.connect(**mysql_config)
cursor = conn.cursor()

for _, row in df2.iterrows():
    sql = "INSERT INTO job_usage (user, jobid, cpu, memory, gpu, node) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(sql, (row['user'], row['jobid'], int(row['cpu']), int(row['memory']), int(row['gpu']), int(row['node'])))

conn.commit()
cursor.close()
conn.close()
print("資料已寫入 MySQL！")