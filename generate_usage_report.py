import paramiko
import re
import pymysql
import mysql.connector
import datetime
import json

# 1. 取得 user_id → (display_name, account) 對照表
mysql_conn = pymysql.connect(
    host='mariadb-galera.pegasus-system',
    user='root',
    password='oc!s2024',
    database='iam'
)
cursor1 = mysql_conn.cursor()
cursor1.execute("SELECT id, display_name, account FROM user")
user_map = {row[0]: (row[1], row[2]) for row in cursor1.fetchall()}
print(f"[DEBUG] user_map keys: {len(user_map)}")  # Debug: user_map key 數量
mysql_conn.close()

# 1.5. 檢查特定 user_id 在 user_map 的對應狀態
user_ids = [
    "807ca7ea-490c-4b68-a910-94af23e65a72",
    "03b2c498-152b-4ab1-bc0e-f136f5a6b4b4",
    "e1f127c9-349a-4342-8bea-b0d6e26cd80e",
    "4bda2d0c-775e-4547-9944-d734cc3e165d",
    "e2772265-185b-4d24-bcea-742b44e2a6ee",
    "00122c32-dda7-4403-b840-faf2ac317ae7",
    "a6d4e932-d88f-4ec8-9736-7550575a2022",
    "73263429-3cad-46dc-a1f6-05c53c870024"
]

print("user_id 對應 user_map 狀態：")
for uid in user_ids:
    value = user_map.get(uid)
    if value is None:
        print(f"user_id: {uid} -> user_map: None")
    else:
        display_name, account = value
        print(f"user_id: {uid} -> display_name: {display_name}, account: {account}")
        if not account:
            print(f"  [WARNING] user_id: {uid} 的 account 為空字串或 None")

# 2. SSH 連線設定
host = "10.2.2.151"
port = 22
username = "root"
password = "Cj86gji42u4uau/6"

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(host, port=port, username=username, password=password)

def run_cmd(cmd):
    stdin, stdout, stderr = client.exec_command(cmd)
    return stdout.read().decode()

# 3. 連線到你要匯入的 MySQL 資料庫
mysql_config = {
    'host': '10.2.240.101',
    'user': 'admin',
    'password': 'admin',
    'database': 'usagereport'
}
conn = mysql.connector.connect(**mysql_config)
cursor2 = conn.cursor()

# 4. 取得所有 namespace 並匯入資料
ns_output = run_cmd("kubectl get ns -o custom-columns=NAME:.metadata.name --no-headers")
namespaces = [line.strip() for line in ns_output.splitlines()]

for ns in namespaces:
    top_output = run_cmd(f"kubectl top pod -n {ns} --no-headers")
    pod_usage = {}
    for line in top_output.splitlines():
        parts = line.split()
        if len(parts) >= 3:
            pod_name = parts[0]
            cpu_usage = parts[1]
            mem_usage = parts[2]
            pod_usage[pod_name] = {'cpu_usage': cpu_usage, 'mem_usage': mem_usage}

    pods_output = run_cmd(f"kubectl get pod -n {ns} -o custom-columns=NAME:.metadata.name --no-headers")
    pods = [line.strip() for line in pods_output.splitlines()]
    for pod in pods:
        # 取得 pod 詳細資訊（json 格式，抓 startTime）
        pod_json_output = run_cmd(f"kubectl get pod {pod} -n {ns} -o json")
        pod_info = json.loads(pod_json_output)
        start_time_str = pod_info['status'].get('startTime')
        live_time = None
        if start_time_str:
            start_time = datetime.datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%SZ")
            now = datetime.datetime.utcnow()
            live_time = (now - start_time).total_seconds()  # 單位：秒

        # 取得 describe pod 內容
        describe_output = run_cmd(f"kubectl describe pod {pod} -n {ns}")
        limits_block = re.search(r"Limits:\n((?:\s{6}.+\n)+)", describe_output)
        gpu_count = 0
        if limits_block:
            for line in limits_block.group(1).strip().splitlines():
                parts = line.strip().split(":")
                if len(parts) == 2 and parts[0].strip() == 'nvidia.com/gpu':
                    try:
                        gpu_count = int(parts[1].strip())
                    except:
                        gpu_count = 0
        user_id_match = re.search(r"userId=([0-9a-zA-Z-]+)", describe_output)
        user_id = user_id_match.group(1) if user_id_match else "N/A"
        user_map_value = user_map.get(user_id, ("N/A", "N/A"))
        _, account = user_map_value
        # 強制 user_name 一定是字串且不為 None 或空字串
        user_name = str(account) if account else 'N/A'

        # Debug print
        print(f"[DEBUG] user_id: {user_id}, user_map_value: {user_map_value}, user_name: {user_name}, pod: {pod}, ns: {ns}")

        if user_id == "N/A":
            print(f"[WARNING] pod {pod} in ns {ns} 沒有 userId")
        if not user_name or user_name == "N/A":
            print(f"[WARNING] user_id {user_id} 對不到 account，pod: {pod}, ns: {ns}")

        cpu_usage = pod_usage.get(pod, {}).get('cpu_usage', 'N/A')
        mem_usage = pod_usage.get(pod, {}).get('mem_usage', 'N/A')

        # 解析 requests 區塊，抓 ephemeral-storage
        requests_block = re.search(r"Requests:\n((?:\s{6}.+\n)+)", describe_output)
        storage_request = 'N/A'
        if requests_block:
            for line in requests_block.group(1).strip().splitlines():
                parts = line.strip().split(":")
                if len(parts) == 2 and parts[0].strip() == 'ephemeral-storage':
                    storage_request = parts[1].strip()

        print(f"要寫入DB的storage_request: {storage_request}, live_time: {live_time}")

        # 匯入 MySQL（移除 account 欄位，只寫 user_name）
        cursor2.execute(
            "INSERT INTO pod_report (user_id, user_name, namespace, pod_name, cpu_usage, memory_usage, gpu_count, storage_request, live_time) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "cpu_usage=VALUES(cpu_usage), "
            "memory_usage=VALUES(memory_usage), "
            "gpu_count=VALUES(gpu_count), "
            "storage_request=VALUES(storage_request), "
            "user_name=VALUES(user_name), "
            "live_time=VALUES(live_time)",
            (user_id, user_name, ns, pod, cpu_usage, mem_usage, gpu_count, storage_request, live_time)
        )
        print(f"已寫入/更新: UserID={user_id}, UserName={user_name}, Namespace={ns}, Pod={pod}, StorageRequest={storage_request}, LiveTime={live_time}")

conn.commit()
cursor2.close()
conn.close()
client.close()