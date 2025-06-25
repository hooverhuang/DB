import pymysql

# 連線到 iam
iam_conn = pymysql.connect(
    host='mariadb-galera.pegasus-system',
    user='root',
    password='oc!s2024',
    database='iam'
)
iam_cursor = iam_conn.cursor()

# 連線到 usagereport
report_conn = pymysql.connect(
    host='10.2.240.101',
    user='admin',
    password='admin',
    database='usagereport'
)
report_cursor = report_conn.cursor()

# 同步 user
iam_cursor.execute("SELECT id, display_name FROM user")
for user_id, display_name in iam_cursor.fetchall():
    report_cursor.execute(
        "INSERT IGNORE INTO user (id, display_name) VALUES (%s, %s)",
        (user_id, display_name)
    )

# 同步 project
iam_cursor.execute("SELECT id, display_name FROM project")
for project_id, display_name in iam_cursor.fetchall():
    report_cursor.execute(
        "INSERT IGNORE INTO project (id, display_name) VALUES (%s, %s)",
        (project_id, display_name)
    )

# 同步 user_project
iam_cursor.execute("SELECT user_id, project_id FROM membership")
for user_id, project_id in iam_cursor.fetchall():
    report_cursor.execute(
        "INSERT IGNORE INTO user_project (user_id, project_id) VALUES (%s, %s)",
        (user_id, project_id)
    )

report_conn.commit()
iam_cursor.close()
iam_conn.close()
report_cursor.close()
report_conn.close()