import clickhouse_connect

# Connect to the running ClickHouse server
client = clickhouse_connect.get_client(host='127.0.0.1', port=8124)

print("Connected to ClickHouse version:", client.server_version)

client.command('''
    CREATE TABLE IF NOT EXISTS test_table (
        id UInt32,
        name String
    ) ENGINE = MergeTree()
    ORDER BY id
''')

data = [
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie')
]
client.insert('test_table', data, column_names=['id', 'name'])

result = client.query('SELECT * FROM test_table')

print("Query Results:")
for row in result.result_rows:
    print(row)
