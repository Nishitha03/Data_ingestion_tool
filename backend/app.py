

import os
import csv
import json
import uuid
from flask import Flask, request, jsonify
from flask_cors import CORS
import clickhouse_driver
import jwt
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)
@app.route('/')
def index():
    return '''
    <!DOCTYPE html>
    <html>
      <head>
        <title>ClickHouse & Flat File Data Ingestion Tool</title>
        <style>
          body { font-family: Arial, sans-serif; margin: 0; padding: 20px; }
          h1 { color: #2c3e50; }
          .container { max-width: 800px; margin: 0 auto; }
          .form-group { margin-bottom: 15px; }
          label { display: block; margin-bottom: 5px; }
          input, select { width: 100%; padding: 8px; }
          button { background: #3498db; color: white; padding: 10px 15px; border: none; cursor: pointer; }
        </style>
      </head>
      <body>
        <div class="container">
          <h1>ClickHouse & Flat File Data Ingestion Tool</h1>
          
          <form id="clickhouse-form">
            <div class="form-group">
              <label>Source:</label>
              <select id="source">
                <option value="ClickHouse">ClickHouse</option>
                <option value="FlatFile">Flat File</option>
              </select>
            </div>
            
            <div id="clickhouse-config">
              <h2>ClickHouse Configuration</h2>
              <div class="form-group">
                <label for="host">Host:</label>
                <input type="text" id="host" value="clickhouse" />
              </div>
              <div class="form-group">
                <label for="port">Port:</label>
                <input type="text" id="port" value="9000" />
              </div>
              <div class="form-group">
                <label for="database">Database:</label>
                <input type="text" id="database" value="default" />
              </div>
              <div class="form-group">
                <label for="user">User:</label>
                <input type="text" id="user" value="default" />
              </div>
              <div class="form-group">
                <label for="jwt">JWT Token:</label>
                <input type="password" id="jwt" value="default" />
              </div>
              <button type="button" id="connect-btn">Connect to ClickHouse</button>
            </div>
            
            <div id="status" style="margin-top: 20px; padding: 10px; background: #f8f9fa;"></div>
            <div id="result" style="margin-top: 20px;"></div>
          </form>
        </div>
        
        <script>
          document.getElementById('connect-btn').addEventListener('click', function() {
            const config = {
              host: document.getElementById('host').value,
              port: document.getElementById('port').value,
              database: document.getElementById('database').value,
              user: document.getElementById('user').value,
              jwtToken: document.getElementById('jwt').value
            };
            
            document.getElementById('status').textContent = 'Connecting...';
            
            fetch('/api/clickhouse/connect', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(config)
            })
            .then(response => response.json())
            .then(data => {
              if (data.success) {
                document.getElementById('status').textContent = 'Connected successfully!';
                // Render with clickable table links
                let tableHtml = '<h3>Available Tables:</h3><ul>';
                data.tables.forEach(table => {
                  tableHtml += '<li><a href="#" class="table-link" data-table="' + table + '">' + table + '</a></li>';
                });
                tableHtml += '</ul>';
                document.getElementById('result').innerHTML = tableHtml;
                
                // Add click event listeners to table links
                document.querySelectorAll('.table-link').forEach(link => {
                  link.addEventListener('click', function(e) {
                    e.preventDefault();
                    const tableName = this.getAttribute('data-table');
                    selectTable(tableName, config);
                  });
                });
              } else {
                document.getElementById('status').textContent = 'Error: ' + data.error;
              }
            })
            .catch(error => {
              document.getElementById('status').textContent = 'Error: ' + error.message;
            });
          });
          
          function selectTable(tableName, config) {
            document.getElementById('status').textContent = 'Selecting table...';
            
            fetch('/api/clickhouse/select-table', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json'
              },
              body: JSON.stringify({
                ...config,
                table: tableName
              })
            })
            .then(response => response.json())
            .then(data => {
              if (data.success) {
                document.getElementById('status').textContent = 'Table selected: ' + tableName;
                
                // Show columns
                let columnsHtml = '<h3>Table Columns:</h3><ul>';
                data.columns.forEach(col => {
                  columnsHtml += '<li>' + col.name + ' (' + col.type + ')</li>';
                });
                columnsHtml += '</ul>';
                
                // Append to result
                document.getElementById('result').innerHTML += columnsHtml;
              } else {
                document.getElementById('status').textContent = 'Error selecting table: ' + data.error;
              }
            })
            .catch(error => {
              document.getElementById('status').textContent = 'Error: ' + error.message;
            });
          }
        </script>
      </body>
    </html>
    '''

@app.route('/api/clickhouse/connect', methods=['POST'])
def connect_clickhouse():
    """Connect to ClickHouse database and return list of tables"""
    try:
        data = request.json
        host = data.get('host')
        port = data.get('port')
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwtToken')
        
        # Create ClickHouse client with JWT token
        client = clickhouse_driver.Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=jwt_token
        )
        
        # Test connection by getting tables
        tables = client.execute("SHOW TABLES")
        tables = [table[0] for table in tables]
        
        return jsonify({
            'success': True,
            'tables': tables
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/clickhouse/select-table', methods=['POST'])
def select_clickhouse_table():
    """Select a specific ClickHouse table and get its columns"""
    try:
        data = request.json
        host = data.get('host')
        port = data.get('port')
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwtToken')
        table = data.get('table')
        
        # Create ClickHouse client
        client = clickhouse_driver.Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=jwt_token
        )
        
        # Get table schema
        columns = client.execute(f"DESCRIBE TABLE {table}")
        columns = [{'name': col[0], 'type': col[1]} for col in columns]
        
        return jsonify({
            'success': True,
            'columns': columns,
            'selectedTable': table
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/clickhouse/columns', methods=['POST'])
def get_clickhouse_columns():
    """Get columns for a specific ClickHouse table"""
    try:
        data = request.json
        host = data.get('host')
        port = data.get('port')
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwtToken')
        table = data.get('table')
        
        # Create ClickHouse client
        client = clickhouse_driver.Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=jwt_token
        )
        
        # Get table schema
        columns = client.execute(f"DESCRIBE TABLE {table}")
        columns = [{'name': col[0], 'type': col[1]} for col in columns]
        
        return jsonify({
            'success': True,
            'columns': columns
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/clickhouse/preview', methods=['POST'])
def preview_clickhouse_data():
    """Preview data from ClickHouse"""
    try:
        data = request.json
        host = data.get('host')
        port = data.get('port')
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwtToken')
        table = data.get('table')
        columns = data.get('columns', [])
        join_config = data.get('joinConfig', None)
        
        # Create ClickHouse client
        client = clickhouse_driver.Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=jwt_token
        )
        
        # Build query
        if not columns:
            columns_str = "*"
        else:
            columns_str = ", ".join(columns)
        
        if join_config and join_config.get('enabled'):
            # Handle JOIN query
            primary_table = table
            secondary_table = join_config.get('secondaryTable')
            join_type = join_config.get('joinType', 'INNER JOIN')
            join_columns = join_config.get('joinColumns', {})
            
            # Build join condition
            join_conditions = []
            for primary_col, secondary_col in join_columns.items():
                join_conditions.append(f"{primary_table}.{primary_col} = {secondary_table}.{secondary_col}")
            
            join_condition = " AND ".join(join_conditions)
            
            query = f"""
                SELECT {columns_str} 
                FROM {primary_table} 
                {join_type} {secondary_table} 
                ON {join_condition} 
                LIMIT 100
            """
        else:
            query = f"SELECT {columns_str} FROM {table} LIMIT 100"
        
        # Execute query
        data = client.execute(query, with_column_types=True)
        rows = data[0]
        column_names = [col[0] for col in data[1]]
        
        # Format results
        result = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(column_names):
                # Handle non-serializable types
                if isinstance(row[i], (datetime, timedelta)):
                    row_dict[col] = str(row[i])
                else:
                    row_dict[col] = row[i]
            result.append(row_dict)
        
        return jsonify({
            'success': True,
            'data': result,
            'columns': column_names
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/flatfile/read', methods=['POST'])
def read_flat_file():
    """Read and parse a flat file"""
    try:
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'error': 'No file provided'
            }), 400
        
        file = request.files['file']
        delimiter = request.form.get('delimiter', ',')
        
        # Create a temporary file path
        temp_dir = os.path.join(os.getcwd(), 'temp')
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            
        file_id = str(uuid.uuid4())
        file_path = os.path.join(temp_dir, f"{file_id}.csv")
        file.save(file_path)
        
        # Read the file to determine columns and preview data
        with open(file_path, 'r', newline='') as f:
            csv_reader = csv.reader(f, delimiter=delimiter)
            header = next(csv_reader)
            
            # Get first 100 rows for preview
            preview_rows = []
            for i, row in enumerate(csv_reader):
                if i >= 100:
                    break
                preview_rows.append(row)
        
        # Structure the preview data
        preview_data = []
        for row in preview_rows:
            row_dict = {}
            for i, col in enumerate(header):
                # Handle index out of range if some rows have fewer columns
                if i < len(row):
                    row_dict[col] = row[i]
                else:
                    row_dict[col] = None
            preview_data.append(row_dict)
        
        return jsonify({
            'success': True,
            'columns': header,
            'preview': preview_data,
            'fileId': file_id
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/clickhouse-to-flatfile', methods=['POST'])
def clickhouse_to_flatfile():
    """Export data from ClickHouse to a flat file"""
    try:
        data = request.json
        host = data.get('host')
        port = data.get('port')
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwtToken')
        table = data.get('table')
        columns = data.get('columns', [])
        delimiter = data.get('delimiter', ',')
        join_config = data.get('joinConfig', None)
        
        # Create ClickHouse client
        client = clickhouse_driver.Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=jwt_token
        )
        
        # Build query
        if not columns:
            columns_str = "*"
        else:
            columns_str = ", ".join(columns)
        
        if join_config and join_config.get('enabled'):
            # Handle JOIN query
            primary_table = table
            secondary_table = join_config.get('secondaryTable')
            join_type = join_config.get('joinType', 'INNER JOIN')
            join_columns = join_config.get('joinColumns', {})
            
            # Build join condition
            join_conditions = []
            for primary_col, secondary_col in join_columns.items():
                join_conditions.append(f"{primary_table}.{primary_col} = {secondary_table}.{secondary_col}")
            
            join_condition = " AND ".join(join_conditions)
            
            query = f"""
                SELECT {columns_str} 
                FROM {primary_table} 
                {join_type} {secondary_table} 
                ON {join_condition}
            """
        else:
            query = f"SELECT {columns_str} FROM {table}"
        
        # Execute query with column types
        result = client.execute(query, with_column_types=True)
        rows = result[0]
        column_names = [col[0] for col in result[1]]
        
        # Create output directory if it doesn't exist
        output_dir = os.path.join(os.getcwd(), 'output')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Generate file name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{table}_{timestamp}.csv"
        file_path = os.path.join(output_dir, file_name)
        
        # Write to CSV file
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter=delimiter)
            writer.writerow(column_names)
            for row in rows:
                # Handle non-serializable types
                sanitized_row = []
                for item in row:
                    if isinstance(item, (datetime, timedelta)):
                        sanitized_row.append(str(item))
                    else:
                        sanitized_row.append(item)
                writer.writerow(sanitized_row)
        
        return jsonify({
            'success': True,
            'recordCount': len(rows),
            'filePath': file_path,
            'fileName': file_name
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/flatfile-to-clickhouse', methods=['POST'])
def flatfile_to_clickhouse():
    """Import data from a flat file to ClickHouse"""
    try:
        data = request.json
        host = data.get('host')
        port = data.get('port')
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwtToken')
        target_table = data.get('targetTable')
        file_id = data.get('fileId')
        columns = data.get('columns', [])
        create_table = data.get('createTable', False)
        
        # Path to the temporary file
        temp_dir = os.path.join(os.getcwd(), 'temp')
        file_path = os.path.join(temp_dir, f"{file_id}.csv")
        
        if not os.path.exists(file_path):
            return jsonify({
                'success': False,
                'error': 'File not found'
            }), 400
        
        # Create ClickHouse client
        client = clickhouse_driver.Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=jwt_token
        )
        
        # Read file header
        with open(file_path, 'r', newline='') as f:
            reader = csv.reader(f)
            header = next(reader)
        
        # Filter columns if specified
        if columns:
            column_indices = [header.index(col) for col in columns if col in header]
            filtered_header = [header[i] for i in column_indices]
        else:
            filtered_header = header
            column_indices = list(range(len(header)))
        
        # Create table if needed
        if create_table:
            # Generate CREATE TABLE statement
            columns_def = ", ".join([f"`{col}` String" for col in filtered_header])
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ({columns_def}) ENGINE = MergeTree() ORDER BY tuple()"
            client.execute(create_table_sql)
        
        # Read data from file
        rows = []
        with open(file_path, 'r', newline='') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                if column_indices:
                    filtered_row = [row[i] for i in column_indices if i < len(row)]
                    # Handle case where row has fewer columns than expected
                    while len(filtered_row) < len(filtered_header):
                        filtered_row.append("")
                    rows.append(filtered_row)
                else:
                    rows.append(row)
        
        # Prepare columns string for INSERT
        columns_str = ", ".join([f"`{col}`" for col in filtered_header])
        
        # Insert data in batches
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            client.execute(
                f"INSERT INTO {target_table} ({columns_str}) VALUES", 
                batch
            )
            total_inserted += len(batch)
        
        return jsonify({
            'success': True,
            'recordCount': total_inserted,
            'targetTable': target_table
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/clickhouse/join-preview', methods=['POST'])
def preview_join():
    """Preview join between two ClickHouse tables"""
    try:
        data = request.json
        host = data.get('host')
        port = data.get('port')
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwtToken')
        primary_table = data.get('primaryTable')
        secondary_table = data.get('secondaryTable')
        join_type = data.get('joinType', 'INNER JOIN')
        join_columns = data.get('joinColumns', {})
        
        # Create ClickHouse client
        client = clickhouse_driver.Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=jwt_token
        )
        
        # Build join condition
        join_conditions = []
        for primary_col, secondary_col in join_columns.items():
            join_conditions.append(f"{primary_table}.{primary_col} = {secondary_table}.{secondary_col}")
        
        join_condition = " AND ".join(join_conditions)
        
        # Execute join query
        query = f"""
            SELECT * 
            FROM {primary_table} 
            {join_type} {secondary_table} 
            ON {join_condition} 
            LIMIT 100
        """
        
        data = client.execute(query, with_column_types=True)
        rows = data[0]
        column_names = [col[0] for col in data[1]]
        
        # Format results
        result = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(column_names):
                # Handle non-serializable types
                if isinstance(row[i], (datetime, timedelta)):
                    row_dict[col] = str(row[i])
                else:
                    row_dict[col] = row[i]
            result.append(row_dict)
        
        return jsonify({
            'success': True,
            'data': result,
            'columns': column_names
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)