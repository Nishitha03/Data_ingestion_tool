

// export default App;
import React, { useState, useEffect } from 'react';
import './App.css';

// API Base URL - change this to match your Flask backend
const API_BASE_URL = 'http://localhost:5000/api';

function App() {
  // State for source selection
  const [source, setSource] = useState('ClickHouse');
  const [target, setTarget] = useState('FlatFile');
  
  // ClickHouse connection config
  const [clickhouseConfig, setClickhouseConfig] = useState({
    host: 'clickhouse',
    port: '9000',
    database: 'default',
    user: 'default',
    jwtToken: 'default',
  });
  
  // Flat file config
  const [flatFileConfig, setFlatFileConfig] = useState({
    file: null,
    fileName: '',
    delimiter: ',',
    fileId: null
  });
  
  // UI states
  const [isConnected, setIsConnected] = useState(false);
  const [availableTables, setAvailableTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [status, setStatus] = useState('Ready');
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [previewData, setPreviewData] = useState([]);
  const [targetTable, setTargetTable] = useState('');
  const [createTable, setCreateTable] = useState(true);
  const [loading, setLoading] = useState(false);
  
  // Join feature
  const [useJoin, setUseJoin] = useState(false);
  const [secondaryTable, setSecondaryTable] = useState('');
  const [secondaryTableColumns, setSecondaryTableColumns] = useState([]);
  const [joinType, setJoinType] = useState('INNER JOIN');
  const [joinColumns, setJoinColumns] = useState({});
  
  // Reset states when source changes
  useEffect(() => {
    setIsConnected(false);
    setAvailableTables([]);
    setSelectedTable('');
    setColumns([]);
    setSelectedColumns([]);
    setResult(null);
    setError(null);
    setPreviewData([]);
    
    // Update target based on source
    if (source === 'ClickHouse') {
      setTarget('FlatFile');
    } else {
      setTarget('ClickHouse');
    }
  }, [source]);
  
  // Handle ClickHouse config changes
  const handleClickhouseConfigChange = (e) => {
    const { name, value } = e.target;
    setClickhouseConfig({
      ...clickhouseConfig,
      [name]: value
    });
  };
  
  // Handle flat file config changes
  const handleFlatFileChange = (e) => {
    if (e.target.name === 'file') {
      const file = e.target.files[0];
      setFlatFileConfig({
        ...flatFileConfig,
        file,
        fileName: file ? file.name : ''
      });
    } else {
      setFlatFileConfig({
        ...flatFileConfig,
        [e.target.name]: e.target.value
      });
    }
  };
  
  // Connect to ClickHouse
  const connectToClickhouse = async () => {
    try {
      setStatus('Connecting...');
      setError(null);
      setLoading(true);
      
      const response = await fetch(`${API_BASE_URL}/clickhouse/connect`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(clickhouseConfig)
      });
      
      const data = await response.json();
      
      if (data.success) {
        setIsConnected(true);
        setAvailableTables(data.tables);
        setStatus('Connected');
      } else {
        setError(data.error);
        setStatus('Error');
      }
    } catch (error) {
      setError(error.message);
      setStatus('Error');
    } finally {
      setLoading(false);
    }
  };
  
  // Upload and process flat file
  const processFlatFile = async () => {
    try {
      setStatus('Processing file...');
      setError(null);
      setLoading(true);
      
      if (!flatFileConfig.file) {
        setError('Please select a file');
        setStatus('Error');
        return;
      }
      
      const formData = new FormData();
      formData.append('file', flatFileConfig.file);
      formData.append('delimiter', flatFileConfig.delimiter);
      
      const response = await fetch(`${API_BASE_URL}/flatfile/read`, {
        method: 'POST',
        body: formData
      });
      
      const data = await response.json();
      
      if (data.success) {
        setIsConnected(true);
        setColumns(data.columns);
        setPreviewData(data.preview);
        setFlatFileConfig({
          ...flatFileConfig,
          fileId: data.fileId
        });
        setStatus('File processed');
      } else {
        setError(data.error);
        setStatus('Error');
      }
    } catch (error) {
      setError(error.message);
      setStatus('Error');
    } finally {
      setLoading(false);
    }
  };
  
  // Handle table selection
  const handleTableSelection = async (tableName) => {
    setSelectedTable(tableName);
    setStatus(`Table selected: ${tableName}`);
    
    // Automatically load columns when a table is selected
    try {
      setStatus('Fetching columns...');
      setError(null);
      setLoading(true);
      
      const response = await fetch(`${API_BASE_URL}/clickhouse/columns`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          ...clickhouseConfig,
          table: tableName
        })
      });
      
      const data = await response.json();
      
      if (data.success) {
        setColumns(data.columns.map(col => col.name));
        setSelectedColumns([]);
        setStatus('Columns fetched');
      } else {
        setError(data.error);
        setStatus('Error');
      }
    } catch (error) {
      setError(error.message);
      setStatus('Error');
    } finally {
      setLoading(false);
    }
  };
  
  // Get columns for a selected table
  const getTableColumns = async () => {
    try {
      setStatus('Fetching columns...');
      setError(null);
      setLoading(true);
      
      const response = await fetch(`${API_BASE_URL}/clickhouse/columns`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          ...clickhouseConfig,
          table: selectedTable
        })
      });
      
      const data = await response.json();
      
      if (data.success) {
        setColumns(data.columns.map(col => col.name));
        setStatus('Columns fetched');
      } else {
        setError(data.error);
        setStatus('Error');
      }
    } catch (error) {
      setError(error.message);
      setStatus('Error');
    } finally {
      setLoading(false);
    }
  };
  
  // Get columns for secondary table (join feature)
  const getSecondaryTableColumns = async () => {
    try {
      if (!secondaryTable) return;
      setLoading(true);
      
      const response = await fetch(`${API_BASE_URL}/clickhouse/columns`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          ...clickhouseConfig,
          table: secondaryTable
        })
      });
      
      const data = await response.json();
      
      if (data.success) {
        setSecondaryTableColumns(data.columns.map(col => col.name));
      } else {
        setError(data.error);
      }
    } catch (error) {
      setError(error.message);
    } finally {
      setLoading(false);
    }
  };
  
  // Preview data
  const fetchPreviewData = async () => {
    try {
      setStatus('Fetching preview...');
      setError(null);
      setLoading(true);
      
      if (source === 'ClickHouse') {
        const joinConfig = useJoin ? {
          enabled: true,
          secondaryTable,
          joinType,
          joinColumns
        } : null;
        
        const response = await fetch(`${API_BASE_URL}/clickhouse/preview`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            ...clickhouseConfig,
            table: selectedTable,
            columns: selectedColumns.length > 0 ? selectedColumns : [],
            joinConfig
          })
        });
        
        const data = await response.json();
        
        if (data.success) {
          setPreviewData(data.data);
          setStatus('Preview loaded');
        } else {
          setError(data.error);
          setStatus('Error');
        }
      }
      // Flat file preview is already shown on upload
    } catch (error) {
      setError(error.message);
      setStatus('Error');
    } finally {
      setLoading(false);
    }
  };
  
  // Handle join preview
  const previewJoin = async () => {
    try {
      setStatus('Fetching join preview...');
      setError(null);
      setLoading(true);
      
      const response = await fetch(`${API_BASE_URL}/clickhouse/join-preview`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          ...clickhouseConfig,
          primaryTable: selectedTable,
          secondaryTable,
          joinType,
          joinColumns
        })
      });
      
      const data = await response.json();
      
      if (data.success) {
        setPreviewData(data.data);
        setColumns(data.columns);
        setStatus('Join preview loaded');
      } else {
        setError(data.error);
        setStatus('Error');
      }
    } catch (error) {
      setError(error.message);
      setStatus('Error');
    } finally {
      setLoading(false);
    }
  };
  
  // Start ingestion process
  const startIngestion = async () => {
    try {
      setStatus('Ingesting data...');
      setError(null);
      setResult(null);
      setLoading(true);
      
      if (source === 'ClickHouse' && target === 'FlatFile') {
        // ClickHouse to Flat File
        const joinConfig = useJoin ? {
          enabled: true,
          secondaryTable,
          joinType,
          joinColumns
        } : null;
        
        const response = await fetch(`${API_BASE_URL}/clickhouse-to-flatfile`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            ...clickhouseConfig,
            table: selectedTable,
            columns: selectedColumns.length > 0 ? selectedColumns : [],
            delimiter: flatFileConfig.delimiter,
            joinConfig
          })
        });
        
        const data = await response.json();
        
        if (data.success) {
          setResult({
            recordCount: data.recordCount,
            filePath: data.filePath,
            fileName: data.fileName
          });
          setStatus('Ingestion completed');
        } else {
          setError(data.error);
          setStatus('Error');
        }
      } else if (source === 'FlatFile' && target === 'ClickHouse') {
        // Flat File to ClickHouse
        if (!targetTable) {
          setError('Please enter a target table name');
          setStatus('Error');
          return;
        }
        
        const response = await fetch(`${API_BASE_URL}/flatfile-to-clickhouse`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            ...clickhouseConfig,
            targetTable,
            fileId: flatFileConfig.fileId,
            columns: selectedColumns.length > 0 ? selectedColumns : [],
            createTable
          })
        });
        
        const data = await response.json();
        
        if (data.success) {
          setResult({
            recordCount: data.recordCount,
            targetTable: data.targetTable
          });
          setStatus('Ingestion completed');
        } else {
          setError(data.error);
          setStatus('Error');
        }
      }
    } catch (error) {
      setError(error.message);
      setStatus('Error');
    } finally {
      setLoading(false);
    }
  };
  
  // Handle column selection
  const handleColumnSelect = (column) => {
    if (selectedColumns.includes(column)) {
      setSelectedColumns(selectedColumns.filter(col => col !== column));
    } else {
      setSelectedColumns([...selectedColumns, column]);
    }
  };
  
  // Handle select all columns
  const handleSelectAllColumns = () => {
    if (selectedColumns.length === columns.length) {
      setSelectedColumns([]);
    } else {
      setSelectedColumns([...columns]);
    }
  };
  
  // Handle join column mapping
  const handleJoinColumnMapping = (primaryCol, secondaryCol) => {
    setJoinColumns({
      ...joinColumns,
      [primaryCol]: secondaryCol
    });
  };
  
  // Effect to load secondary table columns when selected
  useEffect(() => {
    if (secondaryTable) {
      getSecondaryTableColumns();
    }
  }, [secondaryTable]);

  return (
    <div className="app">
      <header>
        <h1>ClickHouse & Flat File Data Ingestion Tool</h1>
      </header>
      
      <div className="container">
        <div className="section">
          <h2>Configure Source and Target</h2>
          
          <div className="source-target-selector">
            <div className="form-group">
              <label>Source:</label>
              <select 
                value={source} 
                onChange={(e) => setSource(e.target.value)}
              >
                <option value="ClickHouse">ClickHouse</option>
                <option value="FlatFile">Flat File</option>
              </select>
            </div>
            
            <div className="form-group">
              <label>Target:</label>
              <div className="info">{target}</div>
            </div>
          </div>
          
          {/* ClickHouse Configuration */}
          {(source === 'ClickHouse' || target === 'ClickHouse') && (
            <div className="config-section">
              <h3>ClickHouse Configuration</h3>
              <div className="form-row">
                <div className="form-group">
                  <label htmlFor="host">Host:</label>
                  <input 
                    type="text" 
                    id="host" 
                    name="host"
                    value={clickhouseConfig.host}
                    onChange={handleClickhouseConfigChange}
                    placeholder="localhost"
                  />
                </div>
                
                <div className="form-group">
                  <label htmlFor="port">Port:</label>
                  <input 
                    type="text" 
                    id="port" 
                    name="port"
                    value={clickhouseConfig.port}
                    onChange={handleClickhouseConfigChange}
                    placeholder="9000"
                  />
                </div>
              </div>
              
              <div className="form-row">
                <div className="form-group">
                  <label htmlFor="database">Database:</label>
                  <input 
                    type="text" 
                    id="database" 
                    name="database"
                    value={clickhouseConfig.database}
                    onChange={handleClickhouseConfigChange}
                    placeholder="default"
                  />
                </div>
                
                <div className="form-group">
                  <label htmlFor="user">User:</label>
                  <input 
                    type="text" 
                    id="user" 
                    name="user"
                    value={clickhouseConfig.user}
                    onChange={handleClickhouseConfigChange}
                    placeholder="default"
                  />
                </div>
              </div>
              
              <div className="form-group">
                <label htmlFor="jwtToken">JWT Token:</label>
                <input 
                  type="password" 
                  id="jwtToken" 
                  name="jwtToken"
                  value={clickhouseConfig.jwtToken}
                  onChange={handleClickhouseConfigChange}
                  placeholder="JWT token"
                />
              </div>
              
              {source === 'ClickHouse' && (
                <button 
                  className="btn primary" 
                  onClick={connectToClickhouse}
                  disabled={!clickhouseConfig.host || !clickhouseConfig.port || loading}
                >
                  {loading ? 'Connecting...' : 'Connect to ClickHouse'}
                </button>
              )}
            </div>
          )}
          
          {/* Flat File Configuration */}
          {source === 'FlatFile' && (
            <div className="config-section">
              <h3>Flat File Configuration</h3>
              
              <div className="form-group">
                <label htmlFor="file">Select File:</label>
                <input 
                  type="file" 
                  id="file" 
                  name="file"
                  accept=".csv,.txt,.tsv"
                  onChange={handleFlatFileChange}
                />
              </div>
              
              <div className="form-group">
                <label htmlFor="delimiter">Delimiter:</label>
                <input 
                  type="text" 
                  id="delimiter" 
                  name="delimiter"
                  value={flatFileConfig.delimiter}
                  onChange={handleFlatFileChange}
                  placeholder=","
                />
              </div>
              
              <button 
                className="btn primary" 
                onClick={processFlatFile}
                disabled={!flatFileConfig.file || loading}
              >
                {loading ? 'Processing...' : 'Process File'}
              </button>
            </div>
          )}
          
          {/* Table Selection for ClickHouse source */}
          {source === 'ClickHouse' && isConnected && (
            <div className="config-section">
              <h3>Table Selection</h3>
              
              <div className="form-group">
                <label>Available Tables:</label>
                <div className="available-tables">
                  <ul className="available-tables-list">
                    {availableTables.map(table => (
                      <li 
                        key={table}
                        className={selectedTable === table ? 'selected' : ''}
                        onClick={() => handleTableSelection(table)}
                      >
                        {table}
                      </li>
                    ))}
                  </ul>
                </div>
                {selectedTable && (
                  <div className="selected-table-info">
                    <p>Selected table: <strong>{selectedTable}</strong></p>
                  </div>
                )}
              </div>
              
              {/* Join Feature */}
              {selectedTable && (
                <div className="join-section">
                  <div className="form-group">
                    <label className="checkbox-label">
                      <input 
                        type="checkbox"
                        checked={useJoin}
                        onChange={() => setUseJoin(!useJoin)}
                      />
                      Use Join with Another Table
                    </label>
                  </div>
                  
                  {useJoin && (
                    <>
                      <div className="form-group">
                        <label htmlFor="secondaryTable">Secondary Table:</label>
                        <select 
                          id="secondaryTable"
                          value={secondaryTable}
                          onChange={(e) => setSecondaryTable(e.target.value)}
                        >
                          <option value="">Select a table</option>
                          {availableTables
                            .filter(table => table !== selectedTable)
                            .map(table => (
                              <option key={table} value={table}>{table}</option>
                            ))}
                        </select>
                      </div>
                      
                      <div className="form-group">
                        <label htmlFor="joinType">Join Type:</label>
                        <select 
                          id="joinType"
                          value={joinType}
                          onChange={(e) => setJoinType(e.target.value)}
                        >
                          <option value="INNER JOIN">INNER JOIN</option>
                          <option value="LEFT JOIN">LEFT JOIN</option>
                          <option value="RIGHT JOIN">RIGHT JOIN</option>
                          <option value="FULL OUTER JOIN">FULL OUTER JOIN</option>
                        </select>
                      </div>
                      
                      {secondaryTable && columns.length > 0 && secondaryTableColumns.length > 0 && (
                        <div className="join-columns">
                          <h4>Join Column Mapping</h4>
                          <div className="join-column-list">
                            {columns.map(primaryCol => (
                              <div key={primaryCol} className="join-column-item">
                                <div className="column-name">{primaryCol}</div>
                                <span>=</span>
                                <select
                                  value={joinColumns[primaryCol] || ''}
                                  onChange={(e) => handleJoinColumnMapping(primaryCol, e.target.value)}
                                >
                                  <option value="">Select column</option>
                                  {secondaryTableColumns.map(secondaryCol => (
                                    <option key={secondaryCol} value={secondaryCol}>
                                      {secondaryCol}
                                    </option>
                                  ))}
                                </select>
                              </div>
                            ))}
                          </div>
                          
                          <button 
                            className="btn secondary" 
                            onClick={previewJoin}
                            disabled={Object.keys(joinColumns).length === 0 || loading}
                          >
                            {loading ? 'Loading Preview...' : 'Preview Join'}
                          </button>
                        </div>
                      )}
                    </>
                  )}
                </div>
              )}
            </div>
          )}
          
          {/* Target Configuration for Flat File to ClickHouse */}
          {source === 'FlatFile' && target === 'ClickHouse' && isConnected && (
            <div className="config-section">
              <h3>Target ClickHouse Configuration</h3>
              
              <div className="form-group">
                <label htmlFor="targetTable">Target Table Name:</label>
                <input 
                  type="text" 
                  id="targetTable" 
                  name="targetTable"
                  value={targetTable}
                  onChange={(e) => setTargetTable(e.target.value)}
                  placeholder="Enter target table name"
                />
              </div>
              
              <div className="form-group">
                <label className="checkbox-label">
                  <input 
                    type="checkbox"
                    checked={createTable}
                    onChange={() => setCreateTable(!createTable)}
                  />
                  Create table if not exists
                </label>
              </div>
            </div>
          )}
          
          {/* Column Selection */}
          {isConnected && columns.length > 0 && (
            <div className="config-section">
              <h3>Column Selection</h3>
              
              <div className="form-group">
                <label className="checkbox-label">
                  <input 
                    type="checkbox"
                    checked={selectedColumns.length === columns.length}
                    onChange={handleSelectAllColumns}
                  />
                  Select All Columns
                </label>
              </div>
              
              <div className="column-list">
                {columns.map(column => (
                  <div key={column} className="column-item">
                    <label className="checkbox-label">
                      <input 
                        type="checkbox"
                        checked={selectedColumns.includes(column)}
                        onChange={() => handleColumnSelect(column)}
                      />
                      {column}
                    </label>
                  </div>
                ))}
              </div>
              
              {source === 'ClickHouse' && !useJoin && (
                <button 
                  className="btn secondary" 
                  onClick={fetchPreviewData}
                  disabled={selectedColumns.length === 0 || loading}
                >
                  {loading ? 'Loading Preview...' : 'Preview Data'}
                </button>
              )}
            </div>
          )}
          
          {/* Preview Data Section */}
          {previewData.length > 0 && (
            <div className="config-section">
              <h3>Data Preview</h3>
              
              <div className="data-preview">
                <table>
                  <thead>
                    <tr>
                      {Object.keys(previewData[0]).map(key => (
                        <th key={key}>{key}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {previewData.slice(0, 10).map((row, index) => (
                      <tr key={index}>
                        {Object.values(row).map((value, i) => (
                          <td key={i}>{String(value !== null ? value : '')}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
                {previewData.length > 10 && (
                  <div className="preview-note">
                    Showing 10 of {previewData.length} records
                  </div>
                )}
              </div>
            </div>
          )}
          
          {/* Action Buttons */}
          {isConnected && (
            <div className="action-section">
              <button 
                className="btn primary" 
                onClick={startIngestion}
                disabled={
                  loading ||
                  (source === 'ClickHouse' && (!selectedTable || selectedColumns.length === 0)) ||
                  (source === 'FlatFile' && target === 'ClickHouse' && !targetTable)
                }
              >
                {loading ? 'Processing...' : 'Start Ingestion'}
              </button>
            </div>
          )}
          
          {/* Status and Results */}
          <div className="status-section">
            <div className="status">
              <strong>Status:</strong> <span className={`status-text ${status === 'Error' ? 'status-error' : status === 'Connected' || status.includes('completed') ? 'status-success' : 'status-info'}`}>{status}</span>
              {loading && <div className="loader"></div>}
            </div>
            
            {error && (
              <div className="error">
                <strong>Error:</strong> {error}
              </div>
            )}
            
            {result && (
              <div className="result">
                <h3>Ingestion Results</h3>
                <div className="result-info">
                  <strong>Records Processed:</strong> {result.recordCount}
                </div>
                
                {result.fileName && (
                  <div className="result-info">
                    <strong>Output File:</strong> {result.fileName}
                  </div>
                )}
                
                {result.targetTable && (
                  <div className="result-info">
                    <strong>Target Table:</strong> {result.targetTable}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;