const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Kết nối PostgreSQL Container
const pool = new Pool({
  host: process.env.DB_HOST || 'postgres',
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'frigate_db',
  user: process.env.DB_USER || 'admin',
  password: process.env.DB_PASSWORD || 'password123',
});

// -------------------------------------------------------------
// Endpoint 1: Health Check
// -------------------------------------------------------------
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    service: 'Frigate Backend API', 
    timestamp: new Date() 
  });
});

// -------------------------------------------------------------
// Endpoint 2: Gọi Stored Procedure (CALL procedure_name($1, $2...))
// Dùng cho các Procedure tạo/sửa/xóa/xử lý logic không trả về dữ liệu bảng
// Body JSON: { "procedureName": "sp_log_event", "params": ["Camera_01", "Motion Detected"] }
// -------------------------------------------------------------
app.post('/api/call-procedure', async (req, res) => {
  const { procedureName, params = [] } = req.body;

  if (!procedureName) {
    return res.status(400).json({ error: 'Missing "procedureName" in request body' });
  }

  try {
    const paramPlaceholders = params.map((_, i) => `$${i + 1}`).join(', ');
    const queryText = `CALL ${procedureName}(${paramPlaceholders});`;

    await pool.query(queryText, params);
    
    res.json({
      success: true,
      message: `Đã thực thi Stored Procedure '${procedureName}' thành công.`,
      executed_at: new Date()
    });
  } catch (err) {
    console.error(`[API ERROR] Stored Procedure '${procedureName}':`, err.message);
    res.status(500).json({ error: err.message });
  }
});

// -------------------------------------------------------------
// Endpoint 3: Gọi Database Function trả về dữ liệu (SELECT * FROM function_name($1, $2...))
// Dùng cho các hàm trả về bảng dữ liệu (Table-Valued Function / Stored Procedure trả kết quả)
// Body JSON: { "functionName": "fn_get_events_by_camera", "params": ["Camera_01"] }
// -------------------------------------------------------------
app.post('/api/call-function', async (req, res) => {
  const { functionName, params = [] } = req.body;

  if (!functionName) {
    return res.status(400).json({ error: 'Missing "functionName" in request body' });
  }

  try {
    const paramPlaceholders = params.map((_, i) => `$${i + 1}`).join(', ');
    const queryText = `SELECT * FROM ${functionName}(${paramPlaceholders});`;

    const result = await pool.query(queryText, params);
    
    res.json({
      success: true,
      total_rows: result.rowCount,
      data: result.rows
    });
  } catch (err) {
    console.error(`[API ERROR] Function '${functionName}':`, err.message);
    res.status(500).json({ error: err.message });
  }
});

// -------------------------------------------------------------
// Endpoint 4: Thực thi truy vấn Raw SQL tổng quát (Parametrized Query)
// Dùng cho Web gọi truy vấn SELECT/UPDATE/INSERT linh hoạt
// Body JSON: { "sql": "SELECT * FROM mqtt_events WHERE topic = $1 LIMIT $2", "params": ["frigate/events", 10] }
// -------------------------------------------------------------
app.post('/api/query', async (req, res) => {
  const { sql, params = [] } = req.body;

  if (!sql) {
    return res.status(400).json({ error: 'Missing "sql" query in request body' });
  }

  try {
    const result = await pool.query(sql, params);
    res.json({
      success: true,
      total_rows: result.rowCount,
      data: result.rows
    });
  } catch (err) {
    console.error(`[API ERROR] Query Error:`, err.message);
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`=== Backend API Server listening on port ${PORT} ===`);
});
