# doc-snowflake

# Snowflake SQL Reference Card

## Table of Contents
- [Data Definition Language (DDL)](#data-definition-language-ddl)
- [Data Manipulation Language (DML)](#data-manipulation-language-dml)
- [Functions and stored procedures](#functions-and-stored-procedures)
- [Query Optimization](#query-optimization)
- [Snowflake-Specific Features](#snowflake-specific-features)
- [Security Best Practices](#security-best-practices)
- [Performance Tips](#performance-tips)
- [Cost Optimization](#cost-optimization)

## Data Definition Language (DDL)

### Basic DDL Commands

#### Database Operations
```sql
-- Create database
CREATE DATABASE database_name;

-- Create database with options
CREATE OR REPLACE DATABASE database_name
    COMMENT = 'Database description'
    DATA_RETENTION_TIME_IN_DAYS = 7;

-- Alter database
ALTER DATABASE database_name
    SET DATA_RETENTION_TIME_IN_DAYS = 14;

-- Drop database
DROP DATABASE IF EXISTS database_name;

-- Use database
USE DATABASE database_name;
```

#### Schema Operations
```sql
-- Create schema
CREATE SCHEMA schema_name;

-- Create schema with options
CREATE OR REPLACE SCHEMA schema_name
    WITH MANAGED ACCESS
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Schema description';

-- Alter schema
ALTER SCHEMA schema_name
    SET DATA_RETENTION_TIME_IN_DAYS = 14;

-- Drop schema
DROP SCHEMA IF EXISTS schema_name;

-- Use schema
USE SCHEMA schema_name;
```

#### Table Operations
```sql
-- Create table
CREATE TABLE table_name (
    column1 VARCHAR(50),
    column2 NUMBER(10,2),
    column3 DATE
);

-- Create table with options
CREATE OR REPLACE TABLE table_name (
    id INTEGER,
    name VARCHAR(100),
    created_at TIMESTAMP_NTZ
)
COMMENT = 'Table description'
CLUSTER BY (id)
DATA_RETENTION_TIME_IN_DAYS = 7;

-- Alter table
ALTER TABLE table_name
    ADD COLUMN new_column VARCHAR(50);

-- Drop table
DROP TABLE IF EXISTS table_name;

-- Truncate table
TRUNCATE TABLE table_name;
```

### Advanced DDL Commands

#### Warehouse Operations
```sql
-- Create warehouse
CREATE WAREHOUSE warehouse_name
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

-- Alter warehouse
ALTER WAREHOUSE warehouse_name
    SET WAREHOUSE_SIZE = 'SMALL';

-- Drop warehouse
DROP WAREHOUSE IF EXISTS warehouse_name;
```

#### Stage Operations
```sql
-- Create internal stage
CREATE STAGE my_stage;

-- Create external stage
CREATE STAGE my_ext_stage
    URL = 's3://bucket/path/'
    CREDENTIALS = (AWS_KEY_ID = 'id' AWS_SECRET_KEY = 'key');

-- Alter stage
ALTER STAGE my_stage
    SET COMMENT = 'Updated stage';

-- Drop stage
DROP STAGE IF EXISTS my_stage;
```

#### File Format Operations
```sql
-- Create file format
CREATE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null');

-- Alter file format
ALTER FILE FORMAT my_csv_format
    SET FIELD_DELIMITER = '|';

-- Drop file format
DROP FILE FORMAT IF EXISTS my_csv_format;
```

#### External Table Operations
```sql
-- Create external table
CREATE EXTERNAL TABLE ext_table (
    col1 STRING,
    col2 NUMBER
)
LOCATION = @my_ext_stage/path/
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
PATTERN = '.*\.csv';

-- Alter external table
ALTER EXTERNAL TABLE ext_table
    REFRESH;

-- Drop external table
DROP EXTERNAL TABLE IF EXISTS ext_table;
```

### Access Control

#### Role and User Management
```sql
-- Create role
CREATE ROLE analyst_role;

-- Grant privileges to role
GRANT USAGE ON DATABASE db_name TO ROLE analyst_role;
GRANT USAGE ON SCHEMA db_name.schema_name TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA db_name.schema_name TO ROLE analyst_role;

-- Create user
CREATE USER username
    PASSWORD = 'securepassword'
    DEFAULT_ROLE = analyst_role;

-- Grant role to user
GRANT ROLE analyst_role TO USER username;

-- Revoke privileges
REVOKE SELECT ON TABLE table_name FROM ROLE analyst_role;

-- Drop role or user
DROP ROLE IF EXISTS analyst_role;
DROP USER IF EXISTS username;
```

#### Resource Monitors
```sql
-- Create resource monitor
CREATE RESOURCE MONITOR monitor_name
    WITH CREDIT_QUOTA = 1000
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- Alter resource monitor
ALTER RESOURCE MONITOR monitor_name
    SET CREDIT_QUOTA = 2000;

-- Drop resource monitor
DROP RESOURCE MONITOR IF EXISTS monitor_name;
```

### Advanced Table Features

#### Time Travel
```sql
-- Query data as of a specific time
SELECT * FROM table_name AT(TIMESTAMP => 'YYYY-MM-DD HH:MI:SS.FF');

-- Query data before a specific statement
SELECT * FROM table_name BEFORE(STATEMENT => 'statement_id');

-- Undrop table (within retention period)
UNDROP TABLE dropped_table_name;
```

#### Table Cloning
```sql
-- Create a zero-copy clone
CREATE TABLE clone_table CLONE source_table;

-- Clone with time travel
CREATE TABLE clone_table CLONE source_table AT(OFFSET => -60*60);
```

#### Views and Materialized Views
```sql
-- Create view
CREATE VIEW view_name AS
    SELECT col1, col2 FROM table_name WHERE condition;

-- Create secure view
CREATE SECURE VIEW secure_view_name AS
    SELECT * FROM table_name;

-- Create materialized view
CREATE MATERIALIZED VIEW mv_name AS
    SELECT col1, SUM(col2) AS total
    FROM table_name
    GROUP BY col1;

-- Refresh materialized view
ALTER MATERIALIZED VIEW mv_name REFRESH;

-- Drop view
DROP VIEW IF EXISTS view_name;
DROP MATERIALIZED VIEW IF EXISTS mv_name;
```

#### Temporary and Transient Objects
```sql
-- Create temporary table (session duration)
CREATE TEMPORARY TABLE temp_table (id INTEGER, data VARCHAR);

-- Create transient table (no fail-safe period)
CREATE TRANSIENT TABLE transient_table (id INTEGER, data VARCHAR);
```

### Data Types and Constraints

#### Common Data Types
```sql
-- Numeric
NUMBER(precision, scale)
INTEGER / INT
FLOAT
DECIMAL(precision, scale)

-- String
VARCHAR(length) / STRING
CHAR(length)
TEXT

-- Date/Time
DATE
TIME
TIMESTAMP / TIMESTAMP_NTZ (no timezone)
TIMESTAMP_LTZ (local timezone)
TIMESTAMP_TZ (with timezone)

-- Semi-structured
VARIANT
OBJECT
ARRAY
```

#### Constraints
```sql
-- Create table with constraints
CREATE TABLE constrained_table (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    age INTEGER CHECK (age >= 18),
    dept_id INTEGER REFERENCES departments(id),
    CONSTRAINT valid_record CHECK (id > 0 AND name IS NOT NULL)
);

-- Add constraint to existing table
ALTER TABLE table_name
    ADD CONSTRAINT pk_constraint PRIMARY KEY (id);
```

### Advanced DDL Features

#### Sequences
```sql
-- Create sequence
CREATE SEQUENCE my_sequence
    START = 1
    INCREMENT = 1
    COMMENT = 'Auto-incrementing ID generator';

-- Use sequence
INSERT INTO table_name (id, name) VALUES (my_sequence.NEXTVAL, 'Test');

-- Alter sequence
ALTER SEQUENCE my_sequence SET INCREMENT = 10;

-- Drop sequence
DROP SEQUENCE IF EXISTS my_sequence;
```

#### Tasks and Streams
```sql
-- Create stream for CDC
CREATE STREAM stream_name ON TABLE source_table;

-- Create task
CREATE TASK task_name
    WAREHOUSE = warehouse_name
    SCHEDULE = 'USING CRON 0 */12 * * * UTC'
    AS
    INSERT INTO target_table
    SELECT * FROM stream_name;

-- Alter task
ALTER TASK task_name SUSPEND;
ALTER TASK task_name RESUME;

-- Drop task/stream
DROP TASK IF EXISTS task_name;
DROP STREAM IF EXISTS stream_name;
```

#### Stored Procedures
```sql
-- Create JavaScript stored procedure
CREATE OR REPLACE PROCEDURE update_stats()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  AS
  $$
    var sql_command = "UPDATE stats SET last_updated = CURRENT_TIMESTAMP()";
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var result = stmt.execute();
    return "Stats updated successfully";
  $$;

-- Execute stored procedure
CALL update_stats();

-- Drop stored procedure
DROP PROCEDURE IF EXISTS update_stats();
```

#### Pipes for Data Loading
```sql
-- Create pipe for auto-ingest
CREATE PIPE my_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO my_table
    FROM @my_stage/data/
    FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

-- Show pipes
SHOW PIPES;

-- Drop pipe
DROP PIPE IF EXISTS my_pipe;
```

### Best Practices

#### Naming Conventions
- Use snake_case for all object names (e.g., `customer_orders` not `CustomerOrders`)
- Prefix temporary tables with `tmp_`
- Prefix views with `v_` and materialized views with `mv_`
- Be consistent with plural/singular for table names

#### Performance Optimization
- Use appropriate clustering keys for large tables
- Consider data lifecycle and set appropriate retention periods
- Use the right data types (e.g., VARCHAR(16777216) consumes more space than needed)
- Create materialized views for frequently accessed aggregated data
- Use transient tables for intermediate processing to reduce costs

#### Security Best Practices
- Follow principle of least privilege with role-based access control
- Use secure views to restrict sensitive data
- Set resource monitors to control costs
- Consider row-level security for multi-tenant applications
- Use column-level security for sensitive fields

#### Development Workflow
- Use schema evolution patterns instead of dropping/recreating objects
- Leverage cloning for testing and development environments
- Script all DDL changes for version control and CI/CD pipelines
- Document objects with COMMENT attributes
- Use tags for object classification and governance

#### Cost Management
- Set auto-suspend and auto-resume on warehouses
- Use resource monitors to cap spending
- Consider data storage costs when designing retention policies
- Leverage transient tables when fail-safe protection isn't needed
- Size warehouses appropriately for workloads

#### Database Objects


#### Create Database
```sql
CREATE DATABASE my_database;
```

#### Create Schema
```sql
CREATE SCHEMA my_database.my_schema;
```

#### Create Table
```sql
CREATE TABLE my_database.my_schema.my_table (
  id NUMBER PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

#### Create Table with Clustering Key
```sql
CREATE TABLE my_database.my_schema.events (
  event_id NUMBER,
  event_date DATE,
  user_id NUMBER,
  event_type VARCHAR
)
CLUSTER BY (event_date, user_id);
```

#### Create View
```sql
CREATE OR REPLACE VIEW my_database.my_schema.active_users AS
  SELECT user_id, name, last_login_date
  FROM my_database.my_schema.users
  WHERE status = 'active';
```

#### Create Materialized View
```sql
CREATE OR REPLACE MATERIALIZED VIEW my_database.my_schema.daily_sales AS
  SELECT 
    date_trunc('day', sale_timestamp) AS sale_date,
    SUM(amount) AS total_amount,
    COUNT(*) AS transaction_count
  FROM my_database.my_schema.sales
  GROUP BY 1;
```

#### Create External Table
```sql
CREATE EXTERNAL TABLE my_database.my_schema.ext_customer_data (
  customer_id NUMBER AS (VALUE:c_id::NUMBER),
  customer_name VARCHAR AS (VALUE:c_name::VARCHAR),
  customer_email VARCHAR AS (VALUE:c_email::VARCHAR)
)
WITH LOCATION = @my_database.my_schema.my_stage/customer_data/
FILE_FORMAT = (TYPE = 'JSON');
```




## Data Manipulation Language (DML)

### Snowflake Data Manipulation Language (DML) Reference Guide

#### Basic DML Operations

#### INSERT
```sql
-- Basic insert
INSERT INTO table_name (column1, column2, ...)
VALUES (value1, value2, ...);

-- Multi-row insert
INSERT INTO table_name (column1, column2)
VALUES 
  (value1, value2),
  (value3, value4);

-- Insert from query results
INSERT INTO target_table (column1, column2)
SELECT column1, column2 
FROM source_table
WHERE condition;

-- Insert all columns
INSERT INTO table_name
VALUES (value1, value2, ...);
```

#### UPDATE
```sql
-- Basic update
UPDATE table_name
SET column1 = value1, column2 = value2
WHERE condition;

-- Update with subquery
UPDATE table_name
SET column1 = (SELECT column_x FROM another_table WHERE condition)
WHERE condition;

-- Update with JOIN logic
UPDATE table_name t1
SET t1.column1 = t2.column2
FROM another_table t2
WHERE t1.id = t2.id;
```

#### DELETE
```sql
-- Basic delete
DELETE FROM table_name
WHERE condition;

-- Delete with subquery
DELETE FROM table_name
WHERE column_name IN (SELECT column_name FROM another_table WHERE condition);

-- Delete all rows
DELETE FROM table_name;

-- Truncate (faster than DELETE for removing all rows)
TRUNCATE TABLE table_name;
```

#### MERGE
```sql
-- Comprehensive MERGE for upsert operations
MERGE INTO target_table t
USING source_table s
  ON t.key = s.key
WHEN MATCHED THEN
  UPDATE SET t.column1 = s.column1, t.column2 = s.column2
WHEN NOT MATCHED THEN
  INSERT (column1, column2) VALUES (s.column1, s.column2);

-- MERGE with conditions
MERGE INTO target_table t
USING source_table s
  ON t.key = s.key
WHEN MATCHED AND s.column1 > 100 THEN
  UPDATE SET t.column1 = s.column1
WHEN NOT MATCHED AND s.column2 != 'IGNORE' THEN
  INSERT (column1, column2) VALUES (s.column1, s.column2);

-- MERGE with DELETE condition
MERGE INTO target_table t
USING source_table s
  ON t.key = s.key
WHEN MATCHED AND s.status = 'INACTIVE' THEN
  DELETE
WHEN MATCHED THEN
  UPDATE SET t.column1 = s.column1
WHEN NOT MATCHED THEN
  INSERT (column1, column2) VALUES (s.column1, s.column2);
```

#### Advanced DML Features

#### Multi-table INSERT
```sql
-- Insert into multiple tables from a single SELECT
INSERT ALL
  INTO table1 (column1, column2) VALUES (val1, val2)
  INTO table2 (columnA, columnB) VALUES (val3, val4)
SELECT 1 FROM dual;
```

#### INSERT with OVERWRITE
```sql
-- Replace all data in a table (with TRUNCATE capability)
INSERT OVERWRITE INTO table_name
SELECT column1, column2
FROM source_table;
```

#### UPDATE with JOIN
```sql
-- More complex UPDATE with JOIN pattern
UPDATE table1 t1
SET column1 = t2.column1
FROM table2 t2
JOIN table3 t3 ON t2.id = t3.id
WHERE t1.id = t2.id AND t3.status = 'ACTIVE';
```

#### Conditional DML
```sql
-- INSERT with conditional logic using CASE
INSERT INTO target_table (column1, column2)
SELECT 
  id,
  CASE 
    WHEN amount > 1000 THEN 'High'
    WHEN amount > 500 THEN 'Medium'
    ELSE 'Low'
  END as tier
FROM source_table;

-- UPDATE with conditional logic
UPDATE table_name
SET 
  status = CASE 
    WHEN days_overdue > 90 THEN 'Critical'
    WHEN days_overdue > 30 THEN 'Warning'
    ELSE status
  END
WHERE account_type = 'Enterprise';
```

#### Copy Variants (JSON) Within a Table
```sql
-- Update by copying an entire JSON object
UPDATE table_name
SET json_column = other_json_column
WHERE condition;

-- Update specific paths in JSON
UPDATE table_name
SET json_column = OBJECT_INSERT(json_column, 'new_key', 'new_value'::VARIANT)
WHERE condition;
```

#### Working with Arrays
```sql
-- Update Array Elements
UPDATE table_name
SET array_column = ARRAY_APPEND(array_column, 'new_value')
WHERE condition;

-- Remove Array Elements
UPDATE table_name
SET array_column = ARRAY_REMOVE(array_column, 'value_to_remove')
WHERE condition;
```

#### Performance Optimizations

#### MERGE Best Practices
- Use the `ON` clause to join on primary keys or unique indexes
- Order source data to match target table clustering keys
- Use micro-partitions efficiently by batching large operations

```sql
-- Efficient MERGE with ordered data
MERGE INTO target_table t
USING (
  SELECT * FROM source_table
  ORDER BY clustering_key
) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE...
```

#### Bulk Operations
```sql
-- Use COPY command for bulk loading
COPY INTO table_name
FROM @stage_name/path
FILE_FORMAT = (TYPE = 'CSV');

-- Bulk INSERT from staged files
INSERT INTO table_name (col1, col2)
SELECT $1, $2
FROM @stage_name/data.csv;
```

#### Micro-partition Optimization
```sql
-- Avoid small, frequent UPDATEs (consolidate updates)
UPDATE table_name
SET 
  column1 = CASE WHEN condition1 THEN value1 ELSE column1 END,
  column2 = CASE WHEN condition2 THEN value2 ELSE column2 END
WHERE condition1 OR condition2;
```

#### Temporary Tables and Transactions

#### Creating and Using Temp Tables
```sql
-- Create a temporary table
CREATE TEMPORARY TABLE temp_results AS
SELECT * FROM main_table WHERE condition;

-- Perform operations on temp table
UPDATE temp_results SET column1 = 'Modified' WHERE condition;

-- Use in final operation
INSERT INTO final_table
SELECT * FROM temp_results;
```

#### Transaction Control
```sql
-- Begin a transaction
BEGIN;

-- Perform multiple DML operations
INSERT INTO table1 VALUES (1, 'A');
UPDATE table2 SET status = 'PROCESSED' WHERE id = 1;

-- Commit the transaction
COMMIT;

-- Or rollback if needed
-- ROLLBACK;
```

#### Advanced Techniques

#### Multi-Statement Transaction
```sql
BEGIN;
  -- Create a temporary stage to hold intermediate results
  CREATE TEMPORARY TABLE tmp_changes AS 
    SELECT id, new_value 
    FROM source_table 
    WHERE condition;
  
  -- Update existing records
  UPDATE target_table t
  SET value = tmp.new_value
  FROM tmp_changes tmp
  WHERE t.id = tmp.id;
  
  -- Insert records that don't exist
  INSERT INTO target_table (id, value)
  SELECT tmp.id, tmp.new_value
  FROM tmp_changes tmp
  LEFT JOIN target_table t ON tmp.id = t.id
  WHERE t.id IS NULL;
  
  -- Log the operation
  INSERT INTO audit_table (operation_date, record_count)
  SELECT CURRENT_TIMESTAMP(), COUNT(*) FROM tmp_changes;
COMMIT;
```

#### Dynamic DML with JavaScript Procedures
```sql
-- Create a stored procedure for dynamic DML
CREATE OR REPLACE PROCEDURE dynamic_update(table_name STRING, set_column STRING, set_value STRING, where_clause STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  var sql_command = "UPDATE " + TABLE_NAME + " SET " + SET_COLUMN + " = '" + SET_VALUE + "'";
  if (WHERE_CLAUSE) {
    sql_command += " WHERE " + WHERE_CLAUSE;
  }
  
  try {
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var result = stmt.execute();
    return "Updated successfully.";
  } catch (err) {
    return "Error: " + err;
  }
$$;

-- Call the procedure
CALL dynamic_update('CUSTOMERS', 'STATUS', 'ACTIVE', 'REGION = \'WEST\'');
```

#### Error Handling in DML
```sql
-- Set error handling mode
ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = TRUE;
ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_UPDATE = TRUE;

-- Implement error handling transaction
BEGIN
  INSERT INTO log_table (operation, status) VALUES ('start_update', 'running');
  
  UPDATE target_table SET column1 = 'new_value' WHERE condition;
  
  -- Check row count
  LET affected_rows := SQLROWCOUNT;
  
  -- Log success
  INSERT INTO log_table (operation, status, affected_rows) 
  VALUES ('update_complete', 'success', :affected_rows);
  
  COMMIT;
EXCEPTION
  WHEN OTHER THEN
    -- Log failure
    INSERT INTO log_table (operation, status, error_message) 
    VALUES ('update_failed', 'error', SQLERRM);
    
    ROLLBACK;
END;
```

#### Best Practices

#### Data Quality
- Use constraints like `NOT NULL`, `UNIQUE`, and `CHECK` to enforce data quality
- Validate data before large DML operations
- Consider using staging tables to validate data before final INSERT/UPDATE

#### Performance
- Batch operations when possible (prefer one large operation over many small ones)
- Use clustering keys effectively for large tables
- Consider time-travel implications of large updates (copying data creates new versions)
- Leverage task system for recurring DML operations

#### Maintenance
- Monitor table sizes before and after large DML operations
- Use `SYSTEM$ESTIMATE_QUERY_COST` function to estimate large DML operation costs
- Consider `TRUNCATE` over `DELETE` when removing all rows (doesn't preserve table history)
- Schedule regular table maintenance via tasks to mitigate fragmentation

#### Time Travel and Fail-safe
- Create backup points before destructive operations:
```sql
CREATE OR REPLACE TABLE table_name_backup CLONE table_name;
```
- Use time travel for point-in-time recovery:
```sql
-- Revert to state before error
CREATE OR REPLACE TABLE table_name CLONE table_name BEFORE (TIMESTAMP => 'timestamp');
```

#### Common Patterns

#### Slowly Changing Dimension (SCD) Type 2
```sql
-- Implement SCD Type 2 with MERGE
MERGE INTO dim_customer t
USING (
  SELECT s.customer_id, s.name, s.email, s.address
  FROM source_customer s
) s
ON t.customer_id = s.customer_id AND t.is_current = TRUE
WHEN MATCHED AND (
  t.name != s.name OR
  t.email != s.email OR
  t.address != s.address
) THEN
  UPDATE SET is_current = FALSE, valid_to = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, address, is_current, valid_from, valid_to)
  VALUES (s.customer_id, s.name, s.email, s.address, TRUE, CURRENT_TIMESTAMP(), NULL);

-- Insert new records for changed dimensions
INSERT INTO dim_customer (
  customer_id, name, email, address, is_current, valid_from, valid_to
)
SELECT 
  s.customer_id, s.name, s.email, s.address, TRUE, CURRENT_TIMESTAMP(), NULL
FROM source_customer s
JOIN dim_customer t 
  ON t.customer_id = s.customer_id 
  AND t.is_current = FALSE
  AND t.valid_to = CURRENT_TIMESTAMP();
```

#### Data Deduplication
```sql
-- Identify duplicates
CREATE TEMPORARY TABLE duplicate_ids AS
SELECT id
FROM (
  SELECT 
    id,
    ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY last_modified_timestamp DESC) as rn
  FROM table_name
)
WHERE rn > 1;

-- Remove duplicates
DELETE FROM table_name
WHERE id IN (SELECT id FROM duplicate_ids);
```

#### Incremental Load Pattern
```sql
-- Identify new & changed records
MERGE INTO target_table t
USING (
  SELECT s.*
  FROM source_table s
  LEFT JOIN target_table t ON s.id = t.id
  WHERE t.id IS NULL                     -- new records
     OR s.last_modified > t.last_modified -- changed records
) s
ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET 
    column1 = s.column1,
    column2 = s.column2,
    last_modified = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (id, column1, column2, last_modified)
  VALUES (s.id, s.column1, s.column2, CURRENT_TIMESTAMP());
```

#### Troubleshooting

#### Common Issues
- Transaction Conflicts: When multiple sessions update the same rows
- Suboptimal performance on large tables: Check clustering keys and consider batching
- Unexpected results in complex MERGE: Validate source data and conditions
- Task failures: Review logs and implement proper error handling

#### Diagnostic Queries
```sql
-- Find blocked transactions
SELECT * FROM TABLE(INFORMATION_SCHEMA.LOCK_DELAYS());

-- Review query history for slow DML
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TYPE IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE')
AND EXECUTION_TIME > 60000  -- Over 1 minute
ORDER BY START_TIME DESC;

-- Check table statistics
SELECT * FROM TABLE(INFORMATION_SCHEMA.TABLE_STORAGE_METRICS('database.schema.table'));
```


### Basic Queries

#### Select Data
```sql
SELECT * FROM my_database.my_schema.customers
WHERE region = 'Northeast'
ORDER BY signup_date DESC
LIMIT 100;
```

#### Insert Data
```sql
INSERT INTO my_database.my_schema.customers (customer_id, name, email)
VALUES 
  (1001, 'Alice Smith', 'alice@example.com'),
  (1002, 'Bob Johnson', 'bob@example.com');
```

#### Update Data
```sql
UPDATE my_database.my_schema.customers
SET status = 'inactive', last_updated = CURRENT_TIMESTAMP()
WHERE last_login_date < DATEADD('day', -90, CURRENT_DATE());
```

#### Delete Data
```sql
DELETE FROM my_database.my_schema.customers
WHERE status = 'deleted' 
  AND last_updated < DATEADD('month', -12, CURRENT_DATE());
```

### Advanced Queries

#### CTE (Common Table Expression)
```sql
WITH monthly_revenue AS (
  SELECT 
    date_trunc('month', order_date) AS month,
    customer_id,
    SUM(order_amount) AS revenue
  FROM my_database.my_schema.orders
  WHERE order_date >= DATEADD('year', -1, CURRENT_DATE())
  GROUP BY 1, 2
)
SELECT 
  month,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(revenue) AS total_revenue,
  AVG(revenue) AS avg_revenue_per_customer
FROM monthly_revenue
GROUP BY 1
ORDER BY 1;
```

#### Window Functions
```sql
SELECT 
  order_id,
  customer_id,
  order_amount,
  order_date,
  SUM(order_amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS customer_running_total,
  AVG(order_amount) OVER (PARTITION BY customer_id) AS customer_avg_order,
  RANK() OVER (PARTITION BY customer_id ORDER BY order_amount DESC) AS order_rank
FROM my_database.my_schema.orders;
```

#### Pivot Tables
```sql
SELECT * FROM (
  SELECT product_category, order_month, sales_amount
  FROM my_database.my_schema.sales
) 
PIVOT (
  SUM(sales_amount) 
  FOR order_month IN ('2024-01', '2024-02', '2024-03', '2024-04')
) AS p
ORDER BY product_category;
```

## Functions and stored procedures
### Snowflake Functions & Stored Procedures Reference Card

### Basic SQL Functions

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `CONCAT(str1, str2, ...)` | Concatenates strings | `SELECT CONCAT('Snow', 'flake') → 'Snowflake'` |
| `LOWER(string)` | Converts string to lowercase | `SELECT LOWER('SNOWFLAKE') → 'snowflake'` |
| `UPPER(string)` | Converts string to uppercase | `SELECT UPPER('snowflake') → 'SNOWFLAKE'` |
| `TRIM(string)` | Removes leading and trailing whitespace | `SELECT TRIM('  snow  ') → 'snow'` |
| `LTRIM(string)` | Removes leading whitespace | `SELECT LTRIM('  snow  ') → 'snow  '` |
| `RTRIM(string)` | Removes trailing whitespace | `SELECT RTRIM('  snow  ') → '  snow'` |
| `SUBSTRING(string, position, [length])` | Extracts substring | `SELECT SUBSTRING('snowflake', 1, 4) → 'snow'` |
| `REPLACE(string, search, replacement)` | Replaces all occurrences | `SELECT REPLACE('snowball', 'ball', 'flake') → 'snowflake'` |
| `REGEXP_REPLACE(string, pattern, replacement)` | Replaces using regex | `SELECT REGEXP_REPLACE('abc123', '[0-9]+', 'X') → 'abcX'` |
| `LENGTH(string)` | Returns string length | `SELECT LENGTH('snow') → 4` |
| `SPLIT_PART(string, delimiter, part_number)` | Splits string and returns specified part | `SELECT SPLIT_PART('snow.flake.com', '.', 2) → 'flake'` |

### Numeric Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ABS(number)` | Absolute value | `SELECT ABS(-5) → 5` |
| `ROUND(number, [decimals])` | Rounds to specified decimal places | `SELECT ROUND(5.678, 2) → 5.68` |
| `FLOOR(number)` | Rounds down to nearest integer | `SELECT FLOOR(5.7) → 5` |
| `CEILING(number)` | Rounds up to nearest integer | `SELECT CEILING(5.3) → 6` |
| `POWER(base, exponent)` | Raises base to the power of exponent | `SELECT POWER(2, 3) → 8` |
| `SQRT(number)` | Square root | `SELECT SQRT(16) → 4` |
| `MOD(dividend, divisor)` | Modulo (remainder) | `SELECT MOD(10, 3) → 1` |

### Date/Time Functions

| Function | Description | Example |
|----------|-------------|---------|
| `CURRENT_DATE()` | Current date | `SELECT CURRENT_DATE() → '2025-04-07'` |
| `CURRENT_TIME()` | Current time | `SELECT CURRENT_TIME() → '14:30:25'` |
| `CURRENT_TIMESTAMP()` | Current date and time | `SELECT CURRENT_TIMESTAMP() → '2025-04-07 14:30:25'` |
| `DATEADD(part, count, date)` | Adds interval to date | `SELECT DATEADD('DAY', 7, '2025-04-01') → '2025-04-08'` |
| `DATEDIFF(part, date1, date2)` | Difference between dates | `SELECT DATEDIFF('DAY', '2025-04-01', '2025-04-07') → 6` |
| `DATE_TRUNC(part, date)` | Truncates to specified precision | `SELECT DATE_TRUNC('MONTH', '2025-04-07') → '2025-04-01'` |
| `EXTRACT(part FROM date)` | Extracts part from date | `SELECT EXTRACT(MONTH FROM '2025-04-07') → 4` |
| `TO_DATE(string, [format])` | Converts string to date | `SELECT TO_DATE('2025-04-07') → '2025-04-07'` |
| `TO_TIMESTAMP(string, [format])` | Converts string to timestamp | `SELECT TO_TIMESTAMP('2025-04-07 14:30:25') → '2025-04-07 14:30:25'` |

### Conditional Functions

| Function | Description | Example |
|----------|-------------|---------|
| `IFF(condition, true_result, false_result)` | Returns result based on condition | `SELECT IFF(5 > 3, 'Yes', 'No') → 'Yes'` |
| `CASE WHEN...THEN...ELSE END` | Multi-condition branching | `SELECT CASE WHEN x > 10 THEN 'High' ELSE 'Low' END` |
| `COALESCE(expr1, expr2, ...)` | Returns first non-null expression | `SELECT COALESCE(NULL, 'Snow', 'Flake') → 'Snow'` |
| `NULLIF(expr1, expr2)` | Returns NULL if expr1 = expr2 | `SELECT NULLIF(5, 5) → NULL` |
| `NVL(expr1, expr2)` | Returns expr2 if expr1 is NULL | `SELECT NVL(NULL, 'Default') → 'Default'` |

### Advanced SQL Functions

### Window Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ROW_NUMBER()` | Sequential row number | `SELECT ROW_NUMBER() OVER (ORDER BY sales DESC)` |
| `RANK()` | Rank with gaps | `SELECT RANK() OVER (PARTITION BY region ORDER BY sales DESC)` |
| `DENSE_RANK()` | Rank without gaps | `SELECT DENSE_RANK() OVER (ORDER BY sales DESC)` |
| `LAG(expr, [offset], [default])` | Access previous row's value | `SELECT LAG(sales, 1, 0) OVER (ORDER BY date)` |
| `LEAD(expr, [offset], [default])` | Access next row's value | `SELECT LEAD(sales, 1, 0) OVER (ORDER BY date)` |
| `FIRST_VALUE(expr)` | First value in window | `SELECT FIRST_VALUE(sales) OVER (PARTITION BY region ORDER BY date)` |
| `LAST_VALUE(expr)` | Last value in window | `SELECT LAST_VALUE(sales) OVER (PARTITION BY region ORDER BY date)` |
| `PERCENT_RANK()` | Relative rank (0 to 1) | `SELECT PERCENT_RANK() OVER (ORDER BY sales)` |

### Aggregate Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(expr)` | Count rows | `SELECT COUNT(*) FROM table` |
| `SUM(expr)` | Sum of values | `SELECT SUM(sales) FROM table` |
| `AVG(expr)` | Average of values | `SELECT AVG(sales) FROM table` |
| `MIN(expr)` | Minimum value | `SELECT MIN(sales) FROM table` |
| `MAX(expr)` | Maximum value | `SELECT MAX(sales) FROM table` |
| `MEDIAN(expr)` | Median value | `SELECT MEDIAN(sales) FROM table` |
| `STDDEV(expr)` | Standard deviation | `SELECT STDDEV(sales) FROM table` |
| `VARIANCE(expr)` | Variance | `SELECT VARIANCE(sales) FROM table` |
| `LISTAGG(expr, [delimiter])` | Concatenates values | `SELECT LISTAGG(name, ', ') FROM table` |
| `ARRAY_AGG(expr)` | Aggregates into array | `SELECT ARRAY_AGG(name) FROM table` |
| `OBJECT_AGG(key, value)` | Aggregates into object | `SELECT OBJECT_AGG(key, value) FROM table` |

### Semi-structured Data Functions

| Function | Description | Example |
|----------|-------------|---------|
| `PARSE_JSON(string)` | Parses JSON string | `SELECT PARSE_JSON('{"name": "Snowflake"}')` |
| `TRY_PARSE_JSON(string)` | Safely parses JSON string | `SELECT TRY_PARSE_JSON('{"name": "Snowflake"}')` |
| `GET_PATH(variant, path)` | Gets value at specified path | `SELECT GET_PATH(v, 'name')` |
| `OBJECT_CONSTRUCT(key, value, ...)` | Creates object | `SELECT OBJECT_CONSTRUCT('name', 'Snowflake')` |
| `ARRAY_CONSTRUCT(element, ...)` | Creates array | `SELECT ARRAY_CONSTRUCT(1, 2, 3)` |
| `ARRAY_SIZE(array)` | Returns array size | `SELECT ARRAY_SIZE([1, 2, 3]) → 3` |
| `ARRAY_CONTAINS(value, array)` | Checks if array contains value | `SELECT ARRAY_CONTAINS(1, [1, 2, 3]) → TRUE` |
| `FLATTEN(expr, [path], [outer], [recursive], [mode])` | Flattens arrays/objects | `SELECT value FROM TABLE(FLATTEN(data, 'items'))` |
| `OBJECT_KEYS(object)` | Returns array of keys | `SELECT OBJECT_KEYS({"a": 1, "b": 2}) → ["a", "b"]` |

### User-Defined Functions (UDFs)

### SQL UDFs

```sql
-- Creating SQL UDF
CREATE OR REPLACE FUNCTION calculate_discount(price FLOAT, discount_percent FLOAT)
RETURNS FLOAT
AS
$$
    price * (1 - discount_percent / 100)
$$;

-- Using SQL UDF
SELECT calculate_discount(100.00, 15) → 85.00
```

### JavaScript UDFs

```sql
-- Creating JavaScript UDF
CREATE OR REPLACE FUNCTION js_reverse_string(str STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    return STR.split('').reverse().join('');
$$;

-- Using JavaScript UDF
SELECT js_reverse_string('snowflake') → 'ekalfwons'
```

### Python UDFs

```sql
-- Creating Python UDF
CREATE OR REPLACE FUNCTION py_fibonacci(n INT)
RETURNS INT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'fibonacci_handler'
AS
$$
def fibonacci_handler(n):
    if n <= 1:
        return n
    return fibonacci_handler(n-1) + fibonacci_handler(n-2)
$$;

-- Using Python UDF
SELECT py_fibonacci(10) → 55
```

### Stored Procedures

### SQL Stored Procedures

```sql
-- Creating SQL Stored Procedure
CREATE OR REPLACE PROCEDURE update_sales_commissions()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE sales_reps
    SET commission = sales * commission_rate
    WHERE sales > 0;
    
    RETURN 'Commission calculation completed';
END;
$$;

-- Calling SQL Stored Procedure
CALL update_sales_commissions() → 'Commission calculation completed'
```

### JavaScript Stored Procedures

```sql
-- Creating JavaScript Stored Procedure
CREATE OR REPLACE PROCEDURE process_new_customers()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Get the current date
    var current_date = snowflake.execute({sqlText: "SELECT CURRENT_DATE()"});
    current_date.next();
    var today = current_date.getColumnValue(1);
    
    // Process new customers
    var stmt = snowflake.createStatement({
        sqlText: `INSERT INTO customer_status (customer_id, status, processed_date)
                  SELECT customer_id, 'NEW', ?
                  FROM customers
                  WHERE create_date = ?`,
        binds: [today, today]
    });
    
    var result = stmt.execute();
    
    // Return success message with count
    return "Processed " + stmt.getNumRowsInserted() + " new customers";
$$;

-- Calling JavaScript Stored Procedure
CALL process_new_customers() → 'Processed 15 new customers'
```

### Python Stored Procedures

```sql
-- Creating Python Stored Procedure
CREATE OR REPLACE PROCEDURE py_clean_data()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'clean_data_process'
AS
$$
def clean_data_process(snowpark_session):
    # Read data
    df = snowpark_session.sql("SELECT * FROM raw_data")
    
    # Clean data
    df = df.na.fill('')  # Replace nulls with empty strings
    df = df.dropDuplicates()  # Remove duplicates
    
    # Write cleaned data
    df.write.mode("overwrite").saveAsTable("clean_data")
    
    return f"Data cleaning completed, processed {df.count()} rows"
$$;

-- Calling Python Stored Procedure
CALL py_clean_data() → 'Data cleaning completed, processed 5280 rows'
```

### Advanced Techniques

### Table Functions

```sql
-- Creating JavaScript Table Function
CREATE OR REPLACE FUNCTION generate_date_range(start_date DATE, end_date DATE)
RETURNS TABLE (date_value DATE)
LANGUAGE JAVASCRIPT
AS
$$
    var result = [];
    var current = new Date(START_DATE);
    var end = new Date(END_DATE);
    
    while (current <= end) {
        result.push({DATE_VALUE: current.toISOString().split('T')[0]});
        current.setDate(current.getDate() + 1);
    }
    
    return result;
$$;

-- Using Table Function
SELECT * FROM TABLE(generate_date_range('2025-04-01', '2025-04-07'));
```

### Error Handling

```sql
-- JavaScript Error Handling
CREATE OR REPLACE PROCEDURE with_error_handling()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    try {
        // Attempt some operation
        snowflake.execute({sqlText: "INSERT INTO target_table SELECT * FROM source_table"});
        return "Success";
    } catch (err) {
        // Handle the error
        snowflake.execute({
            sqlText: "INSERT INTO error_log (error_message, error_time) VALUES (?, CURRENT_TIMESTAMP())",
            binds: [err.message]
        });
        return "Error: " + err.message;
    }
$$;
```

### Transaction Management

```sql
-- SQL Transaction Management
CREATE OR REPLACE PROCEDURE transfer_funds(from_account STRING, to_account STRING, amount FLOAT)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Start transaction
    BEGIN TRANSACTION;
    
    -- Decrease from account
    UPDATE accounts SET balance = balance - :amount WHERE account_id = :from_account;
    
    -- Increase to account
    UPDATE accounts SET balance = balance + :amount WHERE account_id = :to_account;
    
    -- Commit transaction
    COMMIT;
    
    RETURN 'Transfer completed successfully';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'Transfer failed: ' || SQLERRM;
END;
$$;
```

### Dynamic SQL

```sql
-- JavaScript Dynamic SQL
CREATE OR REPLACE PROCEDURE dynamic_query(table_name STRING)
RETURNS TABLE (column_name STRING, data_type STRING)
LANGUAGE JAVASCRIPT
AS
$$
    // Validate input to prevent SQL injection
    if (!/^[A-Za-z0-9_]+$/.test(TABLE_NAME)) {
        throw new Error("Invalid table name");
    }
    
    // Execute dynamic query
    var result = snowflake.execute({
        sqlText: `SELECT column_name, data_type 
                  FROM information_schema.columns 
                  WHERE table_name = UPPER(?)`,
        binds: [TABLE_NAME]
    });
    
    // Process results
    var columns = [];
    while (result.next()) {
        columns.push({
            COLUMN_NAME: result.getColumnValue(1),
            DATA_TYPE: result.getColumnValue(2)
        });
    }
    
    return columns;
$$;
```

### Best Practices

### Performance Optimization

1. **Use Appropriate Data Types**
   - Choose appropriate data types (e.g., NUMBER instead of VARCHAR for numeric values)
   - Use VARCHAR only for variable-length strings

2. **Query Optimization**
   - Filter data early in the query
   - Use appropriate join types (INNER, LEFT, etc.)
   - Avoid SELECT * unless necessary
   - Leverage result caching with appropriate hints

3. **UDF Optimization**
   - Keep UDFs simple and focused
   - Use SQL UDFs when possible (faster than JavaScript/Python)
   - Use vectorized UDFs for batch processing

4. **Resource Management**
   - Set appropriate warehouse size
   - Suspend idle warehouses
   - Use multi-cluster warehouses for variable workloads

### Security Practices

1. **Input Validation**
   - Validate all inputs to prevent SQL injection
   - Use parameterized queries with bind variables

2. **Secure Credentials**
   - Avoid hardcoding credentials in procedures
   - Use Snowflake Secure Objects for sensitive data

3. **Access Control**
   - Apply least privilege principle
   - Use roles and secure views to limit data access
   - Implement row-level security when needed

4. **Audit**
   - Enable query history for auditing
   - Set up alerts for suspicious activities

### Maintenance and Debugging

1. **Modular Design**
   - Break complex procedures into smaller, reusable parts
   - Create helper functions for common operations

2. **Logging**
   - Implement logging for complex operations
   - Log start/end times, parameters, and results

3. **Error Handling**
   - Implement comprehensive error handling
   - Use custom error codes/messages for clarity

4. **Testing**
   - Create test cases for procedures and functions
   - Test edge cases and error conditions

### Development Workflow

1. **Version Control**
   - Use version control systems for SQL code
   - Include comments and documentation

2. **Deployment**
   - Use CI/CD pipelines for deployment
   - Implement blue/green deployment for critical systems

3. **Documentation**
   - Document purpose, parameters, and return values
   - Include examples of usage

4. **Code Review**
   - Conduct peer reviews for complex functions/procedures
   - Use consistent naming conventions


## Query Optimization

### Best Practices

#### Filter Early
```sql
-- Good: Apply filters early
SELECT o.order_id, o.order_date, c.customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= DATEADD('month', -3, CURRENT_DATE());

-- Less efficient: Filter later
SELECT o.order_id, o.order_date, c.customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= DATEADD('month', -3, CURRENT_DATE());
```

#### Avoid SELECT *
```sql
-- Good: Select only needed columns
SELECT customer_id, order_id, order_amount
FROM orders
WHERE order_date > '2024-01-01';

-- Less efficient: Select all columns
SELECT *
FROM orders
WHERE order_date > '2024-01-01';
```

#### Use Semi-joins
```sql
-- Good: Use EXISTS or IN
SELECT customer_id, customer_name
FROM customers
WHERE EXISTS (
  SELECT 1 FROM orders 
  WHERE orders.customer_id = customers.customer_id
  AND order_date > '2024-01-01'
);

-- Alternative approach with semi-join
SELECT DISTINCT c.customer_id, c.customer_name
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date > '2024-01-01';
```

## Snowflake-Specific Features

### Zero-Copy Cloning
```sql
-- Clone a table
CREATE OR REPLACE TABLE my_database.my_schema.customers_backup
CLONE my_database.my_schema.customers;

-- Clone a database
CREATE DATABASE my_database_dev
CLONE my_database;
```

### Time Travel
```sql
-- Query data as it existed 2 hours ago
SELECT * FROM my_database.my_schema.customers
AT(OFFSET => -60*60*2);

-- Query data as it existed at a specific timestamp
SELECT * FROM my_database.my_schema.customers
AT(TIMESTAMP => '2024-02-26 12:00:00'::TIMESTAMP_NTZ);

-- Restore a table to a previous state
CREATE OR REPLACE TABLE my_database.my_schema.customers
CLONE my_database.my_schema.customers BEFORE (STATEMENT => '019a2a82-b170-44c6-af3f-f5c4a99dbf9e');
```

### Semi-Structured Data
```sql
-- Query JSON data
SELECT 
  metadata:customer.id::NUMBER AS customer_id,
  metadata:customer.name::VARCHAR AS customer_name,
  metadata:device.type::VARCHAR AS device_type,
  event_type
FROM my_database.my_schema.events
WHERE metadata:customer.segment::VARCHAR = 'premium';

-- Flatten array of objects
SELECT 
  t.order_id,
  f.value:product_id::NUMBER AS product_id,
  f.value:quantity::NUMBER AS quantity,
  f.value:price::DECIMAL(10,2) AS price
FROM my_database.my_schema.orders t,
LATERAL FLATTEN(input => t.line_items) f;
```

### Secure Data Sharing
```sql
-- Create a share
CREATE OR REPLACE SHARE my_customer_share;

-- Grant usage on database to the share
GRANT USAGE ON DATABASE my_database TO SHARE my_customer_share;
GRANT USAGE ON SCHEMA my_database.my_schema TO SHARE my_customer_share;
GRANT SELECT ON TABLE my_database.my_schema.aggregated_metrics TO SHARE my_customer_share;

-- Add account to share
ALTER SHARE my_customer_share ADD ACCOUNTS = partner_account;
```

## Security Best Practices

### Role-Based Access Control
```sql
-- Create roles with different privilege levels
CREATE ROLE data_analyst;
CREATE ROLE data_scientist;
CREATE ROLE data_engineer;

-- Grant privileges
GRANT USAGE ON WAREHOUSE analytics_wh TO ROLE data_analyst;
GRANT USAGE ON DATABASE analytics_db TO ROLE data_analyst;
GRANT USAGE ON SCHEMA analytics_db.public TO ROLE data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_db.public TO ROLE data_analyst;

-- Grant additional privileges to higher roles
GRANT ROLE data_analyst TO ROLE data_scientist;
GRANT CREATE TABLE ON SCHEMA analytics_db.public TO ROLE data_scientist;
```

### Column-Level Security
```sql
-- Create a masking policy for PII
CREATE OR REPLACE MASKING POLICY email_mask AS
  (val STRING) RETURNS STRING ->
    CASE
      WHEN CURRENT_ROLE() IN ('ADMIN', 'COMPLIANCE') THEN val
      ELSE REGEXP_REPLACE(val, '^(.)(.*)(.)(@.*)$', '\\1***\\3\\4')
    END;

-- Apply the masking policy to a column
ALTER TABLE my_database.my_schema.customers 
MODIFY COLUMN email SET MASKING POLICY email_mask;
```

### Row-Level Security
```sql
-- Create a row access policy
CREATE OR REPLACE ROW ACCESS POLICY region_access AS
  (region VARCHAR) RETURNS BOOLEAN ->
    region IN (SELECT allowed_region FROM access_control WHERE role_name = CURRENT_ROLE());

-- Apply the row access policy to a table
ALTER TABLE my_database.my_schema.customers
ADD ROW ACCESS POLICY region_access ON (region);
```

## Performance Tips

### Warehouse Sizing
```sql
-- Create appropriately sized warehouses for different workloads
CREATE WAREHOUSE reporting_wh 
  WITH WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE etl_wh 
  WITH WAREHOUSE_SIZE = 'X-LARGE'
  AUTO_SUSPEND = 600
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 5
  SCALING_POLICY = 'STANDARD';
```

### Result Caching
```sql
-- Enable result caching at session level
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- Force recomputation by adding a unique comment
SELECT /* 202402271212 */ 
  date_trunc('month', order_date) AS month,
  SUM(order_amount) AS total_sales
FROM my_database.my_schema.orders
GROUP BY 1
ORDER BY 1;
```

### Clustering Keys
```sql
-- Analyze clustering depth
SELECT * FROM TABLE(SYSTEM$CLUSTERING_DEPTH(
  'my_database.my_schema.large_table', '(event_date, customer_id)'));

-- Recluster a table
ALTER TABLE my_database.my_schema.large_table RECLUSTER;
```

## Cost Optimization

### Resource Monitors
```sql
-- Create a resource monitor
CREATE OR REPLACE RESOURCE MONITOR monthly_limit
  WITH CREDIT_QUOTA = 5000
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 80 PERCENT DO NOTIFY
    ON 90 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;
    
-- Assign resource monitor to a warehouse
ALTER WAREHOUSE reporting_wh SET RESOURCE_MONITOR = monthly_limit;
```

### Query Profiling
```sql
-- Examine query profile
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TEXT LIKE '%customer_order_summary%'
ORDER BY START_TIME DESC
LIMIT 10;

-- Get detailed execution statistics
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_ID('019a2a82-b170-44c6-af3f-f5c4a99dbf9e'));
```

### Storage Optimization
```sql
-- Check table storage metrics
SELECT * FROM TABLE(INFORMATION_SCHEMA.TABLE_STORAGE_METRICS('my_database.my_schema.large_table'));

-- Optimize storage with clustering
ALTER TABLE my_database.my_schema.large_table CLUSTER BY (date_column, category);

-- Set appropriate retention periods
ALTER DATABASE my_database
SET DATA_RETENTION_TIME_IN_DAYS = 7;
```

