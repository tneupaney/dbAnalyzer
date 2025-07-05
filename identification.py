import pandas as pd
import time
from sqlalchemy import inspect, text, types
import hashlib
import re # For regex in security checks

# No direct import from a specific handler here.
# The handler will be passed as an argument to the functions.

# --- Schema Discovery ---
def discover_schema(db_handler, db_paths=None):
    """
    Connects to all shards using the provided db_handler and dynamically discovers their schema,
    including tables, columns, primary keys, unique constraints, foreign keys,
    indexes, and triggers.
    """
    engines = db_handler.get_all_shard_engines(db_paths)
    if not engines:
        print("No database connections established for schema discovery. Returning empty schema.")
        return {}

    discovered_schema = {
        'shards': {},
        'relationships': [], # List of all FK relationships across all shards
        'all_triggers': [] # List of all triggers across all shards
    }

    for shard_name, engine in engines.items():
        shard_info = {
            'tables': {},
            'triggers': []
        }
        with engine.connect() as conn:
            inspector = inspect(engine)
            table_names = inspector.get_table_names()

            for table_name in table_names:
                table_info = {
                    'columns': [],
                    'primary_key': [],
                    'unique_constraints': [],
                    'foreign_keys': [],
                    'indexes': []
                }

                # Columns
                columns = inspector.get_columns(table_name)
                for col in columns:
                    table_info['columns'].append({
                        'name': col['name'],
                        'type': str(col['type']), # Convert SQLAlchemy type to string
                        'nullable': col['nullable']
                    })

                # Primary Key
                pk_constraints = inspector.get_pk_constraint(table_name)
                if pk_constraints and 'constrained_columns' in pk_constraints:
                    table_info['primary_key'] = pk_constraints['constrained_columns']

                # Unique Constraints
                unique_constraints = inspector.get_unique_constraints(table_name)
                for uc in unique_constraints:
                    table_info['unique_constraints'].append(uc['column_names'])

                # Foreign Keys
                foreign_keys = inspector.get_foreign_keys(table_name)
                for fk in foreign_keys:
                    table_info['foreign_keys'].append({
                        'constrained_columns': fk['constrained_columns'],
                        'referred_table': fk['referred_table'],
                        'referred_columns': fk['referred_columns']
                    })
                    # Add to global relationships list
                    discovered_schema['relationships'].append({
                        'shard': shard_name,
                        'from_table': table_name,
                        'from_columns': fk['constrained_columns'],
                        'to_table': fk['referred_table'],
                        'to_columns': fk['referred_columns']
                    })

                # Indexes
                indexes = inspector.get_indexes(table_name)
                for idx in indexes:
                    table_info['indexes'].append({
                        'name': idx['name'],
                        'columns': idx['column_names'],
                        'unique': idx['unique']
                    })
                
                shard_info['tables'][table_name] = table_info
            
            # Triggers (using db_handler specific query)
            triggers_query = db_handler.get_trigger_query_sql()
            try:
                triggers_raw = conn.execute(text(triggers_query)).fetchall()
                for trigger_row in triggers_raw:
                    # SQLite: name, sql. MySQL: TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_STATEMENT
                    if hasattr(db_handler, 'get_trigger_query_sql') and 'sqlite_master' in db_handler.get_trigger_query_sql(): # Heuristic for SQLite
                        trigger_name, trigger_sql = trigger_row
                        table_for_trigger = re.search(r'ON\s+([a-zA-Z0-9_]+)', trigger_sql, re.IGNORECASE)
                        table_for_trigger_name = table_for_trigger.group(1) if table_for_trigger else "UNKNOWN_TABLE"
                    else: # Assume MySQL-like structure
                        trigger_name, table_for_trigger_name, trigger_sql = trigger_row
                    
                    shard_info['triggers'].append({
                        'name': trigger_name,
                        'table': table_for_trigger_name, # Store associated table
                        'sql': trigger_sql
                    })
                    discovered_schema['all_triggers'].append({
                        'shard': shard_name,
                        'name': trigger_name,
                        'table': table_for_trigger_name, # Store associated table
                        'sql': trigger_sql
                    })
            except Exception as e:
                print(f"Warning: Could not retrieve trigger information for {shard_name}: {e}")


        discovered_schema['shards'][shard_name] = shard_info
    
    for engine in engines.values():
        engine.dispose()
    
    return discovered_schema

# --- Query Performance Analysis (Dynamic) ---
def analyze_queries_dynamic(db_handler, discovered_schema, connection_details):
    """
    Analyzes query performance by generating synthetic queries based on the
    discovered schema.
    """
    engines = db_handler.get_all_shard_engines(connection_details) # Pass connection_details
    if not engines:
        print("No database connections established for query analysis. Skipping.")
        return pd.DataFrame()

    all_results = []
    
    # Generate synthetic queries based on discovered schema
    synthetic_queries = []

    for shard_name, shard_info in discovered_schema['shards'].items():
        for table_name, table_details in shard_info['tables'].items():
            # Simple SELECT * LIMIT 10
            synthetic_queries.append({
                'name': f'Select Top 10 from {table_name} ({shard_name})',
                'sql': f'SELECT * FROM {table_name} LIMIT 10',
                'shard_key': shard_name,
                'type': 'simple_select',
                'suggested_optimization': 'Basic select, usually optimized by default.'
            })

            # Count rows
            synthetic_queries.append({
                'name': f'Count Rows in {table_name} ({shard_name})',
                'sql': f'SELECT COUNT(*) FROM {table_name}',
                'shard_key': shard_name,
                'type': 'count_rows',
                'suggested_optimization': 'Consider index on primary key for faster counts on large tables.'
            })

            # Select with WHERE clause on a text or numeric column (if available)
            text_cols = [c['name'] for c in table_details['columns'] if 'TEXT' in c['type'].upper() or 'VARCHAR' in c['type'].upper()]
            numeric_cols = [c['name'] for c in table_details['columns'] if 'INT' in c['type'].upper() or 'REAL' in c['type'].upper() or 'DECIMAL' in c['type'].upper()]

            if text_cols:
                col_name = text_cols[0]
                synthetic_queries.append({
                    'name': f'Filter {table_name} by {col_name} (LIKE) ({shard_name})',
                    'sql': f'SELECT * FROM {table_name} WHERE {col_name} LIKE "%test%" LIMIT 5',
                    'shard_key': shard_name,
                    'type': 'filter_like',
                    'suggested_optimization': 'Consider full-text search or leading wildcard optimization for LIKE queries.'
                })
            if numeric_cols:
                col_name = numeric_cols[0]
                synthetic_queries.append({
                    'name': f'Filter {table_name} by {col_name} (Range) ({shard_name})',
                    'sql': f'SELECT * FROM {table_name} WHERE {col_name} > 100 LIMIT 5',
                    'shard_key': shard_name,
                    'type': 'filter_range',
                    'suggested_optimization': f'Ensure index on {table_name}.{col_name} for range queries.'
                })
    
    # Execute synthetic queries
    explain_prefix = db_handler.get_explain_query_plan_prefix()
    for query_info in synthetic_queries:
        query_name = query_info['name']
        sql_query = query_info['sql']
        target_shard_key = query_info['shard_key']
        suggested_optimization = query_info['suggested_optimization']

        engine = engines.get(target_shard_key)
        if not engine:
            print(f"Warning: Shard '{target_shard_key}' not found for query '{query_name}'. Skipping.")
            continue # Skip this query if shard is not available

        with engine.connect() as conn:
            # Escape '%' for pymysql if it's a literal part of the SQL query
            # This is specifically for LIKE clauses where '%' is a wildcard,
            # but pymysql's underlying string formatting might misinterpret it.
            escaped_sql_query = sql_query.replace('%', '%%')

            # Use db_handler's explain prefix
            explain_plan = pd.read_sql(f'{explain_prefix} {escaped_sql_query}', conn)
            plan_details = explain_plan.to_string(index=False)

            start_time = time.time()
            try:
                pd.read_sql(sql_query, conn)
                execution_time = time.time() - start_time
                status = "Success"
                # Heuristic for optimization based on EXPLAIN output
                is_optimized = "SCAN TABLE" not in plan_details.upper() or "USING INDEX" in plan_details.upper()
                if "USING TEMP" in plan_details.upper() or "USING FILESORT" in plan_details.upper():
                    is_optimized = False # Mark as unoptimized if temp tables or filesort are used
            except Exception as e:
                execution_time = -1
                status = f"Error: {e}"
                is_optimized = False

            all_results.append({
                'Query': query_name,
                'Execution Time (s)': f"{execution_time:.4f}" if execution_time != -1 else status,
                'Optimized': is_optimized,
                'Suggested Optimization': suggested_optimization,
                'Query Plan': plan_details
            })
    
    for engine in engines.values():
        engine.dispose()
    
    return pd.DataFrame(all_results)

# --- Index Analysis (Dynamic) ---
def check_indexes_dynamic(db_handler, discovered_schema, connection_details):
    """
    Checks for missing and potentially redundant indexes based on the discovered schema.
    """
    engines = db_handler.get_all_shard_engines(connection_details) # Pass connection_details
    if not engines:
        print("No database connections established for index analysis. Skipping.")
        return [], []

    all_issues = []
    all_suggestions = []

    for shard_name, shard_info in discovered_schema['shards'].items():
        for table_name, table_details in shard_info['tables'].items():
            existing_indexes_for_table = table_details['indexes']
            
            # Check for missing indexes on Foreign Keys
            for fk in table_details['foreign_keys']:
                fk_columns = fk['constrained_columns']
                has_fk_index = any(
                    set(fk_columns).issubset(set(idx['columns']))
                    for idx in existing_indexes_for_table
                )
                if not has_fk_index:
                    issue = f"[{shard_name}] Missing index on foreign key column(s) {fk_columns} in table '{table_name}'."
                    suggestion = f"CREATE INDEX idx_{table_name}_{'_'.join(fk_columns)}_fk ON {table_name}({', '.join(fk_columns)}); -- In {shard_name}"
                    if issue not in all_issues:
                        all_issues.append(issue)
                        all_suggestions.append(suggestion)

            # Heuristic check for common columns that should be indexed
            for col in table_details['columns']:
                col_name = col['name']
                col_type = col['type'].upper()
                
                is_indexed = any(col_name in idx['columns'] for idx in existing_indexes_for_table)
                is_pk = col_name in table_details['primary_key']

                if not is_indexed and not is_pk:
                    if 'ID' in col_name.upper() and col_name != table_details['primary_key']: # Non-PK ID columns
                        issue = f"[{shard_name}] Missing index on potential ID column '{col_name}' in table '{table_name}'."
                        suggestion = f"CREATE INDEX idx_{table_name}_{col_name}_id ON {table_name}({col_name}); -- In {shard_name}"
                        if issue not in all_issues:
                            all_issues.append(issue)
                            all_suggestions.append(suggestion)
                    elif 'DATE' in col_type or 'TIME' in col_type or 'DATE' in col_name.upper() or 'DATETIME' in col_type:
                        issue = f"[{shard_name}] Missing index on date/time column '{col_name}' in table '{table_name}' (often used for filtering/sorting)."
                        suggestion = f"CREATE INDEX idx_{table_name}_{col_name}_date ON {table_name}({col_name}); -- In {shard_name}"
                        if issue not in all_issues:
                            all_issues.append(issue)
                            all_suggestions.append(suggestion)
                    elif 'NAME' in col_name.upper() or 'EMAIL' in col_name.upper() or 'USERNAME' in col_name.upper():
                        issue = f"[{shard_name}] Missing index on text column '{col_name}' in table '{table_name}' (often used for filtering/joining)."
                        suggestion = f"CREATE INDEX idx_{table_name}_{col_name}_text ON {table_name}({col_name}); -- In {shard_name}"
                        if issue not in all_issues:
                            all_issues.append(issue)
                            all_suggestions.append(suggestion)

            # Check for redundant indexes (simple case: index (A) and index (A, B))
            for i, idx1 in enumerate(existing_indexes_for_table):
                for j, idx2 in enumerate(existing_indexes_for_table):
                    if i != j and set(idx1['columns']).issubset(set(idx2['columns'])) and len(idx1['columns']) < len(idx2['columns']):
                        issue = f"[{shard_name}] Potentially redundant index '{idx1['name']}' on columns {idx1['columns']} in table '{table_name}'. It's covered by '{idx2['name']}' on {idx2['columns']}."
                        suggestion = f"Consider dropping index '{idx1['name']}' on {table_name}: DROP INDEX {idx1['name']}; -- In {shard_name}"
                        if issue not in all_issues:
                            all_issues.append(issue)
                            all_suggestions.append(suggestion)
    
    return all_issues, all_suggestions

# --- Data Integrity Checks (Dynamic) ---
def check_data_integrity_dynamic(db_handler, discovered_schema, connection_details):
    """
    Performs data integrity checks across all shards and tables based on
    discovered foreign keys and unique constraints.
    """
    engines = db_handler.get_all_shard_engines(connection_details) # Pass connection_details
    if not engines:
        print("No database connections established for data integrity analysis. Skipping.")
        return []

    all_issues = []

    for shard_name, engine in engines.items():
        with engine.connect() as conn:
            conn.execute(text(db_handler.get_fk_check_on_sql())) # Ensure FKs are ON for checking

            # Check for foreign key violations (orphaned records)
            for fk_rel in discovered_schema['relationships']:
                if fk_rel['shard'] == shard_name: # Only check relationships local to this shard
                    from_table = fk_rel['from_table']
                    from_cols = ', '.join(fk_rel['from_columns'])
                    to_table = fk_rel['to_table']
                    to_cols = ', '.join(fk_rel['to_columns'])

                    # Check if tables exist in the current shard before querying
                    inspector = inspect(engine)
                    if from_table not in inspector.get_table_names() or to_table not in inspector.get_table_names():
                        continue # Skip if tables are not present in this shard

                    try:
                        # This query finds rows in 'from_table' where the FK value does not exist in 'to_table'
                        orphaned_records = pd.read_sql(f"""
                            SELECT {from_cols}
                            FROM {from_table}
                            WHERE {from_cols} NOT IN (SELECT {to_cols} FROM {to_table})
                        """, conn)
                        if not orphaned_records.empty:
                            all_issues.append(f"[{shard_name}] Foreign Key Violation: Orphaned records found in '{from_table}' (columns: {from_cols}) referencing non-existent entries in '{to_table}' (columns: {to_cols}):\n{orphaned_records.to_string(index=False)}")
                    except Exception as e:
                        all_issues.append(f"[{shard_name}] Error checking FK between {from_table} and {to_table}: {e}")

            # Check for duplicate unique columns
            for table_name, table_details in discovered_schema['shards'][shard_name]['tables'].items():
                for unique_cols in table_details['unique_constraints']:
                    cols_str = ', '.join(unique_cols)
                    try:
                        duplicate_unique_entries = pd.read_sql(f"""
                            SELECT {cols_str}, COUNT(*)
                            FROM {table_name}
                            GROUP BY {cols_str}
                            HAVING COUNT(*) > 1
                        """, conn)
                        if not duplicate_unique_entries.empty:
                            all_issues.append(f"[{shard_name}] Duplicate Unique Constraint: Found duplicate entries for unique column(s) '{cols_str}' in table '{table_name}':\n{duplicate_unique_entries.to_string(index=False)}")
                    except Exception as e:
                        all_issues.append(f"[{shard_name}] Error checking unique constraint on {table_name}.{cols_str}: {e}")
    
    for engine in engines.values():
        engine.dispose()
    
    return all_issues

# --- Security Checks (Dynamic) ---
def check_security_dynamic(db_handler, discovered_schema, connection_details):
    """
    Scans all tables for columns that might contain sensitive data,
    and heuristically assesses password field security.
    """
    engines = db_handler.get_all_shard_engines(connection_details) # Pass connection_details
    if not engines:
        print("No database connections established for security analysis. Skipping.")
        return []

    security_findings = []
    
    # Regex patterns for sensitive data detection
    EMAIL_REGEX = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    SSN_REGEX = r'^\d{3}-\d{2}-\d{4}$' # US SSN format
    CREDIT_CARD_REGEX = r'^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|6(?:011|5[0-9]{2})[0-9]{12}|3[47][0-9]{13}|(?:2131|1800|35\d{3})\d{11})$' # Basic CC regex

    for shard_name, engine in engines.items():
        with engine.connect() as conn:
            for table_name, table_details in discovered_schema['shards'][shard_name]['tables'].items():
                for col in table_details['columns']:
                    col_name = col['name'].lower()
                    col_type = col['type'].upper()

                    if 'TEXT' in col_type or 'VARCHAR' in col_type:
                        # Heuristic for sensitive column names
                        if 'password' in col_name:
                            # Sample data to check hashing heuristic
                            try:
                                sample_data_df = pd.read_sql(f"SELECT {col['name']} FROM {table_name} WHERE {col['name']} IS NOT NULL LIMIT 5", conn)
                                if not sample_data_df.empty:
                                    sample_value = str(sample_data_df.iloc[0, 0])
                                    if len(sample_value) == 64 and all(c in '0123456789abcdefABCDEF' for c in sample_value):
                                        security_findings.append(f"[{shard_name}] Table '{table_name}', Column '{col['name']}': Appears to be SHA256 hashed (Good practice).")
                                    elif len(sample_value) < 20 and ' ' not in sample_value and not re.search(r'\W', sample_value):
                                        security_findings.append(f"[{shard_name}] Table '{table_name}', Column '{col['name']}': Might contain plaintext or weakly hashed passwords (CRITICAL: Investigate immediately!). Sample: '{sample_value[:10]}...'")
                                    else:
                                        security_findings.append(f"[{shard_name}] Table '{table_name}', Column '{col['name']}': Password field has an unknown format. (WARNING: Verify hashing method). Sample: '{sample_value[:10]}...'")
                                else:
                                    security_findings.append(f"[{shard_name}] Table '{table_name}', Column '{col['name']}': Potential password field, but no data to analyze.")
                            except Exception as e:
                                security_findings.append(f"[{shard_name}] Error analyzing password column '{col['name']}' in '{table_name}': {e}")

                        # Other sensitive data patterns
                        elif 'email' in col_name:
                            try:
                                sample_data_df = pd.read_sql(f"SELECT {col['name']} FROM {table_name} WHERE {col['name']} IS NOT NULL LIMIT 1", conn)
                                if not sample_data_df.empty and re.match(EMAIL_REGEX, str(sample_data_df.iloc[0, 0])):
                                    security_findings.append(f"[{shard_name}] Table '{table_name}', Column '{col['name']}': Contains email addresses (Sensitive PII).")
                            except Exception: pass # Ignore errors if column doesn't exist or data is bad
                        
                        elif 'ssn' in col_name or 'social_security' in col_name:
                            try:
                                sample_data_df = pd.read_sql(f"SELECT {col['name']} FROM {table_name} WHERE {col['name']} IS NOT NULL LIMIT 1", conn)
                                if not sample_data_df.empty and re.match(SSN_REGEX, str(sample_data_df.iloc[0, 0])):
                                    security_findings.append(f"[{shard_name}] Table '{table_name}', Column '{col['name']}': Contains Social Security Numbers (Highly Sensitive PII).")
                            except Exception: pass
                        
                        elif 'credit_card' in col_name or 'card_number' in col_name or 'cc_num' in col_name:
                            try:
                                sample_data_df = pd.read_sql(f"SELECT {col['name']} FROM {table_name} WHERE {col['name']} IS NOT NULL LIMIT 1", conn)
                                if not sample_data_df.empty and re.match(CREDIT_CARD_REGEX, str(sample_data_df.iloc[0, 0]).replace(' ', '').replace('-', '')):
                                    security_findings.append(f"[{shard_name}] Table '{table_name}', Column '{col['name']}': Contains Credit Card Numbers (PCI Sensitive Data). (CRITICAL: Should be encrypted/tokenized).")
                            except Exception: pass
    
    for engine in engines.values():
        engine.dispose()

    return security_findings

# --- Trigger Performance Analysis (Dynamic) ---
def analyze_triggers_dynamic(db_handler, discovered_schema, connection_details):
    """
    Analyzes the performance impact of discovered triggers by simulating an insert
    that would fire them.
    """
    engines = db_handler.get_all_shard_engines(connection_details) # Pass connection_details
    if not engines:
        print("No database connections established for trigger analysis. Skipping.")
        return []

    trigger_performance_results = []
    
    for trigger_info in discovered_schema['all_triggers']:
        shard_name = trigger_info['shard']
        trigger_name = trigger_info['name']
        trigger_sql = trigger_info['sql']
        table_name = trigger_info['table'] # Get table name from discovered schema

        engine = engines.get(shard_name)
        if not engine:
            trigger_performance_results.append(f"[{shard_name}] Engine not found for trigger '{trigger_name}'. Skipping.")
            continue

        with engine.connect() as conn:
            inspector = inspect(engine)
            if table_name not in inspector.get_table_names():
                trigger_performance_results.append(f"[{shard_name}] Table '{table_name}' for trigger '{trigger_name}' not found. Skipping performance test.")
                continue
            
            # Get column names for the target table to construct a valid insert
            target_table_cols = [col['name'] for col in discovered_schema['shards'][shard_name]['tables'][table_name]['columns']]
            
            # Only test AFTER INSERT triggers for simplicity
            if 'AFTER INSERT' in trigger_sql.upper(): # This heuristic might need refinement for MySQL
                print(f"\nAnalyzing performance of trigger '{trigger_name}' on '{table_name}' in {shard_name}...")
                num_inserts = 100 # Reduced for faster dynamic testing
                insert_data = []
                
                # Construct generic insert data based on column types
                autoincrement_keyword = db_handler.get_autoincrement_keyword()
                for i in range(num_inserts):
                    row_data = {}
                    for col in discovered_schema['shards'][shard_name]['tables'][table_name]['columns']:
                        col_name = col['name']
                        col_type = col['type'].upper()
                        
                        # Skip auto-incrementing PKs if present
                        if col_name in discovered_schema['shards'][shard_name]['tables'][table_name]['primary_key'] and \
                           autoincrement_keyword in col_type: # Use handler's keyword
                            continue
                        
                        # Generate dummy data based on type
                        if 'INT' in col_type:
                            row_data[col_name] = i + 1000000 + (len(trigger_performance_results) * num_inserts) # Unique ID
                        elif 'REAL' in col_type or 'DECIMAL' in col_type:
                            row_data[col_name] = round(100.0 + i * 0.5, 2)
                        elif 'TEXT' in col_type or 'VARCHAR' in col_type:
                            if 'DATE' in col_name.upper() or 'DATETIME' in col_type:
                                row_data[col_name] = f'2025-01-{i%28+1:02d}'
                            elif 'EMAIL' in col_name.upper():
                                row_data[col_name] = f'test{i}@example.com'
                            elif 'NAME' in col_name.upper():
                                row_data[col_name] = f'TestName{i}'
                            else:
                                row_data[col_name] = f'dummy_value_{i}'
                        else:
                            row_data[col_name] = None # Default for unknown types

                    # Ensure customer_id exists for FKs if 'orders' table
                    # This part is still specific to 'orders' and 'customer_id'.
                    # For a truly generic system, you'd need a more robust way to
                    # infer relationships or provide sample data for FKs.
                    if table_name == 'orders' and 'customer_id' in row_data:
                        row_data['customer_id'] = (i % 6) + 1 # Use existing customer IDs

                    insert_data.append(row_data)

                # Filter insert_data to only include columns present in target_table_cols
                # and ensure order for the SQL statement
                insert_cols = [col for col in target_table_cols if col in insert_data[0]]
                insert_values_placeholders = ', '.join([f':{col}' for col in insert_cols])
                insert_cols_str = ', '.join(insert_cols)

                start_time_with_trigger = time.time()
                try:
                    conn.execute(text(db_handler.get_fk_check_off_sql())) # Use handler's FK OFF SQL
                    conn.execute(text("BEGIN;")) # Use BEGIN for MySQL transactions
                    
                    conn.execute(text(f"INSERT INTO {table_name} ({insert_cols_str}) VALUES ({insert_values_placeholders})"),
                                 insert_data)
                    
                    conn.execute(text("COMMIT;"))
                    end_time_with_trigger = time.time()
                    duration_with_trigger = end_time_with_trigger - start_time_with_trigger
                    trigger_performance_results.append(f"[{shard_name}] Trigger '{trigger_name}' on '{table_name}': Inserted {num_inserts} records in {duration_with_trigger:.4f} seconds.")
                    
                    # Verify trigger action (e.g., check audit_log if trigger modifies it)
                    if 'audit_log' in discovered_schema['shards'][shard_name]['tables']:
                        audit_log_count = pd.read_sql("SELECT COUNT(*) FROM audit_log", conn).iloc[0,0]
                        trigger_performance_results.append(f"  - Audit log entries after test: {audit_log_count}.")

                except Exception as e:
                    conn.execute(text("ROLLBACK;"))
                    trigger_performance_results.append(f"[{shard_name}] Error testing trigger '{trigger_name}' on '{table_name}': {e}")
                finally:
                    conn.execute(text(db_handler.get_fk_check_on_sql())) # Use handler's FK ON SQL
            else:
                trigger_performance_results.append(f"[{shard_name}] Trigger '{trigger_name}': Only 'AFTER INSERT' triggers are currently analyzed for performance. Skipping.")
    
    for engine in engines.values():
        engine.dispose()

    return trigger_performance_results

# --- Relationship Performance Analysis ---
def analyze_relationships_performance(db_handler, discovered_schema, connection_details):
    """
    Analyzes the performance impact of foreign key relationships by generating
    and testing synthetic JOIN queries.
    """
    engines = db_handler.get_all_shard_engines(connection_details) # Pass connection_details
    if not engines:
        print("No database connections established for relationship analysis. Skipping.")
        return []

    relationship_performance_results = []
    explain_prefix = db_handler.get_explain_query_plan_prefix()

    for rel in discovered_schema['relationships']:
        shard_name = rel['shard']
        from_table = rel['from_table']
        from_cols = rel['from_columns']
        to_table = rel['to_table']
        to_cols = rel['to_columns']

        engine = engines.get(shard_name)
        if not engine:
            relationship_performance_results.append(f"[{shard_name}] Engine not found for relationship between '{from_table}' and '{to_table}'. Skipping.")
            continue

        with engine.connect() as conn:
            inspector = inspect(engine)
            if from_table not in inspector.get_table_names() or to_table not in inspector.get_table_names():
                relationship_performance_results.append(f"[{shard_name}] Tables '{from_table}' or '{to_table}' not found for relationship analysis. Skipping.")
                continue

            # Construct a synthetic JOIN query
            join_sql = f"""
                SELECT T1.*, T2.*
                FROM {from_table} AS T1
                JOIN {to_table} AS T2
                ON T1.{from_cols[0]} = T2.{to_cols[0]}
                LIMIT 10
            """
            
            # Check if an index exists on the foreign key column in the 'from' table
            from_table_details = discovered_schema['shards'][shard_name]['tables'][from_table]
            has_fk_index = any(set(from_cols).issubset(set(idx['columns'])) for idx in from_table_details['indexes'])
            
            # Check if an index exists on the primary key of the 'to' table (which is often the FK target)
            to_table_details = discovered_schema['shards'][shard_name]['tables'][to_table]
            has_pk_index_on_target = any(set(to_cols).issubset(set(idx['columns'])) for idx in to_table_details['indexes'] if idx['unique']) # PKs are unique indexes

            relationship_performance_results.append(f"[{shard_name}] Analyzing relationship: '{from_table}' ({from_cols[0]}) JOIN '{to_table}' ({to_cols[0]})")
            relationship_performance_results.append(f"  - Index on FK source ({from_table}.{from_cols[0]}): {'Exists' if has_fk_index else 'MISSING'}")
            relationship_performance_results.append(f"  - Index on FK target ({to_table}.{to_cols[0]}): {'Exists' if has_pk_index_on_target else 'MISSING'}")

            try:
                # Use db_handler's explain prefix
                escaped_join_sql = join_sql.replace('%', '%%') # Escape for pymysql
                explain_plan = pd.read_sql(f'{explain_prefix} {escaped_join_sql}', conn)
                plan_details = explain_plan.to_string(index=False)
                
                relationship_performance_results.append(f"  - Query Plan:\n{plan_details}")

                # Heuristic for unoptimized joins based on EXPLAIN output
                if "SCAN TABLE" in plan_details.upper() and "USING INDEX" not in plan_details.upper():
                    relationship_performance_results.append(f"  - WARNING: Join query involves full table scan without index. Consider adding indexes on join columns.")
                elif not has_fk_index: # Still suggest if FK index is missing
                     relationship_performance_results.append(f"  - SUGGESTION: Add index on '{from_table}.{from_cols[0]}' to improve join performance.")
                else:
                    relationship_performance_results.append(f"  - Performance appears reasonable for this synthetic join.")

            except Exception as e:
                relationship_performance_results.append(f"  - Error analyzing join performance: {e}")
        
    for engine in engines.values():
        engine.dispose()

    return relationship_performance_results
