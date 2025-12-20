import psycopg2
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import random
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import duckdb
import numpy as np

# ============================================================================
# Database Connection Configuration
# ============================================================================

# Database connection parameters

ducklake_db  = 'local'
storage_path = '/lakehouse/default/Files/ducklake'
schema_name  = 'test_concurrency'

DB_CONFIG = {
    'host': 'localhost',
    'database': ducklake_db,
    'user': 'postgres',
    'port': 5432
}




# Test configuration
NUM_WORKERS = [1,2,4,8]  # Number of concurrent workers
OPERATIONS_PER_WORKER = 100 # Total operations (INSERT + UPDATE + DELETE)
OPERATION_MIX = {
    'INSERT': 0.60,  # 60% inserts
    'UPDATE': 0.25,  # 25% updates
    'DELETE': 0.15   # 15% deletes
}
TABLES_TO_TEST = [
    'postgres',
    # 'iceberg',
    'ducklake'
]

# Deterministic seed for reproducible results
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# ============================================================================
# Connection Setup (per-process)
# ============================================================================

print("Using multiprocessing - each worker will create its own connections")

# ============================================================================
# Create Test Schema and Tables
# ============================================================================

def setup_tables():
    """Create test schema and postgres, iceberg, and ducklake tables with initial data"""
    from psycopg2.extras import execute_values
    
    # Track insertion times for each table
    insertion_times = {}
    
    # PostgreSQL tables
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        
        # Create schema
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        
        # Create POSTGRES table (only if testing)
        if 'postgres' in TABLES_TO_TEST:
            # Drop table to start from scratch
            cursor.execute(f"DROP TABLE IF EXISTS {schema_name}.lineorder_postgres CASCADE")
            conn.commit()
            
            cursor.execute(f"""
                CREATE TABLE {schema_name}.lineorder_postgres (
                lo_orderkey BIGINT NOT NULL,
                lo_linenumber INTEGER NOT NULL,
                lo_custkey INTEGER,
                lo_partkey INTEGER,
                lo_suppkey INTEGER,
                lo_orderdate INTEGER,
                lo_commitdate INTEGER,
                lo_shipdate INTEGER,
                lo_orderpriority VARCHAR(15),
                lo_shippriority INTEGER,
                lo_shipmode VARCHAR(10),
                lo_quantity NUMERIC(15,2),
                lo_extendedprice NUMERIC(15,2),
                lo_discount NUMERIC(15,2),
                lo_tax NUMERIC(15,2),
                lo_revenue NUMERIC(15,2),
                lo_supplycost NUMERIC(15,2)
            )
        """)
            print(f"✓ Created POSTGRES table: {schema_name}.lineorder_postgres")
            
            # Insert initial seed data for UPDATE and DELETE operations (batch insert)
            print("  ↳ Inserting initial seed data (batch mode)...")
            seed_values = []
            insert_start = time.time()
            for i in range(1, 1000001):  # Prepare 1,000,000 initial rows
                seed_values.append((
                    i, 1, random.randint(1, 1000), random.randint(1, 1000), random.randint(1, 1000),
                    19920101, 19920101, 19920101, '1-URGENT', 0, 'AIR',
                    10.0, 1000.0, 0.05, 0.02, 950.0, 500.0
                ))
            
            # Use execute_values for bulk insert with efficient batch size
            execute_values(cursor, f"""
                INSERT INTO {schema_name}.lineorder_postgres VALUES %s    
            """, seed_values, page_size=5000)  # Process in batches of 5000 rows
            conn.commit()
            insert_end = time.time()
            insertion_times['POSTGRES'] = insert_end - insert_start
            print(f"  ↳ Inserted 1,000,000 seed rows into POSTGRES table in {insertion_times['POSTGRES']:.2f} seconds")
        
        # Create ICEBERG table (only if testing)
        if 'iceberg' in TABLES_TO_TEST:
            # Drop table to start from scratch
            cursor.execute(f"DROP TABLE IF EXISTS {schema_name}.lineorder_iceberg CASCADE")
            conn.commit()
            
            cursor.execute(f"""
            CREATE TABLE {schema_name}.lineorder_iceberg (
                    lo_orderkey BIGINT NOT NULL,
                    lo_linenumber INTEGER NOT NULL,
                    lo_custkey INTEGER,
                    lo_partkey INTEGER,
                    lo_suppkey INTEGER,
                    lo_orderdate INTEGER,
                    lo_commitdate INTEGER,
                    lo_shipdate INTEGER,
                    lo_orderpriority VARCHAR(15),
                    lo_shippriority INTEGER,
                    lo_shipmode VARCHAR(10),
                    lo_quantity NUMERIC(15,2),
                    lo_extendedprice NUMERIC(15,2),
                    lo_discount NUMERIC(15,2),
                    lo_tax NUMERIC(15,2),
                    lo_revenue NUMERIC(15,2),
                    lo_supplycost NUMERIC(15,2)
                ) USING iceberg
            """)
            print(f"✓ Created ICEBERG table: {schema_name}.lineorder_iceberg")
            
            # Insert initial seed data for Iceberg (batch insert)
            print("  ↳ Inserting initial seed data (batch mode)...")
            seed_values = []
            insert_start = time.time()
            for i in range(1, 1000001):
                seed_values.append((
                    i, 1, random.randint(1, 1000), random.randint(1, 1000), random.randint(1, 1000),
                    19920101, 19920101, 19920101, '1-URGENT', 0, 'AIR',
                    10.0, 1000.0, 0.05, 0.02, 950.0, 500.0
                ))
            
            execute_values(cursor, f"""
                INSERT INTO {schema_name}.lineorder_iceberg VALUES %s
            """, seed_values, page_size=len(seed_values))  # Iceberg can handle large batches efficiently
            conn.commit()
            insert_end = time.time()
            insertion_times['ICEBERG'] = insert_end - insert_start
            print(f"  ↳ Inserted 1,000,000 seed rows into ICEBERG table in {insertion_times['ICEBERG']:.2f} seconds")
        
        conn.commit()
        cursor.close()
        
    except Exception as e:
        print(f"Error setting up PostgreSQL tables: {e}")
        conn.rollback()
    finally:
        conn.close()
    
    # DuckDB DuckLake table (only if testing)
    if 'ducklake' in TABLES_TO_TEST:
        duckdb_conn = None
        try:
            duckdb_conn = duckdb.connect()
            duckdb_conn.execute(f"""
                ATTACH OR REPLACE 'ducklake:postgres:dbname={ducklake_db} host=localhost' AS ducklake 
                (DATA_PATH '{storage_path}',DATA_INLINING_ROW_LIMIT 10,OVERRIDE_DATA_PATH )
            """)
            duckdb_conn.execute("USE ducklake")
            
            duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            duckdb_conn.execute(f"DROP TABLE IF EXISTS ducklake.{schema_name}.lineorder_ducklake")
            
            duckdb_conn.execute(f"""
                CREATE TABLE ducklake.{schema_name}.lineorder_ducklake (
                    lo_orderkey BIGINT NOT NULL,
                    lo_linenumber INTEGER NOT NULL,
                    lo_custkey INTEGER,
                    lo_partkey INTEGER,
                    lo_suppkey INTEGER,
                    lo_orderdate INTEGER,
                    lo_commitdate INTEGER,
                    lo_shipdate INTEGER,
                    lo_orderpriority VARCHAR(15),
                    lo_shippriority INTEGER,
                    lo_shipmode VARCHAR(10),
                    lo_quantity DECIMAL(15,2),
                    lo_extendedprice DECIMAL(15,2),
                    lo_discount DECIMAL(15,2),
                    lo_tax DECIMAL(15,2),
                    lo_revenue DECIMAL(15,2),
                    lo_supplycost DECIMAL(15,2)
                )
            """)
            print(f"✓ Created DUCKLAKE table: {schema_name}.lineorder_ducklake")
            
            # Insert initial seed data (batch insert via DataFrame)
            print("  ↳ Inserting initial seed data (batch mode)...")
            seed_data = []
            insert_start = time.time()
            for i in range(1, 1000001):
                seed_data.append({
                    'lo_orderkey': i,
                    'lo_linenumber': 1,
                    'lo_custkey': random.randint(1, 1000),
                    'lo_partkey': random.randint(1, 1000),
                    'lo_suppkey': random.randint(1, 1000),
                    'lo_orderdate': 19920101,
                    'lo_commitdate': 19920101,
                    'lo_shipdate': 19920101,
                    'lo_orderpriority': '1-URGENT',
                    'lo_shippriority': 0,
                    'lo_shipmode': 'AIR',
                    'lo_quantity': 10.0,
                    'lo_extendedprice': 1000.0,
                    'lo_discount': 0.05,
                    'lo_tax': 0.02,
                    'lo_revenue': 950.0,
                    'lo_supplycost': 500.0
                })
            
            # Create DataFrame and bulk insert via DuckDB's DataFrame integration
            seed_df = pd.DataFrame(seed_data)
            duckdb_conn.execute(f"""
                INSERT INTO ducklake.{schema_name}.lineorder_ducklake 
                SELECT * FROM seed_df
            """)
            insert_end = time.time()
            insertion_times['DUCKLAKE'] = insert_end - insert_start
            print(f"  ↳ Inserted 1,000,000 seed rows into DUCKLAKE table in {insertion_times['DUCKLAKE']:.2f} seconds")
            
        except Exception as e:
            print(f"Error setting up DuckLake table: {e}")
        finally:
            if duckdb_conn:
                duckdb_conn.close()
    
    return insertion_times

# ============================================================================
# Mixed Operation Functions (INSERT, UPDATE, DELETE)
# ============================================================================

# Static lists for random selection
ORDER_PRIORITIES = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW']
SHIP_MODES = ['AIR', 'MAIL', 'RAIL', 'SHIP', 'TRUCK', 'REG AIR', 'FOB']
SEED_DATA_RANGE = (1, 1000000)  # Range of pre-existing keys for UPDATE/DELETE

def execute_mixed_operations(worker_id, num_operations, start_key, table_name):
    """Execute a mix of INSERT, UPDATE, and DELETE operations from a single worker"""
    print(f"  Worker {worker_id}: Starting operations...")
    # Set seed per worker for deterministic behavior while maintaining some randomness per worker
    random.seed(RANDOM_SEED + worker_id)
    insert_count = 0
    update_count = 0
    delete_count = 0
    errors = {'insert': 0, 'update': 0, 'delete': 0}
    query_durations = []  # Track individual query durations in milliseconds
    
    # Determine if this is a DuckDB table
    is_ducklake = 'ducklake' in table_name.lower()
    
    # Calculate operation distribution
    num_inserts = int(num_operations * OPERATION_MIX['INSERT'])
    num_updates = int(num_operations * OPERATION_MIX['UPDATE'])
    num_deletes = num_operations - num_inserts - num_updates  # Remainder goes to deletes
    
    # Create operation list and shuffle for randomness
    operations = (['INSERT'] * num_inserts + 
                  ['UPDATE'] * num_updates + 
                  ['DELETE'] * num_deletes)
    random.shuffle(operations)
    
    # Pre-compute ALL operation parameters upfront (common for all table formats)
    operation_params = []
    insert_idx = 0
    
    for op in operations:
        if op == 'INSERT':
            lo_extendedprice = round(random.uniform(1000, 101000), 2)
            lo_discount = round(random.uniform(0, 0.10), 2)
            params = {
                'type': 'INSERT',
                'lo_orderkey': start_key + insert_idx,
                'lo_quantity': random.randint(1, 50),
                'lo_extendedprice': lo_extendedprice,
                'lo_discount': lo_discount,
                'lo_tax': round(random.uniform(0, 0.08), 2),
                'lo_revenue': round(lo_extendedprice * (1 - lo_discount), 2),
                'custkey': random.randint(1, 1000),
                'partkey': random.randint(1, 1000),
                'suppkey': random.randint(1, 1000),
                'orderdate': random.randint(19920101, 19981231),
                'commitdate': random.randint(19920101, 19981231),
                'shipdate': random.randint(19920101, 19981231),
                'orderpriority': random.choice(ORDER_PRIORITIES),
                'shippriority': random.randint(0, 9),
                'shipmode': random.choice(SHIP_MODES),
                'supplycost': round(random.uniform(0, 50000), 2)
            }
            insert_idx += 1
        elif op == 'UPDATE':
            new_price = round(random.uniform(1000, 101000), 2)
            new_discount = round(random.uniform(0, 0.10), 2)
            params = {
                'type': 'UPDATE',
                'target_key': random.randint(*SEED_DATA_RANGE),
                'new_quantity': random.randint(1, 50),
                'new_price': new_price,
                'new_discount': new_discount,
                'new_revenue': round(new_price * (1 - new_discount), 2)
            }
        elif op == 'DELETE':
            params = {
                'type': 'DELETE',
                'target_key': random.randint(*SEED_DATA_RANGE)
            }
        
        operation_params.append(params)
    
    if is_ducklake:
        # Create per-process DuckDB connection
        duckdb_conn = None
        try:
            print(f"  Worker {worker_id}: Connecting to DuckDB...")
            duckdb_conn = duckdb.connect()
            duckdb_conn.execute(f"""
                ATTACH OR REPLACE 'ducklake:postgres:dbname={ducklake_db} host=localhost' AS ducklake 
                (DATA_PATH '{storage_path}',DATA_INLINING_ROW_LIMIT 10,OVERRIDE_DATA_PATH )
            """)
            duckdb_conn.execute("USE ducklake")
            # No internal retries - let application handle conflicts
            duckdb_conn.execute("SET ducklake_max_retry_count = 30 ;")
            print(f"  Worker {worker_id}: Connected to DuckDB, executing {num_operations} operations...")
            
            # Execute pre-computed operations
            progress_interval = max(1, num_operations // 10)  # Print progress every 10%
            for idx, params in enumerate(operation_params):
                # Print progress
                if idx > 0 and idx % progress_interval == 0:
                    pct = (idx / num_operations) * 100
                    print(f"  Worker {worker_id}: {pct:.0f}% complete ({idx}/{num_operations} ops, I:{insert_count} U:{update_count} D:{delete_count} E:{sum(errors.values())})")
                
                try:
                    if params['type'] == 'INSERT':
                        query_start = time.time()
                        duckdb_conn.execute(f"""
                            INSERT INTO {table_name} VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            params['lo_orderkey'],
                            params['custkey'],
                            params['partkey'],
                            params['suppkey'],
                            params['orderdate'],
                            params['commitdate'],
                            params['shipdate'],
                            params['orderpriority'],
                            params['shippriority'],
                            params['shipmode'],
                            params['lo_quantity'],
                            params['lo_extendedprice'],
                            params['lo_discount'],
                            params['lo_tax'],
                            params['lo_revenue'],
                            params['supplycost']
                        ))
                        query_durations.append((time.time() - query_start) * 1000)
                        insert_count += 1
                    
                    elif params['type'] == 'UPDATE':
                        query_start = time.time()
                        duckdb_conn.execute(f"""
                            UPDATE {table_name}
                            SET lo_quantity = ?, 
                                lo_extendedprice = ?, 
                                lo_discount = ?,
                                lo_revenue = ?
                            WHERE lo_orderkey = ? AND lo_linenumber = 1
                        """, (params['new_quantity'], params['new_price'], params['new_discount'], 
                              params['new_revenue'], params['target_key']))
                        query_durations.append((time.time() - query_start) * 1000)
                        update_count += 1
                    
                    elif params['type'] == 'DELETE':
                        query_start = time.time()
                        duckdb_conn.execute(f"""
                            DELETE FROM {table_name}
                            WHERE lo_orderkey = ? AND lo_linenumber = 1
                        """, (params['target_key'],))
                        query_durations.append((time.time() - query_start) * 1000)
                        delete_count += 1
                    
                except Exception as e:
                    if params['type'] == 'INSERT':
                        errors['insert'] += 1
                    elif params['type'] == 'UPDATE':
                        errors['update'] += 1
                    elif params['type'] == 'DELETE':
                        errors['delete'] += 1
                    print(f"Worker {worker_id} {params['type']} error: {e}")
            
        except Exception as e:
            print(f"Worker {worker_id} DuckDB connection error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if duckdb_conn:
                duckdb_conn.close()
    else:
        # Create per-process PostgreSQL connection
        print(f"  Worker {worker_id}: Connecting to PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            cursor = conn.cursor()
            print(f"  Worker {worker_id}: Connected to PostgreSQL, executing {num_operations} operations...")
            
            # Execute pre-computed operations
            progress_interval = max(1, num_operations // 10)  # Print progress every 10%
            for idx, params in enumerate(operation_params):
                # Print progress
                if idx > 0 and idx % progress_interval == 0:
                    pct = (idx / num_operations) * 100
                    print(f"  Worker {worker_id}: {pct:.0f}% complete ({idx}/{num_operations} ops, I:{insert_count} U:{update_count} D:{delete_count} E:{sum(errors.values())})")
                
                try:
                    if params['type'] == 'INSERT':
                        query_start = time.time()
                        cursor.execute(f"""
                            INSERT INTO {table_name} (
                                lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey,
                                lo_orderdate, lo_commitdate, lo_shipdate,
                                lo_orderpriority, lo_shippriority, lo_shipmode,
                                lo_quantity, lo_extendedprice, lo_discount, lo_tax,
                                lo_revenue, lo_supplycost
                            ) VALUES (
                                %s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """, (
                            params['lo_orderkey'],
                            params['custkey'],
                            params['partkey'],
                            params['suppkey'],
                            params['orderdate'],
                            params['commitdate'],
                            params['shipdate'],
                            params['orderpriority'],
                            params['shippriority'],
                            params['shipmode'],
                            params['lo_quantity'],
                            params['lo_extendedprice'],
                            params['lo_discount'],
                            params['lo_tax'],
                            params['lo_revenue'],
                            params['supplycost']
                        ))
                        conn.commit()
                        query_durations.append((time.time() - query_start) * 1000)
                        insert_count += 1
                        
                    elif params['type'] == 'UPDATE':
                        query_start = time.time()
                        cursor.execute(f"""
                            UPDATE {table_name}
                            SET lo_quantity = %s, 
                                lo_extendedprice = %s, 
                                lo_discount = %s,
                                lo_revenue = %s
                            WHERE lo_orderkey = %s AND lo_linenumber = 1
                        """, (params['new_quantity'], params['new_price'], params['new_discount'], 
                              params['new_revenue'], params['target_key']))
                        conn.commit()
                        query_durations.append((time.time() - query_start) * 1000)
                        update_count += 1
                        
                    elif params['type'] == 'DELETE':
                        query_start = time.time()
                        cursor.execute(f"""
                            DELETE FROM {table_name}
                            WHERE lo_orderkey = %s AND lo_linenumber = 1
                        """, (params['target_key'],))
                        conn.commit()
                        query_durations.append((time.time() - query_start) * 1000)
                        delete_count += 1
                        
                except Exception as e:
                    conn.rollback()
                    if params['type'] == 'INSERT':
                        errors['insert'] += 1
                    elif params['type'] == 'UPDATE':
                        errors['update'] += 1
                    elif params['type'] == 'DELETE':
                        errors['delete'] += 1
                    # Uncomment for debugging:
                    # print(f"Worker {worker_id} {params['type']} error: {e}")
            
            cursor.close()
            
        except Exception as e:
            print(f"Worker {worker_id} PostgreSQL error: {e}")
        finally:
            conn.close()
    
    print(f"  Worker {worker_id}: Completed! I:{insert_count} U:{update_count} D:{delete_count} Errors:{sum(errors.values())}")
    return worker_id, insert_count, update_count, delete_count, errors, query_durations

# ============================================================================
# Run Concurrent Mixed Operation Tests
# ============================================================================

def run_concurrent_test(num_workers, ops_per_worker, table_name, table_type):
    """Run a test with specified number of concurrent workers performing mixed operations"""
    print(f"\n{'='*70}")
    print(f"Testing {table_type} with {num_workers} workers...")
    print(f"Operation Mix: {OPERATION_MIX['INSERT']*100:.0f}% INSERT, "
          f"{OPERATION_MIX['UPDATE']*100:.0f}% UPDATE, "
          f"{OPERATION_MIX['DELETE']*100:.0f}% DELETE")
    print(f"{'='*70}")
    
    # Generate deterministic starting keys for each worker (not based on time)
    base_key = 10000000  # Fixed base key for deterministic results
    
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for worker_id in range(num_workers):
            start_key = base_key + (worker_id * ops_per_worker)
            future = executor.submit(execute_mixed_operations, worker_id, ops_per_worker, start_key, table_name)
            futures.append(future)
        
        # Wait for all workers to complete
        results = []
        for future in as_completed(futures):
            results.append(future.result())
    
    # Checkpoint after all workers finish (DuckLake only)
    if 'ducklake' in table_name.lower():
        checkpoint_conn = None
        try:
            checkpoint_conn = duckdb.connect()
            checkpoint_conn.execute(f"""
                ATTACH OR REPLACE 'ducklake:postgres:dbname={ducklake_db} host=localhost' AS ducklake 
                (DATA_PATH '{storage_path}',DATA_INLINING_ROW_LIMIT 10,OVERRIDE_DATA_PATH )
            """)
            checkpoint_conn.execute("USE ducklake")
            checkpoint_conn.execute("CHECKPOINT;")
        except Exception as e:
            print(f"  Warning: Checkpoint failed: {e}")
        finally:
            if checkpoint_conn:
                checkpoint_conn.close()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    total_inserts = sum(r[1] for r in results)
    total_updates = sum(r[2] for r in results)
    total_deletes = sum(r[3] for r in results)
    total_operations = total_inserts + total_updates + total_deletes
    
    total_errors = sum(sum(r[4].values()) for r in results)
    
    # Collect error breakdown by operation type
    insert_errors = sum(r[4]['insert'] for r in results)
    update_errors = sum(r[4]['update'] for r in results)
    delete_errors = sum(r[4]['delete'] for r in results)
    
    # Collect all query durations and calculate p99
    all_durations = []
    for r in results:
        all_durations.extend(r[5])
    p99_latency = np.percentile(all_durations, 99) if all_durations else 0
    
    ops_per_second = total_operations / elapsed_time if elapsed_time > 0 else 0
    
    print(f"  ✓ Inserts: {total_inserts} rows")
    print(f"  ↻ Updates: {total_updates} rows")
    print(f"  ✗ Deletes: {total_deletes} rows")
    print(f"  Total Operations: {total_operations}")
    print(f"  Errors: {total_errors} (INSERT: {insert_errors}, UPDATE: {update_errors}, DELETE: {delete_errors})")
    print(f"  ⏱ Time: {elapsed_time:.2f} seconds")
    print(f"  ⚡ Throughput: {ops_per_second:.2f} operations/second")
    print(f"  📊 P99 Latency: {p99_latency:.2f} ms")
    
    return {
        'table_type': table_type,
        'workers': num_workers,
        'inserts': total_inserts,
        'updates': total_updates,
        'deletes': total_deletes,
        'total_operations': total_operations,
        'errors': total_errors,
        'elapsed_time': elapsed_time,
        'ops_per_second': ops_per_second,
        'p99_latency_ms': p99_latency
    }

# ============================================================================
# Main Execution
# ============================================================================

if __name__ == '__main__':
    # Create tables with seed data
    print("\n" + "="*70)
    print("SETUP: Creating tables and inserting seed data...")
    print("="*70)
    insertion_times = setup_tables()
    
    # Run tests on selected tables
    all_results = []
    
    # Table configuration mapping
    table_configs = {
        'postgres': (f'{schema_name}.lineorder_postgres', 'POSTGRES'),
        'ducklake': (f'ducklake.{schema_name}.lineorder_ducklake', 'DUCKLAKE'),
        'iceberg': (f'{schema_name}.lineorder_iceberg', 'ICEBERG')
    }
    
    for num_workers in NUM_WORKERS:
        for table_key in TABLES_TO_TEST:
            if table_key in table_configs:
                table_name, table_type = table_configs[table_key]
                result = run_concurrent_test(
                    num_workers, 
                    OPERATIONS_PER_WORKER, 
                    table_name,
                    table_type
                )
                all_results.append(result)
                time.sleep(1)  # Brief pause between tests
        
        time.sleep(1)  # Pause before next concurrency level
      
    print("\n" + "="*70)
    print("All tests completed!")
    print("="*70)
    
    # ========================================================================
    # Create Visualization
    # ========================================================================
    
    # Prepare data for plotting
    df = pd.DataFrame(all_results)
    
    # Create figure with two plots (1 row, 2 columns)
    fig, ax1 = plt.subplots(1, 1, figsize=(10, 6))
    fig.suptitle(f'Concurrent Mixed Operations Performance Comparison (Local Storage)\n'
                 f'Initial seed: 1,000,000 rows | {OPERATIONS_PER_WORKER} operations per worker: '
                 f'{OPERATION_MIX["INSERT"]*100:.0f}% INSERT, '
                 f'{OPERATION_MIX["UPDATE"]*100:.0f}% UPDATE, '
                 f'{OPERATION_MIX["DELETE"]*100:.0f}% DELETE', 
                 fontsize=14, fontweight='bold')
    
    # Define colors
    colors = {'POSTGRES': '#2196F3', 'DUCKLAKE': '#4CAF50', 'ICEBERG': '#FF9800'}
    
    # Get unique worker counts and table types
    worker_counts = sorted(df['workers'].unique())
    table_types = df['table_type'].unique()
    
    # Set up bar positions
    x = range(len(worker_counts))
    width = 0.25
    offsets = {0: -width, 1: 0, 2: width}
    
    # Plot: P99 Latency (lower is better)
    for idx, table_type in enumerate(table_types):
        data = df[df['table_type'] == table_type].sort_values('workers')
        bars = ax1.bar([pos + offsets[idx] for pos in x], 
               data['p99_latency_ms'],
               width=width,
               label=table_type,
               color=colors.get(table_type, '#999999'))
        
        # Add value labels on top of bars
        for bar, value in zip(bars, data['p99_latency_ms']):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{value:.1f}',
                    ha='center', va='bottom', fontsize=9)
    
    ax1.set_xlabel('Number of Workers', fontsize=12)
    ax1.set_ylabel('P99 Latency (milliseconds)', fontsize=12)
    ax1.set_title('P99 Query Duration & Conflicts (Lower is Better)', fontsize=13, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(worker_counts)
    ax1.legend(loc='upper left', fontsize=11)
    ax1.grid(True, alpha=0.3, linestyle='--', axis='y')
    
    # Add secondary y-axis for errors/conflicts (only for non-PostgreSQL tables)
    ax2 = ax1.twinx()
    
    # Plot errors as line chart with markers (skip PostgreSQL)
    for idx, table_type in enumerate(table_types):
        if table_type.upper() == 'POSTGRES':
            continue  # Skip PostgreSQL - it has zero conflicts
            
        data = df[df['table_type'] == table_type].sort_values('workers')
        line = ax2.plot([pos + offsets[idx] for pos in x], 
               data['errors'],
               marker='X',
               markersize=10,
               linewidth=2,
               linestyle='--',
               alpha=0.7,
               color=colors.get(table_type, '#999999'),
               label=f'{table_type} Errors')
        
        # Add error count labels
        for pos_idx, (pos, value) in enumerate(zip([pos + offsets[idx] for pos in x], data['errors'])):
            if value > 0:  # Only show label if there are errors
                ax2.text(pos, value, f'{int(value)}',
                        ha='center', va='bottom', fontsize=9, 
                        fontweight='bold', color='red')
    
    ax2.set_ylabel('Transaction Conflicts ', fontsize=12, color='red')
    ax2.tick_params(axis='y', labelcolor='red')
    ax2.legend(loc='upper right', fontsize=10)
    
    plt.tight_layout()
    
    # Display the plot
    plt.show()
    
    # Print summary table
    print("\n" + "="*110)
    print("DETAILED SUMMARY TABLE")
    print("="*110)
    summary_df = df[['table_type', 'workers', 'inserts', 'updates', 'deletes', 
                     'total_operations', 'errors', 'elapsed_time', 'ops_per_second', 'p99_latency_ms']]
    print(summary_df.to_string(index=False))
    print("="*110)
    
    # Print performance comparison
    print("\n" + "="*100)
    print("PERFORMANCE COMPARISON (Operations per Second)")
    print("="*100)
    pivot_df = df.pivot(index='workers', columns='table_type', values='ops_per_second')
    print(pivot_df.to_string())
    print("="*100)
    
    # ========================================================================
    # Data Validation: Verify Final Table State
    # ========================================================================
    print("\n" + "="*100)
    print("DATA VALIDATION: Comparing Final Table States")
    print("="*100)
    
    # Get final counts from PostgreSQL
    pg_conn = psycopg2.connect(**DB_CONFIG)
    pg_cursor = pg_conn.cursor()
    
    if 'postgres' in TABLES_TO_TEST:
        pg_cursor.execute(f"SELECT COUNT(*), SUM(lo_orderkey) FROM {schema_name}.lineorder_postgres")
        pg_count, pg_sum = pg_cursor.fetchone()
        print(f"\nPostgreSQL Table:")
        print(f"  Total Rows: {pg_count:,}")
        print(f"  Sum of lo_orderkey: {pg_sum:,}")
    
    pg_cursor.close()
    pg_conn.close()
    
    # Get final counts from DuckLake
    if 'ducklake' in TABLES_TO_TEST:
        duck_conn = None
        try:
            duck_conn = duckdb.connect()
            duck_conn.execute(f"""
                ATTACH OR REPLACE 'ducklake:postgres:dbname={ducklake_db} host=localhost' AS ducklake 
                (DATA_PATH '{storage_path}',DATA_INLINING_ROW_LIMIT 10,OVERRIDE_DATA_PATH )
            """)
            duck_conn.execute("USE ducklake")
            
            result = duck_conn.execute(f"SELECT COUNT(*), SUM(lo_orderkey) FROM {schema_name}.lineorder_ducklake").fetchone()
            dl_count, dl_sum = result
            print(f"\nDuckLake Table:")
            print(f"  Total Rows: {dl_count:,}")
            print(f"  Sum of lo_orderkey: {dl_sum:,}")
        finally:
            if duck_conn:
                duck_conn.close()
    
    # Compare and explain differences
    if 'postgres' in TABLES_TO_TEST and 'ducklake' in TABLES_TO_TEST:
        print(f"\n{'='*100}")
        print("COMPARISON ANALYSIS:")
        print(f"{'='*100}")
        
        print(f"\nRow Counts:")
        print(f"  PostgreSQL: {pg_count:,}")
        print(f"  DuckLake:   {dl_count:,}")
        print(f"  Difference: {abs(pg_count - dl_count):,}")
        
        print(f"\nSum of lo_orderkey (deterministic column):")
        print(f"  PostgreSQL: {pg_sum:,}")
        print(f"  DuckLake:   {dl_sum:,}")
        print(f"  Match: {'✓ YES' if pg_sum == dl_sum else '✗ NO - DATA LOST OR INCONSISTENT'}")
        
        total_ducklake_errors = df[df['table_type'] == 'DUCKLAKE']['errors'].sum()
        print(f"\nDuckLake Errors: {total_ducklake_errors:,}")
        
        if pg_sum != dl_sum:
            print(f"\n⚠️  WARNING: Sum mismatch indicates data inconsistency!")
            print(f"   DuckLake is losing or corrupting data due to transaction conflicts.")
    
    print("="*100)