import psycopg2
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import random
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import duckdb

# ============================================================================
# Database Connection Configuration
# ============================================================================

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'port': 5432
}


s3 = 's3'
storage_path = 's3://myduck/duck/'

# Test configuration
NUM_WORKERS = [1,8]
OPERATIONS_PER_WORKER = 100 # Total operations (INSERT + UPDATE + DELETE)
OPERATION_MIX = {
    'INSERT': 0.60,  # 60% inserts
    'UPDATE': 0.25,  # 25% updates
    'DELETE': 0.15   # 15% deletes
}
TABLES_TO_TEST = [
    #'postgres',
    'iceberg',
    'ducklake'
]

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
    
    # PostgreSQL tables
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        
        # Create schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS test_concurrent")
        
        # Drop existing tables if they exist (only if they will be tested)
        if 'postgres' in TABLES_TO_TEST:
            cursor.execute("DROP TABLE IF EXISTS test_concurrent.lineorder_postgres CASCADE")
        if 'iceberg' in TABLES_TO_TEST:
            cursor.execute("DROP TABLE IF EXISTS test_concurrent.lineorder_iceberg CASCADE")
        
        # Create POSTGRES table (only if testing)
        if 'postgres' in TABLES_TO_TEST:
            cursor.execute("""
                CREATE TABLE test_concurrent.lineorder_postgres (
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
                lo_supplycost NUMERIC(15,2),
                PRIMARY KEY (lo_orderkey, lo_linenumber)
            )
        """)
            print("✓ Created POSTGRES table: test_concurrent.lineorder_postgres")
            
            # Insert initial seed data for UPDATE and DELETE operations (batch insert)
            print("  ↳ Inserting initial seed data (batch mode)...")
            seed_values = []
            for i in range(1, 50001):  # Prepare 50,000 initial rows
                seed_values.append((
                    i, 1, random.randint(1, 1000), random.randint(1, 1000), random.randint(1, 1000),
                    19920101, 19920101, 19920101, '1-URGENT', 0, 'AIR',
                    10.0, 1000.0, 0.05, 0.02, 950.0, 500.0
                ))
            
            # Use execute_values for bulk insert
            execute_values(cursor, """
                INSERT INTO test_concurrent.lineorder_postgres VALUES %s
            """, seed_values)
            conn.commit()
            print(f"  ↳ Inserted 50,000 seed rows into POSTGRES table")
        
        # Create ICEBERG table (only if testing)
        if 'iceberg' in TABLES_TO_TEST:
            cursor.execute("""
            CREATE TABLE test_concurrent.lineorder_iceberg (
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
            print("✓ Created ICEBERG table: test_concurrent.lineorder_iceberg")
            
            # Insert initial seed data for Iceberg (batch insert)
            print("  ↳ Inserting initial seed data (batch mode)...")
            seed_values = []
            for i in range(1, 50001):
                seed_values.append((
                    i, 1, random.randint(1, 1000), random.randint(1, 1000), random.randint(1, 1000),
                    19920101, 19920101, 19920101, '1-URGENT', 0, 'AIR',
                    10.0, 1000.0, 0.05, 0.02, 950.0, 500.0
                ))
            
            execute_values(cursor, """
                INSERT INTO test_concurrent.lineorder_iceberg VALUES %s
            """, seed_values)
            conn.commit()
            print(f"  ↳ Inserted 50,000 seed rows into ICEBERG table")
        
        conn.commit()
        cursor.close()
        
    except Exception as e:
        print(f"Error setting up PostgreSQL tables: {e}")
        conn.rollback()
    finally:
        conn.close()
    
    # DuckDB DuckLake table (only if testing)
    if 'ducklake' in TABLES_TO_TEST:
        try:
            duckdb_conn = duckdb.connect()
            duckdb_conn.execute(f"""
                ATTACH OR REPLACE 'ducklake:postgres:dbname={s3} host=localhost' AS ducklake 
                (DATA_PATH '{storage_path}',DATA_INLINING_ROW_LIMIT 10,OVERRIDE_DATA_PATH )
            """)
            duckdb_conn.execute("USE ducklake")
            
            duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS test_concurrent")
            duckdb_conn.execute("DROP TABLE IF EXISTS ducklake.test_concurrent.lineorder_ducklake")
            
            duckdb_conn.execute("""
                CREATE TABLE ducklake.test_concurrent.lineorder_ducklake (
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
            print("✓ Created DUCKLAKE table: test_concurrent.lineorder_ducklake")
            
            # Insert initial seed data (batch insert via DataFrame)
            print("  ↳ Inserting initial seed data (batch mode)...")
            seed_data = []
            for i in range(1, 50001):
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
            duckdb_conn.execute("""
                INSERT INTO ducklake.test_concurrent.lineorder_ducklake 
                SELECT * FROM seed_df
            """)
            print(f"  ↳ Inserted 50,000 seed rows into DUCKLAKE table")
            duckdb_conn.close()
            
        except Exception as e:
            print(f"Error setting up DuckLake table: {e}")

# ============================================================================
# Mixed Operation Functions (INSERT, UPDATE, DELETE)
# ============================================================================

# Static lists for random selection
ORDER_PRIORITIES = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW']
SHIP_MODES = ['AIR', 'MAIL', 'RAIL', 'SHIP', 'TRUCK', 'REG AIR', 'FOB']
SEED_DATA_RANGE = (1, 50000)  # Range of pre-existing keys for UPDATE/DELETE

def execute_mixed_operations(worker_id, num_operations, start_key, table_name):
    """Execute a mix of INSERT, UPDATE, and DELETE operations from a single worker"""
    insert_count = 0
    update_count = 0
    delete_count = 0
    errors = {'insert': 0, 'update': 0, 'delete': 0}
    
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
    
    if is_ducklake:
        # Create per-process DuckDB connection
        duckdb_conn = duckdb.connect()
        try:
            duckdb_conn.execute(f"""
                ATTACH OR REPLACE 'ducklake:postgres:dbname={s3} host=localhost' AS ducklake 
                (DATA_PATH '{storage_path}',DATA_INLINING_ROW_LIMIT 10,OVERRIDE_DATA_PATH )
            """)
            duckdb_conn.execute("USE ducklake")
            duckdb_conn.execute("SET ducklake_max_retry_count = 100;")
            
            insert_idx = 0
            for op in operations:
                try:
                    if op == 'INSERT':
                        lo_orderkey = start_key + insert_idx
                        insert_idx += 1
                        lo_quantity = random.randint(1, 50)
                        lo_extendedprice = round(random.uniform(1000, 101000), 2)
                        lo_discount = round(random.uniform(0, 0.10), 2)
                        lo_tax = round(random.uniform(0, 0.08), 2)
                        lo_revenue = round(lo_extendedprice * (1 - lo_discount), 2)
                        
                        duckdb_conn.execute(f"""
                            INSERT INTO {table_name} VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            lo_orderkey,
                            random.randint(1, 1000),
                            random.randint(1, 1000),
                            random.randint(1, 1000),
                            random.randint(19920101, 19981231),
                            random.randint(19920101, 19981231),
                            random.randint(19920101, 19981231),
                            random.choice(ORDER_PRIORITIES),
                            random.randint(0, 9),
                            random.choice(SHIP_MODES),
                            lo_quantity,
                            lo_extendedprice,
                            lo_discount,
                            lo_tax,
                            lo_revenue,
                            round(random.uniform(0, 50000), 2)
                        ))
                        insert_count += 1
                        
                    elif op == 'UPDATE':
                        # Update a random existing row from seed data
                        target_key = random.randint(*SEED_DATA_RANGE)
                        new_quantity = random.randint(1, 50)
                        new_price = round(random.uniform(1000, 101000), 2)
                        new_discount = round(random.uniform(0, 0.10), 2)
                        
                        duckdb_conn.execute(f"""
                            UPDATE {table_name}
                            SET lo_quantity = ?, 
                                lo_extendedprice = ?, 
                                lo_discount = ?,
                                lo_revenue = ?
                            WHERE lo_orderkey = ? AND lo_linenumber = 1
                        """, (new_quantity, new_price, new_discount, 
                             round(new_price * (1 - new_discount), 2), target_key))
                        update_count += 1
                        
                    elif op == 'DELETE':
                        # Delete a random existing row from seed data
                        target_key = random.randint(*SEED_DATA_RANGE)
                        duckdb_conn.execute(f"""
                            DELETE FROM {table_name}
                            WHERE lo_orderkey = ? AND lo_linenumber = 1
                        """, (target_key,))
                        delete_count += 1
                        
                except Exception as e:
                    if op == 'INSERT':
                        errors['insert'] += 1
                    elif op == 'UPDATE':
                        errors['update'] += 1
                    elif op == 'DELETE':
                        errors['delete'] += 1
                    # Uncomment for debugging:
                    print(f"Worker {worker_id} {op} error: {e}")
            
            # Close connection (checkpoint happens after all workers finish)
            duckdb_conn.close()
            
        except Exception as e:
            print(f"Worker {worker_id} DuckDB connection error: {e}")
            import traceback
            traceback.print_exc()
    else:
        # Create per-process PostgreSQL connection
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            cursor = conn.cursor()
            
            insert_idx = 0
            for op in operations:
                try:
                    if op == 'INSERT':
                        lo_orderkey = start_key + insert_idx
                        insert_idx += 1
                        lo_quantity = random.randint(1, 50)
                        lo_extendedprice = round(random.uniform(1000, 101000), 2)
                        lo_discount = round(random.uniform(0, 0.10), 2)
                        lo_tax = round(random.uniform(0, 0.08), 2)
                        lo_revenue = round(lo_extendedprice * (1 - lo_discount), 2)
                        
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
                            lo_orderkey,
                            random.randint(1, 1000),
                            random.randint(1, 1000),
                            random.randint(1, 1000),
                            random.randint(19920101, 19981231),
                            random.randint(19920101, 19981231),
                            random.randint(19920101, 19981231),
                            random.choice(ORDER_PRIORITIES),
                            random.randint(0, 9),
                            random.choice(SHIP_MODES),
                            lo_quantity,
                            lo_extendedprice,
                            lo_discount,
                            lo_tax,
                            lo_revenue,
                            round(random.uniform(0, 50000), 2)
                        ))
                        conn.commit()
                        insert_count += 1
                        
                    elif op == 'UPDATE':
                        # Update a random existing row from seed data
                        target_key = random.randint(*SEED_DATA_RANGE)
                        new_quantity = random.randint(1, 50)
                        new_price = round(random.uniform(1000, 101000), 2)
                        new_discount = round(random.uniform(0, 0.10), 2)
                        
                        cursor.execute(f"""
                            UPDATE {table_name}
                            SET lo_quantity = %s, 
                                lo_extendedprice = %s, 
                                lo_discount = %s,
                                lo_revenue = %s
                            WHERE lo_orderkey = %s AND lo_linenumber = 1
                        """, (new_quantity, new_price, new_discount, 
                             round(new_price * (1 - new_discount), 2), target_key))
                        conn.commit()
                        update_count += 1
                        
                    elif op == 'DELETE':
                        # Delete a random existing row from seed data
                        target_key = random.randint(*SEED_DATA_RANGE)
                        cursor.execute(f"""
                            DELETE FROM {table_name}
                            WHERE lo_orderkey = %s AND lo_linenumber = 1
                        """, (target_key,))
                        conn.commit()
                        delete_count += 1
                        
                except Exception as e:
                    conn.rollback()
                    if op == 'INSERT':
                        errors['insert'] += 1
                    elif op == 'UPDATE':
                        errors['update'] += 1
                    elif op == 'DELETE':
                        errors['delete'] += 1
                    # Uncomment for debugging:
                    # print(f"Worker {worker_id} {op} error: {e}")
            
            cursor.close()
            
        except Exception as e:
            print(f"Worker {worker_id} PostgreSQL error: {e}")
        finally:
            conn.close()
    
    return worker_id, insert_count, update_count, delete_count, errors

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
    
    # Generate unique starting keys for each worker
    base_key = int(time.time() * 1000) + 10000  # Start after seed data
    
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
        try:
            checkpoint_conn = duckdb.connect()
            checkpoint_conn.execute(f"""
                ATTACH OR REPLACE 'ducklake:postgres:dbname={s3} host=localhost' AS ducklake 
                (DATA_PATH '{storage_path}',DATA_INLINING_ROW_LIMIT 10,OVERRIDE_DATA_PATH )
            """)
            checkpoint_conn.execute("USE ducklake")
            checkpoint_conn.execute("CHECKPOINT;")
            checkpoint_conn.close()
        except Exception as e:
            print(f"  Warning: Checkpoint failed: {e}")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    total_inserts = sum(r[1] for r in results)
    total_updates = sum(r[2] for r in results)
    total_deletes = sum(r[3] for r in results)
    total_operations = total_inserts + total_updates + total_deletes
    
    total_errors = sum(sum(r[4].values()) for r in results)
    
    ops_per_second = total_operations / elapsed_time if elapsed_time > 0 else 0
    
    print(f"  ✓ Inserts: {total_inserts} rows")
    print(f"  ↻ Updates: {total_updates} rows")
    print(f"  ✗ Deletes: {total_deletes} rows")
    print(f"  Total Operations: {total_operations}")
    print(f"  Errors: {total_errors}")
    print(f"  ⏱ Time: {elapsed_time:.2f} seconds")
    print(f"  ⚡ Throughput: {ops_per_second:.2f} operations/second")
    
    return {
        'table_type': table_type,
        'workers': num_workers,
        'inserts': total_inserts,
        'updates': total_updates,
        'deletes': total_deletes,
        'total_operations': total_operations,
        'errors': total_errors,
        'elapsed_time': elapsed_time,
        'ops_per_second': ops_per_second
    }

# ============================================================================
# Main Execution
# ============================================================================

if __name__ == '__main__':
    # Create tables with seed data
    print("\n" + "="*70)
    print("SETUP: Creating tables and inserting seed data...")
    print("="*70)
    setup_tables()
    
    # Run tests on selected tables
    all_results = []
    
    # Table configuration mapping
    table_configs = {
        'postgres': ('test_concurrent.lineorder_postgres', 'POSTGRES'),
        'ducklake': ('ducklake.test_concurrent.lineorder_ducklake', 'DUCKLAKE'),
        'iceberg': ('test_concurrent.lineorder_iceberg', 'ICEBERG')
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
    
    # Create figure with single plot
    fig, ax1 = plt.subplots(1, 1, figsize=(10, 6))
    fig.suptitle(f'Concurrent Mixed Operations Performance Comparison (Local Storage)\n'
                 f'{OPERATIONS_PER_WORKER} operations per worker: '
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
    
    # Plot: Overall Throughput
    for idx, table_type in enumerate(table_types):
        data = df[df['table_type'] == table_type].sort_values('workers')
        bars = ax1.bar([pos + offsets[idx] for pos in x], 
               data['ops_per_second'],
               width=width,
               label=table_type,
               color=colors.get(table_type, '#999999'))
        
        # Add value labels on top of bars
        for bar, value in zip(bars, data['ops_per_second']):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{value:.1f}',
                    ha='center', va='bottom', fontsize=9)
    
    ax1.set_xlabel('Number of Workers', fontsize=12)
    ax1.set_ylabel('Operations per Second', fontsize=12)
    ax1.set_title('Overall Throughput', fontsize=13, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(worker_counts)
    ax1.legend(fontsize=11)
    ax1.grid(True, alpha=0.3, linestyle='--', axis='y')
    
    plt.tight_layout()
    
    # Display the plot
    plt.show()
    
    # Print summary table
    print("\n" + "="*100)
    print("DETAILED SUMMARY TABLE")
    print("="*100)
    summary_df = df[['table_type', 'workers', 'inserts', 'updates', 'deletes', 
                     'total_operations', 'errors', 'elapsed_time', 'ops_per_second']]
    print(summary_df.to_string(index=False))
    print("="*100)
    
    # Print performance comparison
    print("\n" + "="*100)
    print("PERFORMANCE COMPARISON (Operations per Second)")
    print("="*100)
    pivot_df = df.pivot(index='workers', columns='table_type', values='ops_per_second')
    print(pivot_df.to_string())
    print("="*100)

