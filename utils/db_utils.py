import duckdb
import pandas as pd
from datetime import datetime, timedelta

def f_load_to_landing(vCon, dfData, vSchema, vTable):
    """
    Loads data into the Landing Layer (Overwrite/Replace).
    Landing is always a snapshot of the latest extraction.
    """
    full_table_name = f"{vSchema}.{vTable}"
    print(f"Loading {len(dfData)} rows to Landing: {full_table_name}")
    
    # Create Schema
    vCon.sql(f"CREATE SCHEMA IF NOT EXISTS {vSchema}")
    
    # Register view
    vCon.register('v_landing_buffer', dfData)
    
    # Create or Replace (Landing is ephemeral)
    vCon.sql(f"CREATE OR REPLACE TABLE {full_table_name} AS SELECT * FROM v_landing_buffer")

def f_process_scd2(vCon, vConfigDict):
    """
    Executes a metadata-driven SCD Type 2 Merge.
    Ends previous records with Yesterday's date. Starts new records with Today.
    """
    src_db = "MovieReleases" 
    src_schema = vConfigDict['source_schema']
    src_table = vConfigDict['source_table']
    tgt_schema = vConfigDict['target_schema']
    tgt_table = vConfigDict['target_table']
    key = vConfigDict['merge_key']
    
    fq_source = f"{src_db}.{src_schema}.{src_table}"
    fq_target = f"{src_db}.{tgt_schema}.{tgt_table}"
    
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"--- Processing SCD2: {fq_source} -> {fq_target} ---")
    
    # 1. Ensure Target Exists (Initialize History if empty)
    vCon.sql(f"CREATE SCHEMA IF NOT EXISTS {src_db}.{tgt_schema}")
    
    try:
        vCon.sql(f"DESCRIBE {fq_target}")
    except:
        print(f"Target {fq_target} does not exist. Initializing from Source...")
        vCon.sql(f"""
            CREATE TABLE {fq_target} AS 
            SELECT *, 
                   CAST('{today}' AS DATE) as valid_from_uda, 
                   CAST(NULL AS DATE) as valid_to_uda,
                   CAST(1 AS BOOLEAN) as is_current_uda
            FROM {fq_source}
        """)
        print("Initialization Complete.")
        return

    # 2. Detect Changes
    # Get list of data columns (exclude meta columns)
    cols = [row[0] for row in vCon.sql(f"DESCRIBE {fq_source}").fetchall()]
    
    # Helper to build hashed string with explicit table alias to avoid ambiguity
    def build_hash_logic(alias):
        # Exclude the primary key from the hash, check content columns
        content_cols = [c for c in cols if c != key]
        # Construct: COALESCE(CAST(s.col AS VARCHAR), '') || ...
        # This explicitly prefixes the alias (s. or t.) to fix the Ambiguous reference error
        concat_str = " || ".join([f"COALESCE(CAST({alias}.{c} AS VARCHAR), '')" for c in content_cols])
        return f"md5({concat_str})"
    
    # 3. Perform the Updates (Close old records)
    # Update target where key matches AND content differs AND is currently active
    update_sql = f"""
        UPDATE {fq_target}
        SET valid_to_uda = CAST('{yesterday}' AS DATE),
            is_current_uda = FALSE
        WHERE is_current_uda = TRUE
        AND {key} IN (
            SELECT s.{key}
            FROM {fq_source} s
            JOIN {fq_target} t ON s.{key} = t.{key}
            WHERE t.is_current_uda = TRUE
            AND {build_hash_logic('s')} != {build_hash_logic('t')}
        )
    """
    vCon.sql(update_sql)
    
    # 4. Insert New/Changed Records
    # Insert where key is new OR key existed but was just closed (changed)
    insert_sql = f"""
        INSERT INTO {fq_target}
        SELECT s.*, 
               CAST('{today}' AS DATE) as valid_from_uda, 
               CAST(NULL AS DATE) as valid_to_uda, 
               TRUE as is_current_uda
        FROM {fq_source} s
        LEFT JOIN {fq_target} t 
            ON s.{key} = t.{key} 
            AND t.is_current_uda = TRUE
        WHERE t.{key} IS NULL 
    """
    vCon.sql(insert_sql)
    print("SCD2 Merge Complete.")