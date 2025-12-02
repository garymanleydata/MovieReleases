import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def f_load_to_landing(vCon, dfData, vSchema, vTable):
    """
    Loads data into the Landing Layer (Overwrite/Replace).
    Landing is always a snapshot of the latest extraction.
    """
    vFullTableName = f"{vSchema}.{vTable}"
    print(f"Loading {len(dfData)} rows to Landing: {vFullTableName}")
    
    # Create Schema
    vCon.sql(f"CREATE SCHEMA IF NOT EXISTS {vSchema}")
    
    # Register view
    vCon.register('v_landing_buffer', dfData)
    
    # Create or Replace (Landing is ephemeral)
    vCon.sql(f"CREATE OR REPLACE TABLE {vFullTableName} AS SELECT * FROM v_landing_buffer")

def f_process_scd2(vCon, vConfigDict):
    """
    Executes a metadata-driven SCD Type 2 Merge.
    Ends previous records with Yesterday's date. Starts new records with Today.
    """
    vSrcDb = "MovieReleases" 
    vSrcSchema = vConfigDict['source_schema']
    vSrcTable = vConfigDict['source_table']
    vTgtSchema = vConfigDict['target_schema']
    vTgtTable = vConfigDict['target_table']
    vKey = vConfigDict['merge_key']
    
    vFqSource = f"{vSrcDb}.{vSrcSchema}.{vSrcTable}"
    vFqTarget = f"{vSrcDb}.{vTgtSchema}.{vTgtTable}"
    
    vToday = datetime.now().strftime('%Y-%m-%d')
    vYesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"--- Processing SCD2: {vFqSource} -> {vFqTarget} ---")
    
    # 1. Ensure Target Exists (Initialize History if empty)
    vCon.sql(f"CREATE SCHEMA IF NOT EXISTS {vSrcDb}.{vTgtSchema}")
    
    try:
        vCon.sql(f"DESCRIBE {vFqTarget}")
    except:
        print(f"Target {vFqTarget} does not exist. Initializing from Source...")
        vCon.sql(f"""
            CREATE TABLE {vFqTarget} AS 
            SELECT *, 
                   CAST('{vToday}' AS DATE) as valid_from_uda, 
                   CAST(NULL AS DATE) as valid_to_uda, 
                   CAST(1 AS BOOLEAN) as is_current_uda
            FROM {vFqSource}
        """)
        print("Initialization Complete.")
        return

    # 2. Detect Changes
    # Get list of data columns (exclude meta columns)
    vCols = [row[0] for row in vCon.sql(f"DESCRIBE {vFqSource}").fetchall()]
    
    # Helper to build hashed string with explicit table alias to avoid ambiguity
    def build_hash_logic(vAlias):
        # Exclude the primary key from the hash, check content columns
        vContentCols = [c for c in vCols if c != vKey]
        # Construct: COALESCE(CAST(s.col AS VARCHAR), '') || ...
        # This explicitly prefixes the alias (s. or t.) to fix the Ambiguous reference error
        vConcatStr = " || ".join([f"COALESCE(CAST({vAlias}.{c} AS VARCHAR), '')" for c in vContentCols])
        return f"md5({vConcatStr})"
    
    # 3. Perform the Updates (Close old records)
    # Update target where key matches AND content differs AND is currently active
    vUpdateSql = f"""
        UPDATE {vFqTarget}
        SET valid_to_uda = CAST('{vYesterday}' AS DATE),
            is_current_uda = FALSE
        WHERE is_current_uda = TRUE
        AND {vKey} IN (
            SELECT s.{vKey}
            FROM {vFqSource} s
            JOIN {vFqTarget} t ON s.{vKey} = t.{vKey}
            WHERE t.is_current_uda = TRUE
            AND {build_hash_logic('s')} != {build_hash_logic('t')}
        )
    """
    vCon.sql(vUpdateSql)
    
    # 4. Insert New/Changed Records
    # Insert where key is new OR key existed but was just closed (changed)
    # FIX: Use 'BY NAME' logic with explicit aliases for the meta columns.
    # This forces DuckDB to map 'poster_url' to 'poster_url' and 'valid_from_uda' to 'valid_from_uda', regardless of position.
    
    vInsertSql = f"""
        INSERT INTO {vFqTarget} BY NAME
        SELECT 
            s.*, 
            CAST('{vToday}' AS DATE) AS valid_from_uda, 
            CAST(NULL AS DATE) AS valid_to_uda, 
            TRUE AS is_current_uda
        FROM {vFqSource} s
        LEFT JOIN {vFqTarget} t 
            ON s.{vKey} = t.{vKey} 
            AND t.is_current_uda = TRUE
        WHERE t.{vKey} IS NULL 
    """
    vCon.sql(vInsertSql)
    print("SCD2 Merge Complete.")

def f_add_surrogate_key(vCon, dfNewData, vTargetTableName, vBusinessKeyCol, vSkColName):
    """
    Generic Surrogate Key Generator.
    1. Reads existing target table to get current SK mappings.
    2. Assigns existing SKs to matching business keys.
    3. Generates NEW SKs (Max + 1) for new business keys.
    4. Returns a dataframe ready to replace the target table.
    """
    print(f"Generating Surrogate Keys ({vSkColName}) for {vTargetTableName}...")
    
    # 1. Check if Target Table Exists
    vTableExists = False
    try:
        vCon.sql(f"DESCRIBE {vTargetTableName}")
        vTableExists = True
    except:
        print(f"Target {vTargetTableName} does not exist. Starting fresh SKs from 1.")

    if vTableExists:
        # 2. Fetch Existing Keys
        # We only need the Business Key and the SK
        dfExistingSk = vCon.sql(f"SELECT DISTINCT {vBusinessKeyCol}, {vSkColName} FROM {vTargetTableName}").df()
        vMaxSk = dfExistingSk[vSkColName].max()
        if pd.isna(vMaxSk): vMaxSk = 0
        
        print(f"Found {len(dfExistingSk)} existing keys. Max SK: {vMaxSk}")
        
        # 3. Join New Data with Existing SKs
        # Left join: preserve all new rows, attach SK if it exists
        dfResult = pd.merge(dfNewData, dfExistingSk, on=vBusinessKeyCol, how='left')
        
        # 4. Separate rows that need new keys
        vMaskNew = dfResult[vSkColName].isna()
        vNewCount = vMaskNew.sum()
        
        if vNewCount > 0:
            print(f"Assigning new SKs to {vNewCount} rows...")
            # Generate range: (Max + 1) to (Max + NewCount)
            vNewSks = np.arange(vMaxSk + 1, vMaxSk + vNewCount + 1)
            dfResult.loc[vMaskNew, vSkColName] = vNewSks
            
        # Ensure SK is integer
        dfResult[vSkColName] = dfResult[vSkColName].astype(int)
        
        return dfResult

    else:
        # Table doesn't exist, just generate SKs from 1
        dfResult = dfNewData.copy()
        dfResult[vSkColName] = range(1, len(dfResult) + 1)
        return dfResult