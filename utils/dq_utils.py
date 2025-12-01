import duckdb
import pandas as pd
from datetime import datetime

def f_log_dq_result(vCon, vNotebookName, vCheckName, vStage, vTargetTable, vStatus, vActual1, vActual2, vThreshold, vMsg, vSourceTable=None):
    """
    Writes the validation result to the central log table in MotherDuck.
    """
    try:
        vSql = f"""
            INSERT INTO MovieReleases.main.data_quality_logs VALUES (
                CURRENT_TIMESTAMP,
                '{vNotebookName}',
                '{vCheckName}',
                '{vStage}',
                '{vTargetTable}',
                '{vSourceTable if vSourceTable else "N/A"}',
                '{vStatus}',
                {vActual1},
                {vActual2 if vActual2 is not None else "NULL"},
                {vThreshold},
                '{vMsg.replace("'", "''")}'
            )
        """
        vCon.sql(vSql)
        print(f"[{vStatus}] {vCheckName}: {vMsg}")
    except Exception as e:
        print(f"CRITICAL: Failed to write to DQ Log Table: {e}")

def f_check_row_count_match(vCon, dfSource, dfTarget, vNotebookName, vStage, vTargetTable, vSourceTable="SourceData", vRaiseError=True):
    """
    Check 1: Row Count Comparison.
    Validates that the row count in the Source matches the Target.
    """
    vCountSource = len(dfSource)
    vCountTarget = len(dfTarget)
    
    if vCountSource == vCountTarget:
        vStatus = "PASS"
        vMsg = f"Counts match ({vCountTarget})"
    else:
        vStatus = "FAIL"
        vMsg = f"Mismatch: Source has {vCountSource}, Target has {vCountTarget}"
        
    f_log_dq_result(
        vCon=vCon,
        vNotebookName=vNotebookName,
        vCheckName="Row Count Match",
        vStage=vStage,
        vTargetTable=vTargetTable,
        vSourceTable=vSourceTable,
        vStatus=vStatus,
        vActual1=vCountSource,
        vActual2=vCountTarget,
        vThreshold=0, # Difference threshold implies 0
        vMsg=vMsg
    )
    
    if vStatus == "FAIL" and vRaiseError:
        raise ValueError(vMsg)

def f_check_duplicate_rows(vCon, dfData, vNotebookName, vStage, vTargetTable, vRaiseError=True):
    """
    Check 2: Duplicate Row Checks.
    Checks for rows that are exact duplicates across ALL columns.
    """
    # Count duplicates
    vDupeCount = dfData.duplicated().sum()
    
    if vDupeCount == 0:
        vStatus = "PASS"
        vMsg = "No duplicate rows found"
    else:
        vStatus = "FAIL"
        vMsg = f"Found {vDupeCount} duplicate rows"
        
    f_log_dq_result(
        vCon=vCon,
        vNotebookName=vNotebookName,
        vCheckName="Duplicate Row Check",
        vStage=vStage,
        vTargetTable=vTargetTable,
        vStatus=vStatus,
        vActual1=vDupeCount,
        vActual2=None,
        vThreshold=0,
        vMsg=vMsg
    )
    
    if vStatus == "FAIL" and vRaiseError:
        raise ValueError(vMsg)

def f_check_duplicate_keys(vCon, dfData, vKeyCols, vNotebookName, vStage, vTargetTable, vRaiseError=True):
    """
    Check 3: Duplicate Key Checks.
    Checks if the specified subset of columns (Business Keys) are unique.
    """
    vDupeCount = dfData.duplicated(subset=vKeyCols).sum()
    
    if vDupeCount == 0:
        vStatus = "PASS"
        vMsg = f"Unique keys confirmed on {vKeyCols}"
    else:
        vStatus = "FAIL"
        vMsg = f"Found {vDupeCount} duplicate keys on {vKeyCols}"
        
    f_log_dq_result(
        vCon=vCon,
        vNotebookName=vNotebookName,
        vCheckName="Duplicate Key Check",
        vStage=vStage,
        vTargetTable=vTargetTable,
        vStatus=vStatus,
        vActual1=vDupeCount,
        vActual2=None,
        vThreshold=0,
        vMsg=vMsg
    )
    
    if vStatus == "FAIL" and vRaiseError:
        raise ValueError(vMsg)