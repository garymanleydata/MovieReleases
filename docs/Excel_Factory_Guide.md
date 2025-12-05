Excel Report Factory: Developer Guide

1. Overview

The Excel Factory (utils.excel_factory.clsExcelReport) is a high-level Python wrapper for xlsxwriter.

Core Philosophy:

Standardization: Enforce corporate branding (Colors, Logos) by default.

Flexibility: Allow total override of fonts and styles for specific elements when needed.

Productivity: Turn 50 lines of raw xlsxwriter code into 1 line of Factory code.

2. Initialization & Theming

The Factory is initialized with a Global Theme. If you do not provide one, it falls back to the hardcoded corporate defaults (Dark Blue/Calibri).

Basic Usage (Default Theme)

from utils.excel_factory import clsExcelReport

# Uses standard branding (Calibri, Size 11, Blue Headers)
vReport = clsExcelReport("My_Report.xlsx")


Advanced Usage (Custom Theme)

You can inject a dictionary to override the global look for a specific client or department report.

vMarketingTheme = {
    "font_name": "Century Gothic",
    "font_size": 12,
    "color_primary": "#FF5733",   # Orange Headers
    "color_secondary": "#FEF9E7"  # Cream Backgrounds
}

vReport = clsExcelReport("Marketing_Campaigns.xlsx", vThemeOverrides=vMarketingTheme)


3. Writing Data

The f_write_dataframe Method

This is the workhorse method. It auto-fits column widths, applies filters, and handles data types automatically.

Standard Call:

vReport.f_write_dataframe(vWorksheet, dfData, vStartRow=2, vStartCol=1)


Flexible Call (Font & Style Overrides):
You can pass a vStyleOverrides dictionary to change the look of just this specific table, without affecting the rest of the report.

# Example: Writing a "Technical Log" table in a Monospace font
vLogStyle = {
    "font_name": "Consolas", 
    "font_size": 9,
    "header_bg_color": "#333333" # Dark Grey headers for logs
}

vReport.f_write_dataframe(
    vWorksheet, 
    dfLogs, 
    vStartRow=5, 
    vStartCol=1, 
    vStyleOverrides=vLogStyle
)


4. Visual Components

Adding Logos

The factory uses Pillow to resize images in memory, preventing your Excel file from becoming bloated (e.g., inserting a 5MB logo will only add ~50KB to the file size).

# Automatically resizes 'logo.png' to be 60 pixels tall
vReport.f_add_logo(vWorksheet, "assets/logo.png", vRow=0, vCol=0, vTargetHeight=60)


Creating Charts

The factory wraps complex chart syntax into a single function call.

vReport.f_add_chart(
    vSheet=vWorksheet,
    vChartType="column",
    vDataRange="=Data!$B$2:$B$10",     # Where the numbers are
    vCategoryRange="=Data!$A$2:$A$10", # Where the labels are
    vPosition="E5",                    # Where to place the chart
    vTitle="Monthly Release Counts",
    vXAxisTitle="Month",
    vYAxisTitle="Count"
)


Conditional Formatting (Traffic Lights)

Apply Green/Red formatting to status columns instantly.

# Applies to cells D2 through D100
vReport.f_add_traffic_lights(vWorksheet, "D2:D100", vPassValue="SUCCESS", vFailValue="FAILURE")


5. API Reference

clsExcelReport(vFilename, vThemeOverrides=None)

vFilename: String. Path to output file (e.g., reports/weekly.xlsx).

vThemeOverrides: Dict. Optional keys: font_name, font_size, color_primary, color_secondary, color_accent, color_success.

f_add_sheet(vSheetName)

Returns: xlsxwriter.Worksheet object.

Logic: Sanitizes name (removes [ ] : * ? / \) and truncates to 31 chars.

f_write_dataframe(vSheet, vDf, vRow, vCol, vIncludeIndex=False, vStyleOverrides=None)

vDf: Pandas DataFrame.

vStyleOverrides: Dict. Allows overriding the theme for this specific table.

f_add_chart(vSheet, vType, vDataRange, vCatRange, vPos, vTitle, ...)

vType: String. Options: column, bar, line, pie, area.

6. Example Workflow: "The Executive Summary"

This snippet demonstrates how to combine these features to build a complex, professional report.

# 1. Init
vReport = clsExcelReport("Executive_Summary.xlsx")

# 2. Create Dashboard Tab
vSheetDash = vReport.f_add_sheet("Dashboard")
vReport.f_add_logo(vSheetDash, "assets/logo.png")

# 3. Add High-Level Data (Standard Theme)
vReport.f_write_dataframe(vSheetDash, dfKPIs, vRow=4, vCol=1)

# 4. Add a Chart next to the data
vReport.f_add_chart(
    vSheetDash, "line", 
    vDataRange="=Dashboard!$C$5:$C$16", 
    vCategoryRange="=Dashboard!$B$5:$B$16", 
    vPosition="F4", 
    vTitle="Release Velocity"
)

# 5. Create Detail Tab (Custom "Technical" Theme)
vSheetDetail = vReport.f_add_sheet("Raw Data Logs")
vTechTheme = {"font_name": "Consolas", "font_size": 9}

vReport.f_write_dataframe(
    vSheetDetail, 
    dfRawLogs, 
    vRow=0, 
    vCol=0, 
    vStyleOverrides=vTechTheme
)

# 6. Finish
vReport.f_close()
