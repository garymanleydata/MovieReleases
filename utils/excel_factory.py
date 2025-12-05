import pandas as pd
import xlsxwriter
import io
from PIL import Image

class clsExcelReport:
    """
    A wrapper class for XlsxWriter to produce standardized, pixel-perfect reports.
    Handles corporate branding, image resizing, charting, and standard formatting.
    """
    
    # --- DEFAULT CONFIGURATION (Corporate Palette) ---
    cColourPrimary = '#2C3E50'   # Dark Blue (Headers)
    cColourSecondary = '#ECF0F1' # Light Grey (Alternating rows/Backgrounds)
    cColourAccent = '#E74C3C'    # Red (Alerts)
    cColourSuccess = '#27AE60'   # Green (Pass)
    cFontName = 'Calibri'
    cFontSize = 11
    
    def __init__(self, vFilename, vThemeOverrides=None):
        self.vWorkbook = xlsxwriter.Workbook(vFilename)
        self.vFilename = vFilename
        
        # Merge Defaults with Overrides
        self.vTheme = {
            'font_name': self.cFontName,
            'font_size': self.cFontSize,
            'color_primary': self.cColourPrimary,
            'color_secondary': self.cColourSecondary,
            'color_accent': self.cColourAccent,
            'color_success': self.cColourSuccess
        }
        
        if vThemeOverrides:
            self.vTheme.update(vThemeOverrides)
        
        # --- DEFINE STANDARD FORMATS ---
        self.vFmtHeader = self.vWorkbook.add_format({
            'bold': True,
            'font_name': self.vTheme['font_name'],
            'font_size': self.vTheme['font_size'],
            'font_color': 'white',
            'bg_color': self.vTheme['color_primary'],
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'
        })
        
        self.vFmtText = self.vWorkbook.add_format({
            'font_name': self.vTheme['font_name'],
            'font_size': self.vTheme['font_size'] - 1, # Slightly smaller for data
            'border': 1,
            'valign': 'top',
            'text_wrap': True
        })
        
        self.vFmtDate = self.vWorkbook.add_format({
            'font_name': self.vTheme['font_name'],
            'font_size': self.vTheme['font_size'] - 1,
            'border': 1,
            'valign': 'top',
            'align': 'center',
            'num_format': 'dd-mmm-yyyy'
        })
        
        self.vFmtTitle = self.vWorkbook.add_format({
            'bold': True,
            'font_name': self.vTheme['font_name'],
            'font_size': self.vTheme['font_size'] + 4, # Larger title
            'font_color': self.vTheme['color_primary'],
            'valign': 'top'
        })

    def f_add_sheet(self, vSheetName):
        """Creates and returns a sanitized worksheet."""
        # Excel allows max 31 chars, no special chars
        vCleanName = "".join([c for c in vSheetName if c.isalnum() or c in " -_"])[:31]
        return self.vWorkbook.add_worksheet(vCleanName)

    def f_add_logo(self, vWorksheet, vLogoPath, vRow=0, vCol=0, vTargetHeight=60):
        """
        Resizes an image in memory and inserts it.
        """
        try:
            with Image.open(vLogoPath) as vImg:
                vAspectRatio = vImg.width / vImg.height
                vNewWidth = int(vTargetHeight * vAspectRatio)
                vImgResized = vImg.resize((vNewWidth, vTargetHeight), Image.Resampling.LANCZOS)
                
                vImgBuffer = io.BytesIO()
                vImgResized.save(vImgBuffer, format='PNG')
                
                vWorksheet.insert_image(vRow, vCol, "logo.png", {
                    'image_data': vImgBuffer,
                    'x_offset': 5,
                    'y_offset': 5
                })
        except Exception as e:
            print(f"Warning: Could not insert logo {vLogoPath}. Error: {e}")

    def f_write_dataframe(self, vWorksheet, dfData, vStartRow, vStartCol, vIncludeIndex=False, vStyleOverrides=None):
        """
        Writes a Pandas DataFrame.
        vStyleOverrides: Optional dict to override font/colors for THIS table only.
        """
        # Handle Style Overrides
        if vStyleOverrides:
            # Merge base theme with specific table overrides
            vLocalTheme = self.vTheme.copy()
            vLocalTheme.update(vStyleOverrides)
            
            # Create temporary formats
            vLocalHeader = self.vWorkbook.add_format({
                'bold': True,
                'font_name': vLocalTheme['font_name'],
                'font_size': vLocalTheme['font_size'],
                'font_color': 'white',
                'bg_color': vLocalTheme.get('header_bg_color', vLocalTheme['color_primary']),
                'border': 1, 'align': 'center', 'valign': 'vcenter'
            })
            vLocalText = self.vWorkbook.add_format({
                'font_name': vLocalTheme['font_name'],
                'font_size': vLocalTheme['font_size'] - 1,
                'border': 1, 'valign': 'top', 'text_wrap': True
            })
            vLocalDate = self.vWorkbook.add_format({
                'font_name': vLocalTheme['font_name'],
                'font_size': vLocalTheme['font_size'] - 1,
                'border': 1, 'valign': 'top', 'align': 'center',
                'num_format': 'dd-mmm-yyyy'
            })
        else:
            vLocalHeader = self.vFmtHeader
            vLocalText = self.vFmtText
            vLocalDate = self.vFmtDate

        # Write Headers
        vCols = dfData.columns.tolist()
        for i, vCol in enumerate(vCols):
            vWorksheet.write(vStartRow, vStartCol + i, vCol, vLocalHeader)
            
            # Simple Auto-Width
            vMaxLen = max(
                dfData[vCol].astype(str).map(len).max() if not dfData.empty else 0,
                len(str(vCol))
            )
            vWorksheet.set_column(vStartCol + i, vStartCol + i, min(vMaxLen + 2, 50))

        # Write Rows
        vCurrentRow = vStartRow + 1
        for _, vRowData in dfData.iterrows():
            for i, vCol in enumerate(vCols):
                vVal = vRowData[vCol]
                
                if pd.api.types.is_datetime64_any_dtype(vVal) or isinstance(vVal, pd.Timestamp):
                    vWorksheet.write_datetime(vCurrentRow, vStartCol + i, vVal, vLocalDate)
                else:
                    vWorksheet.write(vCurrentRow, vStartCol + i, vVal, vLocalText)
            
            vCurrentRow += 1
            
        return vCurrentRow

    def f_add_chart(self, vSheet, vChartType, vDataRange, vCategoryRange, vPosition, vTitle=None, vXAxisTitle=None, vYAxisTitle=None):
        """
        Adds a native Excel chart to the sheet.
        """
        vChart = self.vWorkbook.add_chart({'type': vChartType})
        
        vChart.add_series({
            'name':       vTitle if vTitle else 'Data',
            'categories': vCategoryRange,
            'values':     vDataRange,
            'fill':       {'color': self.vTheme['color_primary']},
            'line':       {'color': self.vTheme['color_primary']}
        })
        
        if vTitle:
            vChart.set_title({'name': vTitle})
        if vXAxisTitle:
            vChart.set_x_axis({'name': vXAxisTitle})
        if vYAxisTitle:
            vChart.set_y_axis({'name': vYAxisTitle})
            
        # Clean up chart look
        vChart.set_legend({'none': True})
        
        vSheet.insert_chart(vPosition, vChart)

    def f_add_traffic_lights(self, vWorksheet, vRange, vPassValue="PASS", vFailValue="FAIL"):
        """
        Applies standard Green/Red formatting to a range.
        """
        vWorksheet.conditional_format(vRange, {
            'type': 'cell', 'criteria': '==', 'value': f'"{vPassValue}"',
            'format': self.vWorkbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        })
        
        vWorksheet.conditional_format(vRange, {
            'type': 'cell', 'criteria': '==', 'value': f'"{vFailValue}"',
            'format': self.vWorkbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        })

    def f_close(self):
        self.vWorkbook.close()