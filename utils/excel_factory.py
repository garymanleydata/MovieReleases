import pandas as pd
import xlsxwriter
import io
from PIL import Image

class clsExcelReport:
    """
    A wrapper class for XlsxWriter to produce standardized, pixel-perfect reports.
    Handles corporate branding, image resizing, and standard formatting.
    """
    
    # --- CONFIGURATION (Corporate Palette) ---
    cColourPrimary = '#2C3E50'   # Dark Blue (Headers)
    cColourSecondary = '#ECF0F1' # Light Grey (Alternating rows/Backgrounds)
    cColourAccent = '#E74C3C'    # Red (Alerts)
    cColourSuccess = '#27AE60'   # Green (Pass)
    cFontName = 'Calibri'
    
    def __init__(self, vFilename):
        self.vWorkbook = xlsxwriter.Workbook(vFilename)
        self.vFilename = vFilename
        
        # --- DEFINE STANDARD FORMATS ---
        self.vFmtHeader = self.vWorkbook.add_format({
            'bold': True,
            'font_name': self.cFontName,
            'font_size': 11,
            'font_color': 'white',
            'bg_color': self.cColourPrimary,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'
        })
        
        self.vFmtText = self.vWorkbook.add_format({
            'font_name': self.cFontName,
            'font_size': 10,
            'border': 1,
            'valign': 'top',
            'text_wrap': True
        })
        
        self.vFmtDate = self.vWorkbook.add_format({
            'font_name': self.cFontName,
            'font_size': 10,
            'border': 1,
            'valign': 'top',
            'align': 'center',
            'num_format': 'dd-mmm-yyyy'
        })
        
        self.vFmtTitle = self.vWorkbook.add_format({
            'bold': True,
            'font_name': self.cFontName,
            'font_size': 14,
            'font_color': self.cColourPrimary,
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
        Prevents 5MB logos from bloating the file.
        """
        try:
            # Open Image with Pillow
            with Image.open(vLogoPath) as vImg:
                # Calculate new dimensions (maintain aspect ratio)
                vAspectRatio = vImg.width / vImg.height
                vNewWidth = int(vTargetHeight * vAspectRatio)
                
                # Resize (LANCZOS is high quality downsampling)
                vImgResized = vImg.resize((vNewWidth, vTargetHeight), Image.Resampling.LANCZOS)
                
                # Save to memory buffer
                vImgBuffer = io.BytesIO()
                vImgResized.save(vImgBuffer, format='PNG')
                
                # Insert into Excel
                vWorksheet.insert_image(vRow, vCol, "logo.png", {
                    'image_data': vImgBuffer,
                    'x_offset': 5,
                    'y_offset': 5
                })
        except Exception as e:
            print(f"Warning: Could not insert logo {vLogoPath}. Error: {e}")

    def f_write_dataframe(self, vWorksheet, dfData, vStartRow, vStartCol, vIncludeIndex=False):
        """
        Writes a Pandas DataFrame with standard formatting and auto-width.
        Returns the row index AFTER the table.
        """
        # Write Headers
        vCols = dfData.columns.tolist()
        for i, vCol in enumerate(vCols):
            vWorksheet.write(vStartRow, vStartCol + i, vCol, self.vFmtHeader)
            
            # Simple Auto-Width Calculation
            # Get max length of data or header, capped at 50 chars
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
                
                # Determine format based on type
                if pd.api.types.is_datetime64_any_dtype(vVal) or isinstance(vVal, pd.Timestamp):
                    vWorksheet.write_datetime(vCurrentRow, vStartCol + i, vVal, self.vFmtDate)
                else:
                    vWorksheet.write(vCurrentRow, vStartCol + i, vVal, self.vFmtText)
            
            vCurrentRow += 1
            
        return vCurrentRow

    def f_add_traffic_lights(self, vWorksheet, vRange, vPassValue="PASS", vFailValue="FAIL"):
        """
        Applies standard Green/Red formatting to a range.
        """
        # Green for Pass
        vWorksheet.conditional_format(vRange, {
            'type': 'cell',
            'criteria': '==',
            'value': f'"{vPassValue}"',
            'format': self.vWorkbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        })
        
        # Red for Fail
        vWorksheet.conditional_format(vRange, {
            'type': 'cell',
            'criteria': '==',
            'value': f'"{vFailValue}"',
            'format': self.vWorkbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        })

    def f_close(self):
        self.vWorkbook.close()