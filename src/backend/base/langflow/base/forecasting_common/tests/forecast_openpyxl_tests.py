from openpyxl import Workbook, worksheet, load_workbook
from openpyxl.cell.cell import Cell
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.styles import Protection
from openpyxl.worksheet.cell_range import CellRange
import re


def main():

    def remove_worksheet_names_from_formula(formula: str) -> str:
        """
        Removes worksheet names from cell references in an Excel formula string.
        Example: "='Summary'!A1 + 'Sheet2'!B2 + C3" -> "A1 + B2 + C3"
        """
        import re
        # Regex matches: optional single-quoted or unquoted worksheet name followed by '!' and a cell reference
        # Handles cases like 'Summary'!A1, Sheet2!B2, etc.
        return re.sub(r"(?:'[^']+'|[A-Za-z0-9_]+)!", "", formula)
    
    string = "='Summary'!E13 + 'Summary'!E16"

    results = remove_worksheet_names_from_formula(string)
    print(results)



if __name__ == "__main__":
    main()
