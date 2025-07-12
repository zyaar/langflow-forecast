#####################################################################
# forecast_excel_validation_builder.py
#
# Helper object which builds an Excel DataValidation rule 
# using openpyxl which can be added to a cell
#
#####################################################################


# from typing import List, Dict, Tuple, Any
# from datetime import datetime
# import pandas as pd
# import numpy as np
# from langflow.schema.dataframe import DataFrame, Data


# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.models.forecast_meta_data import ForecastDataSeriesMetaDataDataType, ForecastMetaDataSeries, ForecastMetaDataSeriesSchema, ForecastDataSeriesMetaDataComparisonType
from langflow.base.forecasting_common.renderers.excel.forecast_excel_base_helpers import ForecastExcelBaseHelpers


# COMPONENT SPECIFIC IMPORTS
# ==========================
import re
from enum import Enum
from openpyxl.cell.cell import Cell
from openpyxl.worksheet.cell_range import CellRange
from openpyxl.worksheet.datavalidation import DataValidation


# GLOBAL CONST
# ============





# CLASSES
# =======



class ForecastExcelValidationRuleBuilder:

    # DATA TYPE validation
    # Map the data type to the an excel data validation formula to validate the data entered into the model,
    # and the error message to provide in excel if the user violates the data entry rule
    ForecastExcelDataTypeToCustomValidationMap = {
        ForecastDataSeriesMetaDataDataType.INT: "MOD({ARG1}, 1) = 0",
        ForecastDataSeriesMetaDataDataType.FLOAT: "ISNUMBER({ARG1})",
        ForecastDataSeriesMetaDataDataType.DATE: "ISNUMBER({ARG1})",
        ForecastDataSeriesMetaDataDataType.CURRENCY: "ISNUMBER({ARG1})",
        ForecastDataSeriesMetaDataDataType.PCT: "AND(ISNUMBER({ARG1}), {ARG1} >= 0, {ARG1} <= 1)"}


    ForecastExcelDataTypeToCustomErrorMap = {
        ForecastDataSeriesMetaDataDataType.INT: "Input must be a whole number (i.e. integer).",
        ForecastDataSeriesMetaDataDataType.FLOAT: "Input must be a number (i.e. integer or decimal).",
        ForecastDataSeriesMetaDataDataType.DATE: "Input must be a date.",
        ForecastDataSeriesMetaDataDataType.CURRENCY: "Input must be a currency (i.e. number).",
        ForecastDataSeriesMetaDataDataType.PCT: "Input must be a percentage (i.e. number), with a value between 0% and 100%."}
    
    
    # COMPARISON TYPE validation
    # Map the comparison type to the an excel data validation formula to validate the date entered into the model, 
    # and the error message to provide in excel if the user violates the data entry rule
    ForecastExcelComparisonTypeToCustomValidationMap = {
        ForecastDataSeriesMetaDataComparisonType.LT: "{ARG1} < {ARG2}",
        ForecastDataSeriesMetaDataComparisonType.LE: "{ARG1} <= {ARG2}",
        ForecastDataSeriesMetaDataComparisonType.GE: "{ARG1} >= {ARG2}",
        ForecastDataSeriesMetaDataComparisonType.GT: "{ARG1} > {ARG2}",
        ForecastDataSeriesMetaDataComparisonType.EQ: "{ARG1} = {ARG2}",
        ForecastDataSeriesMetaDataComparisonType.NE: "{ARG1} <> {ARG2}",
        ForecastDataSeriesMetaDataComparisonType.BETWEEN: "AND({ARG1} >= {ARG2}, {ARG1} <= {ARG3})",
        ForecastDataSeriesMetaDataComparisonType.NOT_BETWEEN: "OR({ARG1} < {ARG2}, {ARG1} > {ARG3})"}


    ForecastExcelComparisonTypeToCustomErrorMap = {
        ForecastDataSeriesMetaDataComparisonType.LT: "{ARG1} must be less than {ARG2}.",
        ForecastDataSeriesMetaDataComparisonType.LE: "{ARG1} must be less than or equal to {ARG2}.",
        ForecastDataSeriesMetaDataComparisonType.GE: "{ARG1} must be greater than or equal to {ARG2}.",
        ForecastDataSeriesMetaDataComparisonType.GT: "{ARG1} must be greater than {ARG2}.",
        ForecastDataSeriesMetaDataComparisonType.EQ: "{ARG1} must be equal to {ARG2}.",
        ForecastDataSeriesMetaDataComparisonType.NE: "{ARG1} cannot be equal to {ARG2}.",
        ForecastDataSeriesMetaDataComparisonType.BETWEEN: "{ARG1} must be between {ARG2} and {ARG3}.",
        ForecastDataSeriesMetaDataComparisonType.NOT_BETWEEN: "{ARG1} can not be betwee {ARG2} and {ARG3}"}
    


    # CLASS METHODS
    # =============

    # generate_data_entry_rule
    # Generate the excel data validation rule to manage checking that the data type of a cell is correct.
    #  
    # INPUTS:
    #   curr_cell - the cell to which the validation will be applied.
    #   data_type - the data type to create a validation rule for.
    #   error_message - (optional) the error message to display when the validation fails.  If not provided, a default message will be used.
    #   error_title - (optional) the error title to display when the validation fails.  If not provided, a default title will be used.
    #   prompt - (optional) the prompt message to display when the cell is selected.  If not provided, no prompt will be displayed.
    #   prompt_title - (optional) the prompt title to display when the cell is selected.  If not provided, no title will be displayed.
    #   allow_blank - (optional) if True, allow blank values in the cell.  If False, then the cell must contain a value which meets the validation criteria.
    # 
    # OUTPUTS:
    #   NA
    #
    # NOTE:  in excel, data validations can't use references that go across worksheets, i.e. can't do data validation for cells in a different worksheet

    @staticmethod
    def generate_data_entry_rule(curr_cell: Cell, 
                                 data_type: ForecastDataSeriesMetaDataDataType,
                                 error_title: str = "Invalid entry",
                                 prompt: str = None,
                                 prompt_title: str = None, 
                                 allow_blank: bool = True,
                                 curr_cell_meta_data: ForecastMetaDataSeries = None):
        
        # convert the curr_cell to a cell reference for a formula
        curr_cell_formula_ref = ForecastExcelBaseHelpers.cell_to_formula_ref(curr_cell, with_ws_name = False)   # NOTE:  no worksheet name for reference, due to limitation of data validation in excel.  this will cause problems with cross worsheet criteria

        # generate the new validation formula term and add to validation rule for cell (or create a new validation for cell)
        formula1 = ForecastExcelValidationRuleBuilder.build_custom_type_validation_formula(data_type, curr_cell_formula_ref)  # NOTE:  no worksheet name for reference, due to limitation of data validation in excel.  this will cause problems with cross worsheet criteria
        rule = ForecastExcelValidationRuleBuilder.add_custom_rule(curr_cell = curr_cell, formula = formula1, allow_blank = allow_blank)

        # add error message (required)
        error_msg = ForecastExcelValidationRuleBuilder.ForecastExcelDataTypeToCustomErrorMap[data_type]

        if(rule.error is None):
            rule.error = error_msg
        else:
            rule.error += f"\r\n{error_msg}"

        rule.errorTitle = error_title
        rule.showErrorMessage = True

        # add prompt (optional)
        if(prompt is not None):
            if(rule.prompt is None):
                rule.prompt = prompt
            else:
                rule.prompt += f"\r\n{prompt}"

            if(prompt_title is not None):
                rule.promptTitle = prompt_title
            
            rule.showInputMessage = True

        rule.allow_blank = (rule.allow_blank and allow_blank)


    # generate_comparison_rule
    # Generate the excel data validation rule to manage checking of a data value against a comparison value (i.e. less than, greater than, equal to, etc.).
    #  
    # INPUTS:
    #   curr_cell - the cell to which the validation will be applied.
    #   comparison_type - the type of comparison to perform.
    #   value - the value to compare against.  This can be a number or a cell reference.
    #   error_message - (optional) the error message to display when the validation fails.
    #   error_title - (optional) the error title to display when the validation fails.
    #   prompt - (optional) the prompt message to display when the cell is selected.
    #   prompt_title - (optional) the prompt title to display when the cell is selected.
    #   allow_blank - (optional) if True, allow blank values in the cell.
    #   curr_formula - (optional) the is the calculation of the value in the curr_cell... if this is provided, then use the formula INSTEAD of the reference to the current cell
    #                  for the custom data validation formula, this is done because data_valation rule in EXCEL triggers BEFORE the field value is recalculated 
    #                  (causing the user to be trapped in the data validation), so in the situation where a validation rule needs to validate a calculated cell, 
    #                  it's easier to put the formula for the calculation instead of a reference to the cell
    #   curr_cell_meta_data - (optional) the meta data for the current cell.  This provides the option to grab additional variables either now or in the future if the function evolves.
    # 
    # OUTPUTS:
    #   NA
    #
    # NOTE:  in excel, data validations can't use references that go across worksheets, i.e. can't do data validation for cells in a different worksheet
    # TODO:  In the future we could have meta-data to specify where to apply it, also, a simple addition is to  check if the action for this row is INPUT, 
    # because if it doesn't have PREDs and it's not an INPUT, there's no reason to apply anything here

    # ZIV
    @staticmethod 
    def generate_comparison_rule(curr_cell: Cell,
                                 comparison_type: ForecastDataSeriesMetaDataComparisonType,
                                 value: str | int | float,
                                 preds: list[str] = None,
                                 error_title: str = "Invalid entry",
                                 prompt: str = None,
                                 prompt_title: str = None,
                                 allow_blank: bool = True,
                                 value2: str | int | float = None,
                                 curr_formula: str = None,
                                 curr_cell_meta_data: ForecastMetaDataSeries = None):
        

        # figure out what we'll use for the curr_cell_reference in the validation formula (i.e. curr_cell_reference < 1)
        # usually it's just a cell formula reference, but if a curr_formula is provided, use the formula instead
        if(curr_formula is None):
            curr_cell_formula_ref = ForecastExcelBaseHelpers.cell_to_formula_ref(curr_cell, with_ws_name = False)   # NOTE:  no worksheet name for reference, due to limitation of data validation in excel.  this will cause problems with cross worsheet criteria
            error_msg_arg1 = "Input"
        else:
            curr_cell_formula_ref = ForecastExcelBaseHelpers.convert_formula_to_sub_term(ForecastExcelBaseHelpers.remove_worksheet_names_from_formula(curr_formula))
            error_msg_arg1 = curr_cell_formula_ref
        

        # create the validation formula
        formula1 = ForecastExcelValidationRuleBuilder.build_custom_comparison_formula(comparison_type, arg1 = curr_cell_formula_ref, arg2 = value, arg3 = value2)


        # figure out where we want to apply this rule, 
        # if there ARE preds, we make the assumption (not always true, but a design simplification that I will probably regret later)
        # then this is a formula, so you need to apply the rule on all the preds, otherwise just the current cell
        # TODO:  In the future we could have meta-data to specify where to apply it, also, a simple addition is to  check if the action for this row is INPUT, 
        # because if it doesn't have PREDs and it's not an INPUT, there's no reason to apply anything here
        if(preds is not None):
            # so we'll need to convert them to formula cell references as well
            target_cells = [curr_cell.parent[cell_ref] for cell_ref in preds]
        else:
            target_cells = [curr_cell]

        for target_cell in target_cells:

            # generate the new validation formula term and add to validation rule for cell (or create a new validation for cell)
            #formula1 = ForecastExcelValidationRuleBuilder.build_custom_comparison_formula(comparison_type, arg1 = target_cell, arg2 = value, arg3 = value2)
            rule = ForecastExcelValidationRuleBuilder.add_custom_rule(curr_cell = target_cell, formula = formula1, allow_blank = allow_blank)
            
            # add error message (required)
            error_msg = ForecastExcelValidationRuleBuilder.ForecastExcelComparisonTypeToCustomErrorMap[comparison_type].format(ARG1=error_msg_arg1, ARG2 = value, ARG3 = value2)

            if(rule.error is None):
                rule.error = error_msg
            else:
                rule.error += f"\r\n{error_msg}"

            rule.errorTitle = error_title
            rule.showErrorMessage = True

            # add prompt (optional)
            if(prompt is not None):
                if(rule.prompt is None):
                    rule.prompt = prompt
                else:
                    rule.prompt += f"\r\n{prompt}"

                if(prompt_title is not None):
                    rule.promptTitle = prompt_title
                
                rule.showInputMessage = True

            rule.allow_blank = (rule.allow_blank and allow_blank)




    # build_custom_type_validation_formula
    # Simple convenience function to build a custom validation formula based on the data type
    #  
    # INPUTS:
    #   validation - the data type to validate
    #   arg - the argument to validate (i.e. a cell reference)
    # 
    # OUTPUTS:
    #   worksheet

    @staticmethod
    def build_custom_type_validation_formula(validation: ForecastDataSeriesMetaDataDataType, arg: str) -> str:
        return(ForecastExcelValidationRuleBuilder.ForecastExcelDataTypeToCustomValidationMap[validation].format(ARG1=arg))
    


    # build_custom_comparison_formula
    # Simple convenience function to build a comparison validation formula
    #  
    # INPUTS:
    #   validation - the data type to validate
    #   arg1 - the first argument in the validation forumla (i.e. a cell reference or constant) (used in all formulas)
    #   arg2 - the second argument in the validation formula (i.e. a cell reference or constant) (used in all formulas)
    #   arg3 - the third argument in the validation formula (i.e. a cell reference or constant) (only used in BETWEEN and NOT_BETWEEN formulas)
    # 
    # OUTPUTS:
    #   worksheet

    @staticmethod
    def build_custom_comparison_formula(validation: ForecastDataSeriesMetaDataDataType, arg1: str, arg2: str, arg3: str = None) -> str:
        if(validation not in [ForecastDataSeriesMetaDataComparisonType.BETWEEN, ForecastDataSeriesMetaDataComparisonType.NOT_BETWEEN]):
            if(arg1 is None or arg2 is None):
                raise ValueError(f"\n*  build_custom_comparison_formula:  Validation type '{validation}' requires two arguments, but one or more arguments are missing.")
            return(ForecastExcelValidationRuleBuilder.ForecastExcelComparisonTypeToCustomValidationMap[validation].format(ARG1=arg1, ARG2=arg2))
        else:
            if(arg1 is None or arg2 is None or arg3 is None):
                raise ValueError(f"\n*  build_custom_comparison_formula:  Validation type '{validation}' requires three arguments, but one or more arguments are missing.")
            return(ForecastExcelValidationRuleBuilder.ForecastExcelComparisonTypeToCustomValidationMap[validation].format(ARG1=arg1, ARG2=arg2, ARG3=arg3))
        
        

    # build_custom_comparison_error_msg
    # Simple convenience function to handle the logic of generating the comparison validation error message
    #  
    # INPUTS:
    #   validation - the data type to validate
    #   arg1 - the first argument in the validation forumla (i.e. a cell reference or constant) (used in all formulas)
    #   arg2 - the second argument in the validation formula (i.e. a cell reference or constant) (used in all formulas)
    #   arg3 - (option) the third argument in the validation formula (i.e. a cell reference or constant) (only used in BETWEEN and NOT_BETWEEN formulas)
    #   curr_formula - (optional) the formula of the calculation (if provided, used in the error message to demonstrate the data rule)
    #   
    # 
    # OUTPUTS:
    #   worksheet

    @staticmethod
    def build_custom_comparison_error_msg(validation: ForecastDataSeriesMetaDataDataType, arg1: str, arg2: str, arg3: str = None, curr_formula: str = None) -> str:
        if(curr_formula is not None):
            arg1 = curr_formula    

        if(validation not in [ForecastDataSeriesMetaDataComparisonType.BETWEEN, ForecastDataSeriesMetaDataComparisonType.NOT_BETWEEN]):
            return(ForecastExcelValidationRuleBuilder.ForecastExcelComparisonTypeToCustomValidationMap[validation].format(ARG1=arg1, ARG2=arg2))
            
        else:
            return(ForecastExcelValidationRuleBuilder.ForecastExcelComparisonTypeToCustomValidationMap[validation].format(ARG1=arg1, ARG2=arg2, ARG3=arg3))
        
        

    # add_custom_rule
    # Adjust the custom formula string to add an additional formula cell rule.  Since we may have multiple data validation criteria on a cell (is integer, is less than 5, etc.),
    # but excel can only have one DataValidation rule per cell, we handle this by creating a custom formula of the form:  
    # =AND((validation_criteria_1), (validation_criteria_2), (validation_criteria_3), etc.)
    # whenever we need more than one validation criteria, and we simply add another formula that returns a true or false, wrap it in ()'s and add it into the AND statement.
    #  
    # INPUTS:
    #   validation - the data validation object to update
    #   curr_cell - the cell to which the validation will be applied.
    #   formula - the formula to add to the validation rule.  This should be a string which is a valid excel formula, but without the leading '='.
    #   allow_blank - (optional) if True, allow blank values in the cell.  If False, then the cell must contain a value which meets the validation criteria.
    # 
    # OUTPUTS:
    #   worksheet
    @staticmethod
    def add_custom_rule(curr_cell: Cell, formula: str, allow_blank: bool = True) -> DataValidation:

        # wrap parens around the new formula, makes it easier for the parser to handle when it can be assured that each individual validation criteria
        # is always wrapped by parens
        formula = f"({formula})"

        # figure out if a rule already exists for this cell or if we need to create a new one:
        validation = ForecastExcelValidationRuleBuilder.get_validation(curr_cell)

        # if there are no old_terms, then set the new_formula to be the incoming formula
        if (validation.formula1 is None or validation.formula1 == ""):
            new_formula = formula

        # if there are old terms, split to the highest level terms by comma, add in the formula, and rejoin with an AND
        else:
            # if multiple formulas terms already exist joined by an AND() term, we need to extract the individual formula terms
            old_terms = ForecastExcelValidationRuleBuilder.regex_split_validation_formula(validation.formula1)
            new_formula = f"AND({','.join(old_terms)}, {formula})"

        # set the new formula in the validation object
        validation.formula1 = f"={new_formula}"

        return(validation)




    # regex_split_validation_formula
    # simple helper function, given an excel custom validation formula of the form '=AND(term1, term2, ...)' or '=OR(term1, term2, ...)', split it into a list of the individual terms
    # and return as a list of strings.
    # INPUTS:
    #   formula - the formula to split
    #
    # OUTPUTS:
    #   list of strings, each string is a term in the formula

    @staticmethod
    def regex_split_validation_formula(validation_formula: str) -> list[str]:

        # check if legit formula (starts with "=")
        if (not validation_formula.startswith("=")):
            raise ValueError(f"\n*  regex_split_validation_formula:  invalid formula {validation_formula}")
        else:
            validation_formula = validation_formula[1:] # remove the equals sign

            # check if the rest of the formula has the form "AND(rest_of_formula)" after the equal sign
            # otherwise, it's not a compound formula, so return as is
            if(not validation_formula.strip().upper().startswith("AND")):
                return([validation_formula])
            
            else:
                #validation_formula1_terms = ForecastExcelValidationRuleBuilder.regex_split_validation_formula(validation.formula1[1:])

                # attempt to remove the AND() and just keep the "rest_of_formula"
                match = re.match(r'^(AND)\((.*)\)$', validation_formula, re.IGNORECASE)

                # if it failed, return as-is
                if not match:
                    return [validation_formula]
                
                # if it succeeded, take the "rest_of_formula" as terms_str and break it into terms
                terms_str = match.group(2)
                terms = ForecastExcelBaseHelpers.find_comma_with_balanced_parens(terms_str)
                return(terms)
        
        # """
        # Splits an Excel formula of the form '=AND(term1, term2, ...)' and returns a list of the terms.
        # Only works for top-level AND/OR formulas with comma-separated terms.
        # """
        # # Remove leading '=' and surrounding whitespace
        # formula = formula.strip()
        # if formula.startswith('='):
        #     formula = formula[1:]

        # # Match AND(...) or OR(...)
        # match = re.match(r'^(AND|OR)\((.*)\)$', formula, re.IGNORECASE)
        # if not match:
        #     return []
        # terms_str = match.group(2)
        # # Split by commas not inside parentheses
        # terms = re.findall(r'(?:[^,(]|\([^)]*\))+', terms_str)
        # # Strip whitespace from each term
        # return [term.strip() for term in terms if term.strip()]
    


    # get_validations_of_cell
    # Given a cell object, return a list of all DataValidation objects which apply to the cell
    #  
    # INPUTS:
    #   validation - the data type to validate
    #   arg - the argument to validate (i.e. a cell reference)
    #   return_cell_range - (opetional) because validation only works on cell RANGES, if the arg being provided is a cell reference (versus another formula),
    #                       then we need to convert it to a range (i.e. A1 -> A1:A1) so that the validation will work properly.  If the arg is a formula, then we don't need to do this.
    # 
    # OUTPUTS:
    #   list of data validations for the cell, or None if there are no validations

    @staticmethod
    def get_validation(curr_cell: Cell)-> DataValidation:

        # figure out if we have any existing rules for this cell
        dv_rules = ForecastExcelValidationRuleBuilder.get_validations_of_cell(curr_cell)

        if(dv_rules is None or len(dv_rules) == 0):
            rule = DataValidation(type = "custom")
            curr_cell.parent.add_data_validation(rule)
            rule.add(CellRange(curr_cell.coordinate, curr_cell.coordinate))

        elif(len(dv_rules) == 1):
                rule = dv_rules[0]
        else:
            raise ValueError(f"\n*  generate_data_entry_rule:  Cell {curr_cell.coordinate} in worksheet {curr_cell.parent.title} has more than one data validation rule.")
        
        return(rule)


    # get_validations_of_cell
    # Given a cell object, return a list of all DataValidation objects which apply to the cell
    #  
    # INPUTS:
    #   validation - the data type to validate
    #   arg - the argument to validate (i.e. a cell reference)
    #   return_cell_range - (opetional) because validation only works on cell RANGES, if the arg being provided is a cell reference (versus another formula),
    #                       then we need to convert it to a range (i.e. A1 -> A1:A1) so that the validation will work properly.  If the arg is a formula, then we don't need to do this.
    # 
    # OUTPUTS:
    #   list of data validations for the cell, or None if there are no validations

    @staticmethod
    def get_validations_of_cell(curr_cell: Cell)-> list[DataValidation] | None:
        ws = curr_cell.parent
        validations = []

        # if no validations, return None
        if(ws.data_validations is None or len(ws.data_validations.dataValidation) == 0):
            return(None)

        # iterate over all dataValidations in the worksheet, and return a list of those which contain the cell
        for dv in ws.data_validations.dataValidation:

            # get the cell range for the data validation
            for cell_range in dv.cells.ranges:

                # if the cell is in the range, add it to the list
                if(curr_cell.coordinate in cell_range):
                    validations.append(dv)

        return(validations)
            



