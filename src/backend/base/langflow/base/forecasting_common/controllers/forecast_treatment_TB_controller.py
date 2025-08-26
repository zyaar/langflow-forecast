from langflow.schema import DataFrame
from langflow.base.forecasting_common.constants import ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel

from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeries,
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions,
                                                                        ForecastMetaDataRange,
                                                                        ForecastMetaDataRangeSchema)

from langflow.base.forecasting_common.controllers.forecast_sum_input_TB_controller import ForecastSumInputTBController


# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import Any, List, Tuple
import copy
import pandas as pd

# CLASSES
# =======
class ForecastTreatmentTBController(ForecastSumInputTBController):

    # calc_treatment_pat_forecast
    # For each month of the forecast, calculate the number of patients in treatment, total and by treatment month, as well as the number of patients leaving each month,
    # total and by treatment month
    #
    # INPUTS:
    #   TBD
    #
    # OUTPUTS:
    # return({"pat_on_treatment": {"treatment_table_data": treatment_table_data, 
    #                              "treatment_table_meta_data": treatment_table_meta_data, 
    #                              "pat_by_treatment_month_data": pat_by_treatment_month_data, 
    #                              "pat_by_treatment_month_meta_data": pat_by_treatment_month_meta_data, 
    #                              "updated_data": updated_data},
    #         "pat_leaving_treatment": {"treatment_table_data": treatment_table_data, 
    #                                   "treatment_table_meta_data": treatment_table_meta_data, 
    #                                   "pat_leaving_by_treatment_month_data": pat_leaving_by_treatment_month_data, 
    #                                   "pat_leaving_by_treatment_month_meta_data": pat_leaving_by_treatment_month_meta_data, 
    #                                   "updated_data": updated_data}})


    @staticmethod
    def calc_treatment_pat_forecast(# self variables passed in
                                    id: str,
                                    display_name: str,
                                    month_prefix: str,
                                    
                                    # current forecast
                                    updated_data: DataFrame | pd.DataFrame,
                                    updated_meta_data: ForecastMetaDataFrame,

                                    # treatment details table
                                    treatment_table_data: DataFrame,
                                    treatment_table_meta_data: ForecastMetaDataFrame,
                                    pc_col_id: str,

                                    # pre-forecast input table
                                    pre_forecast_inputs_data: DataFrame,
                                    pre_forecast_inputs_meta_data: ForecastMetaDataFrame,
                                    pf_col_id: str,
                                    
                                    # pre-forecast patient flow table
                                    pre_forecast_patient_flow_data: DataFrame,
                                    pre_forecast_patient_flow_meta_data: ForecastMetaDataFrame,
                                    pmpf_col_prefix: str) -> dict:



        updated_data = ForecastDataModel.to_pandas(updated_data)

        # Make sure the data is MONTHLY timescale, we assume this is done ahead of using this function
        if(updated_meta_data.get_timescale() != ForecastModelTimescale.MONTH):
            raise ValueError(f"\n*  calc_treatment_pat_forecast:  error, input patient flow is not set to MONTHLY timescale.")


        # get the incoming patient stream id (we need it later for a bunch of things)
        incoming_patient_flow = updated_meta_data.get_last_id(value_series_only = True)

        # Iterate over all treatment months
        num_months_treatment = len(treatment_table_data)

        pat_on_treatment_list_of_col_ids = []
        pat_leaving_treatment_list_of_col_ids = []


        # PATIENTS ON TREATMENT BY TREATMENT MONTH
        # ========================================

        # The overall calcuation for patients in treatment is:
        #
        #   = Patients flow in (PF) * Progression Curve retaining for that month (PC)
        #
        # However, what we use for PF varies depending on the treatment month and the forecast month, because when
        # calculating the earliest MONTHS IN THE FORECAST, for the later MONTH ON TREATMENT, we have to use the
        # pre_forecast data

        for i in range(num_months_treatment):
           curr_on_treat_col_id = ForecastTreatmentTBController._gen_pat_on_treat_id(id = id, month_prefix = month_prefix, month_num = i+1)
           curr_leaving_treat_col_id = ForecastTreatmentTBController._gen_pat_leaving_id(id = id, month_prefix = month_prefix, month_num = i+1)
           
           # =====================
           # FIRST TREATMENT MONTH
           # =====================
           if(i == 0):
               
               # PATIENTS ON TREATMENT BY TREATMENT MONTH
               # ----------------------------------------

               # FIRST TREATMENT MONTH
               # There is a special case for first row (first treatment month), don't need to consider any pre_forecast_inputs, 
               # so it's just one formula for all elements in the row

               # In the DATA calculations:
               # PF = updated_data.iloc[:, incoming_patient_flow]  # TODO:  could we use the same PF as the meta_data below?  Not sure we used a different one
               # PC = treatment_table_data[pc_col_id][i]
               updated_data[curr_on_treat_col_id] = updated_data.loc[:, incoming_patient_flow] * treatment_table_data[pc_col_id][i]


               # In META-DATA calculations:
               # We have to recreate the above DATA calculation in a way that will tell the MODEL ENGINE how to
               # create this same formula in whatever BUILDER we are going to use.  To do the multiplication we'll use a PROD action.  For the pred references,
               # (PF) is straightforward (just the last id coming in), for the progression curve (PC) we need two things... one, a full reference using the full
               # reference operator "." (i.e. object_id.row_id) since it's addressing an ID in a different object (the object is the treatment table object, 
               # that should have been created in steps prior to this function), and the second is a individual element operator ":" (i.e. row_id:element_id)
               # since we need to multiply all elements in the PF by the same single emlement... not element by element.  So the PC reference will end up being
               # in the format: object_id.row_id:element_id
               #
               # PF = incoming_patient_flow
               # PC = {treatment_table_meta_data.get_id()}.{pc_col_id}:{i}
               # note we use the last row from update_meta_data because it doesn't change when we keep adding meta_data to updated_meta_data
               pred_pf_col_id = incoming_patient_flow
               pred_pc_col_id = f"{treatment_table_meta_data.get_id()}.{pc_col_id}:{i}"
               preds_on = [pred_pf_col_id, pred_pc_col_id]

               updated_meta_data = updated_meta_data.add_col_meta_data(frame = updated_meta_data,
                                                                        id = curr_on_treat_col_id,
                                                                        step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                        action = ForecastDataSeriesMetaDataAction.PROD,
                                                                        data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                        display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                        display_name = f"# of patients in '{display_name}' month {i+1}",
                                                                        data_values = updated_data[curr_on_treat_col_id].to_list(),
                                                                        validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                        pred = preds_on,
                                                                        verify_integrity=True,
                                                                        drop_dups = False)
            
           # ==========================
           # REMAINING TREATMENT MONTHS
           # ==========================
           else:
            
            # PATIENTS ON TREATMENT BY TREATMENT MONTH
            # ----------------------------------------
            num_elements = i
            list_of_ranges_on = []

            # REMAINING TREATMENT MONTHS
            # All treatment months OTHER THAN the first month are more complicated because we now have TWO different sets of input data based on which elements in the column
            # we will be calculating.  For DATA, we'll simply have two different calculations, on the handles the part 1 elements, and another that handles the part 2 elements.
            # For META-DATA, the way we handle this is using RANGES.  RANGES are used in an ACTION to specify what PREDs to use which which elements of the calcutions.
            # Since we have two different parts where the preds will be different, we'll need to create two RANGES for the meta_data, combine them in a list and add that to the 
            # add_col_meta_data ACTION command.  When the MODEL ENGINE and BUILDER run, they will keep track of which elements they are applying which RANGE, as we specify.  

            # Part 1
            # In both parts, for PC we use progression curve for that month from the treatment table.  However, for the PF, Part 1 covers for the first i FORECAST MONTHS
            # since those months will be calculated using PRE-FORECAST data (i.e. for all patients in month 2 of treatment their treatment, the first 2 FORECAST MONTHS will be
            # based on data that happened BEFORE THE FORECAST, for patients in month 3 of treatment, the first 3 FORECAST MONTHS will be based on pre-forecast patient data, patients in
            # treatment month 4, the first 4 FORECAST MONTHS will be based on pre-forecast data, etc. etc.).  So we use elements from the pre-forecast object for the first 
            # i FORECAST MONTH.

            # In the DATA calculations:
            # PF = pre_forecast_inputs_data.loc[(num_months_treatment-1-i):(num_months_treatment-2), pf_col_id].values
            # PC = float(treatment_table_data[pc_col_id][i])
            updated_data.loc[0:(i-1), curr_on_treat_col_id] = pre_forecast_inputs_data.loc[(num_months_treatment-1-i):(num_months_treatment-2), pf_col_id].values * float(treatment_table_data[pc_col_id][i])


            # In META_DATA calculations:
            # We have to recreate the above DATA calculation in a way that will tell the MODEL ENGINE how to
            # create this same formula in whatever BUILDER we are going to use.  To do the multiplication we'll use a PROD action.  For the pred references,
            # For the progression curve (PC) we do the same two things we did above when calculating the PATIENTS ON THERAPY... one, a full reference using the full
            # reference operator "." (i.e. object_id.row_id) since it's addressing an ID in a different object (the object is the treatment table object, 
            # that should have been created in steps prior to this function), and the second is a individual element operator ":" (i.e. row_id:element_id)
            # since we need to multiply all elements in the PF by the same single emlement... not element by element.  So the PC reference will end up being
            # in the format: object_id.row_id:element_id.
            #
            # The PF reference is more complicated this time, first, we are referencing a different object (in this case the pre_forecast_inputs object), so
            # we need to use the full reference operator "." (i.e. obj_id.ref_id), BUT WE ALSO need to time shift the data coming in because each of the i elements we are 
            # using for PF come from THE END of the pre-forecast data (i.e. the LAST i elements in the row of pre_forecast_inputs), not the beginning (so if it's patients in treatment
            # month_2, we have 1 element of pre-forecast data, the the LAST MONTH from pre-forecast data, for patients in month_3 of treatment, we use the LAST 2 elements of pre-forecast 
            # data, etc. etc.).  This means that we need to SHIFT RIGHT the elements in the pre-forecast which we grab.  The way we specify this for the BUILDER is 
            # using the ELEMENT-SHIFT operator "[num_elements_to_shift_right]" in the pred reference.  The integer we put in
            # as "num_elements_to_shift_right" will make the ENGINE grab elements to the right (positive int) or left (negative int).  Note: the ENGINE is set-up to automatically return
            # a value of zero whenever we request an element outside of the range of row we are grabbing values from (versus throwing an error).  This is set-up as a convenience, since
            # it's expected that most of the time, that is the desired behavior when we stray outside of the element bounds.
            #
            # PF = {pre_forecast_inputs_meta_data.get_id()}.{pf_col_id}[{(num_months_treatment-1)-i}]
            # PC = {treatment_table_meta_data.get_id()}.{pc_col_id}:{i}
            pred_pf_col_id = f"{pre_forecast_inputs_meta_data.get_id()}.{pf_col_id}[{(num_months_treatment-1)-i}]"
            pred_pc_col_id = f"{treatment_table_meta_data.get_id()}.{pc_col_id}:{i}"           

            list_of_ranges_on.append(ForecastMetaDataRange(count = num_elements, pred = [pred_pf_col_id, pred_pc_col_id], args = None, objs = None))



            # Part 2
            # For all remaining elements in the row, we simply take the forecast input data (time shifted based on the treatment month)
            # and multiply it by the same patient progression curve (for the month of treatment)

            # In the DATA calculations the PC doesn't change:
            # PF = updated_data.iloc[incoming_patient_flow].shift(i).iloc[i:]
            # PC = treatment_table_data[pc_col_id][i]
            updated_data.loc[i:, curr_on_treat_col_id] = updated_data[incoming_patient_flow].shift(i).iloc[i:] * treatment_table_data[pc_col_id][i]


            # In the META_DATA caculations:
            # The PC element uses the same reference type that we specified in part 1.
            # The same two things we did above when calculating the PATIENTS ON THERAPY... one, a full reference using the full
            # reference operator "." (i.e. object_id.row_id) since it's addressing an ID in a different object (the object is the treatment table object, 
            # that should have been created in steps prior to this function), and the second is a individual element operator ":" (i.e. row_id:element_id)
            # since we need to multiply all elements in the PF by the same single emlement... not element by element.  So the PC reference will end up being
            # in the format: object_id.row_id:element_id.
            # For the PF elements, we simply need to grab the patient input data (last id from updated_meta_data), but we need to use previous month's data; the way we 
            # specify this for the BUILDER is using the ELEMENT-SHIFT operator "[num_elements_to_shift_right]" in the pred reference.  The integer we put in
            # as "num_elements_to_shift_right" will make the ENGINE grab elements to the right (positive int) or left (negative int).  In our case, we need the element that is 
            # i elements before (so if it's the column for month_2 of treatment, we need incoming data from two months ago, if it's month_3 we need three months ago, etc. etc.).  
            # Note: the ENGINE is set-up to automatically return a value of zero whenever we request an element outside of the range of row we are grabbing values from 
            # (versus throwing an error).  This is set-up as a convenience, since it's expected that most of the time, that is the desired behavior when we stray outside 
            # of the element bounds.

            # PF = {incoming_patient_flow}[{-i}]
            # PC = (same as above)
            pred_pf_col_id = f"{incoming_patient_flow}[{-i}]"

            list_of_ranges_on.append(ForecastMetaDataRange(count = None, pred = [pred_pf_col_id, pred_pc_col_id], args = None, objs = None))

            # create the meta_data row with the ranges added instead of regular preds
            updated_meta_data = updated_meta_data.add_col_meta_data(frame = updated_meta_data,
                                                                    id = curr_on_treat_col_id,
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                    action = ForecastDataSeriesMetaDataAction.PROD,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    display_name = f"# of patients in '{display_name}' month {i+1}",
                                                                    data_values = updated_data[curr_on_treat_col_id].to_list(),
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    ranges = list_of_ranges_on,
                                                                    verify_integrity=True,
                                                                    drop_dups = False)
            
           pat_on_treatment_list_of_col_ids.append(curr_on_treat_col_id)  # add to list of preds to be used for totals calculation
         
        # ======
        # TOTALS
        # ======


        # Copy the updated data to pat_by_treatment_month_data and meta_data so we can add the totals to those specific dataframes
        pat_by_treatment_month_data = copy.deepcopy(updated_data)
        pat_by_treatment_month_meta_data = copy.deepcopy(updated_meta_data)

        # TODO:  Convert again to pandas (same as top) to see if this fixed the summing to total problem



        # Add by column all the patients by treatment month columns to get the total patients on treatment for that month
        pat_on_treatment_total_id = ForecastTreatmentTBController._gen_pat_on_treat_id(id = id, month_prefix = month_prefix, isTotal=True)
        
        
        pat_by_treatment_month_data = ForecastDataModel.to_pandas(pat_by_treatment_month_data)
        pat_by_treatment_month_data[pat_on_treatment_total_id] = pat_by_treatment_month_data[pat_on_treatment_list_of_col_ids].sum(axis=1)

        pat_by_treatment_month_meta_data = pat_by_treatment_month_meta_data.add_col_meta_data(frame = pat_by_treatment_month_meta_data,
                                                                                              id = pat_on_treatment_total_id,
                                                                                              step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                              action = ForecastDataSeriesMetaDataAction.TOTAL,
                                                                                              data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                                              display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                              display_name = f"Total # patients on '{display_name}'",
                                                                                              data_values = pat_by_treatment_month_data[pat_on_treatment_total_id],
                                                                                              validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                              pred = pat_on_treatment_list_of_col_ids,
                                                                                              verify_integrity=True,
                                                                                              drop_dups = False)
        




        # PATIENTS LEAVING TREATMENT BY TREATMENT MONTH
        # =============================================

        # The overall caculations for patients leaving by treatment month is:
        #
        #   = Num patients by treatment month at the end of the Prev Month (PM) - Num patients by treatment month at the end of the Current Month (CM)
        #
        # However, we calculate this in three different ways depending on what we use for PM

        # we add this to the existing data and meta_data that already has the patients on treatment calculations
        pat_leaving_by_treatment_month_data = copy.deepcopy(pat_by_treatment_month_data)
        pat_leaving_by_treatment_month_meta_data = copy.deepcopy(pat_by_treatment_month_meta_data)


        for i in range(num_months_treatment):
         curr_on_treat_col_id = ForecastTreatmentTBController._gen_pat_on_treat_id(id = id, month_prefix = month_prefix, month_num = i+1)
         curr_leaving_treat_col_id = ForecastTreatmentTBController._gen_pat_leaving_id(id = id, month_prefix = month_prefix, month_num = i+1)

         # =====================
         # FIRST TREATMENT MONTH
         # =====================
         if(i == 0):

            # FIRST TREATMENT MONTH
            # As with the calculation for PATIONS ON TREATMENT BY TREATMENT MONTH, the first TREATMENT MONTH is a special case, we simply subtract the patients by current EOM
            # that we calculated above (pat_leaving_by_treatment_month_data OR pat_leaving_by_treatment_month_meta_data) from the input patient flow (last id of pat_leaving_by_treatment_month_data or update_meta_data).
            # this represents the number of diagnosed patients who get to the end of the first month.

            # In the DATA calculations:
            # PM = pat_leaving_by_treatment_month_data[incoming_patient_flow]
            # CM = pat_leaving_by_treatment_month_data[curr_on_treat_col_id]
            pat_leaving_by_treatment_month_data[curr_leaving_treat_col_id] = pat_leaving_by_treatment_month_data[incoming_patient_flow] - pat_leaving_by_treatment_month_data[curr_on_treat_col_id]


            # In the META-DATA calculation:
            # We specify a SUB for the action and for the preds we feed the last update_meta_data id for PM ref_id and the number of patients at the end of the current month
            # id for the CM ref_id:
            # PM = pat_leaving_by_treatment_month_meta_data.get_last_id()
            # CM = curr_on_treat_col_id
            pred_pm_col_id = incoming_patient_flow
            pred_cm_col_id = curr_on_treat_col_id
            preds_leaving = [pred_pm_col_id, pred_cm_col_id]

            pat_leaving_by_treatment_month_meta_data = pat_leaving_by_treatment_month_meta_data.add_col_meta_data(frame = pat_leaving_by_treatment_month_meta_data,
                                                                                                                  id = curr_leaving_treat_col_id,
                                                                                                                  step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                                                  action = ForecastDataSeriesMetaDataAction.SUB,
                                                                                                                  data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                                                                  display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                                                  display_name = f"# of patients leaving '{display_name}' in month {i+1}",
                                                                                                                  data_values = pat_leaving_by_treatment_month_data[curr_leaving_treat_col_id].to_list(),
                                                                                                                  validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                                                  pred = preds_leaving,
                                                                                                                  verify_integrity=True,
                                                                                                                  drop_dups = False)


         # ==========================
         # REMAINING TREATMENT MONTHS
         # ==========================
         else:
               # All treatment months OTHER THAN the first month are more complicated because we now have TWO TWO different sets of PM data based on which elements in the row
               # we will be calculating.  So we'll need to create two ranges for the meta_data, where each range provides the appropriate references (preds) to use.
               list_of_ranges_leaving = []



               # Part 1
               # For the first month of the forecast (and only the first month), to get the number of patients leaving, we have to subtract the number of patients at the end of the
               # first month (CM) (i.e. pat_leaving_by_treatment_month_data[curr_on_treat_col_id]), that we just calculated from the number of patients in the month BEFORE THE 
               # FORECAST (PM) (i.e. pre_forecast_patient_flow_data) for the same MONTH OF TREATMENT.  Again, we do this ONLY for the first element (i.e. forecast month)
               pfpf_col_id = f"{pmpf_col_prefix}_{i}"


               # In the DATA calculations:
               # PM = pre_forecast_patient_flow_data.loc[0, f"{pmpf_col_prefix}_{i}"]
               # CM = pat_leaving_by_treatment_month_data.loc[0, curr_on_treat_col_id]
               pat_leaving_by_treatment_month_data.loc[0, curr_leaving_treat_col_id] = pre_forecast_patient_flow_data.loc[0, pfpf_col_id] - pat_leaving_by_treatment_month_data.loc[0, curr_on_treat_col_id]


               # In the META-DATA calculations:
               # PM = {pre_forecast_patient_flow_meta_data.get_id()}.{pfpf_col_id}
               # CM = curr_on_treat_col_id
               pred_pm_col_id = f"{pre_forecast_patient_flow_meta_data.get_id()}.{pfpf_col_id}"
               pred_cm_col_id = curr_on_treat_col_id

               list_of_ranges_leaving.append(ForecastMetaDataRange(count = 1, pred = [pred_pm_col_id, pred_cm_col_id], args = None, objs = None))


               # Part 2
               # The remaining months of the forecast are much more straightforward, as we simply subtract the number of patients at the end of the current month (CM) 
               # (i.e. pat_leaving_by_treatment_month_data[curr_on_treat_col_id], same as in part one), from the number of patients at the end of the prevous month (PM)
               # (i.e. pat_leaving_by_treatment_month_data[prev_on_treat_col_id] shifted by 1 month prior) for the same MONTH OF TREATMENT

               # In the DATA calculations:
               # PM = pat_leaving_by_treatment_month_data[prev_on_treat_col_id].shift(1)[1:].reset_index(drop=True)
               # CM = pat_leaving_by_treatment_month_data.loc[1:, curr_on_treat_col_id].reset_index(drop=True)
               pat_leaving_by_treatment_month_data.loc[1:, curr_leaving_treat_col_id] = (pat_leaving_by_treatment_month_data[prev_on_treat_col_id].shift(1)[1:].reset_index(drop=True) - pat_leaving_by_treatment_month_data.loc[1:, curr_on_treat_col_id].reset_index(drop=True)).to_list()
               

               # In the META-DATA calculations:
               # PM = {prev_on_treat_col_id}[{-1}]
               # CM = curr_on_treat_col_id
               pred_pm_col_id = f"{prev_on_treat_col_id}[{-1}]"
               pred_cm_col_id = curr_on_treat_col_id

               list_of_ranges_leaving.append(ForecastMetaDataRange(count = None, pred = [pred_pm_col_id, pred_cm_col_id], args = None, objs = None))


               # create the meta_data row with the ranges added instead of regular preds
               pat_leaving_by_treatment_month_meta_data = pat_leaving_by_treatment_month_meta_data.add_col_meta_data(frame = pat_leaving_by_treatment_month_meta_data,
                                                                                                                     id = curr_leaving_treat_col_id,
                                                                                                                     step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                                                     action = ForecastDataSeriesMetaDataAction.SUB,
                                                                                                                     data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                                                                     display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                                                     display_name = f"# of patients leaving '{display_name}' in month {i+1}",
                                                                                                                     data_values = pat_leaving_by_treatment_month_data[curr_leaving_treat_col_id].to_list(),
                                                                                                                     validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                                                     ranges = list_of_ranges_leaving,
                                                                                                                     verify_integrity=True,
                                                                                                                     drop_dups = False)


         # save the current id as the previous id (this is needed to for calculating patients leaving as we subtract current row from previous row)
         prev_on_treat_col_id = curr_on_treat_col_id
         pat_leaving_treatment_list_of_col_ids.append(curr_leaving_treat_col_id) # add to list of preds to be used for totals calculation



        # ======
        # TOTALS
        # ======
        # generate a totals PATIENTS LEAVING TREATMENT column
        # Add by column all the patients leaving by treatment month columns to get the total patients leaving treatment for that month
        pat_leaving_treat_total_id = ForecastTreatmentTBController._gen_pat_leaving_id(id = id, month_prefix = month_prefix, isTotal=True) # ZIV

        pat_leaving_by_treatment_month_data = ForecastDataModel.to_pandas(pat_leaving_by_treatment_month_data)
        pat_leaving_by_treatment_month_data[pat_leaving_treat_total_id] = pat_leaving_by_treatment_month_data[pat_leaving_treatment_list_of_col_ids].sum(axis=1)

        pat_leaving_by_treatment_month_meta_data = pat_leaving_by_treatment_month_meta_data.add_col_meta_data(frame = pat_leaving_by_treatment_month_meta_data,
                                                                                                              id = pat_leaving_treat_total_id,
                                                                                                              step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                                              action = ForecastDataSeriesMetaDataAction.TOTAL,
                                                                                                              data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                                                              display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                                              display_name = f"Total # patients leaving '{display_name}'",
                                                                                                              data_values = pat_leaving_by_treatment_month_data[pat_leaving_treat_total_id] ,
                                                                                                              validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                                              pred = pat_leaving_treatment_list_of_col_ids,
                                                                                                              verify_integrity=True,
                                                                                                              drop_dups = False)
        


        # ==============
        # RETURN RESULTS
        # ==============
        return({"pat_on_treatment": {"treatment_table_data": treatment_table_data, 
                                     "treatment_table_meta_data": treatment_table_meta_data, 
                                     "pat_by_treatment_month_data": ForecastDataModel.to_langflow(pat_by_treatment_month_data), 
                                     "pat_by_treatment_month_meta_data": pat_by_treatment_month_meta_data, 
                                     "updated_data": updated_data},
                "pat_leaving_treatment": {"treatment_table_data": treatment_table_data, 
                                          "treatment_table_meta_data": treatment_table_meta_data, 
                                          "pat_leaving_by_treatment_month_data": ForecastDataModel.to_langflow(pat_leaving_by_treatment_month_data), 
                                          "pat_leaving_by_treatment_month_meta_data": pat_leaving_by_treatment_month_meta_data, 
                                          "updated_data": updated_data}})





    # calc_treatment_rx_forecast_for_product
    # Calculates a forecast for total number of Rxs for a product expected at every every month of the forecast divided
    #
    # INPUT:
    #
    #   treatment_id (str):  The treatment component's id, as a prefix to each column name generated
    #   treatment_name (str):  The treatment component's display name, used for generating display names in the meta_data
    #   product_id (str):  The column id in the treatment details table that holds the number of Rxs per month in treatment for this product, also used for generating unique column ids for each product's Rx forecast
    #   product_name (str):  The product's display name, used for generating display names in the meta_data
    #   month_prefix (str):  The string to append to the end of an ID to indicate this is a total values by month in a treatment journey
    #   updated_data (DataFrame):  The current forecast data to which we will be adding new columns for the Rx forecast for this product
    #   updated_meta_data (ForecastMetaDataFrame):  The current forecast meta_data to which we will be adding new rows for the Rx forecast for this product
    #   treatment_table_data (DataFrame):  The treatment details table data, which includes the number of Rxs per month in treatment for each product
    #   treatment_table_meta_data (ForecastMetaDataFrame):  The treatment details table meta_data, which includes the meta_data for the number of Rxs per month in treatment for each product
    #
    # OUTPUT:
    # 
    #   Tuple(DataFrame, ForecastMetaDataFrame):  A tuple containing the updated forecast data and updated forecast meta_data with the new columns/rows added for the Rx forecast for this product

    @staticmethod
    def calc_treatment_rx_forecast_for_product(# self variables passed in
                                               treatment_id: str,
                                               treatment_display_name: str,
                                               product_id: str,
                                               product_display_name: str,
                                               month_prefix: str,
                                               
                                               # current forecast
                                               updated_data: DataFrame | pd.DataFrame,
                                               updated_meta_data: ForecastMetaDataFrame,

                                               # treatment details table
                                               treatment_table_data: DataFrame,
                                               treatment_table_meta_data: ForecastMetaDataFrame) -> Tuple[DataFrame, ForecastMetaDataFrame]:
            
            updated_data = ForecastDataModel.to_pandas(updated_data)
            
            # get the total patients per treatment month columns by searching on the unique prefix for all patients on treatment
            curr_patients_on_treatment_prefix = ForecastTreatmentTBController._gen_pat_on_treat_id(id = treatment_id, month_prefix = month_prefix)
            curr_patients_on_treatment_totals_id = ForecastTreatmentTBController._gen_pat_on_treat_id(id = treatment_id, month_prefix = month_prefix, isTotal=True)
            list_of_patients_on_product_columns = [colname for colname in updated_data.columns if (colname.startswith(curr_patients_on_treatment_prefix) and (colname != curr_patients_on_treatment_totals_id))]
            
            # set-up list to hold all col_id references used to calculate the _Total Rx at the end
            list_of_by_treatment_month_rx = []

            # CALCULATE BY TREATMENT MONTH
            # calculate the number of RXs by multipying to total number of patients in each month of their treatment (for every month of the forecast)
            # by the total Rxs provided for a patient at that month in the program
            for i in range(len(list_of_patients_on_product_columns)):
                  # setup
                  pred_col_name = list_of_patients_on_product_columns[i]
                  month_num = int(list_of_patients_on_product_columns[i].removeprefix(curr_patients_on_treatment_prefix))
                  num_rx_col_id = ForecastTreatmentTBController._gen_rx_per_month_id(treatment_id = treatment_id, product_id = product_id, month_prefix = month_prefix, month_num = month_num)


                  # RXs FOR PATIENT IN MONTH i OF TREATMENT
                  # ---------------------------------------
                  # The formula is very simple, number of patients in month i of treatment (NP) * number of Rxs provided to a patient in month i of treatment (NR)

                  # In the DATA calculations:
                  # NP = updated_data[list_of_patients_on_product_columns[i]]
                  # NR = treatment_table_data[product_id][month_num-1]
                  updated_data[num_rx_col_id] = updated_data[list_of_patients_on_product_columns[i]] * treatment_table_data[product_id][month_num-1]


                  # In the META-DATA calculations:
                  # NP = pred_col_name (i.e. the row holding total patients for every forecast month who are in month i of their treatment)
                  # NR = {treatment_table_meta_data.get_id()}.{product_id}:{month_num-1} (i.e. the total number of Rxs for that product given to each patient in month i of their treatment; the reference is a full reference (i.e. ".") since treatment details is a different object, individual element (i.e. ":") since all values in row are multiplied by single constant)
                  np_col_id = pred_col_name
                  nr_col_id = f"{treatment_table_meta_data.get_id()}.{product_id}:{month_num-1}"

                  updated_meta_data = updated_meta_data.add_col_meta_data(frame = updated_meta_data,
                                                                          id = num_rx_col_id,
                                                                          step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                          action = ForecastDataSeriesMetaDataAction.PROD,
                                                                          data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                          display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                          display_name = f"# of '{product_display_name}' Rx for all patients patients in '{treatment_display_name}' in treatment month {i+1}",
                                                                          data_values = updated_data[num_rx_col_id].to_list(),
                                                                          validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                          pred = [np_col_id, nr_col_id],
                                                                          verify_integrity=True,
                                                                          drop_dups = False)
                  
                  list_of_by_treatment_month_rx.append(num_rx_col_id)  # add to list of preds to be used for totals calculation



            # TOTAL RXs FOR PATIENT IN MONTH X OF FORECAST
            # --------------------------------------------
            # Add all the previous ids for Rx by treatment month for that forecast month
            totals_col_id = ForecastTreatmentTBController._gen_rx_per_month_id(treatment_id = treatment_id, product_id = product_id, month_prefix = month_prefix, isTotal=True)
            
            # In the DATA calculations:
            # TODO:  ZIV check if the below line is really working or not
            updated_data[totals_col_id] = updated_data[list_of_by_treatment_month_rx].sum(axis = 1)


            # In the META-DATA calculations:
            # SUM ACTION against all the previous ids for Rx by treatment month for that forecast month stored in list_of_by_treatment_month_rx

            updated_meta_data = updated_meta_data.add_col_meta_data(frame = updated_meta_data,
                                                                    id = totals_col_id,
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                    action = ForecastDataSeriesMetaDataAction.TOTAL,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    display_name = f"# of '{product_display_name}' Rx for patients in '{treatment_display_name}' Total",
                                                                    data_values = updated_data[totals_col_id].to_list(),
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    pred = list_of_by_treatment_month_rx,
                                                                    verify_integrity=True,
                                                                    drop_dups = False)

            return(ForecastDataModel.to_langflow(updated_data), updated_meta_data)
    




    # ================
    # HELPER FUNCTIONS
    # ================

    # generate column IDs for number of patients on treatment by month in treatment
    @staticmethod
    def _gen_pat_on_treat_id(id: str, month_prefix: str, month_num: int = None, isTotal = False):
         # if this is a total line, return a title ID
         if(isTotal):
            return(f"{id}_{ForecastDataModel.TREATMENT_PAT_TOTAL_BY_MONTH}_Total")
         
         # if no month_num provided, just spit out the prefix
         elif(month_num is None):
            return(f"{id}_{ForecastDataModel.TREATMENT_PAT_TOTAL_BY_MONTH}_{month_prefix}_")
         
         # if month_num provided, return a relative ID
         else:
            return(f"{id}_{ForecastDataModel.TREATMENT_PAT_TOTAL_BY_MONTH}_{month_prefix}_{month_num}")
         
    

    # generate column IDs for number of patients leaving treatment by month in treatment
    @staticmethod
    def _gen_pat_leaving_id(id: str, month_prefix: str, month_num: int = None, isTotal = False):
         # if this is a total line, return a title ID
         if(isTotal):
            return(f"{id}_{ForecastDataModel.TREATMENT_PAT_LEAVING_BY_MONTH}_{month_prefix}_Total")
         
         # if no month_num provided, just spit out the prefix
         elif(month_num is None):
            return(f"{id}_{ForecastDataModel.TREATMENT_PAT_LEAVING_BY_MONTH}_{month_prefix}_")
         
         # if month_num provided, return a relative ID
         else:
            return(f"{id}_{ForecastDataModel.TREATMENT_PAT_LEAVING_BY_MONTH}_{month_prefix}_{month_num}")
         


    # generate column IDs for RXs by month in treatment
    @staticmethod
    def _gen_rx_per_month_id(treatment_id: str, product_id: str, month_prefix: str, month_num: int = None, isTotal = False):
         # if this is a total line, return a title ID
         if(isTotal):
              return(f"{treatment_id}_{product_id}_{ForecastDataModel.TREATMENT_PRODUCT_RX_BY_MONTH}_{month_prefix}_Total")

         # if no month_num provided, just spit out the prefix
         elif(month_num is None):
              return(f"{treatment_id}_{product_id}_{ForecastDataModel.TREATMENT_PRODUCT_RX_BY_MONTH}_{month_prefix}_")

         # if month_num provided, return a relative ID
         else:
              return(f"{treatment_id}_{product_id}_{ForecastDataModel.TREATMENT_PRODUCT_RX_BY_MONTH}_{month_prefix}_{month_num}")
              