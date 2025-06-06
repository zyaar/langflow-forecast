from langflow.io import TableInput
from langflow.schema import DataFrame

# FORECAST SPECIFIC IMPORTS
# =========================


# COMPONENT SPECIFIC IMPORTS
# ==========================


# Patient Count Table
testTable = TableInput(
    name="patient_count_table",
    display_name="Patient Counts (by Month)",
    info="For each time period, enter the expected number of patients entering the forecast.",
    required=False,
    show=False,
    dynamic=True,
    table_schema=[
        {
            "name": "date",
            "display_name": "Date",
            "type": "date",
            "description": "Date of patient count",
            #"edit_mode": EditMode.INLINE,
            "disable_edit": True,
        },
        {
            "name": "patient_count_table",
            "display_name": "Patient Count",
            "type": "int",
            "description": "Patient count",
            #"edit_mode": EditMode.INLINE,
        },
    ],
    value=[
        {"date": "2020-01-01", "patient_count": 10},
        {"date": "2020-02-01", "patient_count": 20},
        {"date": "2020-03-01", "patient_count": 30},
    ],
)

print(testTable)
print(type(testTable))
print(testTable.display_name)
dataFrameTest = DataFrame(data=testTable)
print(dataFrameTest)
