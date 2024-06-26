# ETL Automation for Data Warehouse

## Project Overview
This project automates an ETL (Extract, Transform, Load) process from a source system to a Data Warehouse, specifically designed for a medical entity. The objective is to transform operational format data into a set of tables suitable for a Data Warehouse based on the star schema.

### Data
The data includes 3 csv files:

#### Patients
This file contains demographic and personal information about the patients. Here’s a breakdown of its typical columns:

- **Name (VARCHAR)**: The first name of the patient.
- **Family (VARCHAR)**: The family name or surname of the patient.
- **ID (INT)**: A unique identifier for each patient, presumably a national ID or medical record number.
- **Birthdate (DATETIME)**: The patient's date of birth.
- **City (VARCHAR)**: The city where the patient lives.
- **Region (VARCHAR)**: The regional area or district where the patient resides.

#### Tests
This file records the results of various medical tests that patients have undergone. The columns in this file might include:

- **Sugar (FLOAT)**: Measurement of sugar levels in the blood.
- **Fe (FLOAT)**: Measurement of iron (Fe) levels in the blood.
- **Whitcells (FLOAT)**: Count of white blood cells.
- **Redcells (FLOAT)**: Count of red blood cells.
- **Date (DATETIME)**: The date on which the test was conducted.
- **ID (INT)**: The identifier that links the test results to a specific patient in the `Patients.csv`.

#### VisitInfo
This file includes additional information gathered during each patient's visit to the medical facility. Here’s what the columns typically represent:

- **Sick (BOOLEAN)**: Indicates whether the patient was sick at the time of the visit (1 for yes, 0 for no).
- **Active (BOOLEAN)**: Indicates whether the patient was physically active at the time of the visit (1 for yes, 0 for no).
- **Medication (BOOLEAN)**: Indicates whether the patient was taking any medication at the time of the visit (1 for yes, 0 for no).
- **Regilar (BOOLEAN)**: Indicates whether the patient's condition was considered regular at the time of the visit (1 for yes, 0 for no). Note: This seems like a typographical error in the column name and might be intended to say "Regular".
- **Date (DATETIME)**: The date on which the visit occurred.
- **ID (INT)**: The identifier that links the visit information to a specific patient in the `Patients.csv`.

## Technologies Used
- **Python**: For scripting the ETL process.
- **MySQL**: Initial relational database for storing operational data.
- **Snowflake**: Target Data Warehouse platform.# MySQL-SnowFlake-ETL
