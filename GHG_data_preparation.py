# INITIATIONS AND DECLARATIONS
# ---------------------------------------------------------------------------------------------------------------------------


# Importing pre-installed required packages
import pandas as pd
import openpyxl
import sqlite3 as SQL

# Input files from the source: https://ec.europa.eu/eurostat/data/database
# Rows and column naming inconsistent in downloaded files therefore the following manual changes:
    # field names and years to be in the same row
    # adding column for additional features and changing PRODUCT (LABELS) to CPA in trade files
    # stacking WORLD, ROW and DOM in environemntal footprint file

path = r"C:\\myPython\\Analysis\\Analysis\\00_Archive\\02_Updated_Data\\"
connection = SQL.connect(r"C:\\myPython\\Analysis\\Analysis\\00_Archive\\02_Updated_Data\\OUT_data.db")

# Define input files with Eurostat naming conventions
files = {
    "GVA"           :   ["nama_10_a64.xlsx", "Sheet 1"],
    "TRADE_EXPORT"  :   ["ds-059268_export.xlsx", "Sheet 1"],   
    "TRADE_IMPORT"  :   ["ds-059268_import.xlsx", "Sheet 1"],   
    "ENV_EMISSIONS" :   ["env_ac_ainah_r2.xlsx", "Sheet 1"],
    "ENV_FOOTPRINT" :   ["env_ac_io10.xlsx", "Sheet 1"]         
}

# Define EU country codes (based on EU27_2020)
eu = ",".join(f"'{cat}'" for cat in 
              ['AT', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR', 'GR', 'HR', 'HU', 'IE', 'IT', 
               'LT', 'LU', 'LV', 'MT', 'NL', 'PL', 'PT', 'RO', 'SE', 'SI', 'SK'])

# Define NACE categories and aggregation (summing to totals)
nace = ",".join(f"'{cat}'" for cat in 
        ['A01', 'A02', 'A03', 'B', 'C10-C12', 'C13-C15', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21', 'C22', 'C23', 'C24',
        'C25', 'C26', 'C27', 'C28', 'C29', 'C30', 'C31_C32', 'C33', 'D', 'E36', 'E37-E39', 'F', 'G45', 'G46', 'G47', 
        'H49', 'H50', 'H51', 'H52', 'H53', 'I', 'J58', 'J59_J60', 'J61', 'J62_J63', 'K64', 'K65', 'K66', 'L', 'M69_M70', 
        'M71', 'M72', 'M73', 'M74_M75', 'N77', 'N78', 'N79', 'N80-N82', 'O', 'P', 'Q86', 'Q87_Q88', 'R90-R92', 'R93', 
        'S94', 'S95', 'S96', 'T', 'HH_HEAT', 'HH_OTH', 'HH_TRA'])

# Define mapping between CPA and NACE codes (to connect TRADE files with other files)
cpa_to_nace = {
    '01': 'A01',       '02': 'A02',       '03': 'A03',       '05': 'B',         '06': 'B',
    '07': 'B',         '08': 'B',         '09': 'B',         '10': 'C10-C12',   '11': 'C10-C12',
    '12': 'C10-C12',   '13': 'C13-C15',   '14': 'C13-C15',   '15': 'C13-C15',   '16': 'C16',
    '17': 'C17',       '18': 'C18',       '19': 'C19',       '20': 'C20',       '21': 'C21',
    '22': 'C22',       '23': 'C23',       '24': 'C24',       '25': 'C25',       '26': 'C26',
    '27': 'C27',       '28': 'C28',       '29': 'C29',       '30': 'C30',       '31': 'C31_C32',
    '32': 'C31_C32',   '33': 'C33',       '35': 'D',         '36': 'E36',       '37': 'E37-E39',
    '38': 'E37-E39',   '39': 'E37-E39',   '41': 'F',         '42': 'F',         '43': 'F',
    '45': 'G45',       '46': 'G46',       '47': 'G47',       '49': 'H49',       '50': 'H50',
    '51': 'H51',       '52': 'H52',       '53': 'H53',       '55': 'I',         '56': 'I',
    '58': 'J58',       '59': 'J59_J60',   '60': 'J59_J60',   '61': 'J61',       '62': 'J62_J63',
    '63': 'J62_J63',   '64': 'K64',       '65': 'K65',       '66': 'K66',       '68': 'L',
    '69': 'M69_M70',   '70': 'M69_M70',   '71': 'M71',       '72': 'M72',       '73': 'M73',
    '74': 'M74_M75',   '75': 'M74_M75',   '77': 'N77',       '78': 'N78',       '79': 'N79',
    '80': 'N80-N82',   '81': 'N80-N82',   '82': 'N80-N82',   '84': 'O',         '85': 'P',
    '86': 'Q86',       '87': 'Q87_Q88',   '88': 'Q87_Q88',   '90': 'R90-R92',   '91': 'R90-R92',
    '92': 'R90-R92',   '93': 'R93',       '94': 'S94',       '95': 'S95',       '96': 'S96',
    '97': 'T',         '98': 'T',         '99': 'T',         
    'dummy0': 'U', 'dummy1': 'HH_HEAT' ,'dummy2':'HH_OTH', 'dummy3':'HH_TRA'
}

# Used for changing incosistent country names thorughout different original input files
changes = {
    "BE": "Belgium", "DE": "Germany", "ES": "Spain", "FR": "France",
    "GB": "United Kingdom", "GR": "Greece", "IE": "Ireland", "IT": "Italy"
}

# Used to eliminate overlapping categories allowing for correct aggregation (so that TOTAL = sum of individual categories)
drop_categories =  ",".join(f"'{cat}'" for cat in
            ["A", "B-E", "C", "C16-C18", "C22_C23", "C24_C25", "C29_C30", "C31-C33", "E", "G", "G-I", "H", "J", 
             "J58-J60", "K", "L68A", "M", "M_N", "M69-M71", "M73-M75", "N", "O-Q", "Q", "R", "R-U", "S", "HH", 
             "TOTAL_HH"])

# Used to update inconsistent category naming accross different original files
data_quality = {"C10-12": "C10-C12", "C13-15": "C13-C15", "C31_32": "C31_C32", "E37-39": "E37-E39", "J59_60": "J59_J60",
                "J62_63": "J62_J63", "M69_70": "M69_M70", "M74_75": "M74_M75", "N80-82": "N80-N82", "Q87_88": "Q87_Q88",
                "R90-92": "R90-R92"}

# Define summable greenhouse gas categories (GHG) from original Eurostat data:
    # CO2, N2O in CO2 equivalent, CH4 in CO2 equivalent, HFC in CO2 equivalent, PFC in CO2 equivalent, 
    # SF6 in CO2 equivalent, NF3 in CO2 equivalent)
greenhouse_gasses = ["CO2", "N2O_CO2E", "CH4_CO2E", "HFC_CO2E", "PFC_CO2E", "NF3_SF6_CO2E", "GHG"]

# Define columns names
columns = ["GEO_code", "GEO_name", "CAT_code", "CAT_name", "CAT_other"]

# Initiate dataframe object
df = pd.DataFrame()



# DATA LOADING AND FORMATTING
# ---------------------------------------------------------------------------------------------------------------------------


# Load input from downloaded Eurostat files
excel = {key: pd.read_excel(path + value[0], sheet_name=value[1], skiprows=8) for key, value in files.items()}

# Rename columns and stack data
for file_name, excel_data in excel.items():
    years = list(excel_data.columns)[5: ]
    column_name = excel_data.columns[3]
    
    # Specific adjustment for trade files having different structure
    if column_name == "CPA08 (Labels)":
        trade_partners = [x[:x.find(".")] if x.find(".")!=-1 else x for x in list(excel_data.columns)[5: ]]
        # Adding year period from 2008 to 2022 due to different table format
        years=[]
        for _ in range(int(len(trade_partners)/15)):
            years=years+[str(2008 + x)+str(_) for x in range(15)]
        years_={years[_]:trade_partners[_] for _ in range(len(years))}
        years=years_
    
    excel_data.columns = columns + (list(years.keys()) if column_name == "CPA08 (Labels)" else years)
    
    # Stacking data
    for year in years:                        
        temp_df = pd.DataFrame({
            "Indicator": file_name,
            "GEO_code": excel_data["GEO_code"],
            "GEO_name": excel_data["GEO_name"],
            "CAT_code": excel_data["CAT_code"],
            "CAT_name": excel_data["CAT_name"],
            "CAT": column_name if file_name!="ENV_FOOTPRINT" else excel_data["CAT_other"],
            "CAT_other": "" if column_name != "CPA08 (Labels)" else years[year],
            "Period": year[0:4],
            "Value": excel_data[year]
        })
        df = pd.concat([df, temp_df], ignore_index=True)


# ADDRESSING DATA QUALITY ISSUES AND CREATING MAPPING TABLES
# ---------------------------------------------------------------------------------------------------------------------------


# Adjust country codes and names, data quality issue
# Country names adjustment
df.loc[df["GEO_code"].isin(changes.keys()), "GEO_name"]=df["GEO_code"].map(changes)             
# Country code adjustments
df["GEO_code"] = df["GEO_code"].replace({"GR": "EL", "GB": "UK"})                               
# Trading partner adjustment
df["CAT_other"] = df["CAT_other"].replace({"GR": "EL", "GB": "UK"})                             
# CPA to NACE conversion
df["CAT_code"] = df["CAT_code"].str.replace("CPA_", "", regex=False)                            
# Streamlining category naming
df.loc[df["CAT_code"].isin(data_quality.keys()), "CAT_code"]=df["CAT_code"].map(data_quality)   
# Streamlining category naming
df["CAT_code"] = df["CAT_code"].replace({"D35": "D", "L68": "L", "O84": "O", "P85": "P"})       
# Keeping summable categories only
df = df[~df['CAT'].isin(greenhouse_gasses)]                                                     

# Create mapping tables
df_cat_mapping = df[["CAT_code", "CAT_name", "CAT"]].drop_duplicates()
df_cat_mapping = df_cat_mapping[(df_cat_mapping["CAT"] == "NACE_R2 (Labels)") | (df_cat_mapping["CAT"] == "CPA08 (Labels)")]
df_geo_mapping = df[["GEO_code", "GEO_name"]].drop_duplicates()

# Clean and transform data for database
df = df.drop(["GEO_name", "CAT_name"], axis=1)                      
# dropping missing values defined as ":" in original source files
df = df[df["Value"] != ":"]                                         
# series initially loaded as string due to missing values
df["Value"] = pd.to_numeric(df["Value"], errors="coerce")           
# express in millions
df.loc[df["Indicator"] == "TRADE_EXPORT", "Value"] /= 1000000       
df.loc[df["Indicator"] == "TRADE_IMPORT", "Value"] /= 1000000       

# Exclude countries with incomplete data - identified in SQL check later on 
df = df[(df['GEO_code'] != "IE") & (df['GEO_code'] != "LU") & (df['GEO_code'] != "MT")]

# Writing to SQL database
df.to_sql("Database", connection, if_exists="replace", index=False)
df_cat_mapping.to_sql("Mapping_CAT", connection, if_exists="replace", index=False)
df_geo_mapping.to_sql("Mapping_GEO", connection, if_exists="replace", index=False)

# Map CPA to NACE
cpa_nace_mapping = pd.DataFrame(list(cpa_to_nace.items()), columns=['CPA', 'NACE'])
cpa_nace_mapping.to_sql("Mapping_CPA_NACE", connection, if_exists="replace", index=False)



# DATA SET CHECKS
# ---------------------------------------------------------------------------------------------------------------------------

# Comparing TOTALs from the original source files with the aggregates of individual categories
sql_checks=f"""
    WITH tempTable AS (
        SELECT
            Indicator,
            GEO_code,
            Period,
            SUM(Value) AS TOTAL_calc
        FROM Database
        WHERE
            UPPER(CAT_code) <> "TOTAL"
            AND UPPER(CAT_code) <> "TOTAL_HH"
            AND UPPER(CAT_code) <> "TOT_HH"
            AND CAT_code NOT IN ({drop_categories})
            AND CAT_other NOT IN ("WORLD", "EU27_2020_INTRA")
            AND GEO_code IN ({eu}, "EU27_2020")
        GROUP BY
            Indicator,
            GEO_code,
            Period
    )

    SELECT
        table_a.*,
        table_b.Value AS TOTAL,
        table_a.TOTAL_calc/table_b.Value AS Comp
    FROM
        tempTable AS table_a
    LEFT JOIN (
        SELECT
            Indicator,
            GEO_code,
            Period,
            Value
        FROM
            Database
        WHERE
            CAT_code = "TOTAL"
            AND Indicator = "GVA"
    ) AS table_b ON
        table_a.GEO_code = table_b.GEO_code
        AND table_a.Period = table_b.Period
    WHERE
        table_a.Indicator = "GVA" 
        AND table_a.TOTAL_calc/table_b.Value<0.95
        AND table_a.GEO_code <> "EU27_2020"

    UNION 

    SELECT
        table_a.*,
        table_b.Value AS TOTAL,
        table_a.TOTAL_calc/table_b.Value AS Comp
    FROM
        tempTable AS table_a
    LEFT JOIN (
        SELECT
            Indicator,
            GEO_code,
            Period,
            Value
        FROM
            Database
        WHERE
            CAT_code = "TOTAL_HH"
            AND Indicator = "ENV_EMISSIONS"
    ) AS table_b ON
        table_a.GEO_code = table_b.GEO_code
        AND table_a.Period = table_b.Period
    WHERE
        table_a.Indicator = "ENV_EMISSIONS" 
        AND table_a.TOTAL_calc/table_b.Value<0.95
        AND table_a.GEO_code <> "EU27_2020"
    
    UNION 

    SELECT
        table_a.*,
        table_b.Value AS TOTAL,
        table_a.TOTAL_calc/table_b.Value AS Comp
    FROM
        tempTable AS table_a
    LEFT JOIN (
        SELECT
            Indicator,
            GEO_code,
            Period,
            Value
        FROM
            Database
        WHERE
            CAT_code = "TOTAL"
            AND CAT_other = "WORLD"
            AND Indicator IN ("TRADE_EXPORT", "TRADE_IMPORT")
    ) AS table_b ON
        table_a.Indicator = table_b.Indicator
        AND table_a.GEO_code = table_b.GEO_code
        AND table_a.Period = table_b.Period
    WHERE
        table_a.Indicator IN ("TRADE_EXPORT", "TRADE_IMPORT") 
        AND table_a.GEO_code <> "EU27_2020"
        AND table_a.TOTAL_calc/table_b.Value<0.95
    
    UNION 

    SELECT
        table_a.*,
        table_b.Value AS TOTAL,
        table_a.TOTAL_calc/table_b.Value AS Comp
    FROM
        tempTable AS table_a
    LEFT JOIN (
        SELECT
            Indicator,
            GEO_code,
            Period,
            SUM(Value) AS Value
        FROM
            Database
        WHERE
            CAT_code = "TOTAL"
            AND CAT_other = "WORLD"
            AND Indicator IN ("ENV_FOOTPRINT")
        GROUP BY
            Indicator,
            GEO_code,
            Period
    ) AS table_b ON
        table_a.Indicator = table_b.Indicator
        AND table_a.GEO_code = table_b.GEO_code
        AND table_a.Period = table_b.Period
    WHERE
        table_a.Indicator IN ("ENV_FOOTPRINT") 
        AND table_a.TOTAL_calc/table_b.Value<0.95
"""

# Generated for analytical purposes
checks=pd.read_sql_query(sql_checks, connection)



# CREATE GOLDEN SOURCE FOR THE ANALYSIS: table "OUT_analysis"
# ---------------------------------------------------------------------------------------------------------------------------


sql_analysis = f"""
    SELECT 
        db.*
    FROM 
        (SELECT DISTINCT NACE
        FROM Mapping_CPA_NACE) AS filtered_mc
    LEFT JOIN 
        Database AS db ON filtered_mc.NACE = db.CAT_code
    WHERE 
        db.Period = 2019 
        AND db.GEO_code IN ({eu})
        AND Indicator IN ("GVA", "ENV_EMISSIONS")
  
    UNION

    SELECT 
        db.*
    FROM 
        (SELECT DISTINCT NACE
        FROM Mapping_CPA_NACE) AS filtered_mc
    LEFT JOIN 
        Database AS db ON filtered_mc.NACE = db.CAT_code
    WHERE 
        db.Period = 2019 
        AND db.GEO_code="EU27_2020"
        AND db.CAT_other<>"WORLD"
        AND db.Indicator="ENV_FOOTPRINT"   

    UNION
    
    SELECT 
        db.*
    FROM 
        (SELECT DISTINCT NACE
        FROM Mapping_CPA_NACE) AS filtered_mc
    LEFT JOIN 
        (SELECT 
            table_a.Indicator, 
            table_a.GEO_code,
            table_b.NACE AS CAT_code,	
            table_a.CAT,
            table_a.CAT_other,
            table_a.Period,
            table_a.Value       
        FROM Database AS table_a
        LEFT JOIN Mapping_CPA_NACE AS table_b
        ON table_a.CAT_code = table_b.CPA
        ) AS db 
    ON filtered_mc.NACE = db.CAT_code
    WHERE 
        db.Period = 2019 
        AND db.GEO_code IN ({eu})
        AND db.CAT_code NOT IN ("TOTAL", "XX")
        AND db.CAT_other NOT IN ("EU27_2020_INTRA", "WORLD")
        AND db.Indicator IN ("TRADE_EXPORT", "TRADE_IMPORT");
"""

analysis_df = pd.read_sql_query(sql_analysis, connection)
analysis_df.to_sql("OUT_analysis", connection, if_exists="replace", index=False)
connection.close()



# OPTIONAL: export data to Excel files
# ---------------------------------------------------------------------------------------------------------------------------

# Write everything to Excel
with pd.ExcelWriter(path + "OUT_data.xlsx") as writer:
    df.to_excel(writer, sheet_name="Database", index=False)
    df_cat_mapping.to_excel(writer, sheet_name="Mapping_CAT", index=False)
    df_geo_mapping.to_excel(writer, sheet_name="Mapping_GEO", index=False)
    cpa_nace_mapping.to_excel(writer, sheet_name="Mapping_CPA_NACE", index=False)
    checks.to_excel(writer, sheet_name="Checks", index=False)
analysis_df.to_excel(path + "OUT_analysis.xlsx", index=False)




