import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ===================== SQL CONNECTION =====================
def get_sql_connection():
    SQL_DRIVER   = os.getenv("SQL_DRIVER")
    SQL_SERVER   = os.getenv("SQL_SERVER")
    SQL_DATABASE = os.getenv("SQL_DATABASE")
    SQL_AUTH     = os.getenv("SQL_AUTH", "windows")

    if SQL_AUTH.lower() == "windows":
        conn_str = (
            f"DRIVER={{{SQL_DRIVER}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            "Trusted_Connection=yes;"
        )
    else:
        SQL_USERNAME = os.getenv("SQL_USERNAME")
        SQL_PASSWORD = os.getenv("SQL_PASSWORD")
        conn_str = (
            f"DRIVER={{{SQL_DRIVER}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
        )
    return pyodbc.connect(conn_str)


# ===================== LOAD TAGMAP FROM EXCEL =====================
def load_tagmap_from_excel():
    excel_file = os.getenv("EXCEL_FILE_MAIN", "Tagname.xlsx")
    excel_sheet = os.getenv("EXCEL_SHEET_MAIN", "Sheet1")

    print(f"[+] Loading TagMap from Excel → {excel_file}")

    df = pd.read_excel(excel_file, sheet_name=excel_sheet)

    # Normalize header names
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Flexible column detection
    col_map = {
        "plc": ["plc", "plcname", "plc_name"],
        "tagname": ["tagname", "tag_name", "name", "tag"],
        "tagindex": ["tagindex", "tag_index", "index"],
        "tagtype": ["tagtype", "tag_type", "type"],
        "tagdatatype": ["tagdatatype", "tag_data_type", "datatype"],
    }

    resolved = {}

    # Resolve real Excel column names
    for key, candidates in col_map.items():
        matched = None
        for col in df.columns:
            if col in candidates:
                matched = col
                break
        if not matched:
            raise Exception(f"❌ Missing Excel column for: {key}")
        resolved[key] = matched

    # Extract required columns only
    df = df[
        [resolved["plc"], resolved["tagname"], resolved["tagindex"],
         resolved["tagtype"], resolved["tagdatatype"]]
    ]

    # Rename to standard names
    df.columns = ["PLC", "TagName", "TagIndex", "TagType", "TagDataType"]

    print("[+] TagMap Columns Detected:", df.columns.tolist())
    return df



# ===================== MIGRATE FLOATTABLE → MULTILOG =====================
def migrate_float_to_multilog():

    conn = get_sql_connection()

    print("[+] Loading TagMap from Excel...")
    tagmap_df = load_tagmap_from_excel()

    print("[+] Reading FloatTable from SQL...")
    float_df = pd.read_sql("SELECT * FROM dbo.FloatTable", conn)

    # Normalize FloatTable columns
    float_df.columns = [str(c).strip().lower() for c in float_df.columns]

    # Expected columns from FloatTable
    float_map = {
        "readtime": ["dateandtime", "datetime", "readtime"],
        "tagindex": ["tagindex", "tag_index"],
        "tagvalue": ["val", "value"],
        "status": ["status"]
    }

    resolved_float = {}
    for key, candidates in float_map.items():
        matched = None
        for col in float_df.columns:
            if col in candidates:
                matched = col
                break
        if not matched:
            raise Exception(f"❌ Missing column in FloatTable for: {key}")
        resolved_float[key] = matched

    # Rename FloatTable columns
    float_df = float_df[
        [resolved_float["readtime"], resolved_float["tagindex"],
         resolved_float["tagvalue"], resolved_float["status"]]
    ]
    float_df.columns = ["ReadTime", "TagIndex", "TagValue", "Status"]

    # Merge with TagMap
    merged = float_df.merge(tagmap_df, on="TagIndex", how="left")

    # Normalize merged column names
    merged.columns = [str(c).strip() for c in merged.columns]

    # PLC column may be renamed during merge → detect automatically
    possible_plc_cols = [c for c in merged.columns if c.upper().startswith("PLC")]
    if not possible_plc_cols:
        raise Exception("❌ PLC column missing after merge!")

    plc_col = possible_plc_cols[0]  # Use first match

    # Warn missing PLC
    missing = merged[merged[plc_col].isna()]
    if len(missing):
        print("\n[WARNING] Missing TagIndex mappings in Excel:")
        print(missing["TagIndex"].unique())

    # Sort by TagIndex
    merged = merged.sort_values("TagIndex")

    # Prepare SQL insert
    cursor = conn.cursor()
    SQL_TABLE = os.getenv("SQL_TABLE")

    insert_sql = f"""
        INSERT INTO dbo.{SQL_TABLE} (
            ReadTime, PLC, TagIndex, TagName, TagType, TagDataType, TagValue, Status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    print("\n[+] Inserting into plc_multi_log...")

    count = 0
    for _, row in merged.iterrows():
        cursor.execute(
            insert_sql,
            row["ReadTime"],
            row[plc_col],
            int(row["TagIndex"]) if pd.notna(row["TagIndex"]) else None,
            row["TagName"],
            int(row["TagType"]) if pd.notna(row["TagType"]) else None,
            int(row["TagDataType"]) if pd.notna(row["TagDataType"]) else None,
            str(row["TagValue"]) if pd.notna(row["TagValue"]) else None,
            row["Status"]
        )
        count += 1

    conn.commit()
    cursor.close()
    conn.close()

    print("\n✔ Migration Completed")
    print(f"✔ Total rows inserted: {count}")
    print("✔ Sorted by TagIndex")
    print("✔ Missing TagIndex handled")


# ===================== MAIN =====================
if __name__ == "__main__":
    migrate_float_to_multilog()
