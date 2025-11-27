import os
import openpyxl

# ------------------------------
# Create folder structure
# ------------------------------
def create_folders():
    folders = [
        "project",
        "project/readers",
    ]
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
    print("[+] Folder structure created")

# ------------------------------
# Create .env file
# ------------------------------
def create_env():
    content = """# PLC CONNECTION SETTINGS
COMPACTLOGIX_IP=192.168.1.10
MICROLOGIX_IP=192.168.1.20
MICRO800_IP=192.168.1.30

SQL_SERVER=DESKTOP-FMJV2PT\\SQLEXPRESS
SQL_DATABASE=VAD_SUM
SQL_TABLE=production_data
ODBC_DRIVER=ODBC Driver 17 for SQL Server
LOG_FILE=plc_logs.log
"""
    with open("project/.env", "w") as f:
        f.write(content)

    print("[+] .env created")

# ------------------------------
# Create tags.xlsx automatically
# ------------------------------
def create_tags_excel():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "TAGS"

    ws.append(["TAG_NAME", "PLC_TYPE", "ADDRESS", "DATA_TYPE"])

    ws.append(["P2_COOLING_CO2_FLOW_TOT1", "CompactLogix", "40001", "FLOAT"])
    ws.append(["N7_0", "MicroLogix", "N7:0", "INT"])
    ws.append(["SUB_COOLER_NH3_TEMP", "Micro800", "30005", "FLOAT"])

    wb.save("project/tags.xlsx")

    print("[+] tags.xlsx created")

# ------------------------------
# Create logger file
# ------------------------------
def create_log_file():
    open("project/plc_logs.log", "w").close()
    print("[+] plc_logs.log created")

# =====================================================
# WRITER FILES FOR EACH PLC
# =====================================================

def create_reader_files():
    readers = {
        "compactlogix_reader.py": """import time

def read_compactlogix(ip):
    # TODO: Add real EtherNet/IP read logic
    print(f"[CompactLogix] Reading from {ip}")
    return {"status": "ok", "value": 123.45}
""",

        "micrologix_reader.py": """import time

def read_micrologix(ip):
    # TODO: Add real MicroLogix DF1 read logic
    print(f"[MicroLogix] Reading from {ip}")
    return {"status": "ok", "value": 999}
""",

        "micro800_reader.py": """import time

def read_micro800(ip):
    # TODO: Add CIP read logic for Micro800
    print(f"[Micro800] Reading from {ip}")
    return {"status": "ok", "value": 45.6}
""",

        "sql_writer.py": """import os
import pyodbc

def write_to_sql(data):
    try:
        conn = pyodbc.connect(
            f'DRIVER={{{os.getenv("ODBC_DRIVER")}}};'
            f'SERVER={os.getenv("SQL_SERVER")};'
            f'DATABASE={os.getenv("SQL_DATABASE")};'
            f'Trusted_Connection=yes;'
        )
        cursor = conn.cursor()

        cursor.execute(
            f"INSERT INTO {os.getenv('SQL_TABLE')} (tag, value) VALUES (?, ?)",
            data['tag'], data['value']
        )
        conn.commit()
        conn.close()

        print("[SQL] Data inserted successfully")
    except Exception as e:
        print(f"[SQL Error] {e}")
"""
    }

    for filename, content in readers.items():
        with open(f"project/readers/{filename}", "w") as f:
            f.write(content)

    print("[+] Reader files created")

# =====================================================
# CREATE MAIN.PY
# =====================================================

def create_main():
    content = """import os
import time
from dotenv import load_dotenv

from readers.compactlogix_reader import read_compactlogix
from readers.micrologix_reader import read_micrologix
from readers.micro800_reader import read_micro800
from readers.sql_writer import write_to_sql

load_dotenv()

def main():
    print("=== PLC Monitoring System Started ===")
    
    while True:
        try:
            # Read CompactLogix
            compact = read_compactlogix(os.getenv("COMPACTLOGIX_IP"))
            write_to_sql({"tag": "CompactLogix", "value": compact["value"]})

            # Read MicroLogix
            micro = read_micrologix(os.getenv("MICROLOGIX_IP"))
            write_to_sql({"tag": "MicroLogix", "value": micro["value"]})

            # Read Micro800
            micro8 = read_micro800(os.getenv("MICRO800_IP"))
            write_to_sql({"tag": "Micro800", "value": micro8["value"]})

            time.sleep(5)

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
"""
    with open("project/main.py", "w") as f:
        f.write(content)

    print("[+] main.py created")

# =====================================================
# RUN ALL CREATORS
# =====================================================

def generate_project():
    create_folders()
    create_env()
    create_tags_excel()
    create_log_file()
    create_reader_files()
    create_main()
    print("\n🎉 Project structure created successfully!")

# Run script
generate_project()
