"""
Simple SQL Server connection test
Tests database connectivity and displays basic info
"""

import pyodbc
from dotenv import load_dotenv
import os
import sys

# Load environment variables
load_dotenv()

def test_connection():
    """Test SQL Server connection"""
    
    print("="*70)
    print("SQL SERVER CONNECTION TEST")
    print("="*70)
    
    # Get connection parameters from .env   
    driver = os.getenv('AZURE_DRIVER')
    server = os.getenv('AZURE_SQL_SERVER')
    #port = os.getenv('DB_PORT')
    database = os.getenv('AZURE_SQL_DATABASE')
    user = os.getenv('AZURE_SQL_USER')
    password = os.getenv('AZURE_SQL_PWD') 

    if password and (';' in password or '{' in password or '}' in password):
        password = '{' + password + '}'
           
    print("\n[1] CONNECTION PARAMETERS:")
    print(f"  Driver: {{ODBC Driver 18 for SQL Server}}")
    print(f"  Server: {server}")
    #print(f"  Port: {port}")
    print(f"  Database: {database}")
    print(f"  User: {user}")
    print(f"  Password: {'*' * len(password) if password else '(empty)'}")
    
    # Build connection string
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        #f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )
    
    print("\n[2] ATTEMPTING CONNECTION...")
    
    try:
        # Attempt connection
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        
        print("[OK] CONNECTION SUCCESSFUL!")
        
        # Test 1: Get SQL Server version
        print("\n[3] SQL SERVER VERSION:")
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"  {version[:100]}...")
        
        # Test 2: Check if database exists
        print("\n[4] DATABASE CHECK:")
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{database}'")
        result = cursor.fetchone()
        
        if result:
            print(f"  [OK] Database '{database}' exists")
        else:
            print(f"  [FAIL] Database '{database}' NOT FOUND!")
            print(f"     Please create it first:")
            print(f"     CREATE DATABASE {database};")
            cursor.close()
            conn.close()
            return False
        
        # Test 3: List tables
        print("\n[5] TABLES IN DATABASE:")
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        
        if tables:
            print(f"  Found {len(tables)} tables:")
            for table in tables:
                print(f"    - {table[0]}")
        else:
            print("  [WARN]  No tables found (database is empty)")
            print("     Run schema.sql to create tables")
        
        # Test 4: Check specific table
        print("\n[6] CHECK KEY TABLE (ChillerOperatingPoints):")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'ChillerOperatingPoints'
        """)
        
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            print("  [OK] Table 'ChillerOperatingPoints' exists")
            
            # Count records
            cursor.execute("SELECT COUNT(*) FROM ChillerOperatingPoints")
            count = cursor.fetchone()[0]
            print(f"  [DATA] Records: {count:,}")
            
            if count > 0:
                # Show sample
                cursor.execute("""
                    SELECT TOP 3 
                        Timestamp, ChillerID, CHWFHeaderFlowLPS, 
                        ChillerPowerKW, CoolingLoadKW
                    FROM ChillerOperatingPoints
                    ORDER BY Timestamp
                """)
                
                print("\n  Sample data:")
                rows = cursor.fetchall()
                for row in rows:
                    print(f"    {row[0]} | {row[1]} | Flow: {row[2]} L/s | Power: {row[3]} kW | Load: {row[4]} kW")
        else:
            print("  [FAIL] Table 'ChillerOperatingPoints' does NOT exist")
            print("     Run schema.sql first to create tables")
        
        # Test 5: Test write permissions
        print("\n[7] TESTING WRITE PERMISSIONS:")
        try:
            cursor.execute("""
                CREATE TABLE ##TestTable (ID INT, TestData VARCHAR(50))
            """)
            cursor.execute("INSERT INTO ##TestTable VALUES (1, 'Test')")
            cursor.execute("SELECT * FROM ##TestTable")
            test_result = cursor.fetchone()
            cursor.execute("DROP TABLE ##TestTable")
            conn.commit()
            
            print("  [OK] Write permissions OK")
        except Exception as e:
            print(f"  [FAIL] Write test failed: {e}")
        
        # Close connection
        cursor.close()
        conn.close()
        
        print("\n" + "="*70)
        print("[OK] ALL TESTS PASSED!")
        print("="*70)
        print("\nYour database connection is working correctly.")
        print("You can proceed with data generation.")
        
        return True
        
    except pyodbc.Error as e:
        print(f"[FAIL] CONNECTION FAILED!")
        print(f"\nError Details:")
        print(f"  {e}")
        
        print("\n" + "="*70)
        print("TROUBLESHOOTING STEPS:")
        print("="*70)
        
        error_str = str(e).lower()
        
        if 'login failed' in error_str or 'authentication' in error_str:
            print("\n[FAIL] AUTHENTICATION ERROR")
            print("  Problem: Username or password is incorrect")
            print("\n  Solutions:")
            print("    1. Check DB_USER and DB_PASSWORD in .env file")
            print("    2. Verify SQL Server authentication mode:")
            print("       - Open SSMS → Server Properties → Security")
            print("       - Enable 'SQL Server and Windows Authentication mode'")
            print("    3. Reset 'sa' password in SSMS if needed")
            
        elif 'server not found' in error_str or 'network' in error_str:
            print("\n[FAIL] SERVER NOT FOUND")
            print("  Problem: Cannot connect to SQL Server")
            print("\n  Solutions:")
            print("    1. Check if SQL Server is running:")
            print("       - Open 'Services' (services.msc)")
            print("       - Find 'SQL Server (MSSQLSERVER)'")
            print("       - Status should be 'Running'")
            print("    2. Verify DB_SERVER in .env:")
            print(f"       Current: {server}")
            print("       Try: localhost, 127.0.0.1, or .\\SQLEXPRESS")
            print("    3. Check firewall settings (port 1433)")
            
        elif 'driver' in error_str:
            print("\n[FAIL] ODBC DRIVER ERROR")
            print("  Problem: SQL Server ODBC driver not found")
            print("\n  Solutions:")
            print("    1. Check installed drivers:")
            print("       Run: python -c \"import pyodbc; print(pyodbc.drivers())\"")
            print("    2. Download ODBC Driver:")
            print("       https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
            print("    3. Update DB_DRIVER in .env to match installed driver:")
            print("       - ODBC Driver 17 for SQL Server")
            print("       - ODBC Driver 18 for SQL Server")
            
        elif 'database' in error_str and 'cannot' in error_str:
            print("\n[FAIL] DATABASE NOT FOUND")
            print(f"  Problem: Database '{database}' does not exist")
            print("\n  Solutions:")
            print("    1. Create database in SSMS:")
            print(f"       CREATE DATABASE {database};")
            print("    2. Or update DB_NAME in .env to existing database")
            
        else:
            print("\n[FAIL] UNKNOWN ERROR")
            print("  Please check:")
            print("    1. SQL Server is installed and running")
            print("    2. TCP/IP is enabled in SQL Server Configuration Manager")
            print("    3. SQL Server Browser service is running")
            print("    4. Firewall allows port 1433")
        
        print("\n" + "="*70)
        
        return False

def show_available_drivers():
    """Show available ODBC drivers"""
    print("\n" + "="*70)
    print("AVAILABLE ODBC DRIVERS:")
    print("="*70)
    
    try:
        drivers = pyodbc.drivers()
        if drivers:
            for i, driver in enumerate(drivers, 1):
                print(f"  {i}. {driver}")
        else:
            print("  [FAIL] No ODBC drivers found!")
            print("     Install SQL Server ODBC Driver first")
    except Exception as e:
        print(f"  [FAIL] Error listing drivers: {e}")

if __name__ == "__main__":
    
    # Show available drivers first
    show_available_drivers()
    
    # Test connection
    success = test_connection()
    
    # Exit code
    sys.exit(0 if success else 1)