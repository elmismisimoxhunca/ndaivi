#!/usr/bin/env python3

import sqlite3
import os
import sys
import json

# Database path
db_path = '/var/ndaivimanuales/data/crawler.db'

def inspect_database():
    """Inspect the SQLite database structure and content"""
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"Database file not found at {db_path}")
        return
    
    print(f"Database file found at {db_path}, size: {os.path.getsize(db_path)} bytes")
    
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in the database.")
            return
        
        print(f"Found {len(tables)} tables in the database:")
        
        # Inspect each table
        for table in tables:
            table_name = table[0]
            print(f"\n\n=== TABLE: {table_name} ===")
            
            # Get table schema
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            print("\nColumns:")
            for col in columns:
                col_id, col_name, col_type, not_null, default_val, is_pk = col
                print(f"  {col_name} ({col_type})" + 
                      (" PRIMARY KEY" if is_pk else "") + 
                      (" NOT NULL" if not_null else ""))
            
            # Count rows
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            print(f"\nRow count: {row_count}")
            
            # Show sample data (up to 5 rows)
            if row_count > 0:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
                rows = cursor.fetchall()
                
                print("\nSample data:")
                for i, row in enumerate(rows):
                    print(f"  Row {i+1}: {row}")
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_database()
