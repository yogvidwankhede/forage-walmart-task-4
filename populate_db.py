import csv
import sqlite3

# --- Configuration ---
# Updated file paths as per your request
DB_FILE = 'C:/Users/Asus/OneDrive/Desktop/SEP Walmart/forage-walmart-task-4/shipment_database.db'
SPREADSHEET_0 = 'C:/Users/Asus/OneDrive/Desktop/SEP Walmart/forage-walmart-task-4/data/shipping_data_0.csv'
SPREADSHEET_1 = 'C:/Users/Asus/OneDrive/Desktop/SEP Walmart/forage-walmart-task-4/data/shipping_data_1.csv'
SPREADSHEET_2 = 'C:/Users/Asus/OneDrive/Desktop/SEP Walmart/forage-walmart-task-4/data/shipping_data_2.csv'

# This MUST be the actual name of the table in your database, not a file path.
SHIPMENTS_TABLE_NAME = 'shipments'

def process_spreadsheet_0(cursor):
    """
    Processes the self-contained spreadsheet (shipping_data_0.csv) and inserts
    its data directly into the database.

    Args:
        cursor: The SQLite database cursor object.
    """
    print(f"Processing {SPREADSHEET_0}...")
    try:
        with open(SPREADSHEET_0, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader)  # Skip the header row

            for line_num, row in enumerate(reader, start=2):
                # Check if the row has the expected number of columns before unpacking
                if len(row) != 4:
                    print(f"Warning: Malformed row in {SPREADSHEET_0} at line {line_num}. Skipping row: {row}")
                    continue  # Skip to the next row

                # The row format is expected to be [origin, destination, product, quantity]
                origin, destination, product, quantity_str = row
                try:
                    quantity = int(quantity_str)
                    cursor.execute(f"""
                        INSERT INTO {SHIPMENTS_TABLE_NAME} (origin, destination, product, quantity)
                        VALUES (?, ?, ?, ?)
                    """, (origin, destination, product, quantity))
                except ValueError:
                    print(f"Warning: Could not parse quantity '{quantity_str}' in {SPREADSHEET_0} at line {line_num}. Skipping row: {row}")
        print(f"Successfully processed {SPREADSHEET_0}.")
    except FileNotFoundError:
        print(f"Error: {SPREADSHEET_0} not found. Please ensure it is in the same directory.")
        return

def process_spreadsheets_1_and_2(cursor):
    """
    Processes two dependent spreadsheets (shipping_data_1.csv and shipping_data_2.csv).
    It first maps shipment locations from spreadsheet 2, then aggregates product data
    from spreadsheet 1, combines them, and inserts the complete records into the database.

    Args:
        cursor: The SQLite database cursor object.
    """
    print(f"Processing {SPREADSHEET_1} and {SPREADSHEET_2}...")

    # --- Step 1: Read locations from spreadsheet 2 into a dictionary for quick lookup ---
    shipment_locations = {}
    try:
        with open(SPREADSHEET_2, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader) # Skip header

            for line_num, row in enumerate(reader, start=2):
                # Check for malformed rows before unpacking
                if len(row) != 3:
                    print(f"Warning: Malformed row in {SPREADSHEET_2} at line {line_num}. Skipping row: {row}")
                    continue
                
                # Row format: [shipping_identifier, origin, destination]
                identifier, origin, destination = row
                shipment_locations[identifier] = {'origin': origin, 'destination': destination}
    except FileNotFoundError:
        print(f"Error: {SPREADSHEET_2} not found. Cannot process dependent data.")
        return

    # --- Step 2: Read and aggregate products from spreadsheet 1 ---
    # We will process row-by-row and combine with location data.
    try:
        with open(SPREADSHEET_1, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader) # Skip header

            for line_num, row in enumerate(reader, start=2):
                # Check for malformed rows before unpacking
                if len(row) != 3:
                    print(f"Warning: Malformed row in {SPREADSHEET_1} at line {line_num}. Skipping row: {row}")
                    continue

                # Row format: [shipping_identifier, product, quantity]
                identifier, product, quantity_str = row
                
                # Find the corresponding location for this shipment
                location_info = shipment_locations.get(identifier)

                if location_info:
                    try:
                        quantity = int(quantity_str)
                        origin = location_info['origin']
                        destination = location_info['destination']
                        
                        cursor.execute(f"""
                            INSERT INTO {SHIPMENTS_TABLE_NAME} (origin, destination, product, quantity)
                            VALUES (?, ?, ?, ?)
                        """, (origin, destination, product, quantity))
                    except ValueError:
                        print(f"Warning: Could not parse quantity '{quantity_str}' in {SPREADSHEET_1} at line {line_num}. Skipping row: {row}")
                else:
                    print(f"Warning: No location found for shipping_identifier '{identifier}'. Skipping row: {row}")
        print(f"Successfully processed {SPREADSHEET_1} and {SPREADSHEET_2}.")
    except FileNotFoundError:
        print(f"Error: {SPREADSHEET_1} not found. Cannot process dependent data.")


def main():
    """
    Main function to connect to the database and orchestrate the
    population process from all spreadsheets.
    """
    try:
        # Connect to the SQLite database (it will be created if it doesn't exist)
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Create the table if it doesn't already exist
        print(f"Ensuring table '{SHIPMENTS_TABLE_NAME}' exists...")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SHIPMENTS_TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                origin TEXT NOT NULL,
                destination TEXT NOT NULL,
                product TEXT NOT NULL,
                quantity INTEGER NOT NULL
            );
        """)

        # Optional: Clear the table to prevent duplicate data on re-runs
        print(f"Clearing existing data from '{SHIPMENTS_TABLE_NAME}' table...")
        cursor.execute(f"DELETE FROM {SHIPMENTS_TABLE_NAME};")

        # Process the spreadsheets
        process_spreadsheet_0(cursor)
        process_spreadsheets_1_and_2(cursor)

        # Commit the changes to the database and close the connection
        conn.commit()
        print("\nDatabase population complete. All data has been committed.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")


if __name__ == '__main__':
    main()

