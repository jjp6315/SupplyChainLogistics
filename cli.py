import psycopg2

def connect_db():
    try:
        conn = psycopg2.connect(database = "project", 
                        user = "johnpark", 
                        host= 'localhost',
                        password = "Getting Started",
                        port = 5432)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def main():
    print("Welcome to the PostgreSQL CLI!")
    connection = connect_db()
    if not connection:
        print("Connection Failed. Please retry later")
        return

    while True:
        print("\nPlease select an option:")
        print("1. Insert Data")
        print("2. Delete Data")
        print("3. Update Data")
        print("4. Search Data")
        print("5. Aggregate Functions")
        print("6. Sorting")
        print("7. Joins")
        print("8. Grouping")
        print("9. Subqueries")
        print("10. Transactions")
        print("0. Exit")

        choice = input("Enter your choice (0-10): ")
        if choice == "0":
            print("Exiting program...")
            break

        print("\nPlease select a table to operate on:")
        print("1. Orders List")
        print("2. Order Details")
        print("3. Freight Rates")
        print("4. VMI Customers")
        print("5. Customers")
        print("6. Plant Ports")
        print("7. Products Per Plant")
        print("8. Customer Service")
        print("9. Carrier")
        print("10. Warehouse Capacity")
        print("11. Warehouse Cost")
        print("0. Exit")

        table_select = input("Enter your choice (1-11): ")
        if table_select == "1":
            table_name = "orderlist"
        elif table_select == "2":
            table_name = "oderdetail"
        elif table_select == "3":
            table_name = "freighrate"
        elif table_select == "4":
            table_name = "vmicustomers"
        elif table_select == "5":
            table_name = "customer"
        elif table_select == "6":
            table_name = "plantports"
        elif table_select == "7":
            table_name = "productsperplant"
        elif table_select == "8":
            table_name = "service"
        elif table_select == "9":
            table_name = "carrier"
        elif table_select == "10":
            table_name = "whcapacities"
        elif table_select == "11":
            table_name = "whcosts"
        elif table_select == "0":
            print("Exiting program...")
            break
        else:
            print("Invalid table choice. Please try again.")
            continue

        

        if choice == "1":
            try:                
                insert_data(connection, table_name)
            except Exception as e:
                print(f"Error inserting data: {e}")
        elif choice == "2":
            try:
                delete_data(connection, table_name)
            except Exception as e:
                print(f"Error deleting data: {e}")
        elif choice == "3":
            try:
                update_data(connection, table_name)
            except Exception as e:
                print(f"Error updating data: {e}")
        elif choice == "4":
            try:
                search_data(connection, table_name)
            except Exception as e:
                print(f"Error searching data: {e}")
        elif choice == "5":
            try:
                aggregate_functions(connection, table_name)
            except Exception as e:
                print(f"Error performing aggregate function: {e}")
        elif choice == "6":
            try:
                sort_data(connection, table_name)
            except Exception as e:
                print(f"Error sorting data: {e}")
        elif choice == "7":
            try:
                join_data(connection, table_name)
            except Exception as e:
                print(f"Error joining data: {e}")
        elif choice == "8":
            try:
                group_data(connection, table_name)
            except Exception as e:
                print(f"Error grouping data: {e}")
        elif choice == "9":
            try:
                subquery_data(connection, table_name)
            except Exception as e:
                print(f"Error with subquery: {e}")            
        elif choice == "10":
            transaction(connection)
        else:
            print("Invalid choice. Please try again.")



    
def insert_data(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}';
    """)
    columns = [row[0] for row in cursor.fetchall()]

    # Create empty list to store user input values
    values = []
    for col in columns:
        value = input(f"Enter value for {col}: ")
        values.append(f"'{value}'")  # Enclose values in single quotes

    # Build the INSERT query dynamically
    query = f"""
        INSERT INTO {table_name} ({','.join(columns)})
        VALUES ({','.join(values)});
    """

    try:
        cursor.execute(query)
        connection.commit()
        print(f"Data inserted successfully into table '{table_name}'.\n")
    except Exception as e:
        print(f"Error inserting data: {e}")

def delete_data(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10;")
    data = cursor.fetchall()
    # Print table headers
    if data:
        print(f"Preview of the first 10 rows in '{table_name}':")
        print(*([col.name for col in cursor.description]), sep=", ")  # unpack column names

    # Print data rows
    for row in data:
        print(*row, sep=", ")  # unpack row values

    # Prompt user for WHERE clause condition
    condition = input("Enter the WHERE clause condition (e.g., column_name = 'value')\nLeave blank if you want to go back: ")
    # Build the DELETE query
    query = f"""
        DELETE FROM {table_name}
        WHERE {condition};
    """
    try:
        cursor.execute(query)
        connection.commit()
        print(f"Deletion successful from table '{table_name}'.\n")
    except Exception as e:
        print(f"Error deleting data: {e}")

def update_data(connection, table_name):
    cursor = connection.cursor()

    # Get column names for the table
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}';
    """)
    columns = [row[0] for row in cursor.fetchall()]

    # Prompt user for column to update
    print("Available columns:")
    for i, col in enumerate(columns):
        print(f"{i+1}. {col}")
    choice = int(input("Enter the number of the column to update: ")) - 1
    if choice < 0 or choice >= len(columns):
        print("Invalid choice. Please try again.")
        return

    column_to_update = columns[choice]

    # Prompt user for new value
    new_value = input(f"Enter the new value for '{column_to_update}': ")

    # Prompt user for WHERE clause condition
    condition = input("Enter the WHERE clause condition (e.g., another_column = 'value'): ")

    # Build the UPDATE query
    query = f"""
        UPDATE {table_name}
        SET {column_to_update} = '{new_value}'
        WHERE {condition};
    """

    try:
        cursor.execute(query)
        connection.commit()
        print(f"Data updated successfully in table '{table_name}'\n")
    except Exception as e:
        print(f"Error updating data: {e}")

def search_data(connection, table_name):
    cursor = connection.cursor()

    # Prompt user for WHERE clause condition
    condition = input("Enter the WHERE clause condition (e.g., column_name = 'value'): ")

    # Build the SELECT query
    query = f"""
        SELECT * FROM {table_name}
        WHERE {condition};
    """

    try:
        cursor.execute(query)
        data = cursor.fetchall()

        # Print table headers and data if results are found
        if data:
            print(f"Search results for table '{table_name}':")
            print(*([col.name for col in cursor.description]), sep=", ")
            for row in data:
                print(*row, sep=", ")  # unpack row values
        else:
            print(f"No records found in table '{table_name}' matching the condition.\n")

    except Exception as e:
        print(f"Error searching data: {e}")
   

def aggregate_functions(connection, table_name):
    cursor = connection.cursor()

    # Available aggregate functions
    functions = ["SUM", "AVG", "COUNT", "MIN", "MAX"]

    # Prompt user for function choice
    print("Available aggregate functions:")
    for i, func in enumerate(functions):
        print(f"{i+1}. {func}")
    choice = int(input("Enter the number of the function you want to use: ")) - 1
    if choice < 0 or choice >= len(functions):
        print("Invalid choice. Please try again.")
        return

    selected_function = functions[choice]

    # Prompt user for column to aggregate
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}';
    """)
    columns = [row[0] for row in cursor.fetchall()]

    print("Available columns:")
    for i, col in enumerate(columns):
        print(f"{i+1}. {col}")
    choice = int(input("Enter the number of the column to aggregate: ")) - 1
    if choice < 0 or choice >= len(columns):
        print("Invalid choice. Please try again.")
        return

    column_to_aggregate = columns[choice]

    # Build the SELECT query
    query = f"""
        SELECT {selected_function}({column_to_aggregate})
        FROM {table_name};
    """

    try:
        cursor.execute(query)
        data = cursor.fetchall()
        if data:
            # Assuming the result is a single value (common for aggregate functions)
            result = data[0][0]
            print(f"{selected_function}( {column_to_aggregate} ) = {result}")
        else:
            print(f"No data found in table '{table_name}'.")

    except Exception as e:
        print(f"Error performing aggregate function: {e}")

def sort_data(connection, table_name):
    cursor = connection.cursor()

    # Get column names for the table
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}';
    """)
    columns = [row[0] for row in cursor.fetchall()]

    # Prompt user for column to sort by
    print("Available columns for sorting:")
    for i, col in enumerate(columns):
        print(f"{i+1}. {col}")
    choice = int(input("Enter the number of the column to sort by: ")) - 1
    if choice < 0 or choice >= len(columns):
        print("Invalid choice. Please try again.")
        return

    sort_column = columns[choice]

    # Prompt user for sort order (ASC/DESC)
    order = input("Enter sort order (ASC/DESC): ").upper()
    if order not in ("ASC", "DESC"):
        print("Invalid order. Please enter ASC or DESC.")
        return

    # Build the SELECT query with ORDER BY clause
    query = f"""
        SELECT * FROM {table_name}
        ORDER BY {sort_column} {order};
    """

    try:
        cursor.execute(query)
        data = cursor.fetchall()

        # Print table headers and data if results are found
        if data:
            print(f"Sorted results for table '{table_name}':")
            print(*([col.name for col in cursor.description]), sep=", ")  # unpack column names
            for row in data:
                print(*row, sep=", ")  # unpack row values
        else:
            print(f"No data found in table '{table_name}'.")

    except Exception as e:
        print(f"Error sorting data: {e}")


def join_data(connection, table_name):
    cursor = connection.cursor()

    # Get a list of all tables in the database
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    all_tables = [row[0] for row in cursor.fetchall()]

    # Prompt user for the table to join with
    print("Available tables for joining:")
    for i, table in enumerate(all_tables):
        print(f"{i+1}. {table}")
    choice = int(input("Enter the number of the table to join with: ")) - 1
    if choice < 0 or choice >= len(all_tables):
        print("Invalid choice. Please try again.")
        return

    join_table = all_tables[choice]

    # Prompt user for the join column in the current table
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}';
    """)
    columns = [row[0] for row in cursor.fetchall()]

    print(f"Join columns from '{table_name}':")
    for i, col in enumerate(columns):
        print(f"{i+1}. {col}")
    choice = int(input("Enter the number of the column to join on: ")) - 1
    if choice < 0 or choice >= len(columns):
        print("Invalid choice. Please try again.")
        return

    join_column_1 = columns[choice]

    # Prompt user for the join column in the join table
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{join_table}';
    """)
    columns = [row[0] for row in cursor.fetchall()]

    print(f"Join columns from '{join_table}':")
    for i, col in enumerate(columns):
        print(f"{i+1}. {col}")
    choice = int(input("Enter the number of the column to join on: ")) - 1
    if choice < 0 or choice >= len(columns):
        print("Invalid choice. Please try again.")
        return

    join_column_2 = columns[choice]

    # Prompt user for the join type (INNER, LEFT, RIGHT, FULL)
    join_types = ["INNER", "LEFT", "RIGHT", "FULL"]
    print("Available join types:")
    for i, join_type in enumerate(join_types):
        print(f"{i+1}. {join_type}")
    choice = int(input("Enter the number of the join type to use: ")) - 1
    if choice < 0 or choice >= len(join_types):
        print("Invalid choice. Please try again.")
        return

    join_type = join_types[choice]

    # Build the JOIN query
    query = f"""
        SELECT *
        FROM {table_name}
        {join_type} JOIN {join_table}
        ON {table_name}.{join_column_1} = {join_table}.{join_column_2};
    """

    try:
        cursor.execute(query)
        data = cursor.fetchall()

        # Print table headers and data if results are found
        if data:
            print(f"Join results for '{table_name}' and '{join_table}':")
            print(*([col.name for col in cursor.description]), sep=", ")  # unpack column names
            for row in data:
                print(*row, sep=", ")  # unpack row values
        else:
            print(f"No data found after joining tables.")

    except Exception as e:
        print(f"Error joining data: {e}")

def group_data(connection, table_name):
    cursor = connection.cursor()

    # Get column names for the table
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}';
    """)
    columns = [row[0] for row in cursor.fetchall()]

    # Prompt user for column to group by
    print("Available columns for grouping:")
    for i, col in enumerate(columns):
        print(f"{i+1}. {col}")
    choice = int(input("Enter the number of the column to group by: ")) - 1
    if choice < 0 or choice >= len(columns):
        print("Invalid choice. Please try again.")
        return

    group_column = columns[choice]

    # Build the SELECT query with GROUP BY clause
    query = f"""
        SELECT {group_column}, COUNT(*) AS count
        FROM {table_name}
        GROUP BY {group_column};
    """

    try:
        cursor.execute(query)
        data = cursor.fetchall()

        # Print table headers and data if results are found
        if data:
            print(f"Grouped results for table '{table_name}':")
            print("Group", "Count", sep=", ")  # Simplified headers
            for row in data:
                print(*row, sep=", ")  # unpack row values
        else:
            print(f"No data found in table '{table_name}'.")

    except Exception as e:
        print(f"Error grouping data: {e}")


def subquery_data(connection, table_name):
    cursor = connection.cursor()

    # Get column names for the table
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}';
    """)
    columns = [row[0] for row in cursor.fetchall()]

    # Prompt user for column to filter by in the main table
    print(f"Available columns from '{table_name}':")
    for i, col in enumerate(columns):
        print(f"{i+1}. {col}")
    choice = int(input("Enter the number of the column to filter by: ")) - 1
    if choice < 0 or choice >= len(columns):
        print("Invalid choice. Please try again.")
        return

    main_column = columns[choice]

    # Prompt user for the table and column for the subquery
    print("Select table and column for subquery:")
    all_tables = []  # Assuming the user might want to use the same table
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    for row in cursor.fetchall():
        all_tables.append(row[0])

    print("Available tables:")
    for i, table in enumerate(all_tables):
        print(f"{i+1}. {table}")
    choice = int(input("Enter the number of the table for the subquery: ")) - 1
    if choice < 0 or choice >= len(all_tables):
        print("Invalid choice. Please try again.")
        return

    subquery_table = all_tables[choice]

    print(f"Available columns from '{subquery_table}':")
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{subquery_table}';
    """)
    subquery_columns = [row[0] for row in cursor.fetchall()]

    for i, col in enumerate(subquery_columns):
        print(f"{i+1}. {col}")
    choice = int(input("Enter the number of the column for the subquery: ")) - 1
    if choice < 0 or choice >= len(subquery_columns):
        print("Invalid choice. Please try again.")
        return

    subquery_column = subquery_columns[choice]
    condition = input("Enter the WHERE clause condition (e.g., column_name = 'value') for the second table: ")

    # Build the SELECT query with subquery
    query = f"""
        SELECT * FROM {table_name}
        WHERE {main_column} IN (
            SELECT {subquery_column} FROM {subquery_table}
            WHERE {condition}
        );
    """

    try:
        cursor.execute(query)
        data = cursor.fetchall()

        # Print table headers and data if results are found
        if data:
            print(f"Results for table '{table_name}' using subquery:")
            print(*([col.name for col in cursor.description]), sep=", ")  # unpack column names
            for row in data:
                print(*row, sep=", ")  # unpack row values
        else:
            print(f"No data found in table '{table_name}' matching the subquery.")

    except Exception as e:
        print(f"Error using subquery: {e}")


def transaction(connection):
    try:
        # Start the transaction
        cursor = connection.cursor()
        cursor.execute("BEGIN")


        # Prompt user for operations within the transaction
        print("Operations within the transaction:")
        print("1. Perform Inserts, Updates, or Deletes")
        print("2. Rollback the transaction")
        choice = int(input("Enter your choice (1 or 2): "))

        if choice == 1:
            # Asks user if they want to insert, update, or delete data
            print("1. Insert Data")
            print("2. Update Data")
            print("3. Delete Data")
            choice = int(input("Enter your choice (1-3): "))
            if choice == 1:
                table_name = input("Enter the table name to insert data: ")
                insert_data(connection, table_name)
            elif choice == 2:
                table_name = input("Enter the table name to update data: ")
                update_data(connection, table_name)
            elif choice == 3:
                table_name = input("Enter the table name to delete data: ")
                delete_data(connection, table_name)
            else:
                print("Invalid choice. Please try again.")
                connection.rollback()

        elif choice == 2:
            # Rollback the transaction
            connection.rollback()
            print("Transaction rolled back.")

        else:
            print("Invalid choice. Please try again.")
            connection.rollback()  # Rollback in case of invalid choice

        # If no exceptions occur, commit the transaction
        connection.commit()
        print("Transaction successful.")
        

    except Exception as e:
        connection.rollback()
       
        print(f"Transaction error: {e}")



if __name__ == "__main__":
    main()