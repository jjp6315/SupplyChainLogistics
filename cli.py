import psycopg2

def main():
    print("Welcome to the PostgreSQL CLI!\n")
    connection = connect_db()
    if not connection:
        print("Connection Failed. Please retry later")
        return

    while True:
        print("Please select an option:")
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

        print("Please select a table to operate on:")
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

        table_name = input("Enter your choice (1-11): ")
        if table_name == "1":
            table_name = "OrderList"
        elif table_name == "2":
            table_name = "OrderDetails"
        elif table_name == "3":
            table_name = "FreightRates"
        elif table_name == "4":
            table_name = "vmicustomers"
        elif table_name == "5":
            table_name = "customer"
        elif table_name == "6":
            table_name = "plantports"
        elif table_name == "7":
            table_name = "productsperplant"
        elif table_name == "8":
            table_name = "service"
        elif table_name == "9":
            table_name = "carrier"
        elif table_name == "10":
            table_name = "whcapacities"
        elif table_name == "11":
            table_name = "whcosts"
        else:
            print("Invalid table choice. Please try again.")
            continue

        if choice == "0":
            print("Exiting program...")
            break

        if choice == "1":
            try:                
                insert_data(connection, table_name)
            except Exception as e:
                print(f"Error inserting data: {e}")
        # elif choice == "2":
        #     try:
        #         delete_data(connection, table_name)
        #     except Exception as e:
        #         print(f"Error deleting data: {e}")
        # elif choice == "3":
        #     try:
        #         update_data(connection, table_name)
        #     except Exception as e:
        #         print(f"Error updating data: {e}")
        # elif choice == "4":
        #     try:
        #         search_data(connection, table_name)
        #     except Exception as e:
        #         print(f"Error searching data: {e}")
        # elif choice == "5":
        #     try:
        #         aggregate_functions(connection, table_name)
        #     except Exception as e:
        #         print(f"Error performing aggregate function: {e}")
        # elif choice == "6":
        #     try:
        #         sort_data(connection, table_name)
        #     except Exception as e:
        #         print(f"Error sorting data: {e}")
        # elif choice == "7":
        #     try:
        #         join_data(connection, table_name)
        #     except Exception as e:
        #         print(f"Error joining data: {e}")
        # elif choice == "8":
        #     try:
        #         group_data(connection, table_name)
        #     except Exception as e:
        #         print(f"Error grouping data: {e}")
        # elif choice == "9":
        #     try:
        #         subquery_data(connection, table_name)
        #     except Exception as e:
        #         print(f"Error with subquery: {e}")            
        # elif choice == "10":
        #     transaction(connection)
        else:
            print("Invalid choice. Please try again.")

            
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
    
def execute_query(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        return cursor.fetchall() if query.lower().startswith("select") else None
    except Exception as e:
        print(f"Error executing query: {e}")
        connection.rollback() 
        return None
    
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
        print(f"Data inserted successfully into table '{table_name}'.")
    except Exception as e:
        print(f"Error inserting data: {e}")

# def delete_data(connection, table_name):
#     """Deletes data from a table using a transaction."""
#     try:
#         # Prompt user for deletion criteria and construct DELETE query
#         delete_criteria = get_deletion_criteria(table_name)  # Replace with your logic to get criteria
#         delete_query = f"DELETE FROM {table_name} WHERE ..."  # Replace with actual DELETE syntax
#         delete_query = delete_query.format(**delete_criteria)  # Format query with deletion criteria

#         # Use execute_query to execute the query within a transaction
#         execute_query(connection, delete_query)
#         print(f"Data deleted from table '{table_name}'.")
#     except Exception as e:
#         print(f"Error deleting data: {e}")

# def update_data(connection, table_name):
#     """Updates data in a table using a transaction."""
#     try:
#         # Prompt user for update criteria and new data and construct UPDATE query
#         update_data = get_update_data(table_name)  # Replace with your logic to get update info
#         update_query = f"UPDATE {table_name} SET ... WHERE ..."  # Replace with actual UPDATE syntax
#         update_query = update_query.format(**update_data)  # Format query with update data

#         # Use execute_query to execute the query within a transaction
#         execute_query(connection, update_query)
#         print(f"Data updated successfully in table '{table_name}'.")
#     except Exception as e:
#         print(f"Error updating data: {e}")

# def search_data(connection, table_name):
#     """Searches data in a table using a transaction (not strictly necessary)."""
#     try:
#         # Prompt user for search criteria and construct SELECT query
#         search_criteria = get_search_criteria(table_name)  # Replace with your logic to get criteria
#         search_query = f"SELECT * FROM {table_name} WHERE ..."  # Replace with actual SELECT syntax
#         search_query = search_query.format(**search_criteria)  # Format query with search criteria

#         # Use execute_query to execute the query (no transaction needed for SELECT)
#         results = execute_query(connection, search_query)
#         if results:
#             print(f"\nSearch results for table '{table_name}':")
#             # Display results in a user-friendly format (e.g., table)
#             for row in results:
#                 print(row)  # Replace with appropriate formatting
#         else:
#             print(f"No records found in table '{table_name}'.")
#     except Exception as e:
#         print(f"Error searching data: {e}")


# def aggregate_functions(connection, table_name):
#     # Prompt user for aggregate function (SUM, AVG, COUNT, MIN, MAX) and column
#     # Construct SELECT query with the chosen function and column
#     # Use execute_query to execute the query and display the result
#     pass


# def sort_data(connection, table_name):
#     """Sorts data in a table based on a chosen column."""
#     try:
#         # Prompt user for the column to sort by
#         sort_column = input("Enter the column to sort by: ")

#         # Construct a SELECT query with ORDER BY clause
#         query = f"SELECT * FROM {table_name} ORDER BY {sort_column}"

#         # Execute the query and display sorted results
#         results = execute_query(connection, query)
#         if results:
#             print(f"\nSorted results for table '{table_name}':")
#             # Display results in a user-friendly format (e.g., table)
#             for row in results:
#                 print(row)  # Replace with appropriate formatting
#         else:
#             print(f"No records found in table '{table_name}'.")
#     except Exception as e:
#         print(f"Error sorting data: {e}")

# # Placeholders for future implementation
# def join_data(connection, table_name):
#     print("Join functionality not yet implemented.")

# def group_data(connection, table_name):
#     print("Grouping functionality not yet implemented.")

# def subquery_data(connection, table_name):
#     print("Subquery functionality not yet implemented.")

# def transaction(connection):
#     """Demonstrates a basic transaction example with error handling."""
#     try:
#         # Start a transaction
#         connection.begin()

#         # Simulate some database operations within the transaction
#         # (replace with your actual operations)
#         print("Performing some database operations within the transaction...")

#         # Example database operation that might raise an exception
#         # (replace with your specific operation)
#         cursor = connection.cursor()
#         cursor.execute("INSERT INTO some_table (invalid_column) VALUES ('data')")

#         # Commit the transaction if successful
#         connection.commit()
#         print("Transaction committed successfully!")
#     except psycopg2.Error as e:  # Catch database-specific exceptions (psycopg2.Error)
#         print(f"Error during transaction: {e}")
#         # Rollback the transaction if an error occurs
#         connection.rollback()
#         print("Transaction rolled back.")
#     except Exception as e:  # Catch more general exceptions
#         print(f"Unexpected error: {e}")
#         # Consider additional handling or logging for unexpected errors





if __name__ == "__main__":
    main()