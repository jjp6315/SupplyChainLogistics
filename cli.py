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

        if choice == "0":
            break

        # if choice == "1":
            


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
    

if __name__ == "__main__":
    main()