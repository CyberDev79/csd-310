# Name: Justin Morrow
# Team Green: Austin, Justin, Mark, and Tabari
# Date: 02/22/2025
# Assignment: CSD310 Module 10.1 "Bacchus Winery Python Query to MySQL Database"
# Modified Code from: CSD310 Module 7.2 "Movies: Table Queries" and Module 8.2 "Movies: Update & Deletes"


""" import statements """
import mysql.connector # to connect
from mysql.connector import errorcode

import dotenv # to use .env file
from dotenv import dotenv_values

# Adding this for reference of the current date/time. Formated to display AM/PM for the comment at the end of the report
from datetime import datetime
current_date_time = datetime.now()
formatted_date_time = current_date_time.strftime("%m-%d-%Y %I:%M:%S %p")

#using our .env file
secrets = dotenv_values(".env")

""" database config object """
config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True #not in .env file
}

database_name = secrets["DATABASE"]

try:
    """ try/catch block for handling potential MySQL database errors """

    db = mysql.connector.connect(**config)  # connect to the bacchuswinery database

    # output the connection status
    print("\n  Database user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"],
                                                                                       config["database"]))

    # From the assignment, this is used to create a cursor object to execute queries to MySQL
    cursor = db.cursor()

    # MySQL query to display all the records from the distributors table
    print("\n\n-- Displaying the Distributor Records for Bacchus Winery --")
    cursor.execute("SELECT distributor_id, distributor_name, contact_info FROM distributors")
    distributors = cursor.fetchall()
    for distributor in distributors:
        print(f"Distributor ID: {distributor[0]}")
        print(f"Distributor Name: {distributor[1]}")
        print(f"Contact Info: {distributor[2]}\n")

    # MySQL query to display all the records from the employees table
    print("\n\n-- Displaying the Employee Records for Bacchus Winery --")
    cursor.execute("SELECT employee_id, first_name, last_name, position, total_hours_worked FROM employees")
    employees = cursor.fetchall()
    for employee in employees:
        print(f"Employee ID: {employee[0]}")
        print(f"First Name: {employee[1]}")
        print(f"Last Name: {employee[2]}")
        print(f"Position: {employee[3]}")
        print(f"Total (4 Quarters) Hours Worked: {employee[4]:,.0f}\n")

    # MySQL query to display all the records from the suppliers table
    print("\n\n-- Displaying the Supplier Records for Bacchus Winery --")
    cursor.execute("SELECT supplier_id, supplier_name, location, supplied_item_type FROM suppliers")
    suppliers = cursor.fetchall()
    for supplier in suppliers:
        print(f"Supplier ID: {supplier[0]}")
        print(f"Supplier Name: {supplier[1]}")
        print(f"Location: {supplier[2]}")
        print(f"Supplied Item Type: {supplier[3]}\n")

    # MySQL query to display all the records from the supplier_deliveries table
    print("\n\n-- Displaying the Supplier Delivery Records for Bacchus Winery --")
    cursor.execute("SELECT delivery_id, supplier_id, item_type, expected_delivery_date, actual_delivery_date, quantity_delivered FROM supplier_deliveries")
    deliveries = cursor.fetchall()
    for delivery in deliveries:
        print(f"Delivery ID: {delivery[0]}")
        print(f"Supplier ID: {delivery[1]}")
        print(f"Item Type: {delivery[2]}")
        print(f"Expected Delivery Date: {delivery[3]}")
        print(f"Actual Delivery Date: {delivery[4]}")
        expected_delivery_date = delivery[3] # Tuple to store the expected delivery date
        actual_delivery_date = delivery[4] # Tuple to store the actual delivery date
        if actual_delivery_date == expected_delivery_date: # If statement to calculate if the delivery was on time
            print("Shipment Delivery: On time")
        else:
            print("Shipment Delivery: Was late")
        print(f"Quantity Delivered: {delivery[5]:,.0f}\n")

    # MySQL query to display all the records from the wine_grape_variety table
    print("\n\n-- Displaying the Wine Grape Variety Records for Bacchus Winery --")
    cursor.execute("SELECT product_id, wine_name, grape_variety, vintage_year FROM wine_grape_variety")
    varieties = cursor.fetchall()
    for variety in varieties:
        print(f"Product ID: {variety[0]}")
        print(f"Wine Name: {variety[1]}")
        print(f"Grape Variety: {variety[2]}")
        print(f"Vintage Year: {variety[3]}\n")

    # MySQL JOIN query to display all the records from the wine_sales table and dependency wine_grape_variety and distributors tables
    print("\n\n-- Displaying the Wine Sales Records for Bacchus Winery --")
    cursor.execute("""
        SELECT wine_grape_variety.wine_name, distributors.distributor_name, wine_sales.sales_quantity, wine_sales.sale_date, wine_sales.price_per_unit
        FROM wine_sales
        JOIN wine_grape_variety ON wine_sales.product_id = wine_grape_variety.product_id
        JOIN distributors ON wine_sales.distributor_id = distributors.distributor_id
    """)

    wine_sales = cursor.fetchall()
    for sale in wine_sales:
        print(f"Wine Name: {sale[0]}")
        print(f"Distributor: {sale[1]}")
        print(f"Sales Quantity: {sale[2]:,.0f}")
        print(f"Sale Date: {sale[3]}")
        print(f"Price per Unit: ${sale[4]:,.2f}")
        total_sales = sale[2] * sale[4] # Calculation of Sales Quantity (Bottles sold) * Price per Unit (Cost of each bottle)
        print(f"Total Sales: ${total_sales:,.2f}\n")

    # After the Python script finishes iterating through the results in the MySQL database, Notify of auto disconnect
    print(f"\nBracchus Winery Company report completed on {formatted_date_time} for all data in the MySQL {database_name} database.")
    print("All Rights Reserved by: Bellevue University CSD 310 Green Team: Austin, Justin, Mark and Tabari")
    print("\n\nThe database connection will now automatically disconnect...")

except mysql.connector.Error as err:
    """ on error code """

    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("  The supplied username or password are invalid")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("  The specified database does not exist")

    else:
        print(err)

finally:
    """ close the connection to MySQL """

    db.close()