# Name: Justin Morrow
# Date: 02/15/2025
# Assignment: CSD310 Module 7.2 "Movies: Table Queries"
# Purpose:  Query MySQL database tables through the terminal window and a Python program
# Reference: Poehler, J. (2015, December 25). How to iterate through cur.fetchall() in Python? Stack Overflow
#  https://stackoverflow.com/questions/34463901/how-to-iterate-through-cur-fetchall-in-python


""" import statements """
import mysql.connector # to connect
from mysql.connector import errorcode

import dotenv # to use .env file
from dotenv import dotenv_values

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

try:
    """ try/catch block for handling potential MySQL database errors """

    db = mysql.connector.connect(**config)  # connect to the movies database

    # output the connection status
    print("\n  Database user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"],
                                                                                       config["database"]))

    # From the assignment, this is used to create a cursor object to execute queries to MySQL
    cursor = db.cursor()

    # Write four queries, in one Python file.The output from your queries should match the example
    # The first and second query is to select all the fields for the studio and genre tables.

    # Execute a MySQL query to fetch the Studio records (studio_id and studio_name)
    print("\n\n-- DISPLAYING Studio RECORDS --")
    cursor.execute("SELECT studio_id, studio_name FROM studio")
    studios = cursor.fetchall()
    for studio in studios:
        print(f"Studio ID: {studio[0]}")
        print(f"Studio Name: {studio[1]}\n")


    # Execute a MySQL query to fetch the Genre records (genre_id and genre_name)
    print("\n-- DISPLAYING Genre RECORDS --")
    cursor.execute("SELECT genre_id, genre_name FROM genre")
    genres = cursor.fetchall()
    for genre in genres:
        print(f"Genre ID: {genre[0]}")
        print(f"Genre Name: {genre[1]}\n")

    # The third query is to select the movie names for those movies that have a run time of less than two hours.

    # Execute a MySQL query to fetch the Short Film records under 2 hours (film_name and film_runtime)
    print("\n-- DISPLAYING Short Film RECORDS --")
    #cursor.execute("SELECT film_name, film_runtime FROM film") # Fetch all films
    cursor.execute("SELECT film_name, film_runtime FROM film WHERE film_runtime < 120") # Fetch films under 120 min
    #cursor.execute("SELECT film_name, film_runtime FROM film WHERE film_runtime > 120") # Fetch films over 120 min
    films = cursor.fetchall()
    for film in films:
        print(f"Film Name: {film[0]}")
        print(f"Runtime: {film[1]}\n")

    # The fourth query is to get a list of film names, and directors grouped by director.

    # Execute a MySQL query to fetch the Director Name and Film records (film_name and film_director)
    print("\n-- DISPLAYING Director RECORDS in Order --")
    # Per the assignment it shows sorted by Director and not also by film since Gladiator is before Alien
    # To sort by director first and then by film, I would just add , film_name after ORDER BY film_director
    cursor.execute("SELECT film_name, film_director FROM film ORDER BY film_director")
    directors = cursor.fetchall()
    for director in directors:
        print(f"Film Name: {director[0]}")
        print(f"Director: {director[1]}\n")


    # After the Python script finishes iterating through the results in the MySQL database, prompt user to disconnect
    print("\nThose are all the records in the MySQL {} database".format(config["database"]))
    input("\n\n  Press any key to disconnect...")

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
