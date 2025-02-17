# Name: Justin Morrow
# Date: 02/16/2025
# Assignment: CSD310 Module 8.2 "Movies: Update & Deletes"
# Purpose: Learn how to update and delete records from a MySQL database using Python
#Note: Anything newly added from the modified Module 7.2 assignment will be commented with # [New]

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


# [New] This show_films function is from Module 8.2's provided code that I just manually typed out

def show_films(cursor, title):
    # Method to execute an inner join on all tables,
    # iterate over the dataset and output the results ot the terminal window.

    # inner join query
    cursor.execute("SELECT film_name AS Name, film_director AS Director, genre_name AS Genre, studio_name AS 'Studio Name'"
                   " FROM film "
                   "INNER JOIN genre ON film.genre_id = genre.genre_id"
                   " INNER JOIN studio ON film.studio_id = studio.studio_id")

    # get the results from the cursor object
    films = cursor.fetchall()

    print("\n  --  {}  --".format(title))

    # iterate over the film data set and display the results
    for film in films:
        print("Film Name:  {}\nDirector:  {}\nGenre Name ID:  {}\nStudio Name:  {}\n".format(film[0], film[1], film[2], film[3]))




# This beginning of the Try statement was copied from the movies_queries.py script
try:
    """ try/catch block for handling potential MySQL database errors """

    db = mysql.connector.connect(**config)  # connect to the movies database

    # output the connection status
    print("\n  Database user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"],
                                                                                       config["database"]))

    # From the assignment, this is used to create a cursor object to execute queries to MySQL
    cursor = db.cursor()


# [New] Display the list of films currently in the films table of the movies database
    show_films(cursor, "DISPLAYING FILMS")

    # [New] INSERT the new film below into the film table
    # [New] The film_releaseDate and film_runtime aren't seen in the output but mysql>describe film; shows Null=NO
    cursor.execute("INSERT INTO film (film_name, film_director, film_releaseDate, film_runtime, genre_id, studio_id)"
                   " VALUES ('Jurassic World', 'Colin Trevorrow', '2015', 125, "
                   "(SELECT genre_id FROM genre WHERE genre_name = 'SciFi'), "
                   "(SELECT studio_id FROM studio WHERE studio_name = 'Universal Pictures'))")

    # Commit the movies database after adding the new new film above
    db.commit()

    # Display the all the films after the new film was added above with the INSERT operation
    show_films(cursor, "DISPLAYING FILMS AFTER INSERT")

    # UPDATE the film "Alien" to change its genre to "Horror"
    cursor.execute("UPDATE film SET genre_id = (SELECT genre_id FROM genre WHERE genre_name = 'Horror')"
                   " WHERE film_name = 'Alien'")

    # Commit the movies database after changing the genre of the film 'Alien'.
    db.commit()

    # Display the films after the UPDATE operation was commited for the 'Alien' film.
    show_films(cursor, "DISPLAYING FILMS AFTER UPDATE- Changed Alien to Horror")

    # DELETE the film "Gladiator"
    cursor.execute("DELETE FROM film WHERE film_name = 'Gladiator'")

    # Commit the movies database after deleting the film 'Gladiator'.
    db.commit()

    # Display all the films in the movies database after the DELETE operation was performed on the film 'Gladiator'.
    show_films(cursor, "DISPLAYING FILMS AFTER DELETE")

# [NEW] This completes all the newly added items above from the original config movies_queries.py

# Below this statement was a copy and paste from assignment 7.2 other than the closing print statement/auto disconnect

    # After the Python script finishes iterating through the results in the MySQL database, Notify of auto disconnect
    print("\nThis completes all the requirements from Python to the MySQL {} database".format(config["database"]))
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

# [NEW] Added below to reset the database back to default if needed from Windows CMD prompt as administrator:
# cd C:\Program Files\MySQL\MySQL Server 8.0\bin
# "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -p movies < "C:\csd\csd-310\module-6\db_init_2022.sql"