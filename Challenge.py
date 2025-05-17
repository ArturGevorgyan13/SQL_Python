import sqlite3
import mysql.connector as mysql

HOST_NAME="localhost"
USER_NAME="myuser"
PASSWORD_NAME="mypassword"

def main():

    userslite=[

        ("Artur","Bebutyan"),
        ("Arman","Anoyan"),
        ("Garik","Simonyan"),
        ("Anna","Sahakyan")

    ]

    users=[

        ("Suren","Torosyan"),
        ("Ani","Gabrielyan"),
        ("Tatev","Harutyunyan"),
        ("Armine","Xachatryan"),
        ("Flora","Chobanyan")

    ]

    dblite=None

    cursorlite=None

    db=None

    cursor=None

#CONNECTION TO SQLITE

    try:

        dblite=sqlite3.connect(":memory:")

        cursorlite=dblite.cursor()

        cursorlite.execute("SELECT sqlite_version()")

        print(f"sqlite3 version is {cursorlite.fetchone()[0]}!")

    except sqlite3.Error as err:

        print(f"sqlite error {err}!")

        exit(1)

#CONNECTION TO MYSQL

    try:

        db=mysql.connect(host=HOST_NAME,user=USER_NAME,password=PASSWORD_NAME,database="mydatabase")

        cursor=db.cursor(prepared=True)

        cursor.execute("SHOW VARIABLES WHERE variable_name='version'")

        print(f"mysqle version is {cursor.fetchone()[1]}!")   

    except mysql.Error as err:

        print(f"mysql error {err}!")  

        exit(1)

#CREATION OF MYSQL TABLE

    try:

        print("The creation of mysql table!")

        cursor.execute("DROP TABLE IF EXISTS tmp")

        cursor.execute("CREATE TABLE IF NOT EXISTS tmp(id INT PRIMARY KEY AUTO_INCREMENT,name TEXT NOT NULL,surname TEXT NOT NULL)")

#INSERTION PF ITEMS

        print("Inserting rows!")

        cursor.executemany("INSERT INTO tmp(name,surname) VALUES (%s,%s)",users)

        db.commit()

        print(f"{cursor.rowcount} rows are added!")

        print("mysql table data!")

        cursor.execute("SELECT * FROM tmp")

        rows=cursor.fetchall()

        for row in rows:

            print(row)

    except mysql.Error as err:

        print(f"mysql error {err}!")

        exit(1)

#CREATION OF SQLITE TABLE

    try:

        print("Creation of sqlite table!")

        cursorlite.execute("DROP TABLE IF EXISTS tmplite")

        cursorlite.execute("CREATE TABLE IF NOT EXISTS tmplite(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,surname TEXT NOT NULL)" )

    except sqlite3.Error as err:

        print(f"sqlite3 error {err}!")
        
        exit(1)

#COPYING FROM MYSQL TO SQLITE

    try:

        print("Copying!")

        cursor.execute("SELECT * FROM tmp")

        rows=cursor.fetchall()

        for row in rows:

            cursorlite.execute("INSERT INTO tmplite(name,surname) VALUES(?,?)",row[1:])

        dblite.commit()

        for row in rows:

            print(row)

    except sqlite3.Error as err:

        print(f"sqlite error {err}!")

        exit(1)

#CLEANUP

    try:

        print("Drop the tables and close!")

        cursor.execute("DROP TABLE IF EXISTS tmp")

        cursorlite.execute("DROP TABLE IF EXISTS tmplite")

        cursorlite.close()

        dblite.close()

        cursor.close()

        db.close()

    except (mysql.Error,sqlite3.Error) as err:

        print(f"close error {err}!")

        exit(1)

main()