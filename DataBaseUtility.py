#module version
__version__="1.0.0"

#import sqlite3
try:

    import sqlite3

    have_sqlite3=True

except ImportError:

    sqliite3=None

    have_sqlite3=False

#import mysql
try:

    import mysql.connector as mysql

    have_mysql=True

except ImportError:

    mysql=None

    have_mysql=False

#simple error class for BWDB
class BWErr(Exception):

    def __init__(self,message):
        
        self.message=message

        super().__init__(self.message)

#main BWDB class
class BWDB:

    def __init__(self,**kwargs):

        self._db=None

        self._cur=None

        self._dbms=None

        self._database=None

        #populate simple parameters
        if 'user' in kwargs:

            self._user=kwargs['user']
        
        else:

            self._user=None
        
        if 'host' in kwargs:

            self._host=kwargs['host']
        
        else:

            self._host=None

        if 'password' in kwargs:

            self._password=kwargs['password']

        else:

            self._password=None

        #populate properties
        if 'dbms' in kwargs:

            self._dbms=kwargs['dbms']

        if 'database' in kwargs:

            self._database=kwargs['database']

    #property setters/getters
    def get_dbms(self):

        return self._dbms
    
    def set_dbms(self,dbms_str):

        if dbms_str=='mysql':

            if have_mysql:

                self._dbms=dbms_str

            else:

                raise BWErr("mysql not available!")

        elif dbms_str=='sqlite3':

            if have_sqlite3:

                self._dbms=dbms_str

            else:

                raise BWErr("sqlite3 not available!")
            
        else:

            raise BWErr("Invalid case!")

    def get_database(self):

        return self._database
    
    def set_database(self,database_str):
        
        if self._cur:

            self._cur.close()

        if self._db:

            self._db.close()
        
        if self._dbms=='sqlite3':

            self._db=sqlite3.connect(database_str)

            if self._db is None:

                raise BWErr("Invalid case!")
            
            else:

                self._cur=self._db.cursor()

        elif self._dbms=='mysql':

            self._db=mysql.connect(host=self._host,user=self._user,password=self._password,database=self._database)

            if self._db is None:

                raise BWErr("Invalid case!")
            
            else:

                self._cur=self._db.cursor()

        else:

            raise BWErr("Invalid error!")
        
    def get_cursor(self):

        return self._cur
    
    #properties
    dbms=property(fget=get_dbms,fset=set_dbms)

    database=property(fget=get_database,fset=set_database)

    cursor=property(fget=get_cursor)

    #sql metohds
    def sql_do_nocommit(self,sql,parms=()):

        self._cur.execute(sql,parms)

        return self._cur.rowcount
    
    def sql_do(self,sql,parms=()):

        self.sql_do_nocommit(sql,parms)

        self.commit()
        
        return self._cur.rowcount
    
    def sql_do_many_nocommit(self,sql,parms=()):

        self._cur.executemany(sql,parms)

        return self._cur.rowcount
    
    def sql_do_many(self,sql,parms=()):

        self.sql_do_many_nocommit(sql,parms)

        self.commit()

        return self._cur.rowcount
    
    def sql_query(self,sql,parms=()):

        self._cur.execute(sql,parms)

        for row in self._cur:

            yield row

    def sql_query_row(self,sql,parms=()):

        self._cur.execute(sql,parms)

        row=self._cur.fetchone()

        self._cur.fetchall()

        return row
    
    def sql_query_value(self,sql,parms=()):

        return self.sql_query_row(sql,parms)[0]
    
    #utilities
    def have_db(self):

        if self._db is None:

            return False
        
        else:

            return True
        
    def have_cursor(self):

        if self._cur is None:

            return False
        
        else:

            return True
        
    def lastrowid(self):

        return self._cur.lastrowid
    
    def disconnect(self):

        if self.have_cursor():

            self._cur.close()

        if self.have_db():

            self._db.close()

        self._cur=None

        self._db=None

    def begin_transaction(self):

        if self.have_db:

            if self._dbms=='sqlite3':

                self.sql_do("BEGIN TRANSACTION")

            elif self._dbms=='mysql':

                self.sql_do("START TRANSACTION")

    def rollback(self):

        if self.have_db:

            self._db.rollback()

    def commit(self):

        if self.have_db:

            self._db.commit()

    #desctructor
    def __del__(self):

        self.disconnect()

HOST_NAME="localhost"
USER_NAME="myuser"
PASSWORD_NAME="mypassword"

def main():

    try:

        db=BWDB(dbms="mysql",database="mydatabase",host=HOST_NAME,user=USER_NAME,password=PASSWORD_NAME)

        print(f"Dbms is {db.dbms}!")

        db.set_database("mydatabase")

        #start clean
        db.sql_do("DROP TABLE IF EXISTS temp")

        print("Creation of a table!")

        if db.dbms=='sqlite3':

            create_table="CREATE TABLE IF NOT EXISTS temp(id INTEGER PRIMARY KEY,name TEXT NOT NULL,description TEXT)"

        elif db.dbms=='mysql':

            create_table="CREATE TABLE IF NOT EXISTS temp(id INTEGER AUTO_INCREMENT PRIMARY KEY,name VARCHAR(128) NOT NULL,description VARCHAR(128))"

        else:

            raise BWErr("Invalid case!")
        
        #creation of table
        db.sql_do(create_table)

        db.table="temp"

        print("Populate table!")

        insert_rows = (
            ("Jimi Hendrix","Guitar"),
            ("Miles Davis","Trumpet"),
            ("Billy Cobham","Drums"),
            ("Charlie Bird","Saxophone"),
            ("Oscar Peterson","Piano"),
            ("Marcus Miller","Bass"),
        )

        print(f"Not adding {len(insert_rows)} rows (rollback)!")

        db.begin_transaction()

        db.sql_do_many_nocommit("INSERT INTO temp(name,description) VALUES(%s,%s)",insert_rows)

        print("Rollback!")

        db.rollback()

        print(f"Adding {len(insert_rows)} rows!")

        db.begin_transaction()

        numrows=db.sql_do_many_nocommit("INSERT INTO temp(name,description) VALUES(%s,%s)",insert_rows)

        db.commit()

        print(f"{numrows} rows are added!")

        print(f"There are {db.sql_query_row("SELECT COUNT(*) FROM temp")} rows!")

        for row in db.sql_query("SELECT * FROM temp"):

            print(row)

        print("Find more then one row (J)!")

        for row in db.sql_query("SELECT * FROM temp WHERE name LIKE %s",("%J%",)):

            print(row)

        print("Search for Bird!")

        row=db.sql_query_row("SELECT * FROM temp WHERE name LIKE %s",("%Bird%",))

        row_id=None

        if row is not None:

            row_id=row[0]

            print(f"Found row is {row_id}!")

            print(row)

        print(f"Update row {row_id}!")

        numrows=db.sql_do("UPDATE temp SET name='The Bird' WHERE id=%s",(row_id,))

        print(f"{numrows} row(s) updated!")

        print(f"{db.sql_query_row("SELECT * FROM temp WHERE id=%s",(row_id,))}")

        print("Adding a row!")

        db.sql_do("INSERT INTO temp(name,description) VALUES(%s,%s)",("Bob Dylan","Harmonica"))

        row_id=db.lastrowid()

        print(f"Row {row_id} added!")

        print(db.sql_query_row("SELECT * FROM temp WHERE id=%s",(row_id,)))

        print("Delete a row (Cobham)!")

        numrows=db.sql_do("DELETE FROM temp WHERE name LIKE %s",("%Cobham%",))

        print(f"{numrows} row(s) deleted!")

        print("Print remaining rows!")

        for row in db.sql_query("SELECT * FROM temp"):

            print(row)

        #cleanup
        print("Cleanup: drop table temp!")

        db.sql_do("DROP TABLE IF EXISTS temp")

        print("Done!")

    except BWErr as err:

        print(f"Error {err}!")

        exit(1)

if __name__=="__main__":

    main()