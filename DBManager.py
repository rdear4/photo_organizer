"""
Database Manager

Responsible for adding new images and associated data to the db as well as checking to see whether or not the db is correctly intialized.


"""

import sqlite3
import logging

class DBManager:

    def __init__(self, _dbname="images.db", _primaryTableName="Media", _reinit = False):

        self.connection = sqlite3.connect(_dbname)
        self.cursor = self.connection.cursor()
        self.primaryTableName = _primaryTableName

        ### STANDARD INITIALIZATION ###
        # Check if primary table exists
        # If it does not, create it
        if not self.checkIfTableExists(self.primaryTableName):
            print(f"{self.primaryTableName} does not exists!")
            self.createPrimaryTable()
        if not self.checkIfTableExists("Directories"):
            print(f"Directories table does not exists! Creating it now")
            self.createDirectoryTable()

        if _reinit:
            self.dropTable(self.primaryTableName)
            self.createPrimaryTable()
            self.dropTable("Directories")
            self.createDirectoryTable()

    def createDirectoryTable(self):
        try:
            sql = """

                CREATE TABLE Directories (
                    id INTEGER PRIMARY KEY,
                    dirname text,
                    dirpath text UNIQUE,
                    filecount int,
                    fully_searched int DEFAULT 0
                )
            
            """

            res = self.cursor.execute(sql)
            logging.info("Directory table created")
        except Exception as e:
            print(e)
            # raise Exception(f"ERROR CREATING PRIMARY TABLE - {e}")

    def addDirectoryInfoToTable(self, dirInfo):
        try:
            sql = """
                INSERT INTO Directories(dirname, dirpath, filecount)
                VALUES(?,?,?)
            """
            self.cursor.execute(sql, dirInfo)
            self.connection.commit()
        except Exception as e:
            raise Exception(f"Error adding directory info to table - {e}")
        
    def checkIfTableExists(self, tname):
        logging.info(f"Checking if {tname} exists...")

        try:
            self.cursor.execute(f'SELECT count(name) FROM sqlite_master WHERE type=\'table\' AND name=\'{tname}\'')
            return self.cursor.fetchone()[0] == 1

        except Exception as e:
            raise Exception("ERROR CHECKING FOR TABLE EXISTENCE - {e}")

        return False

    def addMediaDataToDB(self, mediaInfo, tname=None):
        if not mediaInfo is None:
            try:
                sql = """
                    INSERT INTO {tn}(name, type, filepath_original, filepath_new, fqpn, size, date, latitude, longitude, hash, cameraModel, exifDateTime, hasAAE)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
                self.cursor.execute(sql.format(tn=tname if not tname == None else self.primaryTableName), mediaInfo)
                self.connection.commit()
            except Exception as e:
                # logging.error(f"There was an error adding that info to the db - {e}")
                # print("There was an error adding that info to the db")
                raise Exception("There was an error adding that info to the db - {e}")
    
    
    def createPrimaryTable(self, tname=None):
        logging.info(f"Creating table {tname if not tname == None else self.primaryTableName}")

        try:
            sql = """

                CREATE TABLE {tname} (
                    id INTEGER PRIMARY KEY,
                    name text,
                    type text,
                    filepath_original text,
                    filepath_new text,
                    fqpn text UNIQUE,
                    size integer,
                    date text,
                    latitude text,
                    longitude text,
                    hash text,
                    cameraModel text,
                    exifDateTime text,
                    hasAAE integer
                )
            
            """.format(tname=tname if not tname == None else self.primaryTableName)

            res = self.cursor.execute(sql)
            logging.info("Media table created")

        except Exception as e:
            print(e)
            # raise Exception(f"ERROR CREATING PRIMARY TABLE - {e}")

    def dropTable(self, tname=None):

        if tname == None:
            raise Exception("Please provide a table name")

        try:
            if self.checkIfTableExists(tname):

                logging.info("Table exists. Dropping...")

                self.cursor.execute(f'DROP TABLE {tname}')

                res = self.connection.commit()

            else:

                print("Table does not exists.")
        
        except Exception as e:
            raise Exception(f"ERROR CREATING PRIMARY TABLE - {e}")

    def closeConnection(self):

        logging.info("Closing connection to db...")

        try:
            self.connection.close()
            logging.info("DB Connection closed!")
        except Exception as e:
            raise Exception(f"ERROR CLOSING DB CONNECTION - {e}")
    
    def getAllRowsFromTable(self, tname=None):

        res = self.cursor.execute(f"SELECT * FROM {tname if not tname is None else self.primaryTableName}")

        return res.fetchall()

    def findDuplicates(self):

        res = self.cursor.execute(f'SELECT * FROM {self.primaryTableName} WHERE hash IN (SELECT hash FROM {self.primaryTableName} GROUP BY hash HAVING COUNT(*) > 1)')

        return res.fetchall()