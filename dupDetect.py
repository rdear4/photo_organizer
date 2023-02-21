from functools import reduce
import os
import exiftool
import hashlib
from PIL import Image
import argparse
import time
import concurrent.futures
import threading
from datetime import datetime
import math

import logging
from DBManager import DBManager

startTime = time.perf_counter()

LOG_FILENAME = "log.txt"

#For use with the 'max' argument in the argparser
max_processing_count = 0

ROOT_PATH = "/Volumes/DATA/"

#Setup the arg parser
parser = argparse.ArgumentParser(description="This script can accept different arguments to modify the behavior of execution")
parser.add_argument("-s", "--searchonly", action="store_true", help="Only find images. Do not process them further beyond that")
parser.add_argument("-p", "--path", action="store", type=str, help="Path of a directory to scan", default=".")
parser.add_argument("-f", "--fetch", help="Fetch all image info currently stored in the db", action="store_true")
parser.add_argument("--drop", help="Drop the images table in the db", action="store_true")
parser.add_argument("--dups", help="list all the images with non distinct hashes", action="store_true")
parser.add_argument("--max", action="store", help="Maximum number of files to process", type=int)
parser.add_argument("-l", help="Enables debug and error ", action="store_true")

args = parser.parse_args()

def writeToLog(error_message):

    task = threading.Thread(target=writeToLogOnSeparateThread, args=[error_message])
    task.start()
    task.join()

def writeToLogOnSeparateThread(err_msg):

    time_as_string = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    with open(LOG_FILENAME, "a") as f:
        f.write(f'{time_as_string} - {err_msg}\n')

def getFiles(dirPath):

    global conn
    global max_processing_count

    if args.max and not max_processing_count < args.max:
        # print("Max number of files to be processed reached")
        return 

    try:

        with concurrent.futures.ThreadPoolExecutor() as exe:

            with os.scandir(dirPath) as it:

                results = []

                for entry in it:

                    #check to see if we've already processed the max number of images (if args.max is defined)
                    if args.max and not max_processing_count < args.max:
                        break

                    if not entry.name.startswith('.') and entry.is_dir():

                        # print(f'DIR: {entry.path}')
                        getFiles(entry.path)

                    #DEBUG ONLY

                    if not entry.name.startswith('.') and entry.is_file():

                        
                        # print(f'********\n{dirPath}/{entry.name}\n{entry.is_dir()}\n{entry.is_file()}\n')
                        results.append(exe.submit(processMedia, f'{dirPath}/{entry.name}'))
                        
                        # processMedia(f'{aDir}/{entry.name}')

                        #DEBUG ONLY
                        max_processing_count+=1

                
                for r in concurrent.futures.as_completed(results):

                    if r.result() == None:
                        continue

                    try: 
                        c = conn.cursor()
                        sql = """
                            INSERT INTO {tname}(name, type, filepath_original, filepath_new, fqpn, size, date, latitude, longitude, hash, cameraModel, exifDateTime, hasAAE)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """
                        c.execute(sql.format(tname=DB_TABLE_NAME), r.result())
                        conn.commit()
                    except ValueError:
                        writeToLog(f'Unable to write to the DB: Result Value unsupported. {r.result()}')

                    except sqlite3.IntegrityError as e:

                        #Write error to file using a separate thread
                        writeToLog(f'Unable to write to the DB: {r.result()[0]} at {r.result()[4]} - {e}')
                        
                    except Exception as e:

                        #Write error to file using a separate thread
                        writeToLog(f'Unable to write to the DB: {r.result()[0]} at {r.result()[4]} - {e}')

    except FileNotFoundError as e:

        writeToLog(f'Unable to find directory: {dirPath} - {e}')

def checkIfTableExists(cur, tbl_name):

    cur.execute(f'SELECT count(name) FROM sqlite_master WHERE type=\'table\' AND name=\'{tbl_name}\'')

    return cur.fetchone()[0] == 1

def createImagesTable(cur):

    sql = """

        CREATE TABLE {tname} (
            id INTEGER PRIMARY KEY,
            name text,
            type text,
            filepath_original text,
            filepath_new text,
            fqpn text,
            size integer,
            date text,
            latitude text,
            longitude text,
            hash text,
            cameraModel text,
            exifDateTime text,
            hasAAE integer
        )
    
    """.format(tname=DB_TABLE_NAME)

    cur.execute(sql)

def getMetadataValue(md, key):

    try:

        # print(md[key])
        return md[key]

    except:
        # print(f'key: {key} Not Found')
        return ""

def processMedia(fp):

    
    path_components = fp.split(".")

    #get the file's extension
    extension = path_components[-1]
    if extension == "AAE":
        return None
    
    #Check to see if the photo has an associated AAE file.
    #convert the fp string into one that ends in AAE
    AAEFilepath = ".".join(path_components[:-1]+["AAE"])
    aaeFilepath = ".".join(path_components[:-1]+["aae"])
    hasAAE = 0
    if os.path.exists(aaeFilepath) or os.path.exists(AAEFilepath):
        hasAAE = 1

    writeToLog(f'Beginning to process file: {fp}')

    try:

        with exiftool.ExifTool() as et:
            # print(fp)
            imgData = et.get_metadata(fp)

            extension = fp.split(".")[-1]

            imgInfo = None
            fullPath = getMetadataValue(imgData, 'SourceFile')
            
            if extension.lower() in ["jpeg", "jpg", "png", "heic"]:
                
                img = Image.open(fullPath)
                imgInfo = (
                    fullPath.split("/")[-1],                                #filename
                    "image",                                                #denotes that the media type is image
                    f'{ROOT_PATH}{"/".join(fullPath.split("/")[1:-1])}',    #filepath-original
                    "",                                                     #filepath_new
                    f'{ROOT_PATH}{fullPath[2:]}',                           #the fully qulaified path name
                    getMetadataValue(imgData, "File:FileSize"),             #size (in bytes)
                    getMetadataValue(imgData, "File:FileModifyDate"),       #date (file modify date)
                    f'{getMetadataValue(imgData, "EXIF:GPSLatitude")}',     #GPS Lat from Exif
                    f'-{getMetadataValue(imgData, "EXIF:GPSLongitude")}' if len(str(getMetadataValue(imgData, "EXIF:GPSLongitude"))) > 0 else "",
                    hashlib.md5(img.tobytes()).hexdigest(),
                    f'{getMetadataValue(imgData, "EXIF:Make")} {getMetadataValue(imgData, "EXIF:Model")}',
                    getMetadataValue(imgData, "EXIF:DateTimeOriginal"),
                    hasAAE
                )

            elif extension.lower() in ["mov", "m4v", "mp4", "avi"]:
                imgInfo = (
                    fullPath.split("/")[-1],                                #filename
                    "video",                                                #denotes that the media type is video
                    f'{ROOT_PATH}{"/".join(fullPath.split("/")[1:-1])}',    #filepath-original
                    "",                                                     #filepath_new
                    f'{ROOT_PATH}{fullPath[2:]}',                           #the fully qulaified path name
                    getMetadataValue(imgData, "File:FileSize"),             #size (in bytes)
                    getMetadataValue(imgData, "File:FileModifyDate"),       #date (file modify date)
                    f'{getMetadataValue(imgData, "Composite:GPSLatitude")}',
                    f'-{getMetadataValue(imgData, "Composite:GPSLongitude")}' if len(str(getMetadataValue(imgData, "Composite:GPSLongitude"))) > 0 else "",
                    hashlib.md5(f'{getMetadataValue(imgData, "File:FileSize")}{getMetadataValue(imgData, "Composite:GPSLatitude")}{getMetadataValue(imgData, "Composite:GPSLongitude")}{getMetadataValue(imgData, "File:FileModifyDate")}'.encode('utf-8')).hexdigest(),
                    f'{getMetadataValue(imgData, "QuickTime:Make")} {getMetadataValue(imgData, "QuickTime:Model")}',
                    getMetadataValue(imgData, "QuickTime:CreateDate"),
                    hasAAE
                )
            else:

                writeToLog(f'Unsupported file type found {extension} for file: {fullPath}')

        
        # return f"Processing {fp.split('/')[-1]} completed"
        return imgInfo

    except Exception as e:

        writeToLog(f'Error processing file: {fp} - {e}')

        return None


def main(dbman):
    
    if args.fetch:

        mediaEntries = dbman.getAllRowsFromTable()

        logging.debug(f"There are {len(mediaEntries)} entries in the table")

    if args.dups:

        logging.info("Finding duplicate entries in db...")

        duplicates = dbman.findDuplicates()
        # c.execute(f'SELECT * FROM {DB_TABLE_NAME} WHERE hash IN (SELECT hash FROM {DB_TABLE_NAME} GROUP BY hash HAVING COUNT(*) > 1)')

        # results = c.fetchall()
        logging.debug(f"{len(duplicates)} duplicate(s) found")
        # hash_list = reduce(lambda a, e: a + [e[9]], results, [])
        

        # for h in hash_list:

        #     c.execute(f'SELECT * FROM {DB_TABLE_NAME} WHERE hash="{h}"')
        #     images = c.fetchall()

        #     for img in images:
        #         print(img[0])

        #     print(" ")

        # return

    # if args.drop:

    #     if checkIfTableExists(c, DB_TABLE_NAME):

    #         c.execute(f'DROP TABLE {DB_TABLE_NAME}')

    #         conn.commit()

    #     return

    #check to see if the images table exists
    # if not checkIfTableExists(c, DB_TABLE_NAME):

    #     print(f'{DB_TABLE_NAME} table does not exist. Creating it now...')
    #     #create the images table
    #     createImagesTable(c)

    #     conn.commit()

    #     print(f'{DB_TABLE_NAME} table created successfully')
    

    # getFiles(args.path)
    

                        
if __name__ == "__main__":
    print("\n\n")
    #setup logging
    fmt = '[%(levelname)s] %(asctime)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=fmt)
    

    logging.info(f'Script started at: {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}')
    
    #create a connection to the SQLite3 db
    # conn = sqlite3.connect('images.db')
    dbManager = DBManager()

    #call the main function with the db connection
    main(dbManager)
    
    #close the connection
    # conn.close()
    dbManager.closeConnection()

    endTime = time.perf_counter()

    total_seconds = math.floor(endTime - startTime)
    hours = total_seconds / 3600 if total_seconds / 3600 > 9 else f'0{total_seconds / 3600}'
    minutes = total_seconds % 3600 / 60 if total_seconds % 3600 / 60 > 9 else f'0{total_seconds % 3600 / 60}'
    seconds = (total_seconds % 3600) % 60 if (total_seconds % 3600) % 60 > 9 else f'0{(total_seconds % 3600) % 60}'

    logging.info(f'Script ellapsed time: {hours}:{minutes}:{seconds}')

    