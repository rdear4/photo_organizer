from functools import reduce
import os
import sqlite3
import exiftool
import hashlib
from PIL import Image
import argparse
import time
import concurrent.futures
import threading
from datetime import datetime
import math

startTime = time.perf_counter()

ERROR_LOG_FILENAME = "errors.txt"

#For use with the 'max' argument in the argparser
max_processing_count = 0

ROOT_PATH = "/Volumes/DATA/"
DB_TABLE_NAME = "Media"

conn = None

#Setup the arg parser
parser = argparse.ArgumentParser(description="This script can accept different arguments to modify the behavior of execution")
parser.add_argument("--path", action="store", type=str, help="Path of a directory to scan", default=".")
parser.add_argument("-fetch", help="Fetch all image info currently stored in the db", action="store_true")
parser.add_argument("-drop", help="Drop the images table in the db", action="store_true")
parser.add_argument("-dups", help="list all the images with non distinct hashes", action="store_true")
parser.add_argument("--max", action="store", help="Maximum number of files to process", type=int)

args = parser.parse_args()

def writeErrorToFile(error_message):

    time_as_string = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    with open(ERROR_LOG_FILENAME, "a") as f:
        f.write(f'{time_as_string} - {error_message}\n')

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
                        task = threading.Thread(target=writeErrorToFile, args=[f'Unable to write to the DB: Result Value unsupported. {r.result()}'])
                        task.start()
                        task.join()

                    except sqlite3.IntegrityError as e:

                        #Write error to file using a separate thread
                        task = threading.Thread(target=writeErrorToFile, args=[f'Unable to write to the DB: {r.result()[0]} at {r.result()[4]} - {e}'])
                        task.start()
                        task.join()

                    except Exception as e:

                        #Write error to file using a separate thread
                        task = threading.Thread(target=writeErrorToFile, args=[f'Unable to write to the DB: {r.result()[0]} at {r.result()[4]} - {e}'])
                        task.start()
                        task.join()

    except FileNotFoundError as e:

        task = threading.Thread(target=writeErrorToFile, args=[f'Unable to find directory: {dirPath} - {e}'])
        task.start()
        task.join()



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

def addVideoToDB(dbConn, imgData):

    fullPath = getMetadataValue(imgData, 'SourceFile')

    imgInfo = (
        fullPath.split("/")[-1],                                #filename
        "video",
        f'{ROOT_PATH}{"/".join(fullPath.split("/")[1:-1])}',    #filepath-original
        "",                                                     #filepath_new
        f'{ROOT_PATH}{fullPath[2:]}',
        getMetadataValue(imgData, "File:FileSize"),             #size (in bytes)
        getMetadataValue(imgData, "File:FileModifyDate"),       #date (file modify date)
        f'{getMetadataValue(imgData, "Composite:GPSLatitude")}',
        f'-{getMetadataValue(imgData, "Composite:GPSLongitude")}' if len(str(getMetadataValue(imgData, "Composite:GPSLongitude"))) > 0 else "",
        hashlib.md5(f'{getMetadataValue(imgData, "Composite:GPSLatitude")}{getMetadataValue(imgData, "Composite:GPSLongitude")}{getMetadataValue(imgData, "File:FileModifyDate")}'.encode('utf-8')).hexdigest(),
        f'{getMetadataValue(imgData, "QuickTime:Make")} {getMetadataValue(imgData, "QuickTime:Model")}',
        getMetadataValue(imgData, "QuickTime:CreateDate")
    )

    
    try: 
        c = dbConn.cursor()
        sql = """
            INSERT INTO {tname}(name, type, filepath_original, filepath_new, fqpn, size, date, latitude, longitude, hash, cameraModel, exifDateTime)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """
        c.execute(sql.format(tname=DB_TABLE_NAME), imgInfo)
        dbConn.commit()
    except Exception as e:
        print(f'Error adding info for {imgInfo[1]} to the db')
        print(e)

def addImageToDB(dbConn, imgData):

    fullPath = getMetadataValue(imgData, 'SourceFile')
    
    #open image using PIL
    img = Image.open(fullPath)

    imgInfo = (
        fullPath.split("/")[-1],                                #filename
        "image",
        f'{ROOT_PATH}{"/".join(fullPath.split("/")[1:-1])}',    #filepath-original
        "",                                                     #filepath_new
        f'{ROOT_PATH}{fullPath[2:]}',
        getMetadataValue(imgData, "File:FileSize"),             #size (in bytes)
        getMetadataValue(imgData, "File:FileModifyDate"),       #date (file modify date)
        f'{getMetadataValue(imgData, "EXIF:GPSLatitude")}',
        f'-{getMetadataValue(imgData, "EXIF:GPSLongitude")}' if len(str(getMetadataValue(imgData, "EXIF:GPSLongitude"))) > 0 else "",
        hashlib.md5(img.tobytes()).hexdigest(),
        f'{getMetadataValue(imgData, "EXIF:Make")} {getMetadataValue(imgData, "EXIF:Model")}',
        getMetadataValue(imgData, "EXIF:DateTimeOriginal")
    )
    
    try: 
        c = dbConn.cursor()
        sql = """
            INSERT INTO {tname}(name, type, filepath_original, filepath_new, fqpn, size, date, latitude, longitude, hash, cameraModel, exifDateTime)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """
        c.execute(sql.format(tname=DB_TABLE_NAME), imgInfo)
        dbConn.commit()
    except Exception as e:
        print(f'Error adding info for {imgInfo[0]} to the db')
        print(e)

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
    
    #convert the fp string into one that ends in AAE
    
    aaeFilepath = ".".join(path_components[:-1]+["AAE"])
    # print("\n****")
    # print(fp)
    # print(aaeFilepath)
    hasAAE = 0
    if os.path.exists(aaeFilepath):
        print(aaeFilepath, "Has AAE file")
        hasAAE = 1

    task = threading.Thread(target=writeErrorToFile, args=[f'Beginning to process file: {fp}'])
    task.start()
    task.join()

    try:

        with exiftool.ExifTool() as et:
            # print(fp)
            imgData = et.get_metadata(fp)

            extension = fp.split(".")[-1]

            imgInfo = None
            fullPath = getMetadataValue(imgData, 'SourceFile')
            # print(f'EXTENSION: {extension.lower()}')
            if extension.lower() in ["jpeg", "jpg", "png"]:
                # addImageToDB(conn, et.get_metadata(fp))
                img = Image.open(fullPath)
                imgInfo = (
                    fullPath.split("/")[-1],                                #filename
                    "image",
                    f'{ROOT_PATH}{"/".join(fullPath.split("/")[1:-1])}',    #filepath-original
                    "",                                                     #filepath_new
                    f'{ROOT_PATH}{fullPath[2:]}',
                    getMetadataValue(imgData, "File:FileSize"),             #size (in bytes)
                    getMetadataValue(imgData, "File:FileModifyDate"),       #date (file modify date)
                    f'{getMetadataValue(imgData, "EXIF:GPSLatitude")}',
                    f'-{getMetadataValue(imgData, "EXIF:GPSLongitude")}' if len(str(getMetadataValue(imgData, "EXIF:GPSLongitude"))) > 0 else "",
                    hashlib.md5(img.tobytes()).hexdigest(),
                    f'{getMetadataValue(imgData, "EXIF:Make")} {getMetadataValue(imgData, "EXIF:Model")}',
                    getMetadataValue(imgData, "EXIF:DateTimeOriginal"),
                    hasAAE
                )

            elif extension.lower() in ["mov", "m4v", "mp4", "avi"]:
                imgInfo = (
                    fullPath.split("/")[-1],                                #filename
                    "video",
                    f'{ROOT_PATH}{"/".join(fullPath.split("/")[1:-1])}',    #filepath-original
                    "",                                                     #filepath_new
                    f'{ROOT_PATH}{fullPath[2:]}',
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

                task = threading.Thread(target=writeErrorToFile, args=[f'Unsupported file type found {extension} for file: {fullPath}'])
                task.start()
                task.join()

        
        # return f"Processing {fp.split('/')[-1]} completed"
        return imgInfo

    except Exception as e:

        task = threading.Thread(target=writeErrorToFile, args=[f'Error processing file: {fp} - {e}'])
        task.start()
        task.join()

        return None


def main(conn):
    
    #setup the cursor
    c = conn.cursor()

    if args.fetch:

        c.execute(f'SELECT * FROM {DB_TABLE_NAME}')

        results = c.fetchall()

        for i in range(0, len(results)):

            for j in range(0, len(results[i])):
                print(j, results[i][j])

        return

    if args.dups:

        c.execute(f'SELECT * FROM {DB_TABLE_NAME} WHERE hash IN (SELECT hash FROM {DB_TABLE_NAME} GROUP BY hash HAVING COUNT(*) > 1)')

        results = c.fetchall()
        
        hash_list = reduce(lambda a, e: a + [e[9]], results, [])
        

        for h in hash_list:

            c.execute(f'SELECT * FROM {DB_TABLE_NAME} WHERE hash="{h}"')
            images = c.fetchall()

            for img in images:
                print(img[0])

            print(" ")

        return

    if args.drop:

        if checkIfTableExists(c, DB_TABLE_NAME):

            c.execute(f'DROP TABLE {DB_TABLE_NAME}')

            conn.commit()

        return

    #check to see if the images table exists
    if not checkIfTableExists(c, DB_TABLE_NAME):

        print(f'{DB_TABLE_NAME} table does not exist. Creating it now...')
        #create the images table
        createImagesTable(c)

        conn.commit()

        print(f'{DB_TABLE_NAME} table created successfully')
    

    getFiles(args.path)

                        
if __name__ == "__main__":

    print(f'Script start at: {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}')
    
    #create a connection to the SQLite3 db
    conn = sqlite3.connect('images.db')

    #call the main function with the db connection
    main(conn)
    print("Main done")
    #close the connection
    conn.close()

    endTime = time.perf_counter()

    seconds = math.floor(endTime - startTime)
    minutes = math.floor( seconds / 60 )
    seconds = seconds - (minutes * 60) if (seconds - (minutes * 60)) > 9 else f'0{seconds - (minutes * 60)}'
    minutes = minutes if minutes > 9 else f'0{minutes}'
    print(f'Script ellapsed time: {minutes}:{seconds}')
    # print(f'{len(dirs)} directories to be processed')

    