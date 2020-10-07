from PIL import Image
import os
import sqlite3
import exiftool
import hashlib
from PIL import Image
import argparse
import time
import concurrent.futures

startTime = time.perf_counter()

ROOT_PATH = "/Volumes/DATA/"
DB_TABLE_NAME = "Media"

#Setup the arg parser
parser = argparse.ArgumentParser(description="This script can accept different arguments to modify the behavior of execution")
parser.add_argument("-fetch", help="Fetch all image info currently stored in the db", action="store_true")
parser.add_argument("-drop", help="Drop the images table in the db", action="store_true")
parser.add_argument("-dups", help="list all the images with non distinct hashes", action="store_true")
args = parser.parse_args()

def getDirectories(dirPath):

    directories = []

    with os.scandir(dirPath) as it:

        for entry in it:

            if not entry.name.startswith('.') and not entry.is_file():

                directories.append(f'{dirPath}/{entry.name}')
                
                directories.extend(getDirectories(f'{dirPath}/{entry.name}'))

    return directories

def checkIfTableExists(cur, tbl_name):

    cur.execute(f'SELECT count(name) FROM sqlite_master WHERE type=\'table\' AND name=\'{tbl_name}\'')

    return cur.fetchone()[0] == 1

def createImagesTable(cur):

    sql = """

        CREATE TABLE {tname} (
            name text,
            type text,
            filepath_original text,
            filepath_new text,
            fqpn text NOT null UNIQUE,
            size integer,
            date text,
            latitude text,
            longitude text,
            hash text,
            cameraModel text,
            exifDateTime text
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

    with exiftool.ExifTool() as et:
        
        imgData = et.get_metadata(fp)

        extension = fp.split(".")[-1]

        imgInfo = None
        fullPath = getMetadataValue(imgData, 'SourceFile')

        if extension in ["jpeg", "jpg", "png"]:
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
                getMetadataValue(imgData, "EXIF:DateTimeOriginal")
            )

        elif extension in ["mov", "m4v"]:
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
        else:
            print(f'Unsupported extension found: {extension}')

    
    # return f"Processing {fp.split('/')[-1]} completed"
    return imgInfo


def main(conn):
    
    #setup the cursor
    c = conn.cursor()

    if args.fetch:

        c.execute(f'SELECT * FROM {DB_TABLE_NAME}')

        results = c.fetchall()

        for e in results:

            print(e)

        return

    if args.dups:

        c.execute(f'SELECT * FROM {DB_TABLE_NAME} WHERE hash IN (SELECT hash FROM images GROUP BY hash HAVING COUNT(*) > 1)')

        results = c.fetchall()

        for e in results:

            print(e)

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


    #check to see if the directories table exists

    #find all subdirectories in the current dir
    dirs = getDirectories('.')

    for aDir in dirs:

        with os.scandir(aDir) as it:

            with concurrent.futures.ThreadPoolExecutor() as exe:

                results = []

                for entry in it:

                    if not entry.name.startswith('.') and entry.is_file:

                        results.append(exe.submit(processMedia, f'{aDir}/{entry.name}'))
                        # print(f'\nGetting Metadata for image...')
                        # processMedia(f'{aDir}/{entry.name}')

                for r in concurrent.futures.as_completed(results):

                    # print(r.result())
                    try: 
                        # c = dbConn.cursor()
                        sql = """
                            INSERT INTO {tname}(name, type, filepath_original, filepath_new, fqpn, size, date, latitude, longitude, hash, cameraModel, exifDateTime)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                        """
                        c.execute(sql.format(tname=DB_TABLE_NAME), r.result())
                        conn.commit()
                    except Exception as e:
                        print(f'Error adding info for {r.result()[0]} to the db')
                        print(e)
                    
                    
                        
if __name__ == "__main__":

    #create a connection to the SQLite3 db
    conn = sqlite3.connect('images.db')

    main(conn)

    # #close the connection
    conn.close()

    endTime = time.perf_counter()

    print(f'Script ellapsed time: {round(endTime - startTime, 4)}')
    # # print(f'{len(dirs)} directories to be processed')

    