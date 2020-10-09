# photo_organizer

I have over 600 GBs of personal photos and videos. I haven't been very diligent when it comes to backing up my data so serveral times over the past 10 years or so I've dumped whole photo libraries into one folder, shared drive, or external HDD. This has led to a significant percentage of that 600+ GBs being duplicate files.

An added wrench in the works is that a a lot of these files were recovered from an overwritten HDD about 15 years ago. In addition to this, these files originate from multiple devices. This leaves me in a situation where many of the files have the same names.

In attempting to remove all the duplicates, I have used various programs including Apple Photos to attempt to consolodate my library and remove all the unwanted files. This has either resulted in excessive processing times or errors or continually fractured libraries.

After years of struggling I figured, eff it, I'll do it myself. This may not be the best way to handle such a task, but it is definitely one way to do it.

Here's my plan:

### Phase 1 - Gather data

1. Point the script at a directory. Have it iterate over all files in the directory
    a. If it finds an image/video have it process that media
    b. If it finds a directory, have it recursively call function to scan directory
2. Iterate over all those subdirectories and find all the images and videos
3. Get a unique hash for each file to aid in the detection of duplicates
    a. For images: Using PIL obtain a hash for the images using only image data and not the complete file
    b. For videos: Extract metadata and create a hash based of location, date, time, and file size
4. Add an entry to a SQLite DB containing imagename, file type, original filepath, fully qualified path name, size, date (file creation date), lat, lon, hash, camera model, and datetime extracted from exif data/video metadata

### Phase 2 - Find Duplicates

Once that is complete, I will write a script to query the db for all nonunique hash values. The script will randomly select 100 of them and then get a list of all the files associated with them. It will copy all those files to a directory for inspection, renaming the files to have the associated hash values in their names so that I can inspect them and verfy that they are actually duplicates.

Once I'm satisfied with the script/s ability to find dupliactes, AND having all my photos/videos backed up, I will run the script again and let it remove all the duplicates itself.

### Phase 3 - Reorganize Files

After all the duplicates have been dealt with, I will run the script again with the flag for reorganizing the files. It will organize them according to the following structure:

`
Year
    -> Month
            -> Day
`

It will use the exif/metadata date (or file creation date if exit not available) to organize them.

## TO DO

+ Add field to DB to track existence of AAE file
+ When reorganizing files, if file has AAE move it to same directory