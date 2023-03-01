"""

Media Finder

All tasks associated with searching directories for applicable media

"""

import os
import logging
from multiprocessing import Process
import time

class MediaFinder:

    def __init__(self, _initialSearchDirectory=".", _queueRef = None, _searchDirsOnly = False):

        self.initialSearchDirectory = _initialSearchDirectory
        # self.files = []
        # self.directories = []
        self.searchComplete = False
        self.queueRef = _queueRef
        
        #create the Process that will search for files
        if not _searchDirsOnly:
            self.SearchProcess = Process(target=self.searchDirectory, args=[self.initialSearchDirectory, self.queueRef])
            self.SearchProcess.start()
        else:
            self.SearchProcess = Process(target=self.searchForDirectories, args=[self.initialSearchDirectory, self.queueRef])
            self.SearchProcess.start()
        
    def stillSearching(self):

        return self.SearchProcess.is_alive()

    def searchForDirectories(self, dirPath, qRef=None):
        
        with os.scandir(dirPath) as dirResults:
            
            fileCount = 0
            for entry in dirResults:
                if not entry.name.startswith(".") and entry.is_dir():
                    
                    self.searchForDirectories(entry.path, qRef)

                if not entry.name.startswith(".") and entry.is_file():
                    
                    fileCount = fileCount + 1
            qRef.put((dirPath, fileCount))
                    
                    

    def searchDirectory(self, dirPath, qRef=None):

        with os.scandir(dirPath) as dirResults:
            
            
            for entry in dirResults:
                if not entry.name.startswith(".") and entry.is_dir():

                    self.searchDirectory(entry.path, qRef)

                if not entry.name.startswith(".") and entry.is_file():

                    # self.files.append(f"{dirPath}/{entry.name}")
                    qRef.put(f"{dirPath}/{entry.name}")
                    # self.queueRef.put(f"{dirPath}/{entry.name}")


        return