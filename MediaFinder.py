"""

Media Finder

All tasks associated with searching directories for applicable media

"""

import os
import logging
from multiprocessing import Process
import time

class MediaFinder:

    def __init__(self, _initialSearchDirectory=".", _queueRef = None):

        self.initialSearchDirectory = _initialSearchDirectory
        self.files = []
        self.searchComplete = False
        self.queueRef = _queueRef
        
        #create the Process that will search for files
        self.SearchProcess = Process(target=self.searchDirectory, args=[self.initialSearchDirectory, self.queueRef])
        self.SearchProcess.start()
        
        # self.searchForFiles()
        # self.searchDirectory(self.initialSearchDirectory)
    def stillSearching(self):

        return self.SearchProcess.is_alive()

    def searchForFiles(self):
        TESTFILESTOTAL = 31 + 246 + 327 + 15 + 3
        self.searchDirectory(self.initialSearchDirectory)
        self.searchComplete = True
        # logging.info("Done searching for files")
        # logging.debug(self.files[0])
        # logging.debug(f"{len(self.files)} files were found to process out of an expected {TESTFILESTOTAL}")

    def searchDirectory(self, dirPath, qRef=None):

        with os.scandir(dirPath) as dirResults:
            tmpFiles = []

            for entry in dirResults:
                if not entry.name.startswith(".") and entry.is_dir():

                    self.searchDirectory(entry.path, qRef)

                if not entry.name.startswith(".") and entry.is_file():

                    self.files.append(f"{dirPath}/{entry.name}")
                    qRef.put(f"{dirPath}/{entry.name}")
                    # self.queueRef.put(f"{dirPath}/{entry.name}")

            self.files.extend(tmpFiles)

        return