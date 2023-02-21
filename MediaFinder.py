"""

Media Finder

All tasks associated with searching directories for applicable media

"""

import os
import logging

class MediaFinder:

    def __init__(self, _initialSearchDirectory="."):

        self.initialSearchDirectory = _initialSearchDirectory
        self.files = []
        self.searchComplete = False
        
        self.searchForFiles()
        # self.searchDirectory(self.initialSearchDirectory)
    def searchForFiles(self):
        TESTFILESTOTAL = 31 + 246 + 327 + 15 + 3
        self.searchDirectory(self.initialSearchDirectory)

        logging.info("Done searching for files")
        logging.debug(self.files[0])
        logging.debug(f"{len(self.files)} files were found to process out of an expected {TESTFILESTOTAL}")

    def searchDirectory(self, dirPath):

        with os.scandir(dirPath) as dirResults:
            tmpFiles = []

            for entry in dirResults:
                if not entry.name.startswith(".") and entry.is_dir():

                    self.searchDirectory(entry.path)

                if not entry.name.startswith(".") and entry.is_file():

                    self.files.append(f"{dirPath}/{entry.name}")

            self.files.extend(tmpFiles)

        return