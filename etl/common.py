from settings import DATA_FOLDER
import json


class GenericETL():

    def __init__(self):
        self.data_source = DATA_FOLDER

    def run(self):
        obj = {"test": "hello",
               "folder": self.data_source}

        obj = json.dumps(obj)
        return obj
