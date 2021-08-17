from common.utils import PICKLE_LOAD
from common.logging import SubProcessLogger
from common.paths import PATH_DATA
import os


class KeywordPreprocessing(SubProcessLogger):

    def preprocessing(self, path):
        raw = self.loadData(path)

    def loadData(self, path):
        raw = dict()
        for name in os.listdir(path):
            file_path = path + f'/{name}'
            if os.path.isfile(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    name = file_path.replace(PATH_DATA, '')
                    raw[name] = f.read()
                    print('loaded', name, raw[name][:90], '...', raw[name][-90:])
        return raw
