from common.utils import PICKLE_LOAD
from common.logging import SubProcessLogger


class KeywordPreprocessing(SubProcessLogger):

    def preprocessing(self, path):
        self.loadData(path)

    def loadData(self, path):
        # self.raw  = ...
        pass
