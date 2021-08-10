from common.paths import *
from common.utils import PICKLE_LOAD, PICKLE_SAVE
from common.logging import ProcessLogger
from settings import KeywordSettings
from preprocessing import KeywordPreprocessing


class Process(ProcessLogger, KeywordSettings, KeywordPreprocessing):
    def __init__(self, job_post_path, keywords_path):
        self.loadData(keywords_path)
        self.preprocessing(job_post_path)  # from KeyworfPreprocessing
        self.process()
        self.finish()

    def loadData(self, keywords_path):
        # self.keywords = ...
        pass

    def process(self):
        # these are stages
        # 1 word keywords
        # 2 word keywords
        # 3 word keywords
        # phrases
        pass

    def finish(self):
        # save data
        # print stats
        pass
