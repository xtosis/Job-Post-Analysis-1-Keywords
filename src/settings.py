from common.paths import *
import pandas as pd


def PANDAS_DISPLAY_SETTINGS():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('max_colwidth', 100)


class KeywordSettings:
    def __init__(self):
        self.process_name = 'Keywords'
