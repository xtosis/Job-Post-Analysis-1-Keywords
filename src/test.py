from common.paths import PATH_DATA
from models import KeywordsModel
from settings import PANDAS_DISPLAY_SETTINGS

PANDAS_DISPLAY_SETTINGS()

PATH_JOB_POSTS = PATH_DATA + '/indeed/samples'
PATH_KEYWORDS = PATH_DATA + '/keywords_data_science.xlsx'
TEMPLATE = 'indeed_samples'

test = KeywordsModel(PATH_JOB_POSTS, PATH_KEYWORDS, TEMPLATE)
