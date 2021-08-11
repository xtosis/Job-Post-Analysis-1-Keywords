from common.paths import PATH_DATA
from keywords import Process

PATH_JOB_POSTS = PATH_DATA + '/indeed'
PATH_KEYWORDS = PATH_DATA + '/keywords_data_science'

test = Process(PATH_JOB_POSTS, PATH_KEYWORDS)
