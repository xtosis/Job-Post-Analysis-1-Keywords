from common.paths import *
from common.utils import PICKLE_LOAD, PICKLE_SAVE
from common.logging import ProcessLogger
from settings import KeywordSettings
from preprocessing import FileToSentencePreprocessor
import pandas as pd
import copy


REPLACE = {
    ' and ': ' & ',
    ' & ': ' and ',
}


class KeywordsModel(ProcessLogger, KeywordSettings):
    def __init__(self, job_post_path, keywords_path, template):

        preprocessor = FileToSentencePreprocessor()
        preprocessor.preprocess(job_post_path, template)

        self.loadKeywords(keywords_path)
        self.process()
        self.finish()

    def loadKeywords(self, keywords_path):

        def add_variation(text, collection):
            text = text.lower()
            collection.add(text)
            for to_find, to_replace in REPLACE.items():
                collection.add(text.replace(to_find, to_replace))
            return collection

        kws = pd.read_excel(keywords_path)
        kws.drop(columns=['count', 'lower', 'type'], inplace=True)

        # making sure there are no duplicate keywords
        assert len(kws['name'].unique()) == len(kws)
        kws.set_index('name', inplace=True, drop=True)

        # filtering categories
        filtered_categories = ['cert', 'project', 'link']
        fil = kws.query(f'category not in {filtered_categories}')
        kws = copy.deepcopy(fil)
        del fil

        # updating variations
        for keyword, variations in kws['variations'].items():
            keyword_variations = set()
            keyword_variations = add_variation(keyword, keyword_variations)

            # adding variations
            if isinstance(variations, str):
                variations = variations.strip()
                variations = variations.split(', ')
                for variation in variations:
                    keyword_variations = add_variation(variation, keyword_variations)

            # sorting variations so that longer phrases are searched first
            temp = {'text': [], 'words': []}
            for keyword_variation in keyword_variations:
                temp['text'].append(keyword_variation)
                temp['words'].append(keyword_variation.count(' ') + 1)
            temp = pd.DataFrame(temp)
            temp.sort_values('words', ascending=False, inplace=True)
            kws.loc[keyword, 'variations'] = temp['text'].values.tolist()

        self.keywords = kws

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
