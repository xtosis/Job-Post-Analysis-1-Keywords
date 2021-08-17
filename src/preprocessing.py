from common.utils import PICKLE_LOAD
from common.logging import SubProcessLogger
from common.paths import PATH_DATA
import os


class KeywordPreprocessing(SubProcessLogger):

    def preprocessing(self, path, data_format):
        raw = self.loadData(path)
        if data_format == 'indeed_samples':
            cleaned = self.indeedSamples(raw)

    def loadData(self, path):
        raw = dict()
        for name in os.listdir(path):
            file_path = path + f'/{name}'
            if os.path.isfile(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    name = file_path.replace(PATH_DATA, '')
                    raw[name] = f.read()
                    self.show_start_and_end('loaded', name, raw[name], 90)
        return raw

    def indeedSamples(self, raw):
        processed = dict()

        for name, cleaned_text in raw.items():

            # putting '&' back
            cleaned_text = cleaned_text.replace('&amp;', '&')

            # sample files format: prefix and suffix
            cleaned_text = self.remove_prefix(cleaned_text, "['")
            cleaned_text = self.remove_suffix(cleaned_text, "']")

            # indeed posts format: prefix and suffix
            cleaned_text = self.remove_prefix(cleaned_text, '<div id="jobDescriptionText" class="jobsearch-jobDescriptionText">')
            cleaned_text = self.remove_suffix(cleaned_text, '</div>')

            # indeed posts format: some posts are still enclosed in div containers
            while cleaned_text.find('<div>') == 0:
                if cleaned_text[-6:] == '</div>':
                    cleaned_text = self.remove_prefix(cleaned_text, '<div>', ignore=True)
                    cleaned_text = self.remove_suffix(cleaned_text, '</div>', ignore=True)
                else:
                    break

            self.show_start_and_end('cleaned', name, cleaned_text, 90)
            processed[name] = cleaned_text

        return processed

    def remove_prefix(self, text, prefix, ignore=False):
        if text[:len(prefix)] == prefix:
            text = text[len(prefix):]
        elif ignore is False:
            print(f'prefix mismatch: {prefix} != {text[:len(prefix)]}')  # TODO LOGGING: warning
        return text

    def remove_suffix(self, text, suffix, ignore=False):
        if text[-len(suffix):] == suffix:
            text = text[:-len(suffix)]
        elif ignore is False:
            print(f'suffix mismatch: {suffix} != {text[-len(suffix):]}')  # TODO LOGGING: warning
        return text
