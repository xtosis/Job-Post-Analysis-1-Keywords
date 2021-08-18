from common.utils import PICKLE_LOAD
from common.logging import SubProcessLogger
from common.paths import PATH_DATA
import os


class Preprocessing:

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

    def show_start_and_end(self, stage, file_name, text, lim):
        print(stage, file_name, text[:lim], '...', text[-lim:])  # TODO LOGGING: debug or info


class KeywordPreprocessing(Preprocessing, SubProcessLogger):

    def preprocessing(self, path, template):
        raw = self.loadData(path)
        process_HTML = False

        # templates
        if template == 'indeed_samples':
            processed = self.indeedSamplesTemplateExtract(raw)
            process_HTML = True

        # html
        if process_HTML:
            processed = self.standardPreprocessing_HTML_replacements(processed)
            processed = self.standardPreprocessing_HTML_tags(processed)

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

    def standardPreprocessing_HTML_replacements(self, text_dict):
        processed_html = dict()

        replace_dict = {'&amp;': '&',  # putting '&' back
                        '\\n': '\n',  # replacing weird newline artifact: \\n
                        "\'": "'"}  # replacing \'

        for name, text in text_dict.items():
            for old, new in replace_dict.items():
                text = text.replace(old, new)
            processed_html[name] = text

        return processed_html

    def standardPreprocessing_HTML_tags(self, text_dict):
        return text_dict

    def indeedSamplesTemplateExtract(self, text_dict):
        processed_template = dict()

        for name, text in text_dict.items():

            # sample files format: prefix and suffix
            text = self.remove_prefix(text, "['")
            text = self.remove_suffix(text, "']")

            # indeed posts format: prefix and suffix
            text = self.remove_prefix(text, '<div id="jobDescriptionText" class="jobsearch-jobDescriptionText">')
            text = self.remove_suffix(text, '</div>')

            # indeed posts format: some posts are still enclosed in div containers
            while text.find('<div>') == 0:
                if text[-6:] == '</div>':
                    text = self.remove_prefix(text, '<div>', ignore=True)
                    text = self.remove_suffix(text, '</div>', ignore=True)
                else:
                    break

            self.show_start_and_end('indeed-samples-template-extract', name, text, 90)
            processed_template[name] = text

        return processed_template
