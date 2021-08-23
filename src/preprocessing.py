from common.utils import PICKLE_LOAD
from common.logging import SubProcessLogger
from common.paths import PATH_DATA
import os
import pandas as pd
import re
import copy
import hashlib


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

    def HTML_tags_get_list(self, text_dict):
        tags = pd.DataFrame(columns=['ends', 'forms'])

        for name, text in text_dict.items():
            for tag in re.findall('<.*?>', text):
                if tag[1] != '/':
                    form = copy.deepcopy(tag)
                    if tag.find(' ') > -1:
                        tag = tag.split(' ')[0] + '>'
                    if tag not in tags.index:
                        row = pd.Series({'ends': False, 'forms': {form}}, name=tag)
                        tags = tags.append(row)
                    else:
                        tags.loc[tag, 'forms'].add(form)
                else:
                    tag = '<' + tag[2:]
                    if tag not in tags.index:
                        print('starting tag not found:', tag)  # TODO LOGGING: error
                    else:
                        tags.loc[tag, 'ends'] = True

        return tags

    def remove_doubled(self, text, char):
        remove = f'{char}{char}'
        while text.find(remove) > -1:
            text = text.replace(remove, char)
        return text

    def HTML_tags_replace_empty(self, text_dict, tags):
        tags_with_endings = copy.deepcopy(tags.query('ends == True'))
        replaced_tags = dict()

        for name, text in text_dict.items():

            size_start = len(text)

            for character in ('\n', ' '):
                text = self.remove_doubled(text, character)
            for tag, forms in tags_with_endings['forms'].items():
                tag_end = tag.replace('<', '</')
                for tag_start in forms:
                    for character in ('\n', ' ', ''):
                        remove = tag_start + character + tag_end
                        text = text.replace(remove, '\n')
                        text = self.remove_doubled(text, '\n')
            text = text.replace('<br>', '\n')
            text = self.remove_doubled(text, '\n')
            replaced_tags[name] = text

            size_end = len(text)
            print('replaced html tags for', name, size_start, 'to', size_end)  # TODO LOGGING: debug

        return replaced_tags

    def HTML_tags_remove(self, text_dict, tags):
        removed_tags = dict()

        for name, text in text_dict.items():

            size_start = len(text)

            for tag, forms in tags['forms'].items():
                for tag_start in forms:
                    text = text.replace(tag_start, '')
                tag_end = tag.replace('<', '</')
                text = text.replace(tag_end, '')
            removed_tags[name] = text

            size_end = len(text)
            print('removed html tags for', name, size_start, 'to', size_end)  # TODO LOGGING: debug

        return removed_tags

    def final_clean_up(self, text_dict):
        cleaned = dict()

        for name, text in text_dict.items():

            size_start = len(text)

            for char in ('\n', ' '):
                text = self.remove_doubled(text, char)
            cleaned[name] = text

            size_end = len(text)
            print('final clean up', name, size_start, 'to', size_end)  # TODO LOGGING: debug

        return cleaned


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

        processed = self.final_clean_up(processed)
        processed = self.splitSentencesThenAnalyze(processed)

        sentences = processed['sentences']
        # type    | pandas.DataFrame
        # --------+-----------------------------------------------------------
        # index   | md5 hash of the sentence without lowering
        # columns | words: # of words in the sentence
        #         | commas: # of commas in the sentence
        #         | sentence: sentence text without lowering

        map_text_hashes = processed['map_text_hashes']
        # type   | dict()
        # -------+------------------------------------------------------------
        # keys   | md5 hash of the full text of every unique post without lowering
        # values | file names

        map_sentences = processed['map_sentences']
        # type    | pandas.DataFrame
        # --------+-----------------------------------------------------------
        # index   | arbitrary
        # columns | files: file names
        #         | id_s: line # of the sentence in the cleaned text of the post
        #         | sentence_hash: md5 hash of the sentence without lowering

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
                        "\\'": "'"}  # replacing \'

        for name, text in text_dict.items():
            for old, new in replace_dict.items():
                text = text.replace(old, new)
            processed_html[name] = text

        return processed_html

    def standardPreprocessing_HTML_tags(self, text_dict):
        tags = self.HTML_tags_get_list(text_dict)
        processed_html = self.HTML_tags_replace_empty(text_dict, tags)
        processed_html = self.HTML_tags_remove(processed_html, tags)
        return processed_html

    def splitSentencesThenAnalyze(self, text_dict):
        # unique post to sentence connections
        map_sentence_lines = pd.DataFrame(columns=['file', 'id_s', 'sentence_hash'])  # index: arbitrary

        # sentences
        unq_sentences = pd.DataFrame(columns=['words', 'commas', 'sentence'])  # index: sentence_hash
        dup_sentences = pd.DataFrame(columns=['count', 'sentence'])  # index: sentence_hash

        # text hashes for files
        map_text_hashes = dict()  # index: text_hash

        # duplicate files
        dup_text_hash = dict()  # index: file name

        for name, text in text_dict.items():
            sentences = text.splitlines()
            text_hash = ''

            # current post to sentence connections (text split into lines)
            cur_p2sc = pd.DataFrame(columns=['file', 'id_s', 'sentence_hash'])

            for i, sentence in enumerate(sentences):

                # --- computing sentence data: words, commas, sentence hash & incrementing text_hash
                words = sentence.count(' ') + 1
                commas = sentence.count(',')
                sentence_hash = hashlib.md5(sentence.encode()).hexdigest()
                text_hash = text_hash + sentence_hash

                # --- checking sentence hashes for duplicates

                # sentence is unique: update unq_sentences
                if sentence_hash not in unq_sentences.index.values:

                    # TODO LOGGING: debug
                    print('splitSentencesThenAnalyze {:} line {:02g} UNQ-SENTENCE: {:}'.format(name, i, sentence))

                    row = pd.Series({'words': words, 'commas': commas}, name=sentence_hash)
                    unq_sentences = unq_sentences.append(row)

                # sentence is a duplicate: update unq_sentences
                else:

                    # TODO LOGGING: debug
                    print('splitSentencesThenAnalyze {:} line {:02g} DUP-SENTENCE: {:}'.format(name, i, sentence[:]))

                    # first occurance: add row
                    if sentence_hash not in dup_sentences.index.values:
                        row = pd.Series({'count': 1, 'sentence': sentence}, name=sentence_hash)
                        dup_sentences = dup_sentences.append(row)

                    # subsequent occurances: increment duplicate count
                    else:
                        val = dup_sentences.loc[sentence_hash, 'count'] + 1
                        dup_sentences.loc[sentence_hash, 'count'] = val

                # --- constructing current p2sc
                cur_p2sc = cur_p2sc.append({
                    'file': name,
                    'id_s': i,
                    'sentence_hash': sentence_hash
                }, ignore_index=True)

            # --- checking post text_hash for duplicates
            text_hash = hashlib.md5(text_hash.encode()).hexdigest()

            # text_hash is unique (unique text in post): update map_text_hashes & map_sentence_lines
            if text_hash not in map_text_hashes.keys():

                # TODO LOGGING: debug
                print('splitSentencesThenAnalyze {:} UNQ-FILE-HASH'.format(name))

                map_text_hashes[text_hash] = name
                map_sentence_lines = map_sentence_lines.append(cur_p2sc, ignore_index=True)

            # text_hash is a duplicate: update dup_text_hash
            else:
                org = map_text_hashes[text_hash]

                # TODO LOGGING: debug
                print('splitSentencesThenAnalyze {:} DUP-FILE-HASH parent: {:}'.format(name, org))

                # first occurance: add original file name to keys
                if org not in dup_text_hash.keys():
                    dup_text_hash[org] = []

                # append duplicate filename
                dup_text_hash[org].append(name)

        # TODO LOGGING start: info
        dup_sentences.sort_values(['count'], inplace=True)
        print('-' * 79)
        print('dup_sentences')
        print(dup_sentences)
        print('-' * 79)
        print('dup_text_hashes')
        print(pd.Series(dup_text_hash, name='dup_text_hashes: org vs dups'))
        # TODO LOGGING end: info

        res = {'sentences': unq_sentences,
               'map_text_hashes': map_text_hashes,
               'map_sentences': map_sentence_lines}

        return res

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
