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

    def show_start_and_end(self, stage, f_hash, f_name, f_text, lim):
        prefix = f'{self.current_process} {f_hash} [{stage}] {f_name}'
        print(prefix, f_text[:lim], '...', f_text[-lim:])  # TODO LOGGING: debug or info

    def remove_doubled(self, text, char):
        remove = f'{char}{char}'
        while text.find(remove) > -1:
            text = text.replace(remove, char)
        return text

    def HTML_tags_get_list(self, previous_map_files):
        tags = pd.DataFrame(columns=['ends', 'forms'])

        for _, file_text in previous_map_files['texts'].items():
            for tag in re.findall('<.*?>', file_text):
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

    def HTML_replacements(self, previous_map_files):

        # --- initializing ---------------------------------------------------
        self.current_process = 'standardPreprocessing_HTML_tags'

        map_file_names = dict()  # map_file_names[file_hash] = file_name
        map_file_texts = dict()  # map_file_texts[file_hash] = file_text

        replace_dict = {'&amp;': '&',  # putting '&' back
                        '\\n': '\n',  # replacing weird newline artifact: \\n
                        "\\'": "'"}  # replacing \'

        # --- processing -----------------------------------------------------
        for previous_file_hash, file_text in previous_map_files['texts'].items():

            # replacing characters
            for old, new in replace_dict.items():
                file_text = file_text.replace(old, new)

            # checking file
            file_name = previous_map_files['names'][previous_file_hash]
            file_hash = hashlib.md5(file_text.encode()).hexdigest()
            register = self.check_file(map_file_names, file_name, file_hash, file_text, 'map_file_texts')
            if register:
                map_file_names[file_hash] = file_name
                map_file_texts[file_hash] = file_text

        current_map_files = {'texts': map_file_texts, 'names': map_file_names}

        return current_map_files

    def HTML_tags_replace_empty(self, previous_map_files, tags):

        # --- initializing ---------------------------------------------------
        self.current_process = 'final_clean_up'

        map_file_names = dict()  # map_file_names[file_hash] = file_name
        map_file_texts = dict()  # map_file_texts[file_hash] = file_text

        tags_with_endings = copy.deepcopy(tags.query('ends == True'))

        # --- processing -----------------------------------------------------
        for previous_file_hash, file_text in previous_map_files['texts'].items():

            size_start = len(file_text)

            # replacing double newlines and double spaces
            for character in ('\n', ' '):
                file_text = self.remove_doubled(file_text, character)

            # replacing empty html tags
            for tag, forms in tags_with_endings['forms'].items():
                tag_end = tag.replace('<', '</')
                for tag_start in forms:
                    for character in ('\n', ' ', ''):
                        remove = tag_start + character + tag_end
                        file_text = file_text.replace(remove, '\n')
                        file_text = self.remove_doubled(file_text, '\n')

            # replacing <br> tags
            file_text = file_text.replace('<br>', '\n')

            # replacing double newlines again
            file_text = self.remove_doubled(file_text, '\n')

            # checking file
            file_name = previous_map_files['names'][previous_file_hash]
            file_hash = hashlib.md5(file_text.encode()).hexdigest()
            register = self.check_file(map_file_names, file_name, file_hash, file_text, 'map_file_texts')
            if register:
                map_file_names[file_hash] = file_name
                map_file_texts[file_hash] = file_text

        current_map_files = {'texts': map_file_texts, 'names': map_file_names}

        return current_map_files

    def HTML_tags_remove(self, previous_map_files, tags):

        # --- initializing ---------------------------------------------------
        self.current_process = 'HTML_tags_remove'

        map_file_names = dict()  # map_file_names[file_hash] = file_name
        map_file_texts = dict()  # map_file_texts[file_hash] = file_text

        # --- processing -----------------------------------------------------
        for previous_file_hash, file_text in previous_map_files['texts'].items():

            size_start = len(file_text)

            # removing html tags
            for tag, forms in tags['forms'].items():
                for tag_start in forms:
                    file_text = file_text.replace(tag_start, '')
                tag_end = tag.replace('<', '</')
                file_text = file_text.replace(tag_end, '')

            # checking file
            file_name = previous_map_files['names'][previous_file_hash]
            file_hash = hashlib.md5(file_text.encode()).hexdigest()
            register = self.check_file(map_file_names, file_name, file_hash, file_text, 'map_file_texts')
            if register:
                map_file_names[file_hash] = file_name
                map_file_texts[file_hash] = file_text

            current_map_files = {'texts': map_file_texts, 'names': map_file_names}

        return current_map_files

    def final_clean_up(self, previous_map_files):

        # --- initializing ---------------------------------------------------
        self.current_process = 'final_clean_up'

        map_file_names = dict()  # map_file_names[file_hash] = file_name
        map_file_texts = dict()  # map_file_texts[file_hash] = file_text

        # --- processing -----------------------------------------------------
        for previous_file_hash, file_text in previous_map_files['texts'].items():

            # cleaning file
            for char in ('\n', ' '):
                file_text = self.remove_doubled(file_text, char)

            # checking file
            file_name = previous_map_files['names'][previous_file_hash]
            file_hash = hashlib.md5(file_text.encode()).hexdigest()
            register = self.check_file(map_file_names, file_name, file_hash, file_text, 'map_file_texts')
            if register:
                map_file_names[file_hash] = file_name
                map_file_texts[file_hash] = file_text

        current_map_files = {'texts': map_file_texts, 'names': map_file_names}

        return current_map_files

    def update_history(self, seq, s_text, s_hash_org, s_hash_new):
        row = pd.Series({'sentence_id': s_hash_org,
                         'seq': seq,
                         'process': self.current_process,
                         'sentence': s_text}, name=s_hash_new)
        self.history = self.history.append(row)


class PreprocessingChecks:

    def check_sentence_length(self, sent_current, sent_hash, msg_level=None, msg_stage=None, sent_before=None, min_len=3, other_data=None):
        # requires two variables:
        # self.current_process
        # self.messages

        res = 1
        #  1 means not too short
        #  0 means too short
        # -1 means zero length
        sent_len = len(sent_current)

        if sent_len < min_len:
            msg = {'sentence_hash': sent_hash,
                   'process': self.current_process,
                   'stage': msg_stage,
                   'level': msg_level,
                   'func': 'check_sentence_length',
                   'data': dict()}

            if sent_before is not None:
                msg['data']['before'] = f'|{sent_before}|'

            # zero length
            if sent_len == 0:
                res = -1
                msg['message'] = 'zero length'
                if msg_level is None:
                    msg['level'] = 'error'

            # too short
            else:
                res = 0
                msg['message'] = f'shorter than {min_len}'
                msg['data']['current'] = f'|{sent_current}|'
                msg['data']['len'] = sent_len
                if msg_level is None:
                    msg['level'] = 'warning'

            if other_data is not None:
                for k, v in other_data.items():
                    msg['data'][k] = f'|{v}|'

            # appending
            self.messages = self.messages.append(msg, ignore_index=True)

        return res

    def check_duplicity(self, data, name, sent_hash, sent):
        stage = 'checking-duplicity'
        original = None

        # filtering for duplicate sent_hashes
        fil = copy.deepcopy(data.query(f'{name}_hash == "{sent_hash}"'))

        # has a unique sent_hash
        if len(fil) == 0:

            # TODO LOGGING: debug
            print(f'{self.current_process} {sent_hash} [{stage}] UNQ: {sent}')

        # has a duplicate sent_hash
        else:

            # TODO LOGGING: debug
            print(f'{self.current_process} {sent_hash} [{stage}] DUP: {sent}')

            # getting parent sent_hash
            original = fil.index.values[0]

            # TODO LOGGING: debug
            print(f'{self.current_process} {sent_hash} [{stage}] parent: {original}')

            # updating dropped data report --------------------------------------
            self.dropped_data_report = self.dropped_data_report.append({
                'target': f'data_unq_sentences_{name}',
                'id': sent_hash,
                'process': self.current_process,
                'stage': stage,
                'reason': 'duplicate',
                'data': {'parent': original, f'sentence_{name}': f'|{sent}|'},
            }, ignore_index=True)

        return original

    def check_file(self, data, f_name, f_hash, f_text, target, min_len=100):

        # --- checking file length -------------------------------------------
        stage = 'checking-file-length'
        file_length = self.check_sentence_length(
            f_text,
            f_hash,
            msg_stage=stage,
            msg_level='error',
            min_len=min_len,
            other_data={'file_name': f_name}
        )

        # if file length is not ok
        if file_length != 1:

            # TODO LOGGING: error
            error_text = f'ABORTED processing file {f_name}: bad length'
            print(f'{self.current_process} {f_hash} [{stage}] {error_text}')

            # updating dropped data report
            self.dropped_data_report = self.dropped_data_report.append({
                'target': target,
                'id': f_name,
                'process': self.current_process,
                'stage': stage,
                'reason': 'bad-length',
                'data': {'file-hash': f_hash, 'file-text': f'|{f_text}|'},
            }, ignore_index=True)

            return False

        # --- checking file duplicity ----------------------------------------
        stage = 'checking-file-duplicity'

        # has a unique sent_hash
        if f_hash not in data.keys():

            # TODO LOGGING: debug
            print(f'{self.current_process} {f_hash} [{stage}] UNQ {f_name}')
            return True

        # file is a duplicate
        else:

            # TODO LOGGING: debug
            print(f'{self.current_process} {f_hash} [{stage}] DUP {f_name}')

            # getting parent file_name
            original = data[f_hash]

            # updating dropped data report
            self.dropped_data_report = self.dropped_data_report.append({
                'target': target,
                'id': f_name,
                'process': self.current_process,
                'stage': stage,
                'reason': 'duplicate',
                'data': {'parent': original}
            }, ignore_index=True)

            return False


class FileToSentencePreprocessor(Preprocessing, PreprocessingChecks, SubProcessLogger):

    def __init__(self, to_strip=' /\\!.:#?-();,*+|$[]', strip_after='lowerring', auto_strip=True):

        # settings for stripSentencesThenAnalyze
        self.to_strip = to_strip
        self.strip_after = strip_after
        self.auto_strip = auto_strip

        self.check_settings()
        self.initialize_dataframes()

    def preprocess(self, path, template):

        raw = self.loadData(path)

        # --- processing at file level ---------------------------------------

        map_files = self.checkFiles(raw)
        process_HTML = False

        # templates
        if template == 'indeed_samples':
            map_files = self.indeedSamplesTemplateExtract(map_files)
            process_HTML = True

        # html
        if process_HTML:
            map_files = self.HTML_replacements(map_files)
            tags = self.HTML_tags_get_list(map_files)
            map_files = self.HTML_tags_replace_empty(map_files, tags)
            map_files = self.HTML_tags_remove(map_files, tags)

        map_files = self.final_clean_up(map_files)

        # --- processing at sentence level -----------------------------------

        sentence_data = self.splitSentencesThenAnalyze(map_files)
        # processed = self.lowerSentencesThenAnalyze(processed)
        # processed = self.stripSentencesThenAnalyze(processed, to_strip, strip_after, auto_strip)

        # --- exit summary ---------------------------------------------------
        # TODO LOGGING: info
        self.history.sort_values(['sentence_id', 'seq'], inplace=True)
        print('-' * 79)
        print('history')
        print(self.history)
        print('-' * 79)
        print('messages')
        print(self.messages)
        print('-' * 79)
        print('dropped data report')
        print(self.dropped_data_report)
        # TODO LOGGING: info

    # --- stages in init method: last to first -------------------------------

    def initialize_dataframes(self):

        # this is the main dataframe that outputs all unique processed sentences and some basic
        # hand engineered data
        self.sentences = pd.DataFrame(columns=[
            'sentence',  # ----- preprocessed sentence text
            'n_words',  # ------ number of words
            'n_commas',  # ----- number of commas
            'word_first',  # --- first word of the sentence
            'word_last',  # ---- last word of the sentence
            'word_big',  # ----- biggest word of the sentence
            'word_freq',  # ---- most frequent word of the sentece (None if all are repeated once)
            'flag_start',  # --- non-alphanumeric character at the start of the sentence
            'flag_end',  # ----- non-alphanumeric character at the end of the sentence
        ])

        # TODO: change this to only contain new formatting patterns (new character to be ignored, etc)
        self.messages = pd.DataFrame(columns=[
            'sentence_hash',  # --- md5 hash of original sentences (no lowering, stripping, etc)
            'process',  # --------- e.g. 'stripStentencesThenAnalyze'
            'stage',  # ----------- e.g. 'before stripping'
            'level',  # ----------- e.g. 'warning', 'error', etc
            'func',  # ------------ e.g. name of the checking fucntion
            'message',  # --------- e.g. 'zero length'
            'data'])  # ----------- e.g. {'before': '|raw sentence|', 'after': '|processed sentence|'}

        # notes data about dropped files and sentences. reasons why a file or sentence might be dropped:
        # - detected as a duplicate
        # - length check failed
        self.dropped_data_report = pd.DataFrame(columns=[
            'target',  # name of the target table that it was supposed to be registered in
            'id',  # the current id or would be id of the record
            'process',
            'stage',
            'reason',
            'data'])

        # notes step by step processing of each sentence
        self.history = pd.DataFrame(columns=[  # index: new hash of sentence after processing
            'sentence_id',
            'seq',  # process sequence number
            'process',
            'sentence'])

        # notes all unique sentence hashes
        self.hashes = pd.DataFrame(columns=[
            # index --------- sentence hash from splitSentencesThenAnalyze (serves as sentence_id in preprocessing only)
            'lowered',  # --- sentence hash from lowerSentencesThenAnalyze
            'stripped'])  # - sentence hash from stripSentencesThenAnalyze (serves as sentence_id in models)

    def check_settings(self):

        # --- stripSentencesThenAnalyze --------------------------------------

        if not isinstance(self.to_strip, str):

            # TODO LOGGING: error
            raise TypeError('setting for splitSentencesThenAnalyze <to_strip> should be a string')

        if not isinstance(self.strip_after, str):

            # TODO LOGGING: error
            raise TypeError('setting for splitSentencesThenAnalyze <strip_after> should be a string')

        if self.strip_after not in ('lowerring', 'splitting'):

            # TODO LOGGING: error
            raise ValueError('param <strip_after> is not recognized')

    # --- stages in preprocess: last to first --------------------------------

    def loadData(self, path):

        # --- initializations ------------------------------------------------
        self.current_process = 'loadData'
        raw = dict()

        # --- processing -----------------------------------------------------
        for file_name in os.listdir(path):
            file_path = path + f'/{file_name}'
            if os.path.isfile(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    file_text = f.read()
                    file_hash = hashlib.md5(file_text.encode()).hexdigest()
                    file_name = file_path.replace(PATH_DATA, '')
                    raw[file_name] = file_text
                    self.show_start_and_end('loaded', file_hash, file_name, file_text, 90)

        # -- sorting file names
        raw = pd.Series(raw, name='raw')
        raw.sort_index(inplace=True)
        raw = raw.to_dict()

        return raw

    def indeedSamplesTemplateExtract(self, previous_map_files):

        # --- initializing ---------------------------------------------------
        self.current_process = 'indeedSamplesTemplateExtract'

        map_file_names = dict()  # map_file_names[file_hash] = file_name
        map_file_texts = dict()  # map_file_texts[file_hash] = file_text

        # --- processing -----------------------------------------------------
        for previous_file_hash, file_text in previous_map_files['texts'].items():

            # sample files format: prefix and suffix
            file_text = self.remove_prefix(file_text, "['")
            file_text = self.remove_suffix(file_text, "']")

            # indeed posts format: prefix and suffix
            file_text = self.remove_prefix(file_text, '<div id="jobDescriptionText" class="jobsearch-jobDescriptionText">')
            file_text = self.remove_suffix(file_text, '</div>')

            # indeed posts format: some posts are still enclosed in div containers
            while file_text.find('<div>') == 0:
                if file_text[-6:] == '</div>':
                    file_text = self.remove_prefix(file_text, '<div>', ignore=True)
                    file_text = self.remove_suffix(file_text, '</div>', ignore=True)
                else:
                    break

            # --- checking file length ---------------------------------------
            file_name = previous_map_files['names'][previous_file_hash]
            file_hash = hashlib.md5(file_text.encode()).hexdigest()
            self.show_start_and_end('text-extracted', file_hash, file_name, file_text, 90)
            register = self.check_file(map_file_names, file_name, file_hash, file_text, 'map_file_texts')
            if register:
                map_file_names[file_hash] = file_name
                map_file_texts[file_hash] = file_text

        current_map_files = {'texts': map_file_texts, 'names': map_file_names}

        return current_map_files

    def checkFiles(self, raw):

        # --- initializing ---------------------------------------------------
        self.current_process = 'checkFiles'

        map_file_names = dict()  # map_file_names[file_hash] = file_name
        map_file_texts = dict()  # map_file_texts[file_hash] = file_text

        # --- processing -----------------------------------------------------
        for file_name, file_text in raw.items():
            file_hash = hashlib.md5(file_text.encode()).hexdigest()
            register = self.check_file(map_file_names, file_name, file_hash, file_text, 'map_file_texts')
            if register:
                map_file_names[file_hash] = file_name
                map_file_texts[file_hash] = file_text

        current_map_files = {'texts': map_file_texts, 'names': map_file_names}

        return current_map_files

    def splitSentencesThenAnalyze(self, map_files):

        # --- initializing ---------------------------------------------------
        self.current_process = 'splitSentencesThenAnalyze'

        # unique post to sentence connections
        map_lines = pd.DataFrame(columns=['file_hash', 'id_s', 'sentence_hash'])  # index: arbitrary

        # sentences
        data_unq_sentences = dict()  # key: sentence_hash, value sentence_text

        # --- processing each file -------------------------------------------
        for file_hash, file_text in map_files['texts'].items():

            # splitting sentences
            sentences = file_text.splitlines()

            # --- processing each sentence -----------------------------------

            reduce_i = 0
            for i, sentence in enumerate(sentences):

                # --- sentence length check: before registering --------------
                stage = 'before-registering'
                sentence_hash = hashlib.md5(sentence.encode()).hexdigest()

                # creating other data for debugging
                file_name = map_files['names'][file_hash]
                other_data = {'file_name': file_name, 'file_hash': file_hash}
                if i > 0:
                    other_data['i-1'] = sentences[i - 1]  # previous sentence
                if i + 1 < len(sentences):
                    other_data['i+1'] = sentences[i + 1]  # next sentence

                sentence_length_before_registering = self.check_sentence_length(
                    sentence,
                    sentence_hash,
                    msg_stage=stage,
                    msg_level='error',
                    other_data=other_data,
                )

                # if length is not ok
                if sentence_length_before_registering != 1:
                    reduce_i = reduce_i + 1

                    # TODO LOGGING: error
                    error_text = 'ABORTED registering sentence: bad length'
                    print(f'{self.current_process} {sentence_hash} [{stage}] {error_text}')

                    # updating dropped data report
                    dropped_data_report_data = {'sentence': f'|{sentence}|'}
                    dropped_data_report_data.update(other_data)
                    self.dropped_data_report = self.dropped_data_report.append({
                        'target': 'map_lines',
                        'id': sentence_hash,
                        'process': self.current_process,
                        'stage': stage,
                        'reason': 'bad-length',
                        'data': dropped_data_report_data,
                    }, ignore_index=True)

                    continue

                # --- registering line number --------------------------------
                map_lines = map_lines.append({
                    'file_hash': file_hash,
                    'id_s': i - reduce_i,
                    'sentence_hash': sentence_hash
                }, ignore_index=True)

                # --- checking duplicity of sentences ------------------------
                stage = 'checking-duplicity'

                # sentence is a duplicate
                if sentence_hash in data_unq_sentences.keys():
                    print(f'{self.current_process} {sentence_hash} [{stage}] DUP: {sentence}')  # TODO LOGGING: debug

                    # updating dropped data report
                    self.dropped_data_report = self.dropped_data_report.append({
                        'target': 'data_unq_sentences',
                        'id': sentence_hash,
                        'process': self.current_process,
                        'stage': stage,
                        'reason': 'duplicate',
                        'data': {'sentence': f'|{sentence}|'}
                    }, ignore_index=True)

                    continue

                # --- registering unique sentence ----------------------------
                print(f'{self.current_process} {sentence_hash} [{stage}] UNQ: {sentence}')  # TODO LOGGING: debug
                data_unq_sentences[sentence_hash] = sentence
                self.update_history(1, sentence, sentence_hash, sentence_hash)

        sentence_data = {'data_unq_sentences': data_unq_sentences, 'map_lines': map_lines}

        return sentence_data

    def lowerSentencesThenAnalyze(self, previous_res):

        # --- initializing ---------------------------------------------------

        self.current_process = 'lowerSentencesThenAnalyze'

        # will contain lowered sentences and their subsequent lowered hashes
        data_unq_sentences_lowered = pd.DataFrame(columns=['lowered_hash', 'parent', 'sentence_lowered'])  # index: unlowered sentence hash

        # --- processing -----------------------------------------------------
        previous_data = previous_res['data_unq_sentences']['sentence']

        for sentence_hash, sentence in previous_data.items():

            # --- sentence length check: before lowering ---------------------
            stage = 'before-lowering'
            sentence_length_before_lowering = self.check_sentence_length(
                sentence,
                sentence_hash,
                msg_stage=stage,
                msg_level='error',
            )

            # if length is not ok
            if sentence_length_before_lowering != 1:

                # TODO LOGGING: error
                error_text = 'ABORTED processing sentence: bad length'
                print(f'{self.current_process} {sentence_hash} [{stage}] {error_text}')

                # updating dropped data report
                self.dropped_data_report = self.dropped_data_report.append({
                    'target': 'data_unq_sentences_lowered',
                    'id': sentence_hash,
                    'process': self.current_process,
                    'stage': stage,
                    'reason': 'bad-length',
                    'data': {'sentence': f'|{sentence}|'},
                }, ignore_index=True)

                continue

            # --- lowering ---------------------------------------------------
            sentence_lowered = sentence.lower()
            lowered_hash = hashlib.md5(sentence_lowered.encode()).hexdigest()
            original = self.check_duplicity(data_unq_sentences_lowered, 'lowered', lowered_hash, sentence_lowered)

            # appending
            row = pd.Series({'lowered_hash': lowered_hash,
                             'parent': original,
                             'sentence_lowered': sentence_lowered},
                            name=sentence_hash)
            data_unq_sentences_lowered = data_unq_sentences_lowered.append(row)

        previous_res['data_unq_sentences_lowered'] = data_unq_sentences_lowered

        return previous_res

    def stripSentencesThenAnalyze(self, previous_res):

        # --- initializing ---------------------------------------------------

        self.current_process = 'stripSentencesThenAnalyze'

        # will contain unique stripped sentences and their subsequent stripped hashes
        data_unq_sentences_stripped = pd.DataFrame(columns=['stripped_hash', 'parent', 'flag_start', 'flag_end', 'sentence_stripped'])  # index: unstripped sentence hash

        # previous data auto selection deciding what version of the sentences to process
        if self.strip_after == 'lowerring':
            previous_data = previous_res['data_unq_sentences_lowered']
            previous_data = previous_data[pd.isnull(previous_data['parent'])]  # dropping children (duplicates)
            previous_data = previous_data['sentence_lowered']

        else:  # splitting
            previous_data = previous_res['data_unq_sentences']['sentence']

        # --- processing -----------------------------------------------------

        for sentence_hash, sentence in previous_data.items():

            # --- sentence length check: before stripping --------------------
            stage = 'before-flagging'
            sentence_length_before_flagging = self.check_sentence_length(
                sentence,
                sentence_hash,
                msg_stage=stage,
                msg_level='error',
            )

            # length is not ok
            if sentence_length_before_flagging != 1:

                # TODO LOGGING: error
                error_text = 'ABORTED processing sentence: bad length'
                print(f'{self.current_process} {sentence_hash} [{stage}] {error_text}')

                # updating dropped data report
                self.dropped_data_report = self.dropped_data_report.append({
                    'target': 'data_unq_sentences_stripped',
                    'id': sentence_hash,
                    'process': self.current_process,
                    'stage': stage,
                    'reason': 'bad-length',
                    'data': {'sentence': f'|{sentence}|'},
                }, ignore_index=True)

                continue

            # --- flagging -----------------------------------------------

            # initializing flags
            flag_start = None
            flag_end = None

            # flagging start
            if not sentence[0].isalnum():
                flag_start = sentence[0]

            # flagging end
            if not sentence[-1].isalnum():
                flag_end = sentence[-1]

            # catching unrecognized non-alphanumeric characters
            stage = 'after-flagging'
            for flag in (flag_start, flag_end):
                if flag is not None:
                    if flag not in self.to_strip:

                        # notifying about new alphanumeric character so it can be added later
                        msg = {'sentence_hash': sentence_hash,
                               'process': self.current_process,
                               'stage': stage,
                               'level': 'info_new_format',
                               'message': 'new alphanumeric character',
                               'data': {'character': f'|{flag}|'}}
                        self.messages = self.messages.append(msg, ignore_index=True)

                        # warning about auto stripping of new alphanumeric characters
                        if self.auto_strip:
                            self.to_strip = self.to_strip + flag
                            msg = {'sentence_hash': sentence_hash,
                                   'process': self.current_process,
                                   'stage': stage,
                                   'level': 'warning',
                                   'message': 'automatic stripping is enabled',
                                   'data': {'character': f'|{flag}|'}}
                            self.messages = self.messages.append(msg, ignore_index=True)

            # --- stripping ----------------------------------------------
            sentence_stripped = sentence.strip(self.to_strip)
            stripped_hash = hashlib.md5(sentence_stripped.encode()).hexdigest()
            stage = 'after-stripping'

            # --- sentence length check: after stripping -----------------
            sentence_length_after_stripping = self.check_sentence_length(
                sentence_stripped,  # sentence text after stripping (sent_current)
                sentence_hash,
                sent_before=sentence,  # sentence text before stripping
                msg_stage=stage,
                msg_level='error',
            )

            if sentence_length_after_stripping != 1:

                # TODO LOGGING: error
                error_text = 'ABORTED appending sentence: bad length'
                print(f'{self.current_process} {sentence_hash} [{stage}] {error_text}')

                # updating dropped data report
                self.dropped_data_report = self.dropped_data_report.append({
                    'target': 'data_unq_sentences_stripped',
                    'id': sentence_hash,
                    'process': self.current_process,
                    'stage': stage,
                    'reason': 'bad-length',
                    'data': {'sentence_stripped': f'|{sentence_stripped}|'},
                }, ignore_index=True)

                continue

            # --- checking duplicity and appending -------------------
            original = self.check_duplicity(data_unq_sentences_stripped, 'stripped', stripped_hash, sentence_stripped)

            # appending
            row = pd.Series({'stripped_hash': stripped_hash,
                             'parent': original,
                             'flag_start': flag_start,
                             'flag_end': flag_end,
                             'sentence_stripped': sentence_stripped},
                            name=sentence_hash)
            data_unq_sentences_stripped = data_unq_sentences_stripped.append(row)

        # --- exit stats -----------------------------------------------------

        # frequency distribution of all unique flag_start and flag_end
        fd_flag_start = data_unq_sentences_stripped['flag_start'].value_counts()
        fd_flag_end = data_unq_sentences_stripped['flag_end'].value_counts()
        fd_flags = pd.concat((fd_flag_start, fd_flag_end), axis=1)
        fd_flags.fillna(0, inplace=True)
        fd_flags['flag_start'] = fd_flags['flag_start'].astype(int)
        fd_flags['flag_end'] = fd_flags['flag_end'].astype(int)
        fd_flags['total'] = fd_flags.sum(axis=1)
        fd_flags.sort_values(['total'], ascending=False, inplace=True)

        # TODO LOGGING: info
        print('-' * 79)
        print('frequency distribution of flags')
        print(fd_flags)
        # TODO LOGGING: info

        previous_res['data_unq_sentences_stripped'] = data_unq_sentences_stripped

        return previous_res
