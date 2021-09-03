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
                'target': f'data_sentences_{name}',
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


class KeywordPreprocessing(Preprocessing, PreprocessingChecks, SubProcessLogger):

    def preprocessing(self, path, template, to_strip=' /\\!.:#?-();,*+|$[]', strip_after='lowerring', auto_strip=True):

        # initializations
        self.messages = pd.DataFrame(columns=[
            'sentence_hash',  # --- md5 hash of original sentences (no lowering, stripping, etc)
            'process',  # --------- e.g. 'stripStentencesThenAnalyze'
            'stage',  # ----------- e.g. 'before stripping'
            'level',  # ----------- e.g. 'warning', 'error', etc
            'func',  # ------------ e.g. name of the checking fucntion
            'message',  # --------- e.g. 'zero length'
            'data'])  # ----------- e.g. {'before': '|raw sentence|', 'after': '|processed sentence|'}

        self.dropped_data_report = pd.DataFrame(columns=[
            'target',  # name of the target table that it was supposed to be registered in
            'id',  # the current id or would be id of the record
            'process',
            'stage',
            'reason',
            'data'])

        raw = self.loadData(path)
        processed = self.checkFiles(raw)
        process_HTML = False

        # templates
        if template == 'indeed_samples':
            processed = self.indeedSamplesTemplateExtract(processed)
            process_HTML = True

        # html
        if process_HTML:
            processed = self.standardPreprocessing_HTML_replacements(processed)
            processed = self.standardPreprocessing_HTML_tags(processed)

        processed = self.final_clean_up(processed)
        processed = self.splitSentencesThenAnalyze(processed)
        processed = self.lowerSentencesThenAnalyze(processed)
        processed = self.stripSentencesThenAnalyze(processed, to_strip, strip_after, auto_strip)

        data_sentences = processed['data_sentences']
        # type    | pandas.DataFrame
        # --------+-----------------------------------------------------------
        # index   | md5 hash of the sentence without lowering
        # columns | words: # of words in the sentence
        #         | commas: # of commas in the sentence
        #         | sentence: sentence text without lowering

        data_sentences_lowered = processed['data_sentences_lowered']
        # type    | pandas.DataFrame
        # --------+-----------------------------------------------------------
        # index   | md5 hash of the sentence without lowering
        # columns | lowered_hash: md5 hash of the sentence with lowering
        #         | role: None, 'paret' or 'child' (children have same lowered hash of parent)
        #         | sentence_lowered: the lowered sentence

        data_sentences_stripped = processed['data_sentences_stripped']
        # type    | pandas.DataFrame
        # --------+-----------------------------------------------------------
        # index   | md5 hash of the sentence without lowering or stripping
        # columns | stripped_hash: md5 hash of the sentence with stripping
        #         | role: None, 'paret' or 'child' (children have same lowered hash of parent)
        #         | flag_start: None or non-alphanumeric character at the start of the sentence before stripping
        #         | flag_end: None or non-alphanumeric character at the end of the sentence before stripping
        #         | sentence_stripped: the stripped sentence

        map_text_hashes = processed['map_text_hashes']
        # type   | dict()
        # -------+------------------------------------------------------------
        # keys   | md5 hash of the full text of every unique post without lowering
        # values | file names

        map_sentence_lines = processed['map_sentence_lines']
        # type    | pandas.DataFrame
        # --------+-----------------------------------------------------------
        # index   | arbitrary
        # columns | files: file names
        #         | id_s: line # of the sentence in the cleaned text of the post
        #         | sentence_hash: md5 hash of the sentence without lowering

        # exit summary
        # TODO LOGGING: info
        print('-' * 79)
        print('messages')
        print(self.messages)
        print('-' * 79)
        print('dropped data report')
        print(self.dropped_data_report)
        # TODO LOGGING: info

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

    def checkFiles(self, text_dict):

        # --- initializing ---------------------------------------------------
        self.current_process = 'checkFiles'

        map_file_names = dict()  # map_file_names[file_hash] = file_name
        map_file_texts = dict()  # map_file_texts[file_name] = file_text

        # --- processing -----------------------------------------------------
        for file_name, file_text in text_dict.items():
            file_hash = hashlib.md5(file_text.encode()).hexdigest()
            register = self.check_file(map_file_names, file_name, file_hash, file_text, 'map_file_texts')
            if register:
                map_file_names[file_hash] = file_name
                map_file_texts[file_name] = file_text

        return map_file_texts

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

        # --- initializing ---------------------------------------------------

        self.current_process = 'splitSentencesThenAnalyze'

        # unique post to sentence connections
        map_sentence_lines = pd.DataFrame(columns=['file', 'id_s', 'sentence_hash'])  # index: arbitrary

        # sentences
        unq_sentences = pd.DataFrame(columns=['words', 'commas', 'sentence'])  # index: sentence_hash
        dup_sentences = pd.DataFrame(columns=['count', 'sentence'])  # index: sentence_hash

        # text hashes for files
        map_text_hashes = dict()  # index: file_hash

        # duplicate files
        dup_text_hash = dict()  # index: file name

        # --- processing each file -------------------------------------------

        for name, text in text_dict.items():

            # --- checking file length ---------------------------------------
            stage = 'before-processing'
            file_length_before_processing = self.check_sentence_length(
                text,
                None,
                msg_stage=stage,
                msg_level='error',
                min_len=100,
                other_data={'file': name}
            )

            # if file length is not ok
            if file_length_before_processing != 1:

                # TODO LOGGING: error
                error_text = 'ABORTED processing file: bad length'
                print(f'{self.current_process} {name} [{stage}] {error_text}')

                # updating dropped data report
                self.dropped_data_report = self.dropped_data_report.append({
                    'target': 'map_text_hashes',
                    'id': name,
                    'process': self.current_process,
                    'stage': stage,
                    'reason': 'bad-length',
                    'data': {'file-text': f'|{text}|'},
                }, ignore_index=True)

                continue

            # --- checking file duplicity ------------------------------------
            stage = 'file-duplicity'
            file_hash = hashlib.md5(text.lower().encode()).hexdigest()

            # file is a duplicate
            if file_hash in map_text_hashes.keys():
                org = map_text_hashes[file_hash]

                # TODO LOGGING: debug
                print(f'{self.current_process} {name} [{stage}] DUP: {org}')

                # first occurance: add original file name to keys
                if org not in dup_text_hash.keys():
                    dup_text_hash[org] = []

                # append duplicate filename
                dup_text_hash[org].append(name)

                # updating dropped data report
                self.dropped_data_report = self.dropped_data_report.append({
                    'target': 'map_text_hashes',
                    'id': name,
                    'process': self.current_process,
                    'stage': stage,
                    'reason': 'duplicate',
                    'data': {'parent': org, 'file_hash': file_hash}
                }, ignore_index=True)

                continue

            # TODO LOGGING: debug
            print(f'{self.current_process} {name} [{stage}] UNQ')

            # registering unique file
            map_text_hashes[file_hash] = name
            sentences = text.splitlines()

            # --- processing each sentence -----------------------------------

            reduce_i = 0
            for i, sentence in enumerate(sentences):

                # --- sentence length check: before registering --------------
                stage = 'before-registering'
                sentence_hash = hashlib.md5(sentence.encode()).hexdigest()

                # creating other data for debugging
                other_data = {'file': name}
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
                        'target': 'map_sentence_lines',
                        'id': sentence_hash,
                        'process': self.current_process,
                        'stage': stage,
                        'reason': 'bad-length',
                        'data': dropped_data_report_data,
                    }, ignore_index=True)

                    continue

                # --- registering line number --------------------------------
                map_sentence_lines = map_sentence_lines.append({
                    'file': name,
                    'id_s': i - reduce_i,
                    'sentence_hash': sentence_hash
                }, ignore_index=True)

                # --- checking duplicity of sentences ------------------------
                stage = 'checking-duplicity'

                # sentence is a duplicate
                if sentence_hash in unq_sentences.index.values:

                    # TODO LOGGING: debug
                    print(f'{self.current_process} {sentence_hash} [{stage}] DUP: {sentence}')

                    # first occurance: add row
                    if sentence_hash not in dup_sentences.index.values:
                        row = pd.Series({'count': 1, 'sentence': sentence}, name=sentence_hash)
                        dup_sentences = dup_sentences.append(row)

                    # subsequent occurances: increment duplicate count
                    else:
                        val = dup_sentences.loc[sentence_hash, 'count'] + 1
                        dup_sentences.loc[sentence_hash, 'count'] = val

                    # updating dropped data report
                    self.dropped_data_report = self.dropped_data_report.append({
                        'target': 'data_sentences',
                        'id': sentence_hash,
                        'process': self.current_process,
                        'stage': stage,
                        'reason': 'duplicate',
                        'data': {'sentence': f'|{sentence}|'}
                    }, ignore_index=True)

                    continue

                # --- registering unique sentence ----------------------------
                words = sentence.count(' ') + 1
                commas = sentence.count(',')
                row = pd.Series({'words': words, 'commas': commas, 'sentence': sentence}, name=sentence_hash)
                unq_sentences = unq_sentences.append(row)

                # TODO LOGGING: debug
                print(f'{self.current_process} {sentence_hash} [{stage}] UNQ: {sentence}')

        # TODO LOGGING start: info
        dup_sentences.sort_values(['count'], inplace=True)
        print('-' * 79)
        print('dup_sentences')
        print(dup_sentences)
        print('-' * 79)
        print('dup_text_hashes')
        print(pd.Series(dup_text_hash, name='dup_text_hashes: org vs dups'))
        # TODO LOGGING end: info

        res = {'data_sentences': unq_sentences,
               'map_text_hashes': map_text_hashes,
               'map_sentence_lines': map_sentence_lines}

        return res

    def lowerSentencesThenAnalyze(self, previous_res):

        # --- initializing ---------------------------------------------------

        self.current_process = 'lowerSentencesThenAnalyze'

        # will contain lowered sentences and their subsequent lowered hashes
        data_sentences_lowered = pd.DataFrame(columns=['lowered_hash', 'parent', 'sentence_lowered'])  # index: unlowered sentence hash

        # --- processing -----------------------------------------------------
        previous_data = previous_res['data_sentences']['sentence']

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
                    'target': 'data_sentences_lowered',
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
            original = self.check_duplicity(data_sentences_lowered, 'lowered', lowered_hash, sentence_lowered)

            # appending
            row = pd.Series({'lowered_hash': lowered_hash,
                             'parent': original,
                             'sentence_lowered': sentence_lowered},
                            name=sentence_hash)
            data_sentences_lowered = data_sentences_lowered.append(row)

        previous_res['data_sentences_lowered'] = data_sentences_lowered

        return previous_res

    def stripSentencesThenAnalyze(self, previous_res, to_strip, strip_after, auto_strip):

        # --- initializing ---------------------------------------------------

        self.current_process = 'stripSentencesThenAnalyze'

        # will contain unique stripped sentences and their subsequent stripped hashes
        data_sentences_stripped = pd.DataFrame(columns=['stripped_hash', 'parent', 'flag_start', 'flag_end', 'sentence_stripped'])  # index: unstripped sentence hash

        # --- making sure inputs to the function are valid -------------------

        # early exit
        if to_strip is None or strip_after is None:

            # TODO LOGGING: info
            print(f'{self.current_process} EARLY EXIT')

            return previous_res

        # previous data auto selection deciding what version of the sentences to process
        if strip_after == 'lowerring':
            previous_data = previous_res['data_sentences_lowered']
            previous_data = previous_data[pd.isnull(previous_data['parent'])]  # dropping children (duplicates)
            previous_data = previous_data['sentence_lowered']

        elif strip_after == 'splitting':
            previous_data = previous_res['data_sentences']['sentence']
        else:

            # TODO LOGGING: error
            raise ValueError('param <strip_after> is not recognized')

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
                    'target': 'data_sentences_stripped',
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
                    if flag not in to_strip:

                        # notifying about new alphanumeric character so it can be added later
                        msg = {'sentence_hash': sentence_hash,
                               'process': self.current_process,
                               'stage': stage,
                               'level': 'info_new_format',
                               'message': 'new alphanumeric character',
                               'data': {'character': f'|{flag}|'}}
                        self.messages = self.messages.append(msg, ignore_index=True)

                        # warning about auto stripping of new alphanumeric characters
                        if auto_strip:
                            to_strip = to_strip + flag
                            msg = {'sentence_hash': sentence_hash,
                                   'process': self.current_process,
                                   'stage': stage,
                                   'level': 'warning',
                                   'message': 'automatic stripping is enabled',
                                   'data': {'character': f'|{flag}|'}}
                            self.messages = self.messages.append(msg, ignore_index=True)

            # --- stripping ----------------------------------------------
            sentence_stripped = sentence.strip(to_strip)
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
                    'target': 'data_sentences_stripped',
                    'id': sentence_hash,
                    'process': self.current_process,
                    'stage': stage,
                    'reason': 'bad-length',
                    'data': {'sentence_stripped': f'|{sentence_stripped}|'},
                }, ignore_index=True)

                continue

            # --- checking duplicity and appending -------------------
            original = self.check_duplicity(data_sentences_stripped, 'stripped', stripped_hash, sentence_stripped)

            # appending
            row = pd.Series({'stripped_hash': stripped_hash,
                             'parent': original,
                             'flag_start': flag_start,
                             'flag_end': flag_end,
                             'sentence_stripped': sentence_stripped},
                            name=sentence_hash)
            data_sentences_stripped = data_sentences_stripped.append(row)

        # --- exit stats -----------------------------------------------------

        # frequency distribution of all unique flag_start and flag_end
        fd_flag_start = data_sentences_stripped['flag_start'].value_counts()
        fd_flag_end = data_sentences_stripped['flag_end'].value_counts()
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

        previous_res['data_sentences_stripped'] = data_sentences_stripped

        return previous_res

    def indeedSamplesTemplateExtract(self, text_dict):

        # --- initializing ---------------------------------------------------
        self.current_process = 'indeedSamplesTemplateExtract'

        map_file_names = dict()  # map_file_names[file_hash] = file_name
        map_file_texts = dict()  # map_file_texts[file_name] = file_text

        # --- processing -----------------------------------------------------
        for file_name, file_text in text_dict.items():

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
            file_hash = hashlib.md5(file_text.encode()).hexdigest()
            self.show_start_and_end('text-extracted', file_hash, file_name, file_text, 90)
            register = self.check_file(map_file_names, file_name, file_hash, file_text, 'map_file_texts')
            if register:
                map_file_names[file_hash] = file_name
                map_file_texts[file_name] = file_text

        return map_file_texts
