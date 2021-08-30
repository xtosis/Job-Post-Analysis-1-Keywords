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


class PreprocessingChecks:

    def check_sentence_length(self, sent_current, sent_hash, msg_level=None, msg_stage=None, sent_before=None, min_len=3):
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

            # appending
            self.messages = self.messages.append(msg, ignore_index=True)

        return res


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
        # processed = self.lowerSentencesThenAnalyze(processed)
        # processed = self.stripSentencesThenAnalyze(processed, to_strip, strip_after, auto_strip)

        data_sentences = processed['data_sentences']
        # type    | pandas.DataFrame
        # --------+-----------------------------------------------------------
        # index   | md5 hash of the sentence without lowering
        # columns | words: # of words in the sentence
        #         | commas: # of commas in the sentence
        #         | sentence: sentence text without lowering

        # data_sentences_lowered = processed['data_sentences_lowered']
        # type    | pandas.DataFrame
        # --------+-----------------------------------------------------------
        # index   | md5 hash of the sentence without lowering
        # columns | lowered_hash: md5 hash of the sentence with lowering
        #         | role: None, 'paret' or 'child' (children have same lowered hash of parent)
        #         | sentence_lowered: the lowered sentence

        # data_sentences_stripped = processed['data_sentences_stripped']
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

    def loadData(self, path):
        raw = dict()
        for name in os.listdir(path):
            file_path = path + f'/{name}'
            if os.path.isfile(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    name = file_path.replace(PATH_DATA, '')
                    raw[name] = f.read()
                    self.show_start_and_end('loaded', name, raw[name], 90)

        # -- sorting file names
        raw = pd.Series(raw, name='raw')
        raw.sort_index(inplace=True)
        raw = raw.to_dict()

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
            file_hash = hashlib.md5(text.lower().encode()).hexdigest()
            file_length_before_processing = self.check_sentence_length(
                text,
                file_hash,
                msg_stage=stage,
                msg_level='error',
                min_len=100,
            )

            # if file length is not ok
            if file_length_before_processing != 1:

                # TODO LOGGING: error
                error_text = 'ABORTED processing file: bad length'
                print(f'{self.current_process} {name} [{stage}] {error_text}')
                continue

            # --- checking file duplicity ------------------------------------
            stage = 'file-duplicity'

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
                continue

            # TODO LOGGING: debug
            print(f'{self.current_process} {name} [{stage}] UNQ')

            # registering unique file
            map_text_hashes[file_hash] = name
            sentences = text.splitlines()

            # --- processing each sentence -----------------------------------

            for i, sentence in enumerate(sentences):

                # --- registering line number --------------------------------
                sentence_hash = hashlib.md5(sentence.encode()).hexdigest()
                map_sentence_lines = map_sentence_lines.append({
                    'file': name,
                    'id_s': i,
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
        print('-' * 79)
        print('message ')
        print(self.messages)
        # TODO LOGGING end: info

        res = {'data_sentences': unq_sentences,
               'map_text_hashes': map_text_hashes,
               'map_sentence_lines': map_sentence_lines}

        return res

    def lowerSentencesThenAnalyze(self, previous_res):

        # --- initializing ---------------------------------------------------

        self.current_process = 'lowerSentencesThenAnalyze'

        # will contain lowered sentences and their subsequent lowered hashes
        data_sentences_lowered = pd.DataFrame(columns=['lowered_hash', 'role', 'sentence_lowered'])  # index: unlowered sentence hash

        # will contain sentences with duplicate lowered hashes
        dup_lowered_hash = pd.DataFrame(columns=['lowered_hash', 'role', 'sentence'])  # index: unlowered sentence hash

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
                continue

            # --- lowering ---------------------------------------------------
            sentence_lowered = sentence.lower()
            lowered_hash = hashlib.md5(sentence_lowered.encode()).hexdigest()

            # --- checking duplicity -----------------------------------------
            stage = 'checking-duplicity'

            # initializing role
            role = None  # None, 'parent' or 'child'

            # filtering for duplicate lowered hashes
            fil = copy.deepcopy(data_sentences_lowered.query(f'lowered_hash == "{lowered_hash}"'))

            # unique lowered_hash
            if len(fil) == 0:

                # TODO LOGGING: debug
                print(f'{self.current_process} {sentence_hash} [{stage}] UNQ: {sentence_lowered}')

            # has a lowered hash duplicate
            else:

                # TODO LOGGING: debug
                print(f'{self.current_process} {sentence_hash} [{stage}] DUP: {sentence_lowered}')

                # registering the child
                role = 'child'
                row = pd.Series({'lowered_hash': lowered_hash, 'role': role, 'sentence': sentence},
                                name=sentence_hash)
                dup_lowered_hash = dup_lowered_hash.append(row)

                # getting unlowered sentence hash of the first occurance...
                # which will be treated as 'parent' of subsequent duplicates
                original = fil.index.values[0]

                # TODO LOGGING: debug
                print(f'{self.current_process} {sentence_hash} [{stage}] parent: {original}')

                # registering parent if not already registered
                if fil.loc[original, 'role'] != 'parent':
                    data_sentences_lowered.loc[original, 'role'] = 'parent'
                    row = pd.Series({'lowered_hash': lowered_hash, 'role': 'parent',
                                     'sentence': previous_data[original]},
                                    name=original)
                    dup_lowered_hash = dup_lowered_hash.append(row)

            # appending
            row = pd.Series({'lowered_hash': lowered_hash, 'role': role, 'sentence_lowered': sentence_lowered},
                            name=sentence_hash)
            data_sentences_lowered = data_sentences_lowered.append(row)

        # TODO LOGGING: info
        dup_lowered_hash.sort_values(['lowered_hash', 'role'], ascending=[False, False], inplace=True)
        print('-' * 79)
        print('dup_lowered_hash')
        print(dup_lowered_hash)
        print('-' * 79)
        print('messages')
        print(self.messages)
        # TODO LOGGING: info

        previous_res['data_sentences_lowered'] = data_sentences_lowered

        return previous_res

    def stripSentencesThenAnalyze(self, previous_res, to_strip, strip_after, auto_strip):

        # --- initializing ---------------------------------------------------

        self.current_process = 'stripSentencesThenAnalyze'

        # will contain unique stripped sentences and their subsequent stripped hashes
        data_sentences_stripped = pd.DataFrame(columns=['stripped_hash', 'role', 'flag_start', 'flag_end', 'sentence_stripped'])  # index: unstripped sentence hash

        # will contain sentences with duplicate stripped hashes
        dup_stripped_hash = pd.DataFrame(columns=['stripped_hash', 'role', 'sentence'])  # index: unstripped sentence hash

        # --- making sure inputs to the function are valid -------------------

        # early exit
        if to_strip is None or strip_after is None:

            # TODO LOGGING: info
            print(f'{self.current_process} EARLY EXIT')

            return previous_res

        # previous data auto selection deciding what version of the sentences to process
        if strip_after == 'lowerring':
            previous_data = previous_res['data_sentences_lowered']
            previous_data = previous_data[previous_data['role'] != 'child']  # dropping children (duplicates)
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

            # length is ok
            else:

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

                else:
                    # --- checking duplicity and appending -------------------
                    stage = 'checking-duplicity'

                    # initializing role
                    role = None  # None, 'parent' or 'child'

                    # filtering for duplicate stripped hashes
                    fil = copy.deepcopy(data_sentences_stripped.query(f'stripped_hash == "{stripped_hash}"'))

                    # unique stripped_hash
                    if len(fil) == 0:

                        # TODO LOGGING: debug
                        print(f'{self.current_process} {sentence_hash} [{stage}] UNQ: {sentence_stripped}')

                    # has a stripped_hash duplicate
                    else:

                        # TODO LOGGING: debug
                        print(f'{self.current_process} {sentence_hash} [{stage}] DUP: {sentence_stripped}')

                        # registering the child
                        role = 'child'
                        row = pd.Series({'stripped_hash': stripped_hash, 'role': role, 'sentence': sentence},
                                        name=sentence_hash)
                        dup_stripped_hash = dup_stripped_hash.append(row)

                        # getting unstripped sentence hash of the first occurance...
                        # which will be treated as 'parent' of subsequent duplicates
                        original = fil.index.values[0]

                        # TODO LOGGING: debug
                        print(f'{self.current_process} {sentence_hash} [{stage}] parent: {original}')

                        # registering parent if not already registered
                        if fil.loc[original, 'role'] != 'parent':
                            data_sentences_stripped.loc[original, 'role'] = 'parent'
                            row = pd.Series({'stripped_hash': stripped_hash, 'role': 'parent',
                                             'sentence': previous_data[original]},
                                            name=original)
                            dup_stripped_hash = dup_stripped_hash.append(row)

                    # appending
                    row = pd.Series({'stripped_hash': stripped_hash,
                                     'role': role,
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
        dup_stripped_hash.sort_values(['stripped_hash', 'role'], ascending=[False, False], inplace=True)
        print('-' * 79)
        print('dup_stripped_hash')
        print(dup_stripped_hash)
        print('-' * 79)
        print('frequency distribution of flags')
        print(fd_flags)
        print('-' * 79)
        print('messages')
        print(self.messages)
        # TODO LOGGING: info

        previous_res['data_sentences_stripped'] = data_sentences_stripped

        return previous_res

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
