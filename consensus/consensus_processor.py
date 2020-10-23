#  Copyright 2019-2020 Thusly, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


import logging
logger = logging.getLogger(__name__)

from collections import Counter, defaultdict

# defaults
MIN_REDUNDANCY = 3
PASS_THRESHOLD = 2

# Note that the basic data structures are sets, dicts, and Counters keyed
# by char position indices. (Not arrays representing the article characters
# linearly.) The set union and intersection and Counter + operators
# are great for this use-case.

class ArticleData(object):
    def __init__(self):
        self.article_sha256 = None
        self.article_filename = None
        self.char_dict = {}

    # assume we don't have access to full article - build a map of indices to
    # chars for every annotation being processed.
    def consider(self, anno):
        anno_range = range(int(anno['start_pos']), int(anno['end_pos']))
        target_text = anno['target_text'].decode('unicode-escape')
        anno_map = dict(zip(anno_range, target_text))
        # belt & suspenders - verify any overlaps are consistent with prior text
        intersection = self.char_dict.viewkeys() & anno_map.viewkeys()
        for k in intersection:
            assert(self.char_dict[k] == anno_map[k])
        self.char_dict.update(anno_map)
        if self.article_sha256 is None:
            self.article_sha256 = anno['article_sha256']
            self.article_filename = anno['article_filename']
        else:
            assert(self.article_sha256 == anno['article_sha256'])
            assert(self.article_filename == anno['article_filename'])

    def set_article_cols(self, row):
        row['article_sha256'] = self.article_sha256
        row['article_filename'] = self.article_filename

    def get(self, char_index):
        return self.char_dict[char_index]


class ContribData(object):
    def __init__(self):
        self.flattened = set()
        self.case_number_dict = {}

    def consider(self, anno):
        # Flatten all of a user's highlights for a given topic into
        # a single set. Otherwise the user could increase the weight of
        # their highlights by overlapping them.
        anno_set = set(range(int(anno['start_pos']), int(anno['end_pos'])))
        self.flattened |= anno_set
        # case numbers from a user must be disjoint.
        # But front-end allows annotation overlaps.
        # Keep the lowest case number assigned by this contributor.
        case_number_keys = self.case_number_dict.viewkeys()
        new_keys = anno_set - case_number_keys
        overlapped_keys = case_number_keys & anno_set
        proposed = int(anno['case_number'])
        new_dict = dict.fromkeys(new_keys, proposed)
        overlapped_dict = {
            k: min(self.case_number_dict[k], proposed)
            for k in overlapped_keys
        }
        self.case_number_dict.update(new_dict)
        self.case_number_dict.update(overlapped_dict)


class TopicData(object):
    def __init__(self):
        self.topic_name = ''
        self.namespace = ''
        self.contrib_dict = defaultdict(ContribData)

    def consider(self, anno):
        contrib_uuid = anno['contributor_uuid']
        contrib_data = self.contrib_dict[contrib_uuid]
        contrib_data.consider(anno)
        if not self.topic_name:
            self.topic_name = anno['topic_name']
            self.namespace = anno['namespace']
        else:
            assert(self.topic_name == anno['topic_name'])
            assert(self.namespace == anno['namespace'])

    def sum_contribs(self):
        topic_counter = Counter()
        for contrib_uuid, contrib_data in self.contrib_dict.iteritems():
           # multi-set addition...similar to adding vectors of 0s and 1s.
           # except just tracking the positions of the 1s.
           topic_counter += Counter(contrib_data.flattened)
        return topic_counter

    def convert_to_ranges(self, positions):
        offsets = []
        if len(positions) != 0:
            indices = sorted(positions)
            start = indices[0]
            end = start + 1
            for i,pos in enumerate(indices):
                if i > 0 and indices[i-1] + 1 != pos:
                    offsets.append({'start_pos': start, 'end_pos': end})
                    start = pos
                end = pos + 1
            offsets.append({'start_pos': start, 'end_pos': end})
        return offsets

    def determine_cases(self, offsets):
        rows = []
        for seq, offset in enumerate(offsets):
            row = dict(offset)
            row['topic_name'] = self.topic_name
            row['namespace'] = self.namespace
            row['case_number'] = seq + 1
            rows.append(row)
        return rows

    def get_consensus(self, determine_passing):
        topic_counter = self.sum_contribs()
        passing_tuples = filter(determine_passing, topic_counter.iteritems())
        passing_indices = dict(passing_tuples).keys()
        offsets = self.convert_to_ranges(passing_indices)
        return offsets

    def get_contrib_count(self):
        return len(self.contrib_dict)


class ConsensusProcessor(object):
    def __init__(self, task_uuid, iaa_config):
        self.task_uuid = task_uuid
        self.iaa_config = iaa_config
        self.article_data = ArticleData()
        # defaultdict - creates entries on access
        self.topics = defaultdict(TopicData)

    def consider(self, task_highlights):
        minimum_redundancy = self.iaa_config.get('minimum_redundancy', MIN_REDUNDANCY)
        for anno in task_highlights:
            if anno['taskrun_count'] < minimum_redundancy:
                continue
            self.article_data.consider(anno)
            topic_name = anno['topic_name']
            topic_data = self.topics[topic_name]
            topic_data.consider(anno)

    def set_text(self, offsets):
        for offset in offsets:
            start = offset['start_pos']
            end = offset['end_pos']
            chars = [self.article_data.get(x) for x in range(start, end)]
            offset['target_text'] = ''.join(chars).encode('unicode-escape')

    def set_links(self, rows):
        for row in rows:
            self.article_data.set_article_cols(row)
            row['task_uuid'] = self.task_uuid

    def get_consensus(self):
        consensus_rows = []
        for topic_name, topic_data in self.topics.iteritems():
            pass_threshold = self.iaa_config.get('pass_threshold', PASS_THRESHOLD)

            def determine_passing(index_comma_total):
                (index, total) = index_comma_total
                return total >= pass_threshold

            offsets = topic_data.get_consensus(determine_passing)
            self.set_text(offsets)
            rows = topic_data.determine_cases(offsets)
            self.set_links(rows)
            consensus_rows.extend(rows)
        return consensus_rows

    def get_answer_consensus(self):
        consensus_rows = []
        for topic_name, topic_data in self.topics.iteritems():
            pass_threshold = self.iaa_config.get('pass_threshold', PASS_THRESHOLD)

            def determine_passing(index_comma_total):
                (index, total) = index_comma_total
                return total >= pass_threshold

            offsets = topic_data.get_consensus(determine_passing)
            # Before we give up, return this answer without highlights
            # if it has been chosen more than threshold times, regardless of
            # highlights - which some answers do not even allow.
            contrib_count = topic_data.get_contrib_count()
            if len(offsets) == 0 and contrib_count >= pass_threshold:
                offsets = [{'start_pos': 0, 'end_pos': 0}]
            self.set_text(offsets)
            rows = topic_data.determine_cases(offsets)
            self.set_links(rows)
            for row in rows:
                row['extra'] = {'contrib_count': contrib_count}
            consensus_rows.extend(rows)
        return consensus_rows
