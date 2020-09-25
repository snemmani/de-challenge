from pyspark import SparkContext
import settings
import os
import re


class IndexGenerator:
    def __init__(self, spark_context: SparkContext):
        self.spark_context = spark_context

    def generate_word_ids(self, input_files_path: str):
        pattern = re.compile('^[0-9]+$')
        files = [file_name for file_name in os.listdir(input_files_path) if pattern.match(file_name)]
        for file_name in files:
            self._generate_word_id_for_file(os.path.join(input_files_path, file_name))

    @staticmethod
    def _generate_dictionary_tuple(row, file_base_name):
        word = re.sub("[,!*().\\[\\]+:;\"'/]", "", row)
        return word, abs(hash(word)) % (10 ** 8), file_base_name

    def _generate_word_id_for_file(self, file_path: str):
        file_base_name = int(os.path.basename(file_path))
        self.spark_context.textFile(file_path) \
            .flatMap(lambda x: x.split(' ')) \
            .map(lambda x: IndexGenerator._generate_dictionary_tuple(x, file_base_name)) \
            .distinct() \
            .saveAsPickleFile(os.path.join(settings.id_files_location, os.path.basename(file_path) + '_id'))

    @staticmethod
    def _concatenate(x, y):
        if not isinstance(x, list):
            x = [x]

        if not isinstance(y, list):
            y = [y]

        return x + y

    @staticmethod
    def _sort(x):
        if isinstance(x[1], list):
            return x[0], sorted(x[1])
        else:
            return x[0], [x[1]]

    def collate_documents(self):
        collated_pickle = self.spark_context.pickleFile(
            os.path.join(settings.id_files_location, '*_id', '*'),
            minPartitions=2)
        collated_pickle.map(lambda x: (x[1], x[2])).distinct()\
            .saveAsPickleFile(os.path.join(settings.id_files_location, 'collated'))
        collated_pickle.map(lambda x: (x[0], x[1])).distinct()\
            .saveAsTextFile(os.path.join(settings.id_files_location, 'dictionary'))

    def create_inverted_index(self):
        self.spark_context.pickleFile(os.path.join(settings.id_files_location, 'collated')) \
            .reduceByKey(IndexGenerator._concatenate).map(IndexGenerator._sort).sortByKey()\
            .saveAsTextFile(os.path.join(settings.id_files_location, 'inverted-index'))

    def run(self, dataset_location):
        # Verify if results directory exists
        if os.path.exists(settings.id_files_location) and len(os.listdir(settings.id_files_location)):
            raise FileExistsError('Results directory not empty, initiating exit...')

        if not os.path.exists(settings.id_files_location):
            os.mkdir(settings.id_files_location)

        self.generate_word_ids(dataset_location)
        self.collate_documents()
        self.create_inverted_index()
