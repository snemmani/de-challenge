from pyspark import SparkContext
import os
import re


class IndexGenerator:
    def __init__(self, spark_context: SparkContext):
        self.spark_context = spark_context

    def generate_word_ids(self, input_files_path: str, output_location: str):
        """Generate Word ID mapping for each word in the files located in the input files location and create id files in the output_location"""
        pattern = re.compile('^[0-9]+$')
        files = [file_name for file_name in os.listdir(input_files_path) if pattern.match(file_name)]
        for file_name in files:
            self._generate_word_id_for_file(os.path.join(input_files_path, file_name), output_location)

    @staticmethod
    def _generate_dictionary_tuple(row, file_base_name):
        """This will remove any special characters from the words and
        return a tuple containing the word, unique index, file name"""
        word = re.sub("[,!*().\\[\\]+:;\"'/]", "", row)
        return word, IndexGenerator.get_hash(word), file_base_name

    @staticmethod
    def get_hash(word):
        """Return 8 character long hash for a given word"""
        return abs(hash(word)) % (10 ** 8)

    def _generate_word_id_for_file(self, file_path: str, output_directory_path: str) -> None:
        """Generate Word ID mapping for each word in the file located in the input files location and create id files in the output_location"""
        file_base_name = int(os.path.basename(file_path))
        self.spark_context.textFile(file_path) \
            .flatMap(lambda x: x.split(' ')) \
            .map(lambda x: IndexGenerator._generate_dictionary_tuple(x, file_base_name)) \
            .distinct() \
            .sortBy(lambda x: x[0])\
            .saveAsPickleFile(os.path.join(output_directory_path, os.path.basename(file_path) + '_id'))

    @staticmethod
    def _concatenate(x, y):
        """Concatenate two items if they are lists, else convert them to a list before concatenation"""
        if not isinstance(x, list):
            x = [x]

        if not isinstance(y, list):
            y = [y]

        return x + y

    @staticmethod
    def _sort(x):
        """Sort the rdd based on the Unique ID"""
        if isinstance(x[1], list):
            return x[0], sorted(x[1])
        else:
            return x[0], [x[1]]

    def collate_documents(self, input_files_path: str, output_directory_path: str):
        """Collate documents created by generate word id function and then created a collated list mapping of each word id and file name
        Create a dictionary that contains the word and its respective id"""
        collated_pickle = self.spark_context.pickleFile(
            os.path.join(input_files_path, '*_id', '*'),
            minPartitions=2)
        collated_pickle.map(lambda x: (x[1], x[2])).distinct()\
            .saveAsPickleFile(os.path.join(output_directory_path, 'collated'))
        collated_pickle.map(lambda x: (x[0], x[1])).distinct()\
            .saveAsTextFile(os.path.join(output_directory_path, 'dictionary'))

    def create_inverted_index(self, input_files_path: str, output_directory_path: str):
        """Create inverted index"""
        self.spark_context.pickleFile(os.path.join(input_files_path, 'collated')) \
            .reduceByKey(IndexGenerator._concatenate).map(IndexGenerator._sort).sortByKey()\
            .saveAsTextFile(os.path.join(output_directory_path, 'inverted-index'))

    def run(self, dataset_location, output_location):
        """Run the pipeline to generate inverted indexes"""
        # Verify if results directory exists
        if os.path.exists(output_location) and len(os.listdir(output_location)):
            raise FileExistsError('Results directory not empty, initiating exit...')

        if not os.path.exists(output_location):
            os.mkdir(output_location)

        self.generate_word_ids(dataset_location, output_location)
        self.collate_documents(output_location, output_location)
        self.create_inverted_index(output_location, output_location)
