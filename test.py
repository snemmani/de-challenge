import unittest
import settings
import tempfile
import os
import shutil
from invertedindex.indexgenerator import IndexGenerator
import xmlrunner


class IndexGeneratorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Create a temporary directory
        cls.tempdir = tempfile.gettempdir()
        cls.dataset_dir = os.path.join(cls.tempdir, 'spark_test_dataset')

        # Create a test instance of the generator
        cls.app: IndexGenerator = IndexGenerator(settings.spark_context)

        # Create the dataset
        os.mkdir(cls.dataset_dir)
        cls.file1string = 'Hello World\nShankar'
        with open(os.path.join(cls.dataset_dir, '1'), 'w') as fileHandle:
            fileHandle.write(cls.file1string)

        cls.file2string = 'Hello World\nNemmani'
        with open(os.path.join(cls.dataset_dir, '2'), 'w') as fileHandle:
            fileHandle.write(cls.file2string)

        # Modify settings to point to test dataset
        settings.dataset_location = cls.dataset_dir
        settings.output_location = os.path.join(cls.dataset_dir, 'id_files')

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dataset_dir)

    def test_create_inverted_index(self):
        self.app.generate_word_ids(self.dataset_dir, settings.output_location)

        assert os.path.exists(settings.output_location)
        assert os.path.exists(os.path.join(settings.output_location, '1_id'))
        assert os.path.exists(os.path.join(settings.output_location, '2_id'))

        self.app.collate_documents(settings.output_location, settings.output_location)
        assert os.path.exists(os.path.join(settings.output_location, 'dictionary'))
        assert os.path.exists(os.path.join(settings.output_location, 'collated'))

        self.app.create_inverted_index(settings.output_location, settings.output_location)
        assert os.path.exists(os.path.join(settings.output_location, 'inverted-index'))
        inverted_index = [eval(elem) for elem in settings.spark_context.textFile(os.path.join(settings.output_location, 'inverted-index')).collect()]
        dictionary = [eval(elem) for elem in settings.spark_context.textFile(os.path.join(settings.output_location, 'dictionary')).collect()]

        for index in inverted_index:
            dict_item = filter(lambda x: x[1] == index[0], dictionary)
            try:
                dict_item_value = next(dict_item)
                for filename in index[1]:
                    filevalue = getattr(self, f'file{filename}string')
                    assert dict_item_value[0] in filevalue
            except StopIteration:
                pass
