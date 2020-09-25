import settings
from invertedindex.indexgenerator import IndexGenerator

if __name__ == "__main__":
    IndexGenerator(settings.spark_context).run(settings.dataset_location)
