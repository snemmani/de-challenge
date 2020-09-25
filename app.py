import settings
from invertedindex.indexgenerator import IndexGenerator
logger = settings.logger.getChild("app")

if __name__ == "__main__":
    IndexGenerator(settings.spark_context).run(settings.dataset_location)
