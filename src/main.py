import argparse
import sys
import traceback

from helpers.factory import ExtractorFactory, TransformerFactory, LoaderFactory
from pipeline import Pipeline
from python_json_config import ConfigBuilder

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Pipeline')
    parser.add_argument('pipeline_config_path', help='Pipeline config json path')
    args = parser.parse_args()

    try:
        pipeline_config = ConfigBuilder().parse_config(config=args.pipeline_config_path)

        extractor = ExtractorFactory.create(pipeline_config.extract)
        transformer = TransformerFactory.create(pipeline_config.transform)
        loader = LoaderFactory.create(pipeline_config.load)

        custom_pipeline = Pipeline(extractor, transformer, loader)
        custom_pipeline.run()

    except ValueError as ve:
        print("Invalid config")
        traceback.print_exc(file=sys.stdout)
    except Exception as e:
        print("Some error occurred")
        traceback.print_exc(file=sys.stdout)
