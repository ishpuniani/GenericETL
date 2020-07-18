import argparse
import json
import sys
import traceback

from factory import ExtractorFactory, TransformerFactory, LoaderFactory
from pipeline import Pipeline
from python_json_config import ConfigBuilder

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Pipeline')
    parser.add_argument('pipeline_config_path', help='Pipeline config json path')
    args = parser.parse_args()

    try:
        # pipeline_config = json.loads(open(args.pipeline_config_path))
        pipeline_config = ConfigBuilder().parse_config(config=args.pipeline_config_path)
        # extract_config = pipeline_config.extract
        # print(extract_config.extract)

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
