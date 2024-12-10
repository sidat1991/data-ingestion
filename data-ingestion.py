import os.path
import json
import sys
from json import JSONDecodeError
import uuid
import opensearchpy.exceptions
from kafka_utils import KafkaClient
from opensearch_utils import OpenSearchUtils
from configuration import OPEN_SEARCH_INDEX
import threading
import linecache


def read_data():
        while True:
                line_no = int(os.environ.setdefault("line_no", "1")) #read one line at a time
                data = linecache.getline(file_path, line_no)
                yield data

def produce():
        for line in read_data():
                try:
                        data = json.loads(line)["after"]
                        line_no = int(os.environ.setdefault("line_no", "1"))
                        kafka_obj.produce(data["key"], json.dumps(data["value"]))
                        os.environ["line_no"] = str(line_no + 1)
                except JSONDecodeError:
                        continue


def consumer():
        open_search_client = OpenSearchUtils()
        try:
                open_search_client.client.indices.create(OPEN_SEARCH_INDEX)
        except opensearchpy.exceptions.RequestError:#handle if index is already present
                pass
        for x in kafka_obj.consume():
                id = uuid.uuid4()
                # store consumed data from kafka to opensearch utils
                open_search_client.client.index(index=OPEN_SEARCH_INDEX, id=id, body=x.decode("utf-8"))


if __name__ == "__main__":
        file_path = os.path.join(os.getcwd(), "sample-data", "stream.jsonl")
        kafka_obj = KafkaClient()
        #Run consumer in the background
        thread = threading.Thread(target=consumer, args=(), kwargs={})
        thread.start()
        while True:
                try:
                        produce()
                except KeyboardInterrupt:
                        sys.exit(0)
