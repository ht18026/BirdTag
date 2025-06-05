import unittest
import json
import os

from lambda_function import handler


class MyTestCase(unittest.TestCase):
    def test_something(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {
                            "name": "birdtag-models-fit5225-g138"
                        },
                        "object": {
                            "key": "videos/kingfisher.mp4"
                        }
                    }
                }
            ]
        }

        response = handler(event, None)
        print(json.dumps(response, indent=2))


if __name__ == '__main__':
    unittest.main()
