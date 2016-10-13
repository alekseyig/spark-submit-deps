from test_spark_submit.subfolder.spark_script import process
from test_spark_submit.test.utils import SparkBaseTestCase


class HelloWorldTest(SparkBaseTestCase):
    def __init__(self, *args, **kwargs):
        super(HelloWorldTest, self).__init__(*args, **kwargs)

    def test_basic(self):
        input_list = ["hello world"]
        result = process(self.sc, input_list)
        assert result == input_list
