from test_spark_submit.subfolder.spark_script import process, process2
from test_spark_submit.test.utils import SparkBaseTestCase

from pyspark.sql.types import Row


class HelloWorldTest(SparkBaseTestCase):
    def __init__(self, *args, **kwargs):
        super(HelloWorldTest, self).__init__(*args, **kwargs)

    def test_basic(self):
        expected_result = ["hello world"]
        result = process(self.sc, expected_result)
        assert expected_result == result

    def test_csv(self):
        expected_result = [Row(C0='hello', C1='world')]
        df = process2(self.sc, 'test_spark_submit/test/resources/test_file.csv')
        assert expected_result == df.collect()

    def test_sql(self):
        expected_result = [Row(C0='hello', C1='world')]
        df = process2(self.sc, 'test_spark_submit/test/resources/test_file.csv')
        self.sql_context.registerDataFrameAsTable(df, 'test_table')
        result = self.sql_context.sql('select * from test_table').collect()
        assert expected_result == result
