import os
import logging
from testtools import TestCase

from pyspark.sql import SQLContext
from pyspark.context import SparkContext


def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)


class SparkBaseTestCase(TestCase):
    def setUp(self):
        """Setup a basic Spark context for testing"""
        super(SparkBaseTestCase, self).setUp()
        quiet_py4j()
        self.sc = SparkContext(os.getenv('SPARK_MASTER', 'local[4]'))
        self.sql_context = SQLContext(self.sc)

    def tearDown(self):
        """ Stops the running Spark context and does a hack to prevent Akka rebinding on the same port. """
        super(SparkBaseTestCase, self).tearDown()
        self.sc.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
        self.sc._jvm.System.clearProperty('spark.driver.port')
