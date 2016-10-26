import sys
from pyspark import SparkContext, SQLContext
import pulp  # this import is just to check that deps where distributed correctly


def process(sc, args):
    input_data = args if args else [1, 2, 3, 4, 5]
    distr_data = sc.parallelize(input_data)
    result = distr_data.collect()
    return result


def process2(sc, csv_file):
    sql_context = SQLContext(sc)
    ddr = sql_context \
        .read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter='\t') \
        .load(csv_file)
    return ddr


def main(args):
    job_name = 'process-region-state-metrics-event'
    app_name = '{0}-{1}'.format(job_name, 'batchId')

    print('initialize context')
    sc = SparkContext(appName=app_name)
    process(sc, args)
    sc.stop()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
