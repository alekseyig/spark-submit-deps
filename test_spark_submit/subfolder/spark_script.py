import sys
from pyspark import SparkContext
import pulp  # this import is just to check that deps where distributed correctly


def process(sc, args):
    input_data = args if args else [1, 2, 3, 4, 5]
    print(">>>> in process ", input_data)
    distr_data = sc.parallelize(input_data)
    result = distr_data.collect()
    print result
    return result


def main(args):
    job_name = 'process-region-state-metrics-event'
    app_name = '{0}-{1}'.format(job_name, 'batchId')

    print('initialize context')
    sc = SparkContext(appName=app_name)
    process(sc, args)
    sc.stop()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
