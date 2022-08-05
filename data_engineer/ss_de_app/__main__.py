''' Main app picked up by Python'''

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import argparse
import processor

def init_spark():
    '''Return an initialized spark session'''
    conf = SparkConf() \
        .setAppName('app')
    sc = SparkContext(conf=conf)
    ss = SparkSession(sc)
    ss.sparkContext.setLogLevel('WARN')
    return ss



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Input the student file and teacher file')
    parser.add_argument('student_file',
            help='full path to the student file')
    parser.add_argument('teacher_file',
            help='full path to the teacher file')
    parser.add_argument('--out',
            metavar='a.json',
            dest='output_file',
            help='optional output file')
    args = parser.parse_args()
    #print(args)
    
    ss = init_spark()


    if args.output_file:
        processor.run(ss, args.student_file,
                args.teacher_file,
                args.output_file)

    else:
        processor.run(ss,
                args.student_file,
                args.teacher_file)

