import findspark
findspark.init()

# spark function
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, desc, asc
import pyspark.sql.functions as F
from pyspark import Row
from pyspark.sql.types import IntegerType

# python function
import re
import copy
import subprocess
from functools import reduce 
import argparse

# sys.args
parser = argparse.ArgumentParser()
parser.add_argument("--appname", help="appnames for spark ui")
args = parser.parse_args()
if args.appname:
    appname = args.appname

folder_name = copy.deepcopy(appname) 

if __name__ == '__main__':

    spark = SparkSession.builder.master("spark://master:7077")\
                        .appName(appname)\
                        .config("spark.driver.memory", "8G")\
                        .config("spark.driver.maxResultSize", "5G")\
                        .config("spark.executor.memory", "25G")\
                        .config("spark.memory.fraction", 0.18)\
                        .config("spark.sql.shuffle.partitions", 100)\
                        .config("spark.eventLog.enabled", "true")\
                        .config("spark.cleaner.periodicGC.interval", "15min")\
                        .getOrCreate()
    spark.sparkContext.addPyFile("function.py")
    from function import *

    # run
    hdfs = "hdfs://master:9000"        
    hdfs_list = hadoop_list(2, "/raw_data/vcf")

    # header
    vcf_header = list()
    for index in range(len(hdfs_list)):
        vcf_header.append(headerVCF(hdfs + hdfs_list[index].decode("UTF-8"), spark))
    header = unionAll(*vcf_header)    
    headerWrite(header, vcf_header, 0, spark).coalesce(1).write.format("text").option("header", "false").mode("overwrite")\
                                             .save("/raw_data/output/vcf_output/"+ folder_name + "//vcf_meta.txt")

    # load case.vcf from HDFS
    case = preVCF(hdfs + hdfs_list[0].decode("UTF-8"), 0, spark)
    control = preVCF(hdfs + hdfs_list[1].decode("UTF-8"), 1, spark)

    # case & control indexing
    case_col = len(case.columns)
    control_col = len(control.columns)

    # merge schema
    col = case.columns + control.columns
    header = col[:case_col] + col[case_col + 9:]

    ### join expresion
    joinEX = [case['CHROM'] == control['CHROM_temp'],
                case['POS'] == control['POS_temp'],
                case['REF'] == control['REF_temp']]

    # write for csv with delimiter "\t"
    case.join(control, joinEX, 'full')\
        .rdd.map(lambda row : selectCol(row, case_col, control_col))\
        .toDF(header).withColumn("POS", F.col("POS").cast(IntegerType()))\
        .dropDuplicates(['CHROM', 'POS']).orderBy(F.col("CHROM"), F.col("POS"))\
        .write.mode('overwrite').option("delimiter", "\t").option("header", "true")\
        .csv("hdfs://master:9000/raw_data/output/vcf_output/" + folder_name + "//vcf_merge.txt")

    spark.stop()