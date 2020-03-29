import findspark
findspark.init()

# Spark function
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Python function
import copy
import argparse

# sys.args
parser = argparse.ArgumentParser()
parser.add_argument("--infoName", help="appnames for spark ui")
parser.add_argument("--c", help="gvcf count")

args = parser.parse_args()
if args.infoName:
    appname = args.infoName
if args.c:
    c = args.c

folder_name = copy.deepcopy(appname) 
gvcf_count = int(copy.deepcopy(c))

if __name__ == '__main__':
    # init variable
    cnt = 0
    num = 10
    part_num = 80

    # Start for Spark Session with write
    spark = SparkSession.builder.master("spark://master:7077")\
                            .appName(appname + "_sample" + str(num) + "_" + str(part_num))\
                            .config("spark.driver.memory", "8G")\
                            .config("spark.driver.maxResultSize", "5G")\
                            .config("spark.executor.memory", "25G")\
                            .config("spark.sql.shuffle.partitions", part_num)\
                            .config("spark.eventLog.enabled", "true")\
                            .config("spark.memory.fraction", 0.02)\
                            .config("spark.cleaner.periodicGC.interval", "15min")\
                            .getOrCreate()
    spark.udf.registerJavaFunction("index2dict", "scalaUDF.Index2dict", ArrayType(StringType()))
    spark.sparkContext.addPyFile("function.py")
    from function import *

    # sample parquet write
    hdfs = "hdfs://master:9000"
    hdfs_list = hadoop_list(gvcf_count, "/raw_data/gvcf")
    vcf_list = list()
    for index in range(len(hdfs_list)):
        vcf_list.append(sampleVCF(hdfs + hdfs_list[index].decode("UTF-8"), spark))
        
    indel_com = spark.read.parquet("/raw_data/output/gvcf_output/" + folder_name + "//info.g.vcf")\
                    .select(["#CHROM","POS","FORMAT"])\
                    .withColumn("FORMAT", F.array_remove(F.split(F.col("FORMAT"), ":"), "GT"))\
                    .orderBy(F.col("#CHROM"), F.col("POS")).persist(StorageLevel.MEMORY_ONLY)
    indel_com.count()

    parquet_list = list(map(lambda arg : parquet_revalue(arg, indel_com), vcf_list))
    for parquet in join_split_inner(parquet_list, num):
        parquet.write.mode('overwrite')\
                .parquet("/raw_data/output/gvcf_output/"+ folder_name + "//" + "sample_" + str(cnt) + ".g.vcf")
        cnt += num
        
    spark.catalog.clearCache()
    spark.stop()