# Spark & python function
import findspark
findspark.init()

# Spark function
from pyspark import Row, StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import pandas_udf, udf, explode, array, when, PandasUDFType
from pyspark.sql.types import IntegerType, StringType, ArrayType, BooleanType, MapType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Python function
import re
import subprocess
import pandas as pd
import pyarrow
from functools import reduce 
from collections import Counter
import copy
import operator
import itertools

if __name__ == "__main__":
# Start for Spark Session
    appname = input("appname, folder name : ")
    folder_name = copy.deepcopy(appname) 
    gvcf_count = int(input("gvcf count : "))

    # Start for Spark Session
    spark = SparkSession.builder.master("spark://master:7077")\
                            .appName(appname)\
                            .config("spark.driver.memory", "8G")\
                            .config("spark.driver.maxResultSize", "5G")\
                            .config("spark.executor.memory", "25G")\
                            .config("spark.memory.fraction", 0.1)\
                            .config("spark.sql.shuffle.partitions", 100)\
                            .config("spark.eventLog.enabled", "true")\
                            .config("spark.cleaner.periodicGC.interval", "15min")\
                            .getOrCreate()

    spark.sparkContext.addPyFile("function.py")

    #### addPyFile import #########
    from function import *

    # main
    #gvcf_count = 20
    #gvcf_count = 20
    hdfs = "hdfs://master:9000"
    hdfs_list = hadoop_list(gvcf_count, "/raw_data/gvcf")
    vcf_header = list()
    vcf_list = list()

    for index in range(len(hdfs_list)):
        vcf_header.append(headerVCF(hdfs + hdfs_list[index].decode("UTF-8"), spark))
        vcf_list.append(preVCF(hdfs + hdfs_list[index].decode("UTF-8"), spark).persist(StorageLevel.MEMORY_ONLY))
    header = unionAll(*vcf_header).cache()
    header.count()
        
    headerWrite(header, vcf_header, 0, spark).write.format("text").option("header", "false").mode("append")\
                                    .save("/raw_data/output/gvcf_output/"+ folder_name + "//" + "gvcf_meta.txt")

    info_last = header.filter(F.col("key") == "##contig").select("value").dropDuplicates(["value"])\
        .withColumn("value", F.split(F.col("value"), ","))\
        .select(F.regexp_replace(select_list(F.col("value"), F.lit(0)), "<ID=", "").alias("#CHROM"), 
                F.concat(F.lit("END="),
                        F.regexp_replace(select_list(F.col("value"), F.lit(1)), "length=", "")).alias("INFO"))

    vcf = column_rename(vcf_join(vcf_list))
    vcf = column_revalue(vcf).persist(StorageLevel.MEMORY_ONLY)
    vcf.count()

    #window
    info_window = Window.partitionBy("#CHROM").orderBy("POS")
    vcf_not_indel = vcf.withColumn("INFO", when(F.col("INFO").isNull(), F.concat(F.lit("END="), F.lead("POS", 1).over(info_window) - 1))\
                                .otherwise(F.col("INFO")))

    not_last = vcf_not_indel.filter(F.col("INFO").isNotNull())
    last = vcf_not_indel.filter(F.col("INFO").isNull())\
                    .drop("INFO").join(info_last, ["#CHROM"], "inner")\
                    .select("#CHROM", "POS", "ID", "REF", "ALT", "INFO", "FORMAT")
    vcf_not_indel = unionAll(*[not_last, last])

    # indel union & parquet write
    unionAll(*[indel_union(vcf), vcf_not_indel])\
                    .orderBy(F.col("#CHROM"), F.col("POS"))\
                    .dropDuplicates(["#CHROM", "POS"])\
                    .write.mode('overwrite')\
                    .parquet("/raw_data/output/gvcf_output/" + folder_name + "//info.g.vcf")

    spark.catalog.clearCache()
    spark.stop()

    ## Parquet write for sample
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
                            .config("spark.memory.fraction", 0.05)\
                            .config("spark.cleaner.periodicGC.interval", "15min")\
                            .getOrCreate()

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

    parquet_list = list(map(parquet_revalue, vcf_list))
    for parquet in join_split_inner(parquet_list, num):
        parquet.write.mode('overwrite')\
                .parquet("/raw_data/output/gvcf_output/"+ folder_name + "//" + "sample_" + str(cnt) + ".g.vcf")
        cnt += num

    spark.catalog.clearCache()
    spark.stop()