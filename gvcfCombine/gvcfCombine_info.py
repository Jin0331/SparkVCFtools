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
parser.add_argument("--appname", help="appnames for spark ui")
parser.add_argument("--c", help="gvcf count")

args = parser.parse_args()
if args.appname:
    appname = args.appname
if args.appname:
    c = args.c

folder_name = copy.deepcopy(appname) 
gvcf_count = int(copy.deepcopy(c))

if __name__ == '__main__':

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
    from function import *
    
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