# Spark & python function
import findspark
findspark.init()

# Spark & python function
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType
import pyspark.sql.functions as F
from pyspark import Row
from pyspark.sql.window import Window

import re
import subprocess

spark = SparkSession.builder.master("spark://master:7077")\
                        .appName("gVCF_combine")\
                        .config("spark.executor.memory", "18G")\
                        .config("spark.executor.core", "3")\
                        .config("spark.sql.shuffle.partitions", 30)\
                        .config("spark.driver.memory", "13G")\
                        .config("spark.driver.maxResultSize", "10G")\
                        .getOrCreate()
    
if __name__ == "__main__":
    spark = SparkSession.builder.master("spark://master:7077")\
                        .appName("gVCF_combine")\
                        .config("spark.executor.memory", "18G")\
                        .config("spark.executor.core", "3")\
                        .config("spark.sql.shuffle.partitions", 20)\
                        .config("spark.driver.maxResultSize", "10G")\
                        .getOrCreate()

    #spark.sparkContext.addPyFile("SparkVCFtools/vcfFilter.py")

    #### addPyFile import #########
    from SelectCol import *
    from vcfFilter import *
    chr_remove_udf = udf(chr_remove)
    ###############################

    # main
    hdfs = "hdfs://master:9000"
    hdfs_list = hadoop_list(2, "/raw_data/gvcf")
    gvcf_list = []
    gvcf_combine_result = []

    for index in range(len(hdfs_list)):
        if index == 0:
            gvcf_list.append(preVCF(hdfs + hdfs_list[index].decode("UTF-8"), 0, spark))
        else:
            gvcf_list.append(preVCF(hdfs + hdfs_list[index].decode("UTF-8"), 1, spark))

    for index in range(1, len(hdfs_list)):
        if index == 1:
            join_vcf = gvcf_list[0].join(gvcf_list[index], ["#CHROM", "POS", "REF"], "full")
        else :
            join_vcf = gvcf_combine_result[index - 2].join(gvcf_list[index], ["#CHROM", "POS", "REF"], "full")
            
        # window
        lookup_window = Window.partitionBy("#CHROM").orderBy("POS").rangeBetween(Window.unboundedPreceding, 0)
        
        # schema & header
        if index == 1:
            sample_col = gvcf_list[0].columns[9:] + gvcf_list[index].columns[9:]
            header = gvcf_list[0].columns + gvcf_list[index].columns[9:] 
        else :
            sample_col = gvcf_combine_result[index - 2].columns[9:] + gvcf_list[index].columns[9:]
            header = gvcf_combine_result[index - 2].columns + gvcf_list[index].columns[9:] 
            
        # null value update
        join_vcf_update = join_vcf.withColumn("INFO", F.last("INFO", ignorenulls = True).over(lookup_window))\
                                        .withColumn("INFO_temp", F.last("INFO_temp", ignorenulls = True).over(lookup_window))
        for col_name in sample_col:
            join_vcf_update = join_vcf_update.withColumn(col_name, F.last(col_name, ignorenulls = True).over(lookup_window))
        
        # finally value append
        gvcf_combine_result.append(join_vcf_update.orderBy(F.col("#CHROM"), F.col("POS"))\
                                .rdd.map(lambda row : selectCol(row, sample_col))\
                                .toDF(header).cache())
        gvcf_combine_result[index - 1].count()
        
        if index != 1:
            gvcf_combine_result[index - 2].unpersist()