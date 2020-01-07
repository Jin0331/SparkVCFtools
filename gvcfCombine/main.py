import findspark
findspark.init()

# Spark & python function
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, col, desc, asc, coalesce, broadcast
from pyspark.sql.functions import substring, length, regexp_replace
from pyspark import Row
import re
    
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

    # load vcf
    # main
    left = preVCF("hdfs://master:9000/raw_data/gvcf/ND02798_eg.raw.vcf", 0, spark)
    right = preVCF("hdfs://master:9000/raw_data/gvcf/ND02809_eg.raw.vcf", 1, spark)

    join_vcf = left.join(right, ["#CHROM", "POS", "REF"], "full")
    #join_vcf = left.join(right, ["#CHROM", "POS", "REF"], "full").cache()
    #join_vcf.count()

    # window
    lookup_window = Window.partitionBy("#CHROM").orderBy("POS").rangeBetween(Window.unboundedPreceding, 0)

    sample_col = left.columns[9:] + right.columns[9:]
    header = left.columns + right.columns[9:] 
    join_vcf_update = join_vcf.withColumn("INFO", F.last("INFO", ignorenulls = True).over(lookup_window))\
                                    .withColumn("INFO_temp", F.last("INFO_temp", ignorenulls = True).over(lookup_window))
    for col_name in sample_col:
        join_vcf_update = join_vcf_update.withColumn(col_name, F.last(col_name, ignorenulls = True).over(lookup_window))
    join_vcf_update = join_vcf_update.orderBy(F.col("#CHROM"), F.col("POS")).rdd.map(lambda row : selectCol(row, sample_col)).toDF(header).cache()
                                            
    join_vcf_update.count()