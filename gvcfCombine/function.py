import re
from pyspark.sql.types import IntegerType

def preVCF(hdfs, flag, spark):
    vcf = spark.sparkContext.textFile(hdfs)

    header_contig = vcf.filter(lambda x : re.match("^#", x))
    col_name = vcf.filter(lambda x : x.startswith("#CHROM")).first().split("\t")

    vcf_data = vcf.filter(lambda x : re.match("[^#][^#]", x))\
                       .map(lambda x : x.split("\t"))\
                       .toDF(col_name_left)\
                       .withColumn("POS", col("POS").cast(IntegerType()))


    ## lookup table
    lookup = vcf_data.filter(substring(col("INFO"), 0, 3).isin("END"))\
                .select(col("#CHROM").alias("CHR"), col("POS").alias("START"),
                        regexp_replace(col("INFO"), "END=", "").cast(IntegerType()).alias("END"), 
                        vcf_data[9]).coalesce(20)

    # flag에 따라 column 명 변경
    if flag == 1:
        for index in range(len(vcf_data.columns[:9])):
            compared_arr = ["#CHROM", "POS", "REF"]
            if vcf_data.columns[index] in compared_arr:
                continue
            vcf_data = vcf_data.withColumnRenamed(vcf_data.columns[index], vcf_data.columns[index] + "_temp") 
    
    return (vcf_data, lookup)
