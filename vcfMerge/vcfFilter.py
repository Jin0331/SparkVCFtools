from pyspark.sql.functions import udf, col, desc, asc
import re

def preVCF(hdfs, flag, spark): # hdfs://, flag 0 == lhs, 1 == rhs
    vcf = spark.sparkContext.textFile(hdfs).map(lambda x : x.split("\t"))
    header = vcf.first()
    step1 = vcf.filter(lambda row : row != header).map(alt_filter).toDF(header)
    return_vcf = step1.select(chr_remove_udf(step1["#CHROM"]).cast("Integer").alias("CHROM"), "*")\
                      .drop(col("#CHROM")).filter(col("FILTER") == "PASS")
    if flag == 1:
        for index in range(len(return_vcf.columns[:9])):
            return_vcf = return_vcf.withColumnRenamed(return_vcf.columns[index], return_vcf.columns[index] + "_temp") 
    return return_vcf

def chr_remove(chrom):
    chrom = re.sub("chr", "", chrom) # "chr" to ""
    if chrom == "X": 
        chrom = "23"
    elif chrom == "Y": 
        chrom = "24"
    elif chrom == "XY" or chrom == "M": 
        chrom = "-99"
    return chrom
chr_remove_udf = udf(chr_remove)

def alt_filter(row):
    if "," in row[4]:
        temp = row[4].split(",")
        temp.sort()
        row_new = [",".join(temp), ]
        
        return row[:4] + row_new + row[5:]
    
    else:
        return row