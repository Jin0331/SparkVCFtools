import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, udf, explode, array, when
from pyspark.sql.types import IntegerType, StringType, ArrayType,BooleanType

import re
import subprocess
from functools import reduce 


def hadoop_list(length, hdfs):
    args = "hdfs dfs -ls "+ hdfs +" | awk '{print $8}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    all_dart_dirs = s_output.split()
    
    return all_dart_dirs[:length]

def hadoop_list(length, hdfs):
    args = "hdfs dfs -ls "+ hdfs +" | awk '{print $8}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    all_dart_dirs = s_output.split()
    
    return all_dart_dirs[:length]

def preVCF(hdfs, flag, spark):
    vcf = spark.sparkContext.textFile(hdfs)
    #header_contig = vcf.filter(lambda x : re.match("^#", x))
    col_name = vcf.filter(lambda x : x.startswith("#CHROM")).first().split("\t")
    vcf_data = vcf.filter(lambda x : re.match("[^#][^#]", x))\
                       .map(lambda x : x.split("\t"))\
                       .toDF(col_name)\
                       .withColumn("POS", F.col("POS").cast(IntegerType()))
    
    if flag == 1:
        for index in range(len(vcf_data.columns) - 1):
            compared_arr = ["#CHROM", "POS", "REF"]
            if vcf_data.columns[index] in compared_arr:
                continue
            vcf_data = vcf_data.withColumnRenamed(vcf_data.columns[index], vcf_data.columns[index] + "_temp") 
    
    return vcf_data

def chunks(lst, n):
    for index in range(0, len(lst), n):
        yield lst[index:index + n]
        
def addIndex(POS, size):
    if POS == 1:
        return POS
    else :
        return int(POS / size + 1) 
addIndex_udf = udf(addIndex, returnType=IntegerType())

# for indel
word_len = udf(lambda col : True if len(col) >= 2 else False, returnType=BooleanType())
ref_melt = udf(lambda ref : list(ref)[1:], ArrayType(StringType()))    

def ref_concat(temp): 
    return_str = []
    for num in range(0, len(temp)):
        return_str.append(temp[num] + "_" + str(int(num + 1)))
    return return_str
ref_concat = udf(ref_concat, ArrayType(StringType()))

def unionAll(*dfs):
    return reduce(DataFrame.unionByName, dfs) 

# for sample value
value_change = udf(lambda value : "./." + value[3:], StringType())

# for POS index
def sampling_func(data, ran):
    N = len(data)
    sample = data.take(range(0, N, ran))
    return sample

def sample_join(left, right):
    return left.select(F.col("#CHROM"), F.col("POS"), F.col("REF"), left.columns[-1])\
        .join(right.select(F.col("#CHROM"), F.col("POS"), F.col("REF"), right.columns[-1]), ["#CHROM", "POS", "REF"], "full")