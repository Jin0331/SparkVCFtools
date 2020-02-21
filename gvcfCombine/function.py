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

def preVCF(hdfs, spark):
    vcf = spark.sparkContext.textFile(hdfs)
    #header_contig = vcf.filter(lambda x : re.match("^#", x))
    col_name = vcf.filter(lambda x : x.startswith("#CHROM")).first().split("\t")
    vcf_data = vcf.filter(lambda x : re.match("[^#][^#]", x))\
                       .map(lambda x : x.split("\t"))\
                       .toDF(col_name)\
                       .withColumn("POS", F.col("POS").cast(IntegerType()))
    sample_name = vcf_data.columns[-1]
    vcf_data = vcf_data.drop("QUAL", "FILTER")
    
    for index in range(len(vcf_data.columns) - 1):
        compared_arr = ["#CHROM", "POS"]
        if vcf_data.columns[index] in compared_arr:
            continue
        vcf_data = vcf_data.withColumnRenamed(vcf_data.columns[index], vcf_data.columns[index] + "_" + sample_name)     
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

def unionAll(*dfs):
    return reduce(DataFrame.unionByName, dfs) 

# for indel
word_len = udf(lambda col : True if len(col) >= 2 else False, returnType=BooleanType())
ref_melt = udf(lambda ref : list(ref)[1:], ArrayType(StringType()))    

def ref_concat(temp): 
    return_str = []
    for num in range(0, len(temp)):
        return_str.append(temp[num] + "_" + str(int(num + 1)))
    return return_str
ref_concat = udf(ref_concat, ArrayType(StringType()))

def ref_max(left, right):
    if left == None:
        return right
    elif right == None:
        return left
    else :
        if len(left) >= len(right):
            return left
        else :
            return right
ref_max = udf(ref_max, StringType())

def info_change(temp):
    some_list = temp.split(";")
    result = [i for i in some_list if i.startswith('DP=')]
    return result[0]
info_change = udf(info_change, StringType())

# for sample value
value_change = udf(lambda value : "./." + value[3:], StringType())

# for POS index
def sampling_func(data, ran):
    N = len(data)
    sample = data.take(range(0, N, ran))
    return sample

def reduce_join(left, right):   
    return_vcf = left.drop(left.columns[-1])\
                     .join(right.drop(right.columns[-1]), ["#CHROM", "POS"], "full")
    
    return return_vcf

def reduce_inner_join(left, right):   
    return_vcf = left.join(right, ["#CHROM", "POS"], "inner")
    
    return return_vcf

def column_name(df_col, name):
    return_list = []
    for col in df_col:
        if col.startswith(name):
            return_list.append(col)
    return return_list

def max_value(value):
    value = list(filter(None, value))
    if len(value) == 0:
        return None
    return max(value)
max_value = udf(max_value, StringType())

def info_min(value):
    value = list(filter(None, value))
    temp = [info for info in value if info.startswith("END=") == False]
    temp = "%".join(temp)
    
    if temp == "":
        return None
    else :
        return temp
info_min = udf(info_min, StringType())

def format_value(value): 
    # ##FORMAT=<ID=SB,Number=4,Type=Integer,Description="Per-sample component statistics which comprise the Fisher's Exact Test to detect strand bias.">
    value = list(filter(None, value))
    if len(value) == 1:
        value.append("GT:DP:GQ:MIN_DP:PL")
    def format_reduce(left, right):
        left, right = left.split(":"), right.split(":")
        if len(left) <= len(right):        
            temp = copy.deepcopy(right)
            right = copy.deepcopy(left)
            left = copy.deepcopy(temp)
        for value in right:
            if value not in left:
                left.append(value)
        return ":".join(left)
    return str(reduce(format_reduce, value))
format_value = udf(format_value, StringType())

def with_vale(temp):
    temp = temp.withColumn("REF", max_value(F.array(column_name(temp.columns, "REF"))))\
     .drop(*column_name(temp.columns, "REF_"))\
     .withColumn("ID", max_value(F.array(column_name(temp.columns, "ID"))))\
     .drop(*column_name(temp.columns, "ID_"))\
     .withColumn("ALT", max_value(F.array(column_name(temp.columns, "ALT"))))\
     .drop(*column_name(temp.columns, "ALT_"))\
     .withColumn("INFO", info_min(F.array(column_name(temp.columns, "INFO"))))\
     .drop(*column_name(temp.columns, "INFO_"))\
     .withColumn("FORMAT", F.array(column_name(temp.columns, "FORMAT")))\
     .drop(*column_name(temp.columns, "FORMAT_"))
    return temp

def indel_union(temp):
    temp = temp.filter(word_len(F.col("REF")))\
            .withColumn("REF", ref_melt(F.col("REF"))).withColumn("REF", ref_concat(F.col("REF")))\
            .withColumn("REF", explode(F.col("REF"))).withColumnRenamed("REF", "REF_temp")\
            .withColumn('REF', split_col.getItem(0)).withColumn('POS_var', split_col.getItem(1))\
            .drop(F.col("REF_temp")).withColumn("POS", (F.col("POS") + F.col("POS_var")).cast(IntegerType()))\
            .drop(F.col("POS_var"))\
            .withColumn('ID', F.lit("."))\
            .withColumn('ALT', F.lit("*,<NON_REF>"))
    return temp

# 10개 기준임.
def join_split(v_list):
    stage1_list = list(chunks(v_list, 5))
    if len(v_list) == 1:
        return v_list    
    stage1 = []
    for vcf in stage1_list:
        if len(vcf) == 1:
            stage1.append(vcf)
        else :
            stage1.append(reduce(reduce_join, vcf))
    return reduce(reduce_join, stage1)

def join_split_inner(v_list):
    stage1_list = list(chunks(v_list, 3))
    stage1 = []
    for vcf in stage1_list:
        stage1.append(reduce(reduce_inner_join, vcf))
    return stage1

def find_duplicate(temp):
    return temp.groupBy(F.col("#CHROM"), F.col("POS")).agg((F.count("*")>1).cast("int").alias("e"))\
         .orderBy(F.col("e"), ascending=False)