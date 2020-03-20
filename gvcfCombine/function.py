# Spark function
from pyspark import Row, StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, explode, array, when
from pyspark.sql.types import IntegerType, StringType, ArrayType, BooleanType, MapType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Python function
import re
import subprocess
from functools import reduce 
import operator
import itertools

def hadoop_list(length, hdfs):
    args = "hdfs dfs -ls "+ hdfs +" | awk '{print $8}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    all_dart_dirs = s_output.split()
    return all_dart_dirs[:length]

def headerVCF(hdfs, spark):
    col_name = ["key", "value"]
    vcf = spark.sparkContext.textFile(hdfs)
    vcf = vcf.filter(lambda x : re.match("^##", x))\
             .map(lambda x : x.split("=", 1))\
             .toDF(col_name)
    return vcf

def select_list(value, index):
    return value[index]
select_list = udf(select_list, StringType())

def preVCF(hdfs, spark):
    vcf = spark.sparkContext.textFile(hdfs)
    col_name = vcf.filter(lambda x : x.startswith("#CHROM")).first().split("\t")
    vcf_data = vcf.filter(lambda x : re.match("[^#][^#]", x))\
                       .map(lambda x : x.split("\t"))\
                       .toDF(col_name)\
                       .withColumn("POS", F.col("POS").cast(IntegerType()))\
                       .withColumn("FORMAT", F.array_remove(F.split(F.col("FORMAT"), ":"), "GT"))\
                       .withColumn("INFO", when(F.col("INFO").startswith("END="), None).otherwise(F.col("INFO")))
    
    sample_name = vcf_data.columns[-1]
    vcf_data = vcf_data.drop("QUAL", "FILTER", sample_name)
    
    for index in range(len(vcf_data.columns)):
        compared_arr = ["#CHROM", "POS"]
        if vcf_data.columns[index] in compared_arr:
            continue
        #####
        vcf_data = vcf_data.withColumn(vcf_data.columns[index], F.array(vcf_data.columns[index]))
        #####
        vcf_data = vcf_data.withColumnRenamed(vcf_data.columns[index], vcf_data.columns[index] + "_" + sample_name)     
    return vcf_data

firstremove = udf(lambda value : value[1:], ArrayType(StringType()))
def sampleVCF(hdfs, spark):
    vcf = spark.sparkContext.textFile(hdfs)
    col_name = vcf.filter(lambda x : x.startswith("#CHROM")).first().split("\t")
    vcf_data = vcf.filter(lambda x : re.match("[^#][^#]", x))\
                       .map(lambda x : x.split("\t"))\
                       .toDF(col_name)\
                       .withColumn("POS", F.col("POS").cast(IntegerType()))
    
    sample_name = vcf_data.columns[-1]
    vcf_data = vcf_data.select(F.col("#CHROM"), F.col("POS"), F.col("FORMAT"), F.col(sample_name))\
                       .withColumn("FORMAT", F.array_remove(F.split(F.col("FORMAT"), ":"), "GT"))\
                       .withColumn(sample_name, firstremove(F.split(F.col(sample_name), ":")))\
                       .withColumn(sample_name, F.map_from_arrays(F.col("FORMAT"), F.col(sample_name)))
    return vcf_data.select("#CHROM", "POS", sample_name)

def gatkVCF(hdfs, spark):
    vcf = spark.sparkContext.textFile(hdfs)
    #header_contig = vcf.filter(lambda x : re.match("^#", x))
    col_name = vcf.filter(lambda x : x.startswith("#CHROM")).first().split("\t")
    vcf_data = vcf.filter(lambda x : re.match("[^#][^#]", x))\
                       .map(lambda x : x.split("\t"))\
                       .toDF(col_name)
    return vcf_data

def chunks(lst, n):
    for index in range(0, len(lst), n):
        yield lst[index:index + n]

def unionAll(*dfs):
    return reduce(DataFrame.unionByName, dfs) 

# for indel
ref_melt = udf(lambda ref : list(ref)[1:], ArrayType(StringType()))    

def ref_concat(temp): 
    return_str = []
    for num in range(0, len(temp)):
        return_str.append(temp[num] + "_" + str(int(num + 1)))
    return return_str
ref_concat = udf(ref_concat, ArrayType(StringType()))

#@pandas_udf(ArrayType(StringType()))

# for sample value
value_change = udf(lambda value : "./." + value[3:], StringType())

def reduce_join(left, right):   
    return_vcf = left.join(right, ["#CHROM", "POS"], "full")

    ###
    remove_colname = right.columns[2:]
    l_name = left.columns
    r_name = right.columns
    v_name = return_vcf.columns
    name_list = ["REF", "ID", "ALT", "INFO", "FORMAT"]
    
    for name in name_list:
        if name == "INFO":
            return_vcf = return_vcf.withColumn(column_name(l_name, name)[0], 
                                       when(F.isnull(column_name(l_name, name)[0]), F.col(column_name(r_name, name)[0]))\
                                       .when(F.isnull(column_name(r_name, name)[0]), F.col(column_name(l_name, name)[0]))
                                       .otherwise(F.array_union(*column_name(v_name, name))))
            
        return_vcf = return_vcf.withColumn(column_name(l_name, name)[0], 
                                       when(F.isnull(column_name(l_name, name)[0]), F.col(column_name(r_name, name)[0]))\
                                       .when(F.isnull(column_name(r_name, name)[0]), F.col(column_name(l_name, name)[0]))
                                       .otherwise(F.array_union(*column_name(v_name, name))))
    return_vcf = return_vcf.drop(*remove_colname)
                                        
    return return_vcf

def column_rename(vcf):
    name_list = ["REF", "ID", "ALT", "INFO", "FORMAT"]
    for name in name_list:
        vcf = vcf.withColumnRenamed(column_name(vcf.columns, name)[0], name)
    return vcf
    
def reduce_inner_join(left, right):   
    return_vcf = left.join(right, ["#CHROM", "POS"], "inner")
    return return_vcf

def column_name(df_col, name):
    return_list = []
    for col in df_col:
        if col.startswith(name):
            return_list.append(col)
    return return_list

info_remove = udf(lambda value : True if value.startswith("END=") else False, BooleanType())

def column_revalue(vcf):
    # info 값 수정 필요
    name_list = ["ID", "REF","ALT", "INFO", "FORMAT"]
    for name in name_list:
        if name == "FORMAT":
            vcf = vcf.withColumn(name, F.array_sort(F.array_distinct(F.flatten(F.col(name)))))
            vcf = vcf.withColumn(name, F.concat(F.lit("GT:"), F.array_join(F.col(name), ":")))
        else:
            vcf = vcf.withColumn(name, F.array_max(F.col(name)))
    return vcf

def indel_union(temp):
    split_col = F.split("REF_temp", '_')
    temp = temp.filter(F.length(F.col("REF")) >= 2)\
            .withColumn("REF", ref_melt(F.col("REF"))).withColumn("REF", ref_concat(F.col("REF")))\
            .withColumn("REF", explode(F.col("REF"))).withColumnRenamed("REF", "REF_temp")\
            .withColumn('REF', split_col.getItem(0)).withColumn('POS_var', split_col.getItem(1))\
            .drop(F.col("REF_temp")).withColumn("POS", (F.col("POS") + F.col("POS_var")).cast(IntegerType()))\
            .drop(F.col("POS_var"))\
            .withColumn('ID', F.lit("."))\
            .withColumn('ALT', F.lit("*,<NON_REF>"))
    return temp

def value_merge(find_value, sample):
    del sample["GT"]
    if len(find_value) == len(sample):
        return_string = list(sample.values())
        return ":".join(return_string)
    else :
        return_string = []
        for value in find_value:
            format_value = sample.get(value)
            if format_value == None:
                format_value = "."
            return_string.append(format_value)      
        return ":".join(return_string)
value_merge = udf(value_merge, StringType())

def index2dict(value, index):
    temp = ["." for i in range(len(index))]
    sample = dict(zip(index, temp))
    merge_dict = {**value, **sample}
    sort_dict = sorted(merge_dict.items(), key=operator.itemgetter(0))
    return reduce(lambda x, y: (0, x[1] + ":" + y[1]), sort_dict)[1]
index2dict = udf(index2dict, StringType())

nullCheck = udf(lambda value : len(value) == 0, BooleanType())

def str2list(value):
    temp = ["." for index in range(len(value))]
    return temp
str2list = udf(str2list, ArrayType(StringType()))

def dictsort(value):
    sort_dict = sorted(value.items(), key=operator.itemgetter(0))
    return reduce(lambda x, y: (0, x[1] + ":" + y[1]), sort_dict)[1]
dictsort = udf(dictsort, StringType())

def find_duplicate(temp):
    return temp.groupBy(F.col("#CHROM"), F.col("POS")).agg((F.count("*")>1).cast("int").alias("e"))\
               .orderBy(F.col("e"), ascending=False)
# 10개 기준임.
def join_split(v_list):
    stage1_list = list(chunks(v_list, 5))
    stage1 = []
    for vcf in stage1_list:
        stage1.append(reduce(reduce_join, vcf))
    return reduce(reduce_join, stage1)

def join_inner(v_list):
    if len(v_list) <= 1:
        return v_list[0]
    else :
        return v_list[0].join(v_list[1], ["#CHROM", "POS"], "inner")

def join_split_inner(v_list, num):
    stage1_list = list(chunks(v_list, num))
    stage1 = []
    for vcf in stage1_list:
        temp = list(chunks(vcf, 2))
        temp = list(map(join_inner, temp))
        stage1.append(reduce(reduce_inner_join, temp))
    return stage1

def vcf_join(v_list):
    chunks_list = list(chunks(v_list, 10))
    map_list = list(map(join_split, chunks_list))
    if len(map_list) <= 1:
        return map_list[0]
    elif len(map_list) == 2:
        return reduce(reduce_join, map_list)
    else:
        flag = True
        while flag == True:
            if len(map_list) == 2:
                flag = False
            else :
                map_list = list(chunks(map_list, 2))
                map_list = list(map(join_split, map_list))
        return reduce(reduce_join, map_list)    

def headerWrite(vcf, vcf_list, index, spark):
    contig = vcf_list[index].toPandas()
    contig_dup = contig["key"].drop_duplicates().tolist()
    contig_len = list(range(len(contig_dup)))
    contig_DF = spark.createDataFrame(list(zip(contig_dup, contig_len)), ["key", "index"])

    return vcf.dropDuplicates(["value"]).join(contig_DF, ["key"], "inner")\
              .select(F.concat_ws("=", F.col("key"), F.col("value")).alias("meta"))\
              .coalesce(1)

def parquet_revalue(vcf, indel_com):
    sample_w = Window.partitionBy(F.col("#CHROM")).orderBy(F.col("POS")).rangeBetween(Window.unboundedPreceding, Window.currentRow)  
    temp = indel_com.join(vcf, ["#CHROM", "POS"], "full").repartition(F.col("#CHROM"))
    sample_name = temp.columns[-1]
    
    temp = temp.withColumn(sample_name, F.last(sample_name, ignorenulls=True).over(sample_w))\
             .withColumn("key", F.map_keys(F.col(sample_name)))\
             .withColumn("index", F.array_remove(F.array_except("FORMAT", "key"), "SB"))\
             .drop("key")

    null_not_value = temp.filter(F.map_keys(F.col(sample_name)) != F.col("FORMAT")).drop("index")\
                     .repartition(F.col("#CHROM"), F.col("POS"), F.col("FORMAT"), F.col(sample_name))\
                     .withColumn(sample_name, F.concat(F.lit("./.:"), index2dict_2(F.col(sample_name), F.col("FORMAT"))))\
                     .drop("FORMAT")
    
    null_value = temp.filter(F.map_keys(F.col(sample_name)) == F.col("FORMAT")).drop("FORMAT", "index")\
             .withColumn(sample_name, F.concat(F.lit("./.:"), F.array_join(F.map_values(F.col(sample_name)), ":")))
    
    value_union = null_not_value.union(null_value)
    return value_union

def index2dict_2(value, FORMAT):
    temp = ["." for i in range(len(FORMAT))]
    FORMAT = dict(zip(FORMAT, temp))
    for index in list(value.keys()):
        if index in FORMAT:
            FORMAT[index] = value[index]
    return ":".join(list(FORMAT.values()))
index2dict_2 = udf(index2dict_2, StringType())

################## unused function
word_len = udf(lambda col : True if len(col) >= 2 else False, returnType=BooleanType())

def info_change(temp):
    some_list = temp.split(";")
    result = [i for i in some_list if i.startswith('DP=')]
    return result[0]
info_change = udf(info_change, StringType())

def list_flat(value):
    value = list(filter(None, value))
    value = list(itertools.chain(*value))
    del value[0]
    return value
list_flat = udf(list_flat, ArrayType(StringType()))

def list2dict_del(value):
    temp = ["." for index in range(len(value))]
    sample = dict(zip(value, temp))
    return sample
list2dict_del = udf(list2dict_del, returnType=MapType(StringType(), StringType()))

def join_split_inner_2(v_list, num):
    stage1 = list(chunks(v_list, num))
    stage2 = [list(value) for value in list(map(chunks, stage1, itertools.repeat(2)))]
    stage3 = list()
    for index in range(len(stage1)):
        temp = list(map(sample_inner, stage2[index]))
        stage3.append(tmep)
    return stage3

def sample_inner_del(v_list):
    if len(v_list) <= 1:
        return v_list[0]
    else :
        return v_list[0].join(v_list[1], ["#CHROM", "POS"], "inner")