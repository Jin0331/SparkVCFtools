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
        compared_arr = ["#CHROM", "POS", "REF"]
        if vcf_data.columns[index] not in compared_arr:
            vcf_data = vcf_data.withColumn(vcf_data.columns[index], F.array(vcf_data.columns[index]))
            vcf_data = vcf_data.withColumnRenamed(vcf_data.columns[index], vcf_data.columns[index] + "_" + sample_name)     
    vcf_data = vcf_data.withColumnRenamed("REF", "REF_" + sample_name)    
    vcf_data = vcf_data.withColumn("count", F.length(vcf_data.columns[3]))
    
    # window & split col parameter 
    sample_ref = vcf_data.columns[3]
    indel_window = Window.partitionBy("#CHROM").orderBy("POS")
    split_col = F.split("REF_temp", '_')
    ###
    
    vcf_data = vcf_data.withColumn("count", when(F.col("count") >= 2, F.lead("POS", 1).over(indel_window) - F.col("POS") - F.lit(1))\
                                           .otherwise(F.lit(0)))

    not_indel = vcf_data.drop("count").withColumn(sample_ref, F.array(F.col(sample_ref)))
    indel = vcf_data.filter(F.col("count") >= 1)\
                .withColumn(sample_ref, ref_melt(F.col(sample_ref), F.col("count"))).drop("count")\
                .withColumn(sample_ref, explode(F.col(sample_ref))).withColumnRenamed(sample_ref, "REF_temp")\
                .withColumn(sample_ref, F.array(split_col.getItem(0))).withColumn('POS_var', split_col.getItem(1))\
                .drop(F.col("REF_temp")).withColumn("POS", (F.col("POS") + F.col("POS_var")).cast(IntegerType()))\
                .drop(F.col("POS_var"))\
                .withColumn(vcf_data.columns[2], F.array(F.lit(".")))\
                .withColumn(vcf_data.columns[4], F.array(F.lit("*, <NON_REF>")))
    
    return not_indel.unionByName(indel) 

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

def ref_melt(ref, count):
    temp = list(ref)[1:count + 1]
    return_str = []
    for num in range(len(temp)):
        return_str.append(temp[num] + "_" + str(int(num + 1)))
    return return_str
ref_melt = udf(ref_melt, ArrayType(StringType()))


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
        else :
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

def join_split_inner_2(v_list, num):
    stage1_list = list(chunks(v_list, num))
    stage1 = []
    for vcf in stage1_list:
        stage1.append(reduce(reduce_inner_join, vcf))
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
               .withColumnRenamed("#CHROM", "CHROM")
    
    # scala UDF
    null_not_value = temp.filter(F.map_keys(F.col(sample_name)) != F.col("FORMAT"))\
                     .repartition(200,F.col("CHROM"), F.col("POS"), F.col("FORMAT"), F.col(sample_name))\
                     .selectExpr("CHROM", "POS","index2dict({}, FORMAT) as {}".format(sample_name, sample_name))\
                     .withColumn(sample_name,  F.concat(F.lit("./.:"), F.array_join(F.col(sample_name), ":")))
    
    null_value = temp.filter(F.map_keys(F.col(sample_name)) == F.col("FORMAT")).drop("FORMAT")\
                     .withColumn(sample_name, F.concat(F.lit("./.:"), F.array_join(F.map_values(F.col(sample_name)), ":")))
    
    value_union = null_not_value.union(null_value).withColumnRenamed("CHROM", "#CHROM")
    return value_union