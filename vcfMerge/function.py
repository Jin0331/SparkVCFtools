# spark function
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, desc, asc
import pyspark.sql.functions as F
from pyspark import Row
from pyspark.sql.types import IntegerType

# python function
import re
import copy
import subprocess
from functools import reduce 
import argparse

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

def gatkVCF(hdfs, spark):
    vcf = spark.sparkContext.textFile(hdfs)
    #header_contig = vcf.filter(lambda x : re.match("^#", x))
    col_name = vcf.filter(lambda x : x.startswith("#CHROM")).first().split("\t")
    vcf_data = vcf.filter(lambda x : re.match("[^#][^#]", x))\
                    .map(lambda x : x.split("\t"))\
                    .toDF(col_name)
    return vcf_data

def headerWrite(vcf, vcf_list, index, spark):
    contig = vcf_list[index].toPandas()
    contig_dup = contig["key"].drop_duplicates().tolist()
    contig_len = list(range(len(contig_dup)))
    contig_DF = spark.createDataFrame(list(zip(contig_dup, contig_len)), ["key", "index"])

    return vcf.dropDuplicates(["value"]).join(contig_DF, ["key"], "inner")\
            .select(F.concat_ws("=", F.col("key"), F.col("value")).alias("meta"))\
            .coalesce(1)

def unionAll(*dfs):
    return reduce(DataFrame.unionByName, dfs) 

def preVCF(hdfs, flag, spark): # hdfs://, flag 0 == lhs, 1 == rhs
    vcf = spark.sparkContext.textFile(hdfs)
    col_name = vcf.filter(lambda x : x.startswith("#CHROM")).first().split("\t")
    vcf_data = vcf.filter(lambda x : re.match("[^#][^#]", x))\
                        .map(lambda x : x.split("\t"))

    step1 = vcf_data.map(alt_filter).toDF(col_name)
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

def rowTodict(format_, row):
    return_col = []
    for ref in row:
        temp_dict = dict()
        temp = ref.split(":")
        for index in range(len(temp)):
            temp_dict[format_[index]] = temp[index]
        return_col.append(temp_dict)
    return return_col

def dictToFormat(col_value, d_format):
    result_return = []
    for temp in col_value:
        temp_col = []
        for keys in d_format:
            if keys in temp:
                temp_col.append(temp[keys])
            else:
                temp_col.append(".")
        result_return.append(":".join(temp_col))
    return tuple(result_return)
    
def selectCol(row, lhs_len, rhs_len, missing = "."):   
    # INFO re      
    AC, AN = 0, 0 
    if row[9] == None :
        GT = row[lhs_len + 9:]
    elif row[lhs_len + 9] == None :
        GT = row[9:lhs_len]
    else:
        GT = row[9:lhs_len]+row[lhs_len + 9:]
        
    for temp in GT:
        if temp == None:
            break
        else:
            if "0/1:" in temp:
                AC += 1
                AN += 1
            elif "1/1:" in temp:
                AC += 2
                AN += 1
            elif "0/0:" in temp:
                AN += 1
    AN *= 2
    
    # rhs가 null
    if row.CHROM_temp == None:
        temp = tuple()
        for ref in range(rhs_len - 9):
            temp += (missing,) # GC
            
        if row[7] != ".":
            info = row[7].split(";")
            del info[0]
            del info[1]
            info.insert(1, "SF=0")
            info = ";".join(info)
            info = ("AC="+str(AC)+";AN="+str(AN)+";"+info,)
        else :
            info = ("AC="+str(AC)+";AN="+str(AN)+";SF=0", )
        return row[:5] + (float(row.QUAL),) + (row.FILTER, ) + info + (row[8],) + row[9:lhs_len] + temp
    # lhs가 null
    elif row.CHROM == None :
        temp = tuple()
        for ref in range(lhs_len - 9):
            temp += (missing,) # GC
        if row[lhs_len + 7] != ".":
            info = row[lhs_len + 7].split(";")
            del info[0]
            del info[1]
            info.insert(1, "SF=1")
            info = ";".join(info)
            info = ("AC="+str(AC)+";AN="+str(AN)+";"+info,)
        else :
            info = ("AC="+str(AC)+";AN="+str(AN)+";SF=1",)
            
        return row[lhs_len:lhs_len + 5] + (float(row.QUAL_temp), ) + (row.FILTER_temp, ) + info + (row.FORMAT_temp,) + temp + row[lhs_len + 9:]
    # case, control 둘다 존재
    else:
        # QUAL re-calculation
        format_, lhs_format, rhs_format = row[8].split(":") + row[lhs_len + 8].split(":"), row[8].split(":"), row[lhs_len + 8].split(":")
        dup_format, lhs_col, rhs_col = [], rowTodict(lhs_format, row[9:lhs_len]), rowTodict(rhs_format, row[lhs_len + 9:])
        # format duplicate
        for dup in format_:
            if dup not in dup_format:
                dup_format.append(dup)
        
        result_lhs, result_rhs = dictToFormat(lhs_col, dup_format), dictToFormat(rhs_col, dup_format)
        # qual re-calcualtion # 100
        col_total = lhs_len + rhs_len - 18
        lhs_QUAL = float(row.QUAL) * ((lhs_len - 9) / col_total)
        rhs_QUAL = float(row.QUAL_temp) * ((rhs_len - 9) / col_total)
        QUAL = lhs_QUAL + rhs_QUAL
        
        if row[lhs_len + 7] == ".":
            info = row[7]
        elif row[7] == ".":
            info = row[lhs_len + 7]
        else:
            info = row[7]
        info = info.split(";")      
        del info[0]
        del info[1]
        info.insert(1, "SF=0,1")
        info = ";".join(info)
        info = ("AC="+str(AC)+";AN="+str(AN)+";"+info,)        
        #return row[:5]+(QUAL,)+(row[6],)+info+(row[8],)+row[9:lhs_len]+row[lhs_len + 9:]
        return row[:5]+(QUAL,)+(row[6],)+info+(":".join(dup_format), )+result_lhs + result_rhs
