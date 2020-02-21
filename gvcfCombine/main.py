# Spark & python function
import findspark
findspark.init()

# Spark function
from pyspark import Row, StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import pandas_udf, udf, explode, array, when
from pyspark.sql.types import IntegerType, StringType, ArrayType,BooleanType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import copy

if __name__ == "__main__":
# Start for Spark Session
    appname = input("appname, folder name : ")
    folder_name = copy.deepcopy(appname) 
    gvcf_count = int(input("gvcf count : "))
    
    spark = SparkSession.builder.master("spark://master:7077")\
                            .appName(appname)\
                            .config("spark.driver.memory", "8G")\
                            .config("spark.driver.maxResultSize", "5G")\
                            .config("spark.executor.memory", "25G")\
                            .config("spark.sql.execution.arrow.enabled", "true")\
                            .config("spark.sql.execution.arrow.fallback.enabled", "true")\
                            .config("spark.network.timeout", "9999s")\
                            .config("spark.files.fetchTimeout", "9999s")\
                            .config("spark.sql.shuffle.partitions", 80)\
                            .config("spark.eventLog.enabled", "true")\
                            .getOrCreate()

    spark.sparkContext.addPyFile("function.py")

    #### addPyFile import #########
    from function import *

    # main
    #gvcf_count = 20
    hdfs = "hdfs://master:9000"
    hdfs_list = hadoop_list(gvcf_count, "/raw_data/gvcf")
    vcf_list = list()

    for index in range(len(hdfs_list)):
        vcf_list.append(preVCF(hdfs + hdfs_list[index].decode("UTF-8"), spark).cache())

    if gvcf_count == 20 or gvcf_count == 15:
        temp1 = join_split(vcf_list[:10])
        temp2 = join_split(vcf_list[10:])
        vcf = reduce(reduce_join, [temp1, temp2])
    elif gvcf_count == 25:
        temp1 = join_split(vcf_list[:10])
        temp2 = join_split(vcf_list[10:20])
        temp3 = join_split(vcf_list[20:])
        temp4 = reduce(reduce_join, [temp1, temp2])
        vcf = reduce(reduce_join, [temp3, temp4])
    elif gvcf_count > 5 and gvcf_count <= 10:
        temp1 = reduce(reduce_join, vcf_list[:5])
        temp2 = reduce(reduce_join, vcf_list[5:])
        vcf = reduce(reduce_join, [temp1, temp2])
    elif gvcf_count <= 5:
        vcf = reduce(reduce_join, vcf_list)
    
    vcf = with_vale(vcf).cache()
    vcf.count()    
    print("full outer join with updated value!!\n")
        
    # info window
    info_window = Window.partitionBy("#CHROM").orderBy("POS")
    vcf_not_indel = vcf.withColumn("INFO", when(F.col("INFO").isNull(), F.concat(F.lit("END="), F.lead("POS", 1).over(info_window) - 1))\
                                .otherwise(F.col("INFO")))

    # dropduplicates할 때, indel 삭제되는 경우 있음.
    # indel union
    split_col = F.split("REF_temp", '_')
    indel = indel_union(vcf)

    indel_com = unionAll(*[indel, vcf_not_indel])\
                    .orderBy(F.col("#CHROM"), F.col("POS"))\
                    .dropDuplicates(["#CHROM", "POS"])\
                    .cache()                 
    indel_com.count()

    indel.unpersist()
    vcf.unpersist()
    print("indel add completed!!\n")

    # parquet write
    sample_w = Window.partitionBy(F.col("#CHROM")).orderBy(F.col("POS")).rangeBetween(Window.unboundedPreceding, Window.currentRow)  
    parquet_list = list()
    cnt = 0

    ## sample & info comibne, value window
    for index in range(len(vcf_list)):
        temp = indel_com.select(["#CHROM","POS"])\
            .join(vcf_list[index].select(["#CHROM", "POS"] + vcf_list[index].columns[7:]), ["#CHROM", "POS"], "full")
            # sample window    
        temp = temp.withColumn(temp.columns[-1], F.last(temp.columns[-1], ignorenulls=True).over(sample_w))
        parquet_list.append(temp)

    ## parquet write
    for parquet in join_split_inner(parquet_list):
        time.sleep(30)
        parquet.write.mode('overwrite')\
               .parquet("/raw_data/output/gvcf_output/"+ folder_name + "//" + "sample_" + str(cnt) + ".g.vcf")
            
        for index in range(cnt, cnt + 3):
            if len(vcf_list) - 1 < index:
                break
            vcf_list[index].unpersist()
        cnt += 3
        print("sample" + str(cnt - 1) + "done !!")

    indel_com.write.mode('overwrite')\
                    .parquet("/raw_data/output/gvcf_output/" + folder_name + "//info.g.vcf")
        
            #.withColumn(sample_name, value_change(F.col(sample_name)))