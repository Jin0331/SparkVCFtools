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
                            .config("spark.driver.maxResultSize", "8G")\
                            .config("spark.executor.memory", "24G")\
                            .config("spark.executor.core", 3)\
                            .config("spark.sql.execution.arrow.enabled", "false")\
                            .config("spark.sql.execution.arrow.fallback.enabled", "false")\
                            .config("spark.network.timeout", "9999s")\
                            .config("spark.files.fetchTimeout", "9999s")\
                            .config("spark.sql.shuffle.partitions", 40)\
                            .config("spark.eventLog.enabled", "true")\
                            .getOrCreate()

    spark.sparkContext.addPyFile("function.py")

    #### addPyFile import #########
    from function import *

    # main
    hdfs = "hdfs://master:9000"
    hdfs_list = hadoop_list(gvcf_count, "/raw_data/gvcf")

    vcf_list = list()
    vcf_join_list = list()  
    addIndex_udf = F.udf(addIndex, returnType=IntegerType())

    # gVCF load

    for index in range(len(hdfs_list)):
        if index == 0:
            vcf_list.append(preVCF(hdfs + hdfs_list[index].decode("UTF-8"), 0, spark).cache())
            inner_pos = vcf_list[index].select(F.col("#CHROM"), F.col("POS"), F.col("REF"))
            info = vcf_list[index].select(vcf_list[index].columns[:9])
        else:
            vcf_list.append(preVCF(hdfs + hdfs_list[index].decode("UTF-8"), 1, spark).cache())
            inner_pos_right = vcf_list[index].select(F.col("#CHROM"), F.col("POS"), F.col("REF"))
            
            if index % 2 != 0:
                vcf_join_list.append(sample_join(vcf_list[index - 1], vcf_list[index]).cache())
            if index % 2 == 0 and index == len(hdfs_list) - 1:
                vcf_join_list.append(vcf_list[index].select(F.col("#CHROM"), F.col("POS"), F.col("REF"), vcf_list[index].columns[-1]).cache())
            
            # for column null value
            info = info.join(vcf_list[index].select(vcf_list[index].columns[:9]), ["#CHROM", "POS", "REF"], "full")\
                .withColumn("ID", when(F.col("ID").isNull(), F.col("ID_temp")).otherwise(F.col("ID")))\
                .withColumn("ALT",when(F.col("ALT").isNull(), F.col("ALT_temp")).otherwise(F.col("ALT")))\
                .withColumn("FORMAT", when(F.col("FORMAT").isNull(), F.col("FORMAT_temp")).otherwise(F.col("FORMAT")))\
                .withColumn("QUAL", F.lit(".")).withColumn("FILTER", F.lit("."))\
                .withColumn("INFO", when(F.col("INFO").startswith("END") == False, F.col("INFO"))\
                            .when(F.col("INFO_temp").startswith("END") == False, F.col("INFO_temp")))\
                .drop("INFO_temp", "ID_temp", "ALT_temp", "FORMAT_temp", "QUAL_temp", "FILTER_temp")
            
            # for index
            inner_pos = inner_pos.join(inner_pos_right, ["#CHROM", "POS", "REF"], "inner")

            if index == len(hdfs_list) - 1:
                info = info.dropDuplicates()
                inner_pos = inner_pos.dropDuplicates()      
    
    # indel union
    split_col = F.split("REF_temp", '_')
    indel = info.filter(word_len(F.col("REF")))\
                .withColumn("REF", ref_melt(F.col("REF"))).withColumn("REF", ref_concat(F.col("REF")))\
                .withColumn("REF", explode(F.col("REF"))).withColumnRenamed("REF", "REF_temp")\
                .withColumn('REF', split_col.getItem(0)).withColumn('POS_var', split_col.getItem(1))\
                .drop(F.col("REF_temp")).withColumn("POS", (F.col("POS") + F.col("POS_var")).cast(IntegerType()))\
                .drop(F.col("POS_var"))\
                .withColumn('ID', F.lit("."))\
                .withColumn('ALT', F.lit("*,<NON_REF>"))

    # window
    info_window = Window.partitionBy("#CHROM").orderBy("POS")
    info = info.withColumn("INFO", when(F.col("INFO").isNull(), F.concat(F.lit("END="), F.lead("POS", 1).over(info_window) - 1))\
                                .otherwise(F.col("INFO")))\
            .orderBy(F.col("#CHROM"), F.col("POS"))

    info = unionAll(*[info, indel]).orderBy(F.col("#CHROM"), F.col("POS")).cache()
    info.count()

    # pos_index
    inner_pos = spark.createDataFrame(inner_pos.drop(F.col("REF")).orderBy(F.col("#CHROM"), F.col("POS"))\
                .toPandas().groupby("#CHROM", group_keys=False).apply(sampling_func, ran = 13).sort_index())\
                .withColumnRenamed("#CHROM", "chr_temp")\
                .withColumnRenamed("POS", "pos_temp")\
                .orderBy(F.col("#CHROM"), F.col("POS")).cache()
    inner_pos.count()
    print("gVCF lode & join (inner, full) done@")

    pos_index = Window.partitionBy("#CHROM").orderBy("POS").rangeBetween(Window.unboundedPreceding, Window.currentRow)
    ex = [info["#CHROM"] == inner_pos["chr_temp"], info["POS"] == inner_pos["pos_temp"]]
    temp = info.select(F.col("#CHROM"), F.col("POS")).join(inner_pos, ex, "full")\
            .drop(F.col("chr_temp"))\
            .dropDuplicates()\
            .withColumn("POS_INDEX", when(F.col("pos_temp").isNull(), F.last(F.col("pos_temp"), ignorenulls=True).over(pos_index))\
                            .otherwise(F.col("pos_temp")))\
            .drop(F.col("pos_temp")).orderBy(F.col("#CHROM"), F.col("POS"))

    info_index = info.join(temp, ["#CHROM", "POS"], "inner").orderBy(F.col("#CHROM"), F.col("POS")).dropDuplicates()\
                    .repartition(F.col("#CHROM"), F.col("POS_INDEX")).cache()

    info_index.count()
    info.unpersist()
    print("POS_INDEX done!")

    # sample window & parquet write
    vcf_list_pos_index = list()
    sample_w = Window.partitionBy(F.col("#CHROM"), F.col("POS_INDEX")).orderBy(F.col("POS")).rangeBetween(Window.unboundedPreceding, Window.currentRow)  

    for index in range(len(vcf_join_list)):
        temp = info_index.select(["#CHROM","POS","REF","POS_INDEX"])\
            .join(vcf_join_list[index].select(["#CHROM", "POS", "REF"] + vcf_join_list[index].columns[3:]), ["#CHROM", "POS", "REF"], "full")
        
        # sample window
        for sample_name in temp.columns[4:]:     
            temp = temp.withColumn(sample_name, when(F.col(sample_name).isNull(), F.last(sample_name, ignorenulls=True).over(sample_w))\
                                                    .otherwise(F.col(sample_name)))\
                        .withColumn(sample_name, value_change(F.col(sample_name)))
        vcf_list_pos_index.append(temp)
        
    for write_parquet in vcf_list_pos_index:
        write_parquet.drop(F.col("POS_INDEX"))\
                    .write.partitionBy("#CHROM")\
                    .mode('overwrite')\
                    .parquet("/raw_data/output/gvcf_output/"+ folder_name + "//" + "_".join(write_parquet.columns[4:]) + ".g.vcf")
        
    info_index.drop(F.col("POS_INDEX"))\
            .write.partitionBy("#CHROM").mode('overwrite')\
            .parquet("/raw_data/output/gvcf_output/" + folder_name + "//info.g.vcf")
    
    print("paquet write done!!")