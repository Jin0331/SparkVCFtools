import findspark
findspark.init()

# Spark & python function
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc, asc, coalesce, broadcast
from pyspark import Row
    
if __name__ == "__main__":
    spark = SparkSession.builder.master("spark://master:7077")\
                        .appName("vcf_merge_1212")\
                        .config("spark.executor.memory", "18G")\
                        .config("spark.executor.core", "3")\
                        .config("spark.sql.shuffle.partitions", 20)\
                        .getOrCreate()

    spark.sparkContext.addPyFile("SparkVCFtools/vcfFilter.py")
    spark.sparkContext.addPyFile("SparkVCFtools/SelectCol.py")

    #### addPyFile import #########
    from SelectCol import *
    from vcfFilter import *
    chr_remove_udf = udf(chr_remove)
    ###############################
    case = preVCF("hdfs://master:9000/vcf/case_merge_vcf", 0, spark).cache()
    case.count()

    # control vcf load
    control = preVCF("hdfs://master:9000/vcf/control_vcf", 1, spark).cache()
    control.count()

    # case & control indexing
    case_col = len(case.columns)
    control_col = len(control.columns)

    # merge schema
    col = case.columns + control.columns
    header = col[:case_col] + col[case_col + 9:]

    ### join expresion
    joinEX = [
                case['CHROM'] == control['CHROM_temp'],
                case['POS'] == control['POS_temp'],
                case['REF'] == control['REF_temp']
            ]

    join_result = case.join(control, joinEX, 'full').cache()
    join_result.count()

    # unpersist
    case.unpersist()
    control.unpersist()

    # write delim
    join_result.rdd.map(lambda row : selectCol(row, case_col, control_col))\
                .toDF(header).dropDuplicates(['CHROM', 'POS'])\
                .write.option("delimiter", "\t").csv("hdfs://master:9000/vcf/merge_1216_re6.txt")