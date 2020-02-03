import findspark
findspark.init()

# Spark & python function
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc, asc
from pyspark import Row
    
if __name__ == "__main__":
    spark = SparkSession.builder.master("spark://master:7077")\
                            .appName("gVCF_combine")\
                            .config("spark.driver.memory", "8G")\
                            .config("spark.driver.maxResultSize", "8G")\
                            .config("spark.executor.memory", "24G")\
                            .config("spark.executor.core", 3)\
                            .config("spark.sql.execution.arrow.enabled", "true")\
                            .config("spark.sql.execution.arrow.fallback.enabled", "true")\
                            .config("spark.network.timeout", 10000000)\
                            .config("spark.sql.shuffle.partitions", 40)\
                            .config("spark.eventLog.enabled", "true")\
                            .getOrCreate()

    spark.sparkContext.addPyFile("vcfFilter.py")
    spark.sparkContext.addPyFile("SelectCol.py")

    #### addPyFile import #########
    from SelectCol import *
    from vcfFilter import *

    # output file name
    output = input("parquet output name : ")

    # load case.vcf from HDFS
    case = preVCF("hdfs://master:9000/raw_data/vcf/case_merge.vcf", 0, spark)
    control = preVCF("hdfs://master:9000/raw_data/vcf/control.vcf", 1, spark)

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
    join_result = case.join(control, joinEX, 'full')
    
    # write delim \t
    join_result.rdd.map(lambda row : selectCol(row, case_col, control_col))\
                .toDF(header).dropDuplicates(['CHROM', 'POS'])\
                .write.mode('overwrite').option("delimiter", "\t")\
                .csv("hdfs://master:9000/raw_data/vcf/output/" + output + ".txt")