def preVCF(hdfs, flag, spark):
    vcf = spark.sparkContext.textFile(hdfs)
    # drop ---> QUAL FILTER column
    header_contig = vcf.filter(lambda x : re.match("^#", x))
    col_name = vcf.filter(lambda x : x.startswith("#CHROM")).first().split("\t")
    vcf_data = vcf.filter(lambda x : re.match("[^#][^#]", x))\
                       .map(lambda x : x.split("\t"))\
                       .toDF(col_name)\
                       .withColumn("POS", F.col("POS").cast(IntegerType()))\
                       .drop(F.col("QUAL")).drop(F.col("FILTER"))
    
    if flag == 1:
        for index in range(len(vcf_data.columns[:9])):
            compared_arr = ["#CHROM", "POS", "REF"]
            if vcf_data.columns[index] in compared_arr:
                continue
            vcf_data = vcf_data.withColumnRenamed(vcf_data.columns[index], vcf_data.columns[index] + "_temp") 
    
    return vcf_data

def endRecalc(pos, left, right, flag=None):
    #endRecalc(left_pos, right_pos, left, right, flag):
    if left.startswith("END=") == False or right.startswith("END=") == False :
        if flag == "left":
            return left
        elif flag == "right":
            return right
        else:
            return left
    else :
        left_end, right_end = int(left.replace("END=", "")), int(right.replace("END=", ""))
        if pos == left_end or pos == right_end:
            return "."
        else :
            if left_end > right_end:
                end_position = right_end
            else:
                end_position = left_end
            end_position = "END=" + str(end_position)
            return end_position
    
def selectCol(row, sample):   
    if row["ID_temp"] == None:       
        return_row = row[:2] + (row["ID"], row["REF"], row["ALT"], ".", ".", endRecalc(row["POS"], row["INFO"], row["INFO_temp"], "left"), row["FORMAT"])     
    elif row["ID"] == None:
        return_row = row[:2] + (row["ID_temp"], row["REF"], row["ALT_temp"], ".", ".", endRecalc(row["POS"], row["INFO"], row["INFO_temp"], "right"), row["FORMAT_temp"]) 
    else:
        return_row = row[:2] + (row["ID"], row["REF"], row["ALT"], ".", ".", endRecalc(row["POS"], row["INFO"], row["INFO_temp"]), row["FORMAT"])
        
    for sample_col in sample[:-1]:
        return_row += (row[sample_col],)
        return_row += (row[-1],)
    return return_row

def hadoop_list(length, hdfs):
    args = "hdfs dfs -ls "+ hdfs +" | awk '{print $8}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    all_dart_dirs = s_output.split()
    
    return all_dart_dirs[:length]