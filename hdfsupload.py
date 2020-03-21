import subprocess as subp
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--path', help="The files path for uploading hdfs. /home/[user-name]/[files]")
parser.add_argument('--type', help="gvcf or vcf")
args = parser.parse_args()

path = args.path
file_type = args.type

if __name__ == '__main__':
    subp.call("hdfs dfs -rm -r /upload", shell=True)
    subp.call("hdfs dfs -mkdir /upload", shell=True)

    if file_type == "vcf":
        subp.call("hdfs dfs -mkdir /upload/vcf", shell=True)
        subp.call("hdfs dfs -copyFromLocal %s/* /upload/vcf/" % (path), shell=True)
    elif file_type == "gvcf":
        subp.call("hdfs dfs -mkdir /upload/gvcf", shell=True)
        subp.call("hdfs dfs -copyFromLocal %s/* /upload/gvcf/" % (path), shell=True)
    else:
        print("invaild option!. --type vcf or --type gvcf")
        os._exit(00)

    