import subprocess as subp
import argparse, os
import pandas as pd

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

parser = argparse.ArgumentParser()
parser.add_argument('--H_slaves', nargs='*', help="Hadoop slaves. ex) --slaves master slave1 salve2")
parser.add_argument('--S_slaves', nargs='*', help="Spark slaves. ex) --slaves master slave1 salve2")
parser.add_argument("--equal", type=str2bool, nargs='?',
                        const=True, default=False,
                        help="if Hadoop slaves equal Spark slaves, True.")


args = parser.parse_args()

hadoop_slaves = args.H_slaves
spark_slaves = args.S_slaves
slaves_value = args.equal

if slaves_value == True:
    H_df = pd.DataFrame(hadoop_slaves)
    H_df.to_csv("/usr/local/hadoop/etc/hadoop/workers", header=None, index=None, sep=" ", mode='w')
    H_df.to_csv("/usr/local/spark/conf/slaves", header=None, index=None, sep=" ", mode='w')

else :
    if len(hadoop_slaves) == 0 or len(hadoop_slaves):
        os._exit(00)
    
    H_df = pd.DataFrame(hadoop_slaves)
    S_df = pd.DataFrame(spark_slaves)

    H_df.to_csv("/usr/local/hadoop/etc/hadoop/workers", header=None, index=None, sep=" ", mode='w')
    S_df.to_csv("/usr/local/spark/conf/slaves", header=None, index=None, sep=" ", mode='w')


### hdfs format
subp.call("hdfs namenode -format", shell=True)
subp.call("/usr/local/hadoop/sbin/start-dfs.sh", shell=True)


