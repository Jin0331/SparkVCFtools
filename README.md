![image](https://user-images.githubusercontent.com/42958809/172269760-e3d586c3-8df0-4f1d-b6e4-e9c0043e936a.png)

```
- KCC2020 데이터베이스분야 최우수논문
- DOI : 10.5626/JOK.2021.48.3.358
```

# Abstract
With the development of next-generation sequencing (NGS) techniques, a large volume of genomic data is being produced and accumulated, and parallel and distributed computing has become an essential tool. Generally, NGS data processing entails two main steps: obtaining read alignment results in BAM format and extracting variant information in genome variant call format (GVCF) or variant call format (VCF). However, each step requires a long execution time due to the size of the data. In this study, we propose a new GVCF file sorting/merging module using distributed parallel clusters to shorten the execution time. In the proposed algorithm, Spark is used as a distributed parallel cluster. The sorting/merge process is performed in two steps according to the structural characteristics of the GVCF file in order to use the resources in the cluster efficiently. The performance was evaluated by comparing our method with the GATK's CombineGVCFs module based on sorting and merging execution time of multiple GVCF files. The outcomes suggest the effectiveness of the proposed method in reducing execution time. The method can be used as a scalable and powerful distributed computing tool to solve the GVCF file sorting/merge problem.


# SparkVCFtools

``Hadoop 3.1.1 & Spark 2.4.5``

* VCF Merge

* gVCFCombine

### **For use**

* gVCF

``python3 gvcfCombine_info.py --appname [test] --c [20] <<- info file!``

``python3 gvcfCombine_sample.py --infoName [test] --c [20] <<- sample file!``
