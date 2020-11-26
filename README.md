# (KCC2020 우수논문) 분산병렬 클러스터를 이용한 GVCF(Genome Variant Call Format) 파일 정렬/병합

# 초록
차세대 시퀀싱(next generation sequencing, NGS) 기법의 발달로 인하여 방대한 유전체 데이터의 분산, 병렬처리가 필수적인 방법론으로 대두되고 있다. NGS 유전체 데이터 처리는 데이터 규모로 인하여 일반적으로 매우 긴 실행 시간을 필요로 한다. 본 논문에서는 GVCF 파일 정렬/병합 실행 시간을 단축하기 위하여 분산병렬 클러스터를 이용한 새로운 GVCF 파일 정렬/병합 모듈을 제안한다. 제안하는 모듈에서는 분산병렬 클러스터인 Spark를 사용하며, 클러스터 내의 자원을 효율적으로 사용하기 위해 GVCF 파일의 특성을 고려한 두 단계의 과정으로 정렬/병합을 진행한다. 성능 평가를 위하여 GATK의 CombineGVCFs 모듈과 제안하는 모듈의 GVCF 파일의 개수에 따른 정렬/병합 실행시간을 측정하여 비교 및 평가를 진행하였다. 실험 결과에 의하여 제안하는 방식이 실행시간을 매우 효율적으로 단축시키고 있음을 확인하였으며, 제안하는 방식의 유용성을 입증하였다.


# SparkVCFtools

``Hadoop 3.1.1 & Spark 2.4.5``

* VCF Merge

* gVCFCombine

### **For use**

* gVCF

``python3 gvcfCombine_info.py --appname [test] --c [20] <<- info file!``

``python3 gvcfCombine_sample.py --infoName [test] --c [20] <<- sample file!``
