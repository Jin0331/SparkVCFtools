# 🔧 **SparkVCFTools**

> 기간 : 2020.03 - 2020.06 (약 3개월)
>
> 기획/개발 - 1인
>
> 프로젝트 환경 - Ubuntu 18.04 LTS Bionic Beaver
>
> **KCC2020(한국컴퓨터종합학술대회) 데이터베이스분야 최우수논문**

[![dh](https://github.com/Jin0331/SparkVCFtools/assets/42958809/ffdec167-b116-4be6-b190-e7e4294fd0ed)](https://www.dbpia.co.kr/Journal/articleDetail?nodeId=NODE10536773)

> **이진우, 원정임, 윤지희, "분산병렬 클러스터 컴퓨팅을 이용한 GVCF(Genome Variant Call Format) 파일의 정렬 및 병합 방법", Journal of KIISE(KCI 우수등재), 2021. (제1저자)**
>
>***DOI : 10.5626/JOK.2021.48.3.358***

## 🔧 **한줄소개**

***Apache Spark를 이용한 gVCF(genome variant call format) 파일의 정렬과 병합을 신속하게 도와주는 Software***

<br>

## 🔧 **적용 기술**

* ***언어***

    Python, Scala

* ***프레임워크***

    Apache Hadoop, Apache Spark, Docker, Docker Swarm


* ***오픈 소스***

    PySpark

* ***버전 관리***

  ​	Git / Github

  <br>


## 🔧 **개발 배경**

1. 유전자/변이 분석에 일반적으로 사용되는 Broad Institute의 GATK(genome analysis toolkit)는 NGS(차세대 시퀀싱) 데이터 분석 및 처리를 위한 단계별 모듈을 제공
2. NGS 데이터(FastQ 파일)는 품질 평가 및 정렬 과정을 거쳐 BAM 파일로 변환
3. “HaplotypeCaller” 모듈은 BAM 파일로부터 변이 추출 연산을 수행하여 그 결과로 GVCF 파일을 생성
4. “CombineGVCFs” 모듈은 다수의 GVCF 파일을 정렬/병합하여 단일 GVCF 파일을 생성
5. 생성된 단일 GVCF 파일은 “GenotypeGVCFs” 모듈을 거쳐 최종적인 VCF(variant call format) 파일로 생성되고 이를 통하여 유전자/변이 분석 진행

![image-3](https://github.com/Jin0331/SparkVCFtools/assets/42958809/fcbef258-29c2-4c17-9785-00e63612caef) | ![image-5](https://github.com/Jin0331/SparkVCFtools/assets/42958809/a2e3c432-c935-405b-b1cc-ca6b936a62c2)
---|---

>CombineGVCFs 모듈은 개인별로 생성된 다수의 GVCF(genome variant call format) 파일을 정렬 및 병합하여 단일 GVCF 파일을 생성하는 기능을 수행
하지만, 다수의 GVCF 파일에 속한 변이(행)을 순차적으로 접근하여, 비교 분석 후 정렬 및 병합을 진행하기 때문에 다른 모듈에 비하여 매우 긴 시간이 소요
>
>따라서 본 연구에서는 기존 모듈의 실행시간을 단축하기 위하여, 분산병렬 클러스터(Spark)를 이용한 GVCF 파일 정렬/병합 모듈을 제안

<br>

## 🔧 **gVCF 파일 형식**

* GATK HaplotypeCaller를 실행할 때 -ERC GVCF (또는 -ERC BP_RESOLUTION) 옵션을 넣고 실행하게 되면 output으로 GVCF 생성 

* GVCF는 VCF의 한 종류로, genomic VCF를 뜻함

* 대상이 가진 모든 변이를 표현함. 이때 변이는 GVCF 파일에서 row(행)을 나타냄

* variant site(변이정보 존재)과 non-variant block(변이정보 존재하지 않음)으로 해당 대상의 모든 변이정보를 나타냄

    <img width="733" alt="image-8" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/50cb2f4d-464e-4306-b027-55cf1819003e">
    
    ![alt text](https://github.com/Jin0331/SparkVCFtools/assets/42958809/7263e680-a0fe-4e31-bbad-10f65cbd3bd9) | ![alt text](https://github.com/Jin0331/SparkVCFtools/assets/42958809/4f32effe-f58e-4757-80a1-e266b41e7922)
    ---|---

<br>

## 🔧 **개발 아이디어**

* CombineGVCFs의 GVCF 파일 정렬 및 병합 예시
    <img width="816" alt="image-11" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/51bb3edf-9226-4b46-895c-15ba0de4e8c4">

* Fixed fields는 변이의 정보를 나타내며, Genotype fields 해당 변이에 대한 각 대상이 가진 유전적 정보를 나타냄. 

* 즉, Fixed fields에 Genotype fields의 정보가 일치되는 형태

* 따라서 주어진 GVCF 파일의 Fixed fields 열을 정렬 및 병합하고(Phase 1 단계), 정렬 및 병합된 Fixed fields 열에 Genotype fields 열을 정렬 및 병합(Phase 2)하는 두 단계로 진행
  
    ![image-7](https://github.com/Jin0331/SparkVCFtools/assets/42958809/5d863968-828d-4367-b269-55cff17e57cb)
  
<br>

## 🔧 **핵심 기능**

* ***전체 구성도***
  
  <img width="500" alt="image-12" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/bb5df255-13b5-4b66-8e7d-8c7ce412f041">

* ***Phase 1***

    * Phase 1 Fixed fields, FORMAT 열 병합 알고리즘 및 예시

        <img width="520" alt="image-13" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/ed11c4a0-9d37-4145-93a0-2de0af742fa9"> | <img width="781" alt="image-15" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/20d476d5-f4e2-4286-9209-4db9320f0367">
        ---|---

    * Phase 1 Fixed fields, FORMAT 열 정렬 알고리즘 및 예시

        <img width="767" alt="image-16" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/9a163a11-5497-4acf-a92d-457a12effcdd">

    * Phase 1 Action

        <img width="500" alt="image-17" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/d084226f-3628-45d1-ae5e-5379d46854d8">

* ***Phase 2***

    * Phase 2 Genotype fields 열 병합 및 정렬 알고리즘

        <img width="780" alt="image-18" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/ecae774a-200b-4f6a-b0bb-7df5a0517099">

        * 예시

            <img width="780" alt="image-19" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/2f99e14b-2438-4fae-bea0-d3442d48948f"> | <img width="687" alt="image-20" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/ddd5bd72-81b9-40e8-ba9b-d5630e2b9be7">
            ---|---

    * Phase 1의 병합된 GVCF 파일(Fixed fields + FORMAT 열)과 Phase 2의 병합된 GVCF 파일(Genotype fields 열)과 병합하여 최종적으로 병합된 GVCF 파일을 생성

        <img width="755" alt="image-21" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/d2505fbf-30ee-4223-a669-029f58891215">

    * Phase 2 Action

        <img width="500" alt="image-22" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/863e3f3f-fd62-43f1-b6fc-c471d0622a78">


## 🔧 **성능 평가**

* ***Docker, Docker Swarm을 이용한 클러스터 구축***

    <img width="765" alt="image-23" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/06239a30-188d-4738-92a7-9443aaf6168b">

* ***사용 데이터***

    <img width="746" alt="image-24" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/a145f815-992f-4973-b562-bdfc5d75c73c">

* ***GATK의 CombineGVCFs와의 비교***

    <img width="500" alt="image-25" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/9737635f-c9c8-4a88-bae4-a00df5a00568">

    >CombineGVCFs는 GVCF 파일의 개수가 증가할수록 실행시간이 큰 폭 증가​
    >반면에 제안하는 모듈은 실행시간이 일정한 폭으로 증가.​
    >병합되는 GVCF 파일의 개수에 따라 제안하는 모듈은 CombineGVCFs에 비해 실행시간이 최소 75%에서 최대 87%까지 단축되는 것을 확인​

<br>

* ***Worker 개수에 따른 GVCF 파일 병합 실행시간 비교***

    <img width="500" alt="image-26" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/0c46f0ad-538e-462e-be2f-070b5deece1f">

    >실행시간의 비교는 제안하는 모듈로 진행하며, GVCF 파일 20개에 대한 병합​
    >
    >Spark 클러스터에서 사용 가능한 Worker의 수가 증가함에 따라 병합의 실행시간이 감소​
    >
    >특히, Worker를 2개 사용했을 때와 10개 사용했을 때의 병합 실행시간은 약 3배 정도의 차이가 발생​

<br>

* ***Phase 1과 Phase 2의 정렬 및 병합 실행시간 비교***
​
    <img width="500" alt="image-27" src="https://github.com/Jin0331/SparkVCFtools/assets/42958809/4cd993d3-ff72-408e-ac5d-6b7038b1b00c">

    >Worker 개수는 10개이고, Core와 Memory는 50, 250GB으로 설정​
    >
    >병합되는 GVCF 파일이 2개부터 10개까지는 Phase 2의 병합 실행시간이 빠른 것으로 나타나지만, 15개부터는 Phase 1보다 병합 실행시간이 느려지는 것을 확인 ​
    >
    >또한, Phase 1은 GVCF 파일의 개수가 증가함에 따라 실행시간이 균일하게 증가하지만, Phase 2는 증가함에 따라 실행시간 불규칙적으로 증가​

<br>

## 🔧 **결론**

- 분산병렬 클러스터를 이용한 GVCF 파일 정렬 및 병합 모듈을 제안​

    다수의 GVCF 파일 정렬 및 병합 를 위해 Spark 적용​

    GVCF 파일의 특성을 이용하여 Phase 1과 Phase 2로 정렬 및 병합 진행​

    기존 파일 병합 및 모듈인 CombineGVCFs 보다 실행시간이 최소 75%에서 최대 87%까지 단축되는 것을 확인
