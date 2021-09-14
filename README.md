# gdc-pipeline

## Layout
1. Apps - pyspark applications
2. Terraform - AWS infra for supporting GDC data analysis and exploration
3. Dockerfiles - Local spark configuration

### Terraform
1. S3 Bucket for holding parsed gdc data

### Apps
1. gdc-expression: pyspark data transformation of HTSeq-FPKM-UQ Gene Expression Quantification from TCGA-BRCA dataset. Expression matrix on gene id. 