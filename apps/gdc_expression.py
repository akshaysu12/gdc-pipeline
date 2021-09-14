import requests 
import json

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import StringType, StructType, DoubleType
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import first
from pyspark.sql.functions import udf, explode, regexp_replace

def init_spark():
  conf = SparkConf()
  conf.setMaster("spark://spark-master:7077")
  conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')
    
  sc = SparkContext(conf=conf)
  sql = SparkSession.builder.getOrCreate()
  return sql,sc

def get_data(uuid, sample_submitter_id):
    response = requests.get(indexd_endpt + "/" + uuid)
    urls = json.loads(response.content.decode("utf-8"))["urls"]
    url = [x for x in urls if x.startswith("s3://")]
    url = url[0]

    return url


## The GDC endpoints for files and the NCI endpoint to query for the S3 URL
files_endpt = 'https://api.gdc.cancer.gov/files'
indexd_endpt = 'https://nci-crdc.datacommons.io/index/index/'
s3_tcga_bucket = 'tcga-2-open'


## Inputs to this method are the UUID and submitter ID from the GDC endpoint query
## Fields to be returned as a comma separated list
fields = ','.join([
      "file_name"
    , "cases.primary_site"
    , "cases.case_id"
    , "cases.project.project_id"
    , "cases.submitter_id"
    , "cases.samples.submitter_id"
    , "cases.samples.sample_id"
])
size = 1
project_id = "TCGA-BRCA"
data_type = "Gene Expression Quantification"
workflow_type = "HTSeq - FPKM-UQ"
filters = {
    "op":"and",
    "content":[
        {"op": "in",
        "content":{
            "field": "cases.project.project_id",
            "value": [project_id]
            }
        },
        {"op": "in",
        "content":{
            "field": "files.data_type",
            "value": [data_type]
            }
        },
        {"op": "in",
        "content":{
            "field": "files.analysis.workflow_type",
            "value": [workflow_type]
            }
        }
    ]
}
params = {
    "filters": json.dumps(filters),
    "fields": fields,
    "format": "JSON",
    "size": size
    }

# Get GDC data
query_response = requests.get(files_endpt, params = params)
json_response = json.loads(query_response.content.decode("utf-8"))["data"]["hits"]

# Initialize Spark
sql,sc = init_spark()

# read json in parallel then repartition
df = sql.read.json(sc.parallelize([json_response]))

# explode creates a new row for each element in the samples array: [{'sample_id': '1acae6e0-3100-4de8-a436-e15cdbe1322b', 'submitter_id': 'TCGA-BH-A0DK-01A'}]
# just one object so a nested row basically
# the other two commands turn the generic row into a DF then flatten the nested row
uf = df.select("id",explode(df.cases.samples)).toDF("id","samples").select('id','samples.submitter_id','samples.sample_id')

# Get S3 links then modify for s3a connector 
urldf = udf(get_data)
inputPath=uf.withColumn('Result', urldf('id', 'submitter_id'))
updatedPath = inputPath.withColumn('ResultModified', regexp_replace('Result', 's3', 's3a'))
inputList = list(updatedPath.select('ResultModified').toPandas()['ResultModified'])

# This section parallelizes reading of all the files and stores data in a single data frame
schema = schema = StructType() \
    .add("GeneID", StringType(), True) \
    .add("Expression",DoubleType(), True)
data = sql.read.option("sep","\t").csv(inputList[0],header=False,mode="DROPMALFORMED",schema=schema)

## Add a column with file path, which adds the s3 file path from which row is extracted
data = data.withColumn("fullpath", input_file_name())

## Add a column which is a substring of full s3 path and gives filename so that we can match and join with Json data
data = data.withColumn("file", data.fullpath.substr(55,100))
data = data.withColumn("EnsemblGene",data.GeneID.substr(1,15))

## Join the data with the frame that includes submitter ID
joinedf = data.join(updatedPath,data["fullpath"]==updatedPath["ResultModified"])

## Select only relevant columns
allgene = joinedf.select("EnsemblGene" ,joinedf["submitter_id"].getItem(0), "Expression").withColumnRenamed("submitter_id[0]","submitter_id")

## Pivot the dataframe to form the expression matrix
pivotframe = allgene.groupBy('EnsemblGene').pivot('submitter_id').agg(first("Expression"))

## Write the data frame to the output bucket
output_bucket="gdc-explore"
pivotframe.write.mode("overwrite").parquet("s3a://"+ output_bucket + "/tcga-expression/" + project_id + "/" + workflow_type.replace(" ",""))
