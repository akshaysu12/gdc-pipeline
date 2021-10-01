import requests 
import json
# import boto3

gdc_endpt = 'https://api.gdc.cancer.gov/files'
nci_endpt = 'https://nci-crdc.datacommons.io/index'

project_id = "TCGA-LIHC"
data_type = "Gene Expression Quantification"

filters = {
  "op":"and",
  "content":
  [
    {
      "op": "in",
      "content":{
        "field": "cases.project.project_id",
        "value": [project_id]
      }
    },
    {
      "op": "in",
      "content": {
        "field": "files.data_type",
        "value": [data_type]
      }
    },
    {
      "op": "in",
      "content": {
        "field": "access",
        "value": ["open"]
      }
    }
  ]
}

params = {
    "filters": json.dumps(filters),
    "format": "JSON",
}

def get_sample_ids(endpoint, params, getAll=False, limit=100):
  results = []
  curr= 0
  page=0
  while curr < limit:
    query_response = requests.get(f"{endpoint}?from={page*limit}&size={limit}", params = params)
    response = json.loads(query_response.content.decode("utf-8"))["data"]
    pagination = response['pagination']
    
    for hit in response['hits']:
      results.append(hit['id'])

    if getAll:
      curr = pagination['page']
      limit = pagination['pages']
    else:
      curr = len(results)
    page += 1
  
  return results

def get_s3_paths(endpoint, sample_ids):
  res = []
  for id in sample_ids:
    query_response = requests.get(endpoint + "/" + id)
    urls = json.loads(query_response.content.decode("utf-8"))["urls"]
    res = res + [x for x in urls if x.startswith("s3://")]
  
  return res



# Get GDC data
sample_ids = get_sample_ids(endpoint=gdc_endpt, params=params, limit=10)
results = get_s3_paths(endpoint=nci_endpt, sample_ids=sample_ids)
glue_files_string = ",".join(results)
print(glue_files_string)
# static_ten_files = "s3://tcga-2-open/344254ce-6ae0-4889-9fdc-13332085f8b0/4d3842d1-c643-4659-98e9-8d613144fa91.FPKM.txt.gz,s3://tcga-2-open/63a0969b-4c2a-448a-9641-9731559d8581/429c985a-3f84-4f99-91c0-710b33d82646.FPKM.txt.gz,s3://tcga-2-open/e52eed91-a5d8-49b0-8fee-e12d028763e7/3e2a3ad6-cc31-4b8f-9738-3266a423ccd2.FPKM.txt.gz,s3://tcga-2-open/15fdad9d-ba94-4d93-ab31-836f9eaf5518/c45cd340-4895-400a-a50e-19df90b67355.htseq.counts.gz,s3://tcga-2-open/2c07088a-d295-46bd-b209-54f640e602d5/5ead465c-2dae-4ae2-97a1-5b98e9e013ec.FPKM-UQ.txt.gz,s3://tcga-2-open/5a3ca8f9-a524-4c9e-9d74-e7681a3daf2a/c8c2df2e-6b18-4c73-a96e-1437a64cb579.htseq.counts.gz,s3://tcga-2-open/e308c4e3-3184-4c38-9462-16eab825240f/d7561963-2c69-493c-bfb9-e357407dd70d.FPKM.txt.gz,s3://tcga-2-open/38837e89-0d90-44bd-bf61-adc5ff7360dc/fdb62f73-33a7-44c3-950c-739383b9dd30.FPKM-UQ.txt.gz,s3://tcga-2-open/bb7b1deb-27c6-4514-945c-1d5e10f5efc0/2c2e26d3-fd8e-4dab-a72e-fa62d680de75.htseq.counts.gz,s3://tcga-2-open/bc63e37c-e534-4da4-afc6-5f9f194b9398/4077e82f-cb78-4977-bd03-600a3da93ffb.FPKM.txt.gz"

# client = boto3.client('glue')
# response = client.start_job_run(
#     JobName='string',
#     JobRunId='string',
#     Arguments={
#         's3_files': glue_files_string
#     },
#     AllocatedCapacity=123,
#     Timeout=123,
#     MaxCapacity=123.0,
#     SecurityConfiguration='string',
#     NotificationProperty={
#         'NotifyDelayAfter': 123
#     },
#     WorkerType='Standard'|'G.1X'|'G.2X',
#     NumberOfWorkers=123
# )