import requests 
import json

projects_endpt = 'https://api.gdc.cancer.gov/projects?from=0&size=70'
cases_endpt = 'https://api.gdc.cancer.gov/cases'
files_endpt = 'https://api.gdc.cancer.gov/files'


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
size = 10
project_id = "TCGA-LIHC"
data_type = "Gene Expression Quantification"
experimental_strategy = "RNA-Seq"
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
          "field": "files.experimental_strategy",
          "value": [experimental_strategy]
          }
      },
      {"op": "in",
      "content":{
          "field": "access",
          "value": ["open"]
          }
      }
  ]
}

params = {
  "filters": json.dumps(filters),
  "format": "JSON",
  "fields": fields,
}

# Get GDC data
query_response = requests.get(files_endpt, params = params)
# query_response = requests.get(projects_endpt)
# query_response = requests.get(cases_endpt, params=params)
# res = json.loads(query_response.content.decode("utf-8"))["data"]['hits']
res = json.loads(query_response.content.decode("utf-8"))["data"]
print(res)

# sample_id = "bc63e37c-e534-4da4-afc6-5f9f194b9398"
# indexd_endpt = 'https://nci-crdc.datacommons.io/index'
# response = requests.get(indexd_endpt + "/" + sample_id)
# res = json.loads(response.content.decode("utf-8"))
# print(res)
# urls = res['urls']
# print(urls)
