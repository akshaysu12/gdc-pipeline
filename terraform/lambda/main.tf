resource "aws_iam_role" "get_s3_files_lambda_role" {
  name = "get-s3-files-lambda-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

data "archive_file" "get_s3_files_zip" {
  type = "zip"
  source_file = "src/python/get_s3_files_lambda.py"
  output_path = "get_s3_files_lambda.zip"
}

resource "aws_lambda_function" "get_s3_files_lambda" {
  filename      = "get_s3_files_lambda.zip"
  function_name = "get_s3_files"
  role          = aws_iam_role.get_s3_files_lambda_role.arn
  handler       = "index.test"

  source_code_hash = data.archive_file.get_s3_files_lambda_zip.output_base64sha256

  runtime = "python3.7"
}