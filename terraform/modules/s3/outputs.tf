output "bucket_id" {
  value = aws_s3_bucket.mwaa.id
}

output "bucket_name" {
  value = aws_s3_bucket.mwaa.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.mwaa.arn
}