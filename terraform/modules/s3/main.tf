data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "mwaa" {
  bucket = "${var.project_name}-mwaa-${var.environment}-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
  tags   = merge(var.tags, { Name = "${var.project_name}-mwaa-bucket-${var.environment}" })
}

resource "aws_s3_bucket_public_access_block" "mwaa" {
  bucket                  = aws_s3_bucket.mwaa.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "mwaa" {
  bucket = aws_s3_bucket.mwaa.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mwaa" {
  bucket = aws_s3_bucket.mwaa.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_object" "folders" {
  for_each = toset(["dags/", "plugins/"])
  bucket   = aws_s3_bucket.mwaa.id
  key      = each.value
  content  = ""
}