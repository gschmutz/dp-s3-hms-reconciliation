import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from allure_uploader import upload_to_s3, upload_to_allure_server


def test_upload_to_s3(tmp_path):    
    # Create a temporary directory with some files
    test_dir = tmp_path / "test_results"
    test_dir.mkdir()
    (test_dir / "file1.txt").write_text("This is file 1")
    (test_dir / "file2.txt").write_text("This is file 2")

    # Mock S3 client
    class MockS3Client:
        def __init__(self):
            self.uploaded_files = []

        def upload_file(self, local_path, bucket, s3_path):
            self.uploaded_files.append((local_path, bucket, s3_path))

    mock_s3_client = MockS3Client()
    bucket_name = "my-test-bucket"
    s3_prefix = "test-prefix"

    # Call the function to test
    upload_to_s3(mock_s3_client, "table_metrics_compare/report", bucket_name, "reports")


