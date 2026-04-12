import os
import io
import pandas as pd
from minio import Minio
from datetime import datetime
from typing import Optional


class MinioClient:
    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None,
    ):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket = bucket or os.getenv("MINIO_BUCKET", "vol-surface-raw")
        
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
        secure=False,
        )
        self._ensure_bucket()
    
    def _ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
    
    def upload_parquet(
        self,
        df: pd.DataFrame,
        prefix: str,
        filename: Optional[str] = None,
    ) -> str:
        """
        Upload a DataFrame as Parquet to MinIO.
        
        Args:
            df: DataFrame to upload
            prefix: Directory prefix (e.g., 'options', 'macro')
            filename: Optional filename, defaults to timestamp
        
        Returns:
            Full object path in bucket
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{prefix}_{timestamp}.parquet"
        
        object_path = f"{prefix}/{filename}"
        
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=object_path,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/parquet",
        )
        
        print(f"Uploaded {object_path} to {self.bucket}")
        return object_path
    
    def download_parquet(self, object_path: str) -> pd.DataFrame:
        """Download a Parquet object from MinIO."""
        response = self.client.get_object(self.bucket, object_path)
        data = response.read()
        df = pd.read_parquet(io.BytesIO(data))
        response.close()
        response.release_conn()
        return df
    
    def list_objects(self, prefix: str = "") -> list:
        """List objects with optional prefix."""
        return [
            obj.object_name
            for obj in self.client.list_objects(self.bucket, prefix=prefix)
        ]


if __name__ == "__main__":
    client = MinioClient()
    
    test_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    client.upload_parquet(test_df, "test", "test.parquet")
    print("MinIO client ready")
