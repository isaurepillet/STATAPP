import s3fs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class S3Connection:
    def __init__(self, bucket_name):
        """
        Établir la connexion et après utiliser les fonctions de read et write
        """
        try:
            self.s3 = s3fs.S3FileSystem(client_kwargs={"endpoint_url": "https://minio.lab.sspcloud.fr"})
            self.bucket = bucket_name
            print("Connection successful")
        except Exception as e:
            self.s3 = None
            print(f"Connection not established: {e}")

    def get_tables_from_s3(self, directory):
        try:
            with self.s3.open(f"{self.bucket}/{directory}", "rb") as file_in:
                df = pd.read_parquet(file_in)
            return df
        except Exception as e:
            print(f"Error reading Parquet file from S3: {e}")
            return None

    def read_file_from_s3(self, directory, columns_to_select=None, dtype_spec=None, sep=None):
        try:
            with self.s3.open(f"{self.bucket}/{directory}", "rb") as file_in:
                df = pd.read_csv(file_in, usecols=columns_to_select, dtype=dtype_spec, sep=sep, low_memory=False)
            return df
        except Exception as e:
            print(f"Error reading file from S3: {e}")
            return None

    def from_pandas_to_parquet_store_in_s3(self, df, directory):
        try:
            with self.s3.open(f"{self.bucket}/{directory}", "wb") as file_out:
                df.to_parquet(file_out)
            print(f"Fichier Parquet écrit avec succès dans {directory}")
        except s3fs.errors.S3Error as e:
            print(f"Erreur S3: {e}")
        except Exception as e:
            print(f"Une erreur inattendue s'est produite: {e}")

    def normalize_address(self, address):
        if pd.isna(address) or address.strip() == '':
            return None
        try:
            normalized = expand_address(address)
            return "; ".join(normalized)
        except Exception as e:
            print(f"Erreur avec l'adresse '{address}': {e}")
            return None
