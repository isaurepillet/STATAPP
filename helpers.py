import pandas as pd
import numpy as np 
import math as mt
import s3fs
import statsmodels.api as sm

class s3_connection:
    def __init__(self):
        """
        Établir la connexion et après utiliser les fonctions de read et write
        """
        try:
            self.s3 = s3fs.S3FileSystem(client_kwargs={"endpoint_url": "https://minio.lab.sspcloud.fr"})
            print("Connection successful")
        except Exception as e:
            self.s3 = None
            print(f"Connection not established, debug: {e}")

    def from_pandas_to_parquet_store_in_s3(self, df, directory):
        try:
            with self.s3.open(directory, "wb") as file_out:
                df.to_parquet(file_out)
        except Exception as e:
            print(f"Cher lecteur, cette fonction écrit dans le dossier spécifié, mais vous n'avez pas les droits :( {e}")

    def get_tables_from_s3(self, directory):
        try:
            with self.s3.open(directory, "rb") as file_in:
                df = pd.read_parquet(file_in)
            return df
        except Exception as e:
            print(f"Error reading Parquet file from S3: {e}")
            return None

    def read_csv_from_s3(self, directory, columns_to_select=None, dtype_spec=None):
        try:
            with self.s3.open(directory, "rb") as file_in:
                df = pd.read_csv(file_in, usecols=columns_to_select, dtype=dtype_spec)
            return df
        except Exception as e:
            print(f"Error reading CSV file from S3: {e}")
            return None

    def read_text_from_s3(self, directory):
        try:
            with self.s3.open(directory, "r") as file_in:
                df = pd.read(file_in)
            return df
        except Exception as e:
            print(f"Error reading text file from S3: {e}")
            return None