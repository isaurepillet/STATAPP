import pandas as pd
import numpy as np 
import pyarrow as pa
import pyarrow.parquet as pq
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

    def get_tables_from_s3(self, directory):
        try:
            with self.s3.open(directory, "rb") as file_in:
                df = pd.read_parquet(file_in)
            return df
        except Exception as e:
            print(f"Error reading Parquet file from S3: {e}")
            return None

    def read_file_from_s3(self, directory, columns_to_select=None, dtype_spec=None, sep=None):
        try:
            with self.s3.open(directory, "rb") as file_in:
                df = pd.read_csv(file_in, usecols=columns_to_select, dtype=dtype_spec, sep=sep)
            return df
        except Exception as e:
            print(f"Error reading file from S3: {e}")
            return None

    def get_tables_from_s3(self,directory):
        
        with self.s3.open(directory, "rb") as file_in:
          df = pd.read_parquet(file_in)
        return df

    def dataframe_to_parquet(self, df, parquet_file_path):
        
        try:
            # Convertir le DataFrame en table pyarrow
            table = pa.Table.from_pandas(df)

            # Écrire la table dans un fichier Parquet
            pq.write_table(table, parquet_file_path)

            print(f"Fichier Parquet créé avec succès à l'emplacement : {parquet_file_path}")

        except Exception as e:
            print(f"Erreur lors de la conversion du DataFrame en Parquet : {e}")

        except Exception as e:
            print(f"Erreur lors de la conversion du fichier CSV en Parquet : {e}")

    def normalize_address(self, address):
        if pd.isna(address) or address.strip() == '':
            return None  
        try:
            normalized = expand_address(address)  
            return "; ".join(normalized)  
        except Exception as e:
            print(f"Erreur avec l'adresse '{address}': {e}")
            return None

    