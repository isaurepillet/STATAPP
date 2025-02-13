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

    def convert_txt_to_parquet_direct(self, txt_content, output_directory, delimiter=None):
        """
        Convertit un texte (chaîne de caractères) en DataFrame et l'enregistre en Parquet dans S3
        
        :param txt_content: Le contenu du fichier texte (string)
        :param output_directory: Le chemin du fichier Parquet dans S3 (ex: "bucket/directory/output.parquet")
        :param delimiter: Le séparateur des colonnes dans le fichier texte ("," pour CSV, "\t" pour TSV, None pour espace)
        """
        try:
            # Transformer le contenu texte en DataFrame
            lines = txt_content.strip().split("\n")
            df = pd.DataFrame([line.split(delimiter) for line in lines])

            # Sauvegarder en Parquet directement dans S3
            with self.s3.open(output_directory, "wb") as file_out:
                df.to_parquet(file_out)
            print(f"Parquet enregistré avec succès : {output_directory}")
        
        except Exception as e:
            print(f"Erreur lors de la conversion TXT -> Parquet : {e}")
