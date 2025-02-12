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

    def read_csv_from_s3(self, directory, columns_to_select=None, dtype_spec=None, sep=None):
        try:
            with self.s3.open(directory, "rb") as file_in:
                df = pd.read_csv(file_in, usecols=columns_to_select, dtype=dtype_spec, sep=sep)
            return df
        except Exception as e:
            print(f"Error reading CSV file from S3: {e}")
            return None

    def read_text_file_from_s3(self, directory):
        try:
            with self.s3.open(directory, "r", encoding="utf-8") as file_in:
                content = file_in.read()  # Lire tout le contenu du fichier
            return content
        except FileNotFoundError:
            print(f"Erreur : le fichier {directory} n'existe pas.")
        except Exception as e:
            print(f"Erreur lors de la lecture du fichier texte depuis S3 : {e}")
        return None
    
    def convertir_et_sauvegarder_txt_en_parquet(fichier_txt, fichier_parquet, delimiter=","):
    
        try:
        # Lire le fichier texte en DataFrame pandas
            df = pd.read_csv(fichier_txt, delimiter=delimiter)

        # Sauvegarder le DataFrame en fichier Parquet
            df.to_parquet(fichier_parquet, engine="pyarrow", index=False)

            print(f"✅ Fichier Parquet sauvegardé : {fichier_parquet}")

        except Exception as e:
            print(f"❌ Erreur : {e}")
