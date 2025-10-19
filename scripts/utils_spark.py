from pyspark.sql import DataFrame

def clean_dataframe(df: DataFrame, cols_to_dropna: list):
    """
    Supprime les valeurs manquantes et les doublons.
    """
    return df.dropna(subset=cols_to_dropna).dropDuplicates()
