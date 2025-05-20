# Databricks notebook source
class BronzeLoader:
    """
    Classe responsável por carregar dados brutos da camada Bronze a partir de arquivos JSON.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_json(self, path: str, schema: StructType = None) -> DataFrame:
        """
        Carrega um DataFrame de arquivos JSON a partir do caminho especificado.

        :param path: Caminho para os arquivos JSON.
        :param schema: Schema opcional para leitura dos dados.
        :return: DataFrame com os dados brutos.
        """
        if schema:
            return self.spark.read.schema(schema).json(path)
        return self.spark.read.json(path)