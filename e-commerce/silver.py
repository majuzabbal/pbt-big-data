# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim

class SilverTransformer:
    """
    Classe genérica para transformar dados da camada Bronze na Silver.
    Permite aplicar operações de limpeza, filtro e normalização de forma modular.
    """

    def drop_nulls(self, df: DataFrame, required_columns: list[str]) -> DataFrame:
        """
        Remove linhas com valores nulos nas colunas obrigatórias.

        :param df: DataFrame de entrada
        :param required_columns: Lista de colunas obrigatórias
        :return: DataFrame sem valores nulos nas colunas especificadas
        """
        return df.dropna(subset=required_columns)

    def filter_positive_values(self, df: DataFrame, column: str) -> DataFrame:
        """
        Filtra linhas com valores positivos em uma coluna numérica.

        :param df: DataFrame de entrada
        :param column: Nome da coluna a ser filtrada
        :return: DataFrame filtrado
        """
        return df.filter(col(column) > 0)

    def normalize_text_column(self, df: DataFrame, column: str) -> DataFrame:
        """
        Normaliza texto: aplica `trim` e `lower` a uma coluna de texto.

        :param df: DataFrame de entrada
        :param column: Nome da coluna de texto a ser normalizada
        :return: DataFrame com a coluna normalizada
        """
        return df.withColumn(column, lower(trim(col(column))))

    def transform(self, df: DataFrame,
                  required_columns: list[str],
                  value_column: str,
                  text_column: str) -> DataFrame:
        """
        Executa transformação completa: remove nulos, filtra valores positivos e normaliza texto.

        :param df: DataFrame bruto
        :param required_columns: Colunas que não devem conter nulos
        :param value_column: Coluna numérica para filtro de valores positivos
        :param text_column: Coluna de texto a ser normalizada
        :return: DataFrame transformado
        """
        df_clean = self.drop_nulls(df, required_columns)
        df_filtered = self.filter_positive_values(df_clean, value_column)
        df_normalized = self.normalize_text_column(df_filtered, text_column)
        return df_normalized