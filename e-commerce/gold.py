# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, count

class GoldAggregator:
    """
    Classe responsável por gerar métricas analíticas na camada Gold,
    agregando dados por categoria de produto.
    """

    def aggregate(self, df: DataFrame) -> DataFrame:
        """
        Realiza agregações na camada Gold:
        - Média de valor por pedido por categoria
        - Total de pedidos por categoria

        :param df: DataFrame da camada Silver
        :return: DataFrame com métricas agregadas por categoria (camada Gold)
        """
        return (
            df.groupBy("product_category")
              .agg(
                  avg("order_value").alias("avg_order_value"),
                  count("order_id").alias("total_orders")
              )
              .orderBy("product_category")
        )