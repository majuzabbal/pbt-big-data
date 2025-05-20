# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from typing import Callable, List

class PipelinePropertyTester:
    def __init__(self, pipeline_func: Callable[[DataFrame], DataFrame], propriedades: List[Callable[[DataFrame], DataFrame]]):
        """
        Inicializa o PipelinePropertyTester.

        :param pipeline_func: Função de pipeline que será testada.
        :param propriedades: Lista de funções de propriedades. Cada função recebe um DataFrame e retorna um DataFrame com uma coluna 'check' booleana.
        """
        self.pipeline_func = pipeline_func
        self.propriedades = propriedades

    def run(self, df_input: DataFrame):
        """
        Executa o pipeline e testa as propriedades no dataframe de saída.

        :param df_input: DataFrame de entrada para o pipeline.
        :return: Dicionário com os resultados dos testes de propriedades.
        """
        try:
            df_saida = self.pipeline_func(df_input)
        except Exception as e:
            return {"Erro no pipeline": f"Erro ao executar o pipeline: {str(e)}"}
        
        resultados = {}
        for prop in self.propriedades:
            nome = prop.__name__
            try:
                df_check = prop(df_saida)

                if "check" not in df_check.columns:
                    raise ValueError("A função de propriedade deve retornar um DataFrame com a coluna 'check'.")

                total_falhas = df_check.filter(~col("check")).count()
                
                if total_falhas > 0:
                    print(f"[{nome}] Falhas encontradas:")
                    display(df_check.filter(~col("check")))

                resultados[nome] = {
                    "falhas": total_falhas,
                    "status": "Sucesso" if total_falhas == 0 else "Falha Parcial"
                }
            except Exception as e:
                resultados[nome] = {
                    "status": "Erro",
                    "erro": str(e)
                }

        return resultados
