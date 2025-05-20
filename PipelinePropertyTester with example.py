# Databricks notebook source
# Importações
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Callable, List

# Classe de teste de propriedades
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

# Spark session
spark = SparkSession.builder.getOrCreate()

# Função de pipeline de exemplo
def pipeline_exemplo(df):
    return df.withColumn("valor_ajustado", col("valor") * 2)

# Propriedade 1: valores devem ser positivos
def propriedade_valor_positivo(df):
    return df.withColumn("check", col("valor") > 0)

# Propriedade 2: valor_ajustado deve ser o dobro do valor original
def propriedade_valor_ajustado_correto(df):
    return df.withColumn("check", col("valor_ajustado") == col("valor") * 2)

# Criando dados de entrada
dados = [
    (1, ),   # válido
    (-2, ),  # inválido
    (3, )    # válido
]
df_input = spark.createDataFrame(dados, ["valor"])

# Rodando os testes
propriedades = [propriedade_valor_positivo, propriedade_valor_ajustado_correto]
tester = PipelinePropertyTester(pipeline_exemplo, propriedades)

resultados = tester.run(df_input)

# Exibindo os resultados
print(resultados)