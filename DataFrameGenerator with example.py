# Databricks notebook source
# Databricks Notebook: Classe para geração de DataFrames de Teste

import random
import numpy as np
import string
from pyspark.sql.types import *
from datetime import datetime, timedelta, date  # Import necessário para manipulação de datas

class DataFrameGenerator:
    def __init__(self, spark):
        """
        Inicializa a classe com a sessão Spark.
        """
        self.spark = spark

    def _generate_unique_values(self, campo, num_rows, params):
        """
        Gera uma lista de valores únicos para um campo específico,
        de acordo com o tipo de dado e os parâmetros informados.
        """
        if isinstance(campo.dataType, IntegerType):
            return self._generate_unique_integers(num_rows, params)
        elif isinstance(campo.dataType, DoubleType):
            return self._generate_unique_doubles(num_rows, params)
        elif isinstance(campo.dataType, StringType):
            return self._generate_unique_strings(num_rows, params)
        elif isinstance(campo.dataType, BooleanType):
            if num_rows > 2:
                raise ValueError(f"Não é possível gerar mais que 2 valores únicos para a coluna boolean '{campo.name}'")
            return [True, False]
        elif isinstance(campo.dataType, DateType):
            return self._generate_unique_dates(num_rows, params)
        else:
            return [None] * num_rows

    def _generate_unique_integers(self, num_rows, params):
        valor_min = params.get('min', 0)
        valor_max = params.get('max', 100)
        total_possiveis = valor_max - valor_min + 1
        if total_possiveis < num_rows:
            raise ValueError("Intervalo insuficiente para gerar inteiros únicos.")
        
        distribution = params.get('distribution', 'normal')
        if distribution == 'uniform':
            return random.sample(range(valor_min, valor_max + 1), num_rows)
        else:
            uniques = set()
            attempts = 0
            while len(uniques) < num_rows and attempts < num_rows * 10:
                media = (valor_min + valor_max) / 2.0
                desvio = (valor_max - valor_min) / 6.0
                val = int(np.random.normal(media, desvio))
                if valor_min <= val <= valor_max:
                    uniques.add(val)
                attempts += 1
            if len(uniques) < num_rows:
                raise ValueError("Não foi possível gerar inteiros únicos suficientes com distribuição normal.")
            return list(uniques)

    def _generate_unique_doubles(self, num_rows, params):
        valor_min = params.get('min', 0.0)
        valor_max = params.get('max', 100.0)
        distribution = params.get('distribution', 'normal')
        if distribution == 'uniform':
            uniques = set()
            while len(uniques) < num_rows:
                uniques.add(np.random.uniform(valor_min, valor_max))
            uniques = list(uniques)
        else:
            uniques = set()
            attempts = 0
            while len(uniques) < num_rows and attempts < num_rows * 10:
                media = (valor_min + valor_max) / 2.0
                desvio = (valor_max - valor_min) / 6.0
                val = np.random.normal(media, desvio)
                if valor_min <= val <= valor_max:
                    uniques.add(val)
                attempts += 1
            if len(uniques) < num_rows:
                raise ValueError("Não foi possível gerar doubles únicos suficientes com distribuição normal.")
            uniques = list(uniques)
        if 'round' in params:
            decimals = params['round'] if isinstance(params['round'], int) else 0
            uniques = [round(val, decimals) for val in uniques]
        return uniques

    def _generate_unique_strings(self, num_rows, params):
        length = params.get('length', 10)
        formato = params.get('format', 'lower')
        letras = string.ascii_uppercase if formato == 'upper' else string.ascii_lowercase
        uniques = set()
        while len(uniques) < num_rows:
            s = ''.join(random.choice(letras) for _ in range(length))
            uniques.add(s)
        return list(uniques)
    
    def _generate_unique_dates(self, num_rows, params):
        """
        Gera uma lista de datas únicas com base em um intervalo.
        Os parâmetros esperados são 'start_date' e 'end_date' no formato 'YYYY-MM-DD'.
        Caso não informados, usa-se um intervalo padrão.
        """
        start_date_str = params.get('start_date', '2000-01-01')
        end_date_str = params.get('end_date', '2020-12-31')
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        if start_date > end_date:
            raise ValueError("start_date deve ser menor ou igual a end_date")
        total_days = (end_date - start_date).days + 1
        if total_days < num_rows:
            raise ValueError("Intervalo de datas insuficiente para gerar datas únicas.")
        day_offsets = random.sample(range(total_days), num_rows)
        dates = [start_date + timedelta(days=offset) for offset in day_offsets]
        return dates

    def _generate_value(self, campo, params):
        """
        Gera um único valor para um campo com base no tipo e parâmetros,
        considerando também a probabilidade de ser nulo.
        """
        null_rate = params.get('null_rate', 0.0)
        if random.random() < null_rate:
            return None

        if isinstance(campo.dataType, IntegerType):
            valor_min = params.get('min', 0)
            valor_max = params.get('max', 100)
            distribution = params.get('distribution', 'normal')
            if distribution == 'uniform':
                return random.randint(valor_min, valor_max)
            else:
                media = (valor_min + valor_max) / 2.0
                desvio = (valor_max - valor_min) / 6.0
                val = int(np.random.normal(media, desvio))
                return max(valor_min, min(val, valor_max))
        elif isinstance(campo.dataType, DoubleType):
            valor_min = params.get('min', 0.0)
            valor_max = params.get('max', 100.0)
            distribution = params.get('distribution', 'normal')
            if distribution == 'uniform':
                val = np.random.uniform(valor_min, valor_max)
            else:
                media = (valor_min + valor_max) / 2.0
                desvio = (valor_max - valor_min) / 6.0
                val = np.random.normal(media, desvio)
                val = max(valor_min, min(val, valor_max))
            if 'round' in params:
                decimals = params['round'] if isinstance(params['round'], int) else 0
                val = round(val, decimals)
            return val
        elif isinstance(campo.dataType, StringType):
            length = params.get('length', 10)
            formato = params.get('format', 'lower')
            letras = string.ascii_uppercase if formato == 'upper' else string.ascii_lowercase
            return ''.join(random.choice(letras) for _ in range(length))
        elif isinstance(campo.dataType, BooleanType):
            return random.choice([True, False])
        elif isinstance(campo.dataType, DateType):
            start_date_str = params.get('start_date', '2000-01-01')
            end_date_str = params.get('end_date', '2020-12-31')
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            if start_date > end_date:
                raise ValueError("start_date deve ser menor ou igual a end_date")
            total_days = (end_date - start_date).days + 1
            random_offset = random.randint(0, total_days - 1)
            return start_date + timedelta(days=random_offset)
        else:
            return None

    def generate(self, schema, num_rows, parametros_colunas={}):
        """
        Gera um DataFrame do Spark com dados aleatórios com base no schema e parâmetros.

        Parâmetros:
          - schema: Objeto do tipo StructType do Spark.
          - num_rows: Número de linhas desejadas no DataFrame.
          - parametros_colunas: Dicionário contendo configurações para cada coluna.
        
        Retorna:
          - DataFrame do Spark com os dados gerados.
        """
        # Pré-cálculo para colunas com valores únicos
        unique_values = {}
        for campo in schema.fields:
            params = parametros_colunas.get(campo.name, {})
            if params.get('unique', False):
                unique_values[campo.name] = self._generate_unique_values(campo, num_rows, params)
                
        # Índice para rastrear os valores únicos já utilizados
        unique_index = {col: 0 for col in unique_values.keys()}
        dados = []
        for _ in range(num_rows):
            linha = []
            for campo in schema.fields:
                params = parametros_colunas.get(campo.name, {})
                if params.get('unique', False):
                    valor = unique_values[campo.name][unique_index[campo.name]]
                    unique_index[campo.name] += 1
                else:
                    valor = self._generate_value(campo, params)
                linha.append(valor)
            dados.append(tuple(linha))
        
        return self.spark.createDataFrame(dados, schema)

# --------------------------------------------------
# Exemplo de uso:
# --------------------------------------------------

# Definindo um schema de exemplo
schema_exemplo = StructType([
    StructField("id", IntegerType(), True),
    StructField("valor", DoubleType(), True),
    StructField("nome", StringType(), True),
    StructField("flag", BooleanType(), True),
    StructField("data_evento", DateType(), True)
])

# Parâmetros customizados para cada coluna
parametros = {
    "id": {"min": 1, "max": 10000000, "unique": True, "null_rate": 0.05, "distribution": "uniform"},
    # Para floats, você pode definir "min" negativo para permitir números negativos.
    "valor": {"min": -100.0, "max": 100.0, "round": 2, "distribution": "normal"},
    "nome": {"format": "upper", "length": 8, "unique": True},
    "flag": {"null_rate": 0.1},
    # Configuração para datas: informe start_date e end_date conforme desejado.
    "data_evento": {"start_date": "1990-01-01", "end_date": "2020-12-31", "unique": False, "null_rate": 0.0}
}

# Instanciando a classe passando a sessão Spark
generator = DataFrameGenerator(spark)

# Gerando o DataFrame com 1000 linhas
df_teste = generator.generate(schema_exemplo, 500000, parametros)

# Exibindo o DataFrame
display(df_teste)