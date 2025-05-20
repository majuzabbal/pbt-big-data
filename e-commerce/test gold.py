# Databricks notebook source
# MAGIC %run /Users/majuzabbal@gmail.com/tcc/e-commerce/gold

# COMMAND ----------

# MAGIC %run /Users/majuzabbal@gmail.com/tcc/DataFrameGenerator

# COMMAND ----------

# MAGIC %run /Users/majuzabbal@gmail.com/tcc/PipelinePropertyTester

# COMMAND ----------

aggregator = GoldAggregator()

def gold_pipeline(df: DataFrame) -> DataFrame:
    return aggregator.aggregate(df)

# COMMAND ----------

from pyspark.sql.functions import col

# Propriedade 1: média dos pedidos deve ser positiva
def propriedade_media_positiva(df: DataFrame) -> DataFrame:
    return df.withColumn("check", col("avg_order_value") > 0)

# Propriedade 2: total de pedidos deve ser inteiro ≥ 0
def propriedade_total_pedidos_valido(df: DataFrame) -> DataFrame:
    return df.withColumn("check", col("total_orders") >= 0)

# Propriedade 3: categoria de produto não pode ser nula ou vazia
def propriedade_categoria_valida(df: DataFrame) -> DataFrame:
    return df.withColumn("check", col("product_category").isNotNull() & (col("product_category") != ""))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, BooleanType, DateType

# Novo schema adaptado para SilverTransformer do e-commerce
schema_ecommerce = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_value", DoubleType(), True),
    StructField("product_category", StringType(), True),
    StructField("customer_active", BooleanType(), True),
    StructField("order_date", DateType(), True)
])

# Parâmetros customizados
parametros_ecommerce = {
    "order_id": {
        "min": 100000, "max": 999999, "unique": True, "null_rate": 0.01, "distribution": "uniform"
    },
    "order_value": {
        "min": -50.0, "max": 500.0, "round": 2, "distribution": "normal"
    },
    "product_category": {
        "format": "alpha", "length": 10, "unique": False, "null_rate": 0.02
    },
    "customer_active": {
        "null_rate": 0.05
    },
    "order_date": {
        "start_date": "2021-01-01", "end_date": "2023-12-31", "null_rate": 0.0
    }
}

# Instanciando e gerando o DataFrame
generator = DataFrameGenerator(spark)

# Executar o gerador de dados
df_entrada = generator.generate(schema_ecommerce, 10000, parametros_ecommerce)

# COMMAND ----------

# Propriedades da Gold
propriedades_gold = [
    propriedade_media_positiva,
    propriedade_total_pedidos_valido,
    propriedade_categoria_valida
]

# Testando a agregação Gold
tester_gold = PipelinePropertyTester(gold_pipeline, propriedades_gold)
resultados_gold = tester_gold.run(df_entrada)

print(resultados_gold)

# COMMAND ----------

import time
import psutil
import os
import threading
import gc

def monitor_cpu(interval_sec, duration_sec, cpu_usage_list):
    process = psutil.Process(os.getpid())
    for _ in range(int(duration_sec / interval_sec)):
        cpu = process.cpu_percent(interval=None)  # sem bloqueio
        cpu_usage_list.append(cpu)
        time.sleep(interval_sec)

def run_test_with_performance_cpu(tester, df_input, timeout_sec=10):
    process = psutil.Process(os.getpid())
    gc.collect()

    cpu_usages = []
    monitor_thread = threading.Thread(target=monitor_cpu, args=(0.1, timeout_sec, cpu_usages))
    monitor_thread.start()

    start_cpu_times = process.cpu_times()
    start_time = time.time()
    resultados = tester.run(df_input)
    end_time = time.time()
    end_cpu_times = process.cpu_times()

    monitor_thread.join()

    user_cpu_time = end_cpu_times.user - start_cpu_times.user
    system_cpu_time = end_cpu_times.system - start_cpu_times.system
    total_cpu_time = user_cpu_time + system_cpu_time
    cpu_percent_avg = sum(cpu_usages) / len(cpu_usages) if cpu_usages else 0
    tempo_exec = end_time - start_time
    gc_counts = gc.get_count()

    print(f"Tempo de execução: {tempo_exec:.3f} segundos")
    print(f"Tempo CPU total (user+system): {total_cpu_time:.3f} segundos")
    print(f"CPU % médio durante execução: {cpu_percent_avg:.2f}%")
    print(f"GC counts (gerações 0,1,2): {gc_counts}")

    return resultados

# Teste da camada gold
tester_gold = PipelinePropertyTester(gold_pipeline, propriedades_gold)
resultados_gold = run_test_with_performance_cpu(tester_gold, df_entrada)
print("Resultados dos testes Gold:", resultados_gold)