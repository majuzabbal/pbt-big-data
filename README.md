# pbt-big-data

Repositório com todos os códigos do TCC **“IMPLEMENTAÇÃO DE TESTES BASEADOS EM PROPRIEDADES EM AMBIENTES DE PROCESSAMENTO DE BIG DATA”**, desenvolvendo o `DataFrameGenerator`, o `PipelinePropertyTester` e exemplos de uso (incluindo caso de e‑commerce).

---

## 🚀 Visão Geral

Este projeto implementa um framework de Property‑Based Testing (PBT) para ambientes distribuídos de Big Data usando PySpark.  
Principais componentes:
- **DataFrameGenerator**: gera DataFrames sintéticos seguindo esquemas definidos.
- **PipelinePropertyTester**: executa e valida propriedades em pipelines medalhão (bronze → silver → gold).
- Exemplos de aplicação em scripts Python, inclusive um caso de e‑commerce.

---

## ⚙️ Pré-requisitos

- Python 3.8+  
- Apache Spark ≥ 3.0  
- Databricks (opcional, para execução no notebook)  

💻 Instalação
Clone o repositório:

git clone https://github.com/majuzabbal/pbt-big-data.git
cd pbt-big-data

## 📚 Uso
1. DataFrameGenerator
Definição: classe que recebe um esquema (tipos, restrições) e gera um DataFrame PySpark correspondente.

Como usar:

from DataFrameGenerator import DataFrameGenerator

schema = {
    "id": "integer",
    "value": "float",
    "category": ["A", "B", "C"]
}
gen = DataFrameGenerator(spark_session, schema, num_rows=1000)
df = gen.generate()
df.show()
Exemplo completo:
Rode DataFrameGenerator with example.py.

2. PipelinePropertyTester
Definição: ferramenta para aplicar propriedades (invariantes, relações entre colunas, cardinalidade, etc.) a cada etapa do pipeline medalhão.

Como usar:

from PipelinePropertyTester import PipelinePropertyTester

tester = PipelinePropertyTester(spark_session, pipeline_stages)
tester.add_property("silver_count", lambda df: df.count() > 0)
tester.run_all()
Exemplo completo:
Rode PipelinePropertyTester with example.py.

## ✅ Testes
Este repositório foca no PBT; não há testes unitários tradicionais, mas você pode:

Adaptar exemplos para novas propriedades.

Rodar scripts de exemplo para validar integração e desempenho.

## 🤝 Contribuição
Fork este repositório.

Crie uma branch: git checkout -b feature/minha-melhoria.

Faça commit das mudanças: git commit -m 'Adiciona nova propriedade X'.

Envie para o branch original: git push origin feature/minha-melhoria.

Abra um Pull Request.

## 📝 Licença
Este projeto está licenciado sob MIT License.






