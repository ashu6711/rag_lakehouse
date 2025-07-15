from setuptools import find_packages, setup

requirements = [
    "minio==7.2.15",
    "pandas==2.3.1",
    "pyspark==4.0.0",
    "beautifulsoup4==4.12.3",
    "lxml==5.3.0",
    "apache-airflow-providers-apache-spark==4.4.0",
    "langchain-community", 
    "chromadb",
    "ollama",
    "sentence-transformers",
    "langchain-experimental"
]

setup(
    name="airflow_fw",
    version="1.0",
    description="Python Proj",
    author="Ashutosh",
    author_email="",
    packages=find_packages(),
    install_requires=requirements,
)
