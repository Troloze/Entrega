from setuptools import setup, find_packages

setup(
    name="sistemas_distribuidos_utils",  # Nome do pacote
    version="0.1",  # Versão do pacote
    packages=find_packages(where="tarefas"),  # Encontra os pacotes na pasta 'tarefas'
    package_dir={"": "tarefas"},
    description="Biblioteca utilitária para sistemas distribuídos.",
    author="Tiago Gonçalves Maia Geraldine",
    author_email="tigogoncalves@discente.ufg.br",
)