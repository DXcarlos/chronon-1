from setuptools import setup, find_packages

setup(
    name='test_ctr_model',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'xgboost==2.1.3',
        'numpy==1.26.4',
        'pandas==2.2.3',
        'google-cloud-bigquery==3.38.0',
        'google-cloud-bigquery-storage==2.34.0',
        'db-dtypes==1.4.4',
    ],
)
