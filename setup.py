import setuptools

setuptools.setup(
    name='bq-cdc-pipeline',
    version='0.0.1',
    install_requires=[],
    packages=setuptools.find_packages(),
    py_modules=['bq_common', 'config', 'utils'],
)
