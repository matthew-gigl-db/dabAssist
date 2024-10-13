from setuptools import setup, find_packages

setup(
    name='dab_assist'
    ,version='0.1'
    ,packages=find_packages(include=['dab_assist', 'dab_assist.*'])
    # ,install_requires=[
    #     # List your package dependencies here
    #     # subprocess and tempfile are required
    # ]
)