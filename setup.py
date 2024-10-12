from setuptools import setup, find_packages

setup(
    name='dabAssist',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        # List your package dependencies here
        "subprocess"
        ,"tempfile"
        ,"json"
        ,"pathlib"
    ],
)