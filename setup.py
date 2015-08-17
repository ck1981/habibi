from setuptools import setup, find_packages

description = "Habibi is a testing tool which scalarizr team uses to mock scalr's side of communication."

cfg = dict(
    name="habibi",
    version=open('version').read().strip(),
    description=description,
    long_description=description,
    author="Scalr Inc.",
    author_email="spike@scalr.com",
    url="https://scalr.net",
    license="GPL",
    platforms="any",
    packages=find_packages(),
    include_package_data=True,
    install_requires=['peewee==2.6.3', 'pyOpenSSL']
)
setup(**cfg)
