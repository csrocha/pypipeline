from setuptools import setup

setup(
    name='yamlPipelineFactory',
    version='0.1.0',
    packages=['yamlPipelineFactory'],
    url='',
    license='GPL v3',
    author='Cristian S. Rocha',
    author_email='csrocha@gmail.com',
    description='Pipeline builder from YAML blueprint.',
    install_requires=['PyYAML', 'pandas'],
    test_require=['pytest', 'pytest-asyncio', 'pytest-pep8']
)
