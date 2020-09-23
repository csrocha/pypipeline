from setuptools import setup

setup(
    name='assemply',
    version='0.1.0',
    packages=['assemply'],
    url='',
    license='GPL v3',
    author='Cristian S. Rocha',
    author_email='csrocha@gmail.com',
    description='Pipeline builder from YAML blueprints.',
    setup_requires=['pytest-runner', 'requests'],
    install_requires=['PyYAML'],
    tests_require=['pytest', 'pytest-asyncio', 'pytest-pep8', 'pytest-mock', 'pytest-cov']
)
