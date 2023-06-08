from setuptools import setup, find_packages

setup(
    name='databricks-reusable-job-clusters',
    version='1.0.0',
    author='Sri Tikkireddy',
    author_email='sri.tikkireddy@databricks.com',
    description='A package for building airflow operators to reuse Jobs Clusters',
    packages=find_packages(exclude=['tests']),
    package_data={'': ['infinite_loop_notebook.template']},
    install_requires=[
        "aiohttp>=3.6.3, <4",
        "requests>=2.27,<3",
    ],
    extras_require={
            'dev': [
                'pytest',
                'pytest-cov',
                'pytest-asyncio',
            ]
        },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    keywords='Databricks Clusters',
)