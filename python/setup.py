from setuptools import setup, find_packages

setup(
    name='databricks-reusable-job-clusters',
    version='1.0.0',
    author='Your Name',
    author_email='your@email.com',
    description='A dummy package for demonstration purposes',
    packages=find_packages(exclude=['tests']),
    package_data={'': ['infinite_loop_notebook.template']},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    keywords='dummy package',
)