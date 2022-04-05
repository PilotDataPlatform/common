import setuptools

setuptools.setup(
    name='common',
    version='0.0.20',
    author='Indoc Research',
    author_email='etaylor@indocresearch.org',
    description='Generates entity ID and connects with Vault (secret engine) to retrieve credentials',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    install_requires=[
        'python-dotenv==0.19.1',
    ],
    include_package_data=True,
    package_data={
        '': ['*.crt'],
    },
)
