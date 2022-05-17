import setuptools

setuptools.setup(
    name='pilot-platform-common',
    version='0.0.26-dev',
    author='Indoc Research',
    author_email='etaylor@indocresearch.org',
    description='Generates entity ID and connects with Vault (secret engine) to retrieve credentials',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    install_requires=[
        'python-dotenv==0.19.1',
        'httpx==0.22.0',
        'redis>=4.0.0<5.0.0'
    ],
    include_package_data=True,
    package_data={
        '': ['*.crt'],
    },
)
