"""MemQ setuptools
See:
https://packaging.python.org/guides/distributing-packages-using-setuptools/
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / 'README.md').read_text(encoding='utf-8')

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name='memq-client',  # Required
    version='0.2.7',  # Required
    description='MemQ Python Client',  # Optional
    long_description=long_description,  # Optional
    long_description_content_type='text/markdown',  # Optional (see note above)
    author='Ambud Sharma',  # Optional
    author_email='logging@pinterest.com',  # Optional
    classifiers=[  # Optional
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache 2.0',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords='memq-client',  # Optional
    package_dir={'': 'src'},  # Optional
    packages=find_packages(where='src'),  # Required
    python_requires='>=3.6, <4',
    install_requires=['zstandard', 'boto3'],  # Optional
    extras_require={  # Optional
        'test': ['coverage'],
    },
    entry_points={  # Optional
        'console_scripts': [
            'sample=memq.example_batch_consumer:main',
        ],
    },

    project_urls={  # Optional
        'Bug Reports': '#logging-incidents',
    },
)