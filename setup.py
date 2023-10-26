from setuptools import setup, find_packages
import os

# Reading the version from a file to avoid importing the package itself
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'VERSION')) as version_file:
    version = version_file.read().strip()

# Reading the long description from README.md
with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='convector',  # Name of package
    version=version,  # Version of package
    description='A tool for transforming conversational data to a unified format',  # A short description
    long_description=long_description,  # A long description read from README.md
    long_description_content_type='text/markdown',  # Specifying that the long description is in markdown format
    
    url='https://github.com/teilomillet/convector',  # URL of project
    
    author='Teïlo Millet', 
    author_email='teilomillet@proton.me', 
    
    classifiers=[  # Classifiers help users find your project and should match the "trove classifiers" on PyPI
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    
    keywords='conversational data transformation',  # Keywords
    
    packages=find_packages(where='src', exclude=['contrib', 'docs', 'tests']), # Automatically discover and include all packages

    package_dir={'':'src'},
    
    python_requires='>=3.6, <4',  
    
    install_requires=[  # Dependencies for package
        'tqdm',
        'pandas',
        'zstandard',
        'polars'
        # Add other dependencies here
    ],
    
    
    entry_points={  # Creating executable commands
        'console_scripts': [
            'convector=convector.cli:convector', # 'cli' refers to the name of main group function
        ],
    },
    
    project_urls={  # Additional URLs related to project
        'Bug Reports': 'https://github.com/teilomillet/convector/issues',
        'Source': 'https://github.com/teilomillet/convector/',
    },
)
