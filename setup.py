from setuptools import setup, find_packages

setup(
    name='fake_hisepy',
    version='0.1.2',  # You can change the version as needed
    author='Paul Mariz',
    author_email='paul.mariz@alleninstitute.org',
    description='A brief description of hisepy',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/aifimmunology/fake_hisepy',  # Replace with your own URL
    packages=find_packages(),
    install_requires=[
        # List your project dependencies here
        # e.g., 'numpy', 'pandas'
        'numpy',
        'pandas',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering',

        # Pick your license as you wish (see also "license" above)
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
          "Programming Language :: Python :: 3.11",
        # "Programming Language :: Python :: 3.10",
        # "Programming Language :: Python :: 3.11",
        # "Programming Language :: Python :: 3 :: Only",
        # Add more classifiers as appropriate
        # See https://pypi.org/classifiers/ for a list
    ],
    python_requires='>=3.6',  # Adjust the Python version as needed
)
