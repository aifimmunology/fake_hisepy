from setuptools import setup, find_packages

setup(
    name='hisepy',
    version='0.1.0',  # You can change the version as needed
    author='Your Name',
    author_email='your.email@example.com',
    description='A brief description of hisepy',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/hisepy',  # Replace with your own URL
    packages=find_packages(),
    install_requires=[
        # List your project dependencies here
        # e.g., 'numpy', 'pandas'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',  # Change as appropriate
        'Intended Audience :: Developers',
        'Natural Language :: English',
        # Add more classifiers as appropriate
        # See https://pypi.org/classifiers/ for a list
    ],
    python_requires='>=3.6',  # Adjust the Python version as needed
)
