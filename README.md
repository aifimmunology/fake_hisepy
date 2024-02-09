# Fake Hisepy

Fake placeholder for the HISE Python SDK

## Installation

```bash
pip install fake_hisepy
```

### Virtual Environment
```bash
conda env create -f environment.yml
```

```bash
conda activate fake_hisepy
```

```bash
conda deactivate
```

```bash
conda remove -n fake_hisepy --all
```


### Build
Make sure you have the latest version of PyPA’s build installed: 

```bash
python3 -m pip install --upgrade build
```

Run this command from the same directory where pyproject.toml is located:

```bash
python3 -m build
```

conda build and copy the --output-folder

```bash
conda build fake_hisepy --output-folder conda_build
```

remove the intermediate files

```bash
conda build purge
```


To have conda build upload to anaconda.org automatically, use
```bash
conda config --set anaconda_upload yes
```

### Upload
```bash
python3 -m twine upload --repository testpypi dist/*
```

Install the anaconda client
conda install anaconda-client


Log into your Anaconda.org account from your terminal with the command:
anaconda login

Upload using:
anaconda upload ./conda_build/noarch/fake_hisepy-0.1.0-py_0.tar.bz2


You can log out of your Anaconda.org account with the command:
anaconda logout