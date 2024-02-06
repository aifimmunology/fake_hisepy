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

convert to other formats

```bash
conda convert --platform all /opt/homebrew/Caskroom/miniconda/base/conda-bld/noarch/fake_hisepy-0.1.0-py_0.tar.bz2 -o dist_conda/
```


To have conda build upload to anaconda.org automatically, use
```bash
conda config --set anaconda_upload yes
```

### Upload
```bash
python3 -m twine upload --repository testpypi dist/*
```
