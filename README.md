# AVM Study Notebooks

### Setup virtual environment

**Anaconda**

- conda create --name avm-study python=3.9 pip
- conda install --force-reinstall -y -q --name avm-study -c conda-forge --file requirements.txt
- conda activate avm-study
- conda deactivate
- conda env remove -n avm-study

**Install dependencies individually**

```bash
pip install fastparquet geopandas geopy joblib lightgbm openpyxl pandas postcodes_uk recordlinkage scikit-learn scipy seaborn xgboost
```
