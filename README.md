# missioncontrol-v2
An alternate view of crash and stability

## Installation instructions
The python files assume a base conda installation. To install the python dependencies, create a conda environment using the `environment.yml` file:

```python
cd missioncontrol-v2
conda env create -f environment.yml
```

This will create a conda env named `mc2`.


## Instructions to run
To run the python data pull routine, the conda environment needs to be activated

```python
conda activate mc2

```



## Model data download
There are python files to download the required data in `src/`. Creating a conda env with `environment.yml` should download all the required dependencies to get it running. See the README in `src` for more info. The scripts assume bigquery credentials are stored in a file whose path can be passed to `src.crud.main(creds_loc=creds_loc)`.

## Run model
To run the model, R needs to be installed, along with packages:

- brms (to run model)
- curl
- data.table (for data manipulation)
- future
- glue
- parallel
- reticulate (to call python)
- rjson


# Gotchas
- if you run the data pulling code shortly after a new release, and did not pull data in the
previous days, then those days' data will be missing for the previous major release versions.

## Using buildhub_bid.py

```
$ python buildhub_bid.py
{'67.0b19': [('67.0b19', '20190509185224', 1557440284038)], '67.0b18': [('67.0b18', '20190506235559', 1557198168463)], '67.0b17': [('67.0b17', '20190505015947', 1557029673054)], '67.0b16': [('67.0b16', '20190502232159', 1556853606531), ('67.0b16', '20190502224831', 1556845098897)], '67.0b15': [('67.0b15', '20190429125729', 1556568682185)], '67.0b14': [('67.0b14', '20190424140259', 1556129999744)], '67.0b13': [('67.0b13', '20190422163745', 1555961835650)], '67.0b12': [('67.0b12', '20190418160535', 1555623291044)], '67.0b11': [('67.0b11', '20190415085659', 1555336602366)], '67.0b10': [('67.0b10', '20190411084603', 1554986422095)], '67.0b9': [('67.0b9', '20190408123043', 1554735105625)], '67.0b8': [('67.0b8', '20190404130536', 1554387637419), ('67.0b8', '20190404040123', 1554356102631)], '67.0b7': [('67.0b7', '20190331141835', 1554108515909)], '67.0b6': [('67.0b6', '20190328152334', 1553800576367)], '67.0b5': [('67.0b5', '20190325125126', 1553534107445)], '67.0b4': [('67.0b4', '20190322012752', 1553242114648), ('67.0b4', '20190321164326', 1553202047019)], '67.0b3': [('67.0b3', '20190318154932', 1552936543292)]}


release
{'67.0.4': [('67.0.4', '20190619235627', 1560996835776)], '67.0.3': [('67.0.3', '20190618025334', 1560836951266)], '67.0.2': [('67.0.2', '20190607204818', 1560068562495), ('67.0.2', '20190605225443', 1559794985112)], '67.0.1': [('67.0.1', '20190529130856', 1559144445386)], '67.0': [('67.0', '20190516215225', 1558053461988), ('67.0', '20190513195729', 1557789027847)]}

```

## Test
```
$ ipython
import sys
sys.path.insert(0, '/Users/wbeard/repos/missioncontrol-v2/src/')
import src.crud as crud
%load_ext autoreload
%autoreload 2
```

# TODO
- clean up environment.yml
