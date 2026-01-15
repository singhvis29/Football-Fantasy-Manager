# Football-Fantasy-Manager
EDA and predictive modeling on EPL/FPL data 

## Project structure

- **src/**: Main Python package with project code  
  - **src/data/**: Data ingestion and loading utilities  
  - **src/features/**: Feature engineering code  
  - **src/models/**: Model definitions and training scripts  
  - **src/evaluation/**: Evaluation and metrics  
  - **src/utils/**: Shared helpers and utilities  
- **notebooks/**: Jupyter notebooks for EDA and experimentation  
- **data/**  
  - **data/raw/**: Immutable raw data dumps  
  - **data/processed/**: Cleaned and feature-ready data  
- **models/**: Saved model artifacts, checkpoints, and metadata  
- **scripts/**: CLI-style scripts to run common workflows  
- **tests/**: Automated tests for the `src` code  

The `data_ingestion.py` module has been moved to `src/data/data_ingestion.py`.
