# DETDE

DETDE is a data extraction tool to extract object-centric event logs from ERP systems.

## Installation

First, make sure [Graphviz](https://graphviz.org/download/) is installed.
Then run:
```bash
pip install -U -r requirements.txt
```

## Usage
* You might need to adjust timestamp formats in ***globals.py*** to your database.
* To change the extracted database schemas modify *sql_schemas_X.py* where ***X*** is the respective database type.
* Run main.py to launch the application.
