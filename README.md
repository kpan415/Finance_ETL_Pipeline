# ETL Pipeline for Credit Card and Loan Application Analysis

## Table of Contents

- [Overview](#-overview)
- [Workflow Diagram](#-workflow-diagram)
- [Getting Started](#getting-started)
- [Some Output Preview](#some-output-preview)
- [Possoble Improvements](#possoble-improvements)
- [License](#license)

## Overview

This project uses the following technologies to build and manage an ETL piepline for a [Credit Card dataset](https://github.com/kpan415/Finance_ETL_Pipeline/tree/main/data) and a [Loan Application dataset](https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json). In this project, PySpark features and functions are used to extract, transform, and load data into a MySQL Database. The stored data is subsequently utilized for [interactive querying and generating visualizations](#some-output-preview). For more details, please check out the [project requirement file](link).

- Python (Requests, MySQL Connector, Tabulate)
- MySQL Database
- Apache Spark (PySpark Core, PySpark SQL, PySpark DataFrame)
- Python Visualization and Analytics libraries (Matplotlib)

## Workflow Diagram

!(https://github.com/kpan415/Finance_ETL_Pipeline/blob/main/src/visualization/ETL%20workflow%20diagram.jpg?raw=true)

## Getting Started

1. Clone the entire repository to your local machine.
2. Set up and activate a virtual environment in the project's root directory:
    - On Windows: 
        - 'python -m venv venv' followed by 'venv\Scripts\activate'
    - On Mac: 
        - 'python -m venv venv' followed by 'source venv/bin/activate'
3. Install the required libraries by running:
    - pip install -r requirements.txt
4. Execute the main script:
    - python main.py

## Some Output Preview

!(https://github.com/kpan415/Finance_ETL_Pipeline/blob/main/src/visualization/Front%20end%20console%20sample%20output.PNG?raw=true)

!(https://github.com/kpan415/Finance_ETL_Pipeline/blob/main/src/visualization/Data%20visualization%20sample%20output.PNG?raw=true)

## Possoble Improvements

- A front-end web application used to display data obtained from the database.
- Use Tableau for better data analysis and presentation.

## License

This project uses the following license: [MIT License](https://github.com/rajib1007/Project_3/blob/main/LICENSE).