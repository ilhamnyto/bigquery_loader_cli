# BigQuery Loader Command Line Interface

This project is a personal learning project aimed at loading data from your database or files into BigQuery using command line interface. you can also using SSH tunnel to connect to your database to load the data from it. the program will automatically load your selected table to your BigQuery project.

## Features

- Load data from database or files to BigQuery

## Supported Database
- [x] MySQL
- [x] PostgreSQL

## Supported Files
- [x] CSV
- [x] Excel
- [x] JSON

## Installation

To run this project locally, follow these steps:

1. Clone the repository: `git clone https://github.com/ilhamnyto/bigquery_loader_cli.git`
2. Create a Virtual Environment: `virtualenv venv`
3. Activate virtualenv `source venv/Scripts/activate` (Windows) or `source venv/bin/activate` (Linux)
4. Install dependencies: `pip install -r requirements.txt`.
5. Place your BigQuery projects service account json file in the root folder.
6. If you want to load data from files you can place the file ing the `files` folder. 
7. Rename your service account json file as `credentials.json` so it will automatically detected by the program (Optional).
8. Run the program: `python app.py`

## License

This project is licensed under the [MIT License](./LICENSE).

