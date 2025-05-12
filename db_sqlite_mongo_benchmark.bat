
@echo off

echo.
echo Creating the SQLite databases...
python -m scripts.01_create_sqlite_databases
echo.
echo Converting the dataset CSV files to JSON...
python -m scripts.02_convert_csv_to_json
echo.
echo Creating the MongoDB databases...
python -m scripts.03_create_mongo_databases
echo.
echo Running the queries without indexing...
python -m scripts.04_run_queries
echo Check the results folder
echo.
echo Creating the database indexes...
python -m scripts.05_create_indexes
echo.
echo Running the first 3 queries and comparing query performance before and after indexing...
python -m scripts.06_compare_query_performance
echo Check the results folder
echo Script completed successfully!!!
pause