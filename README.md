# Database Burn

simulate concurrency read and update to database making an heavy load.

## background
someday my friend told me that he met problem when sync sql server database from China to USA. he ask me how to optimize this.  
To test various options that sql server provide. I need a benchmark took to make lots of writes to database. search though the internet I found hammerdb a tpcc tool can do this, but unfortunately it only support sql server 2017 and above, I need test sql server 2014. so I decide write a simple tool.

## Usage

the database connection is all you need. other parameters you can use default value.

### first prepare data rows
```shell
python main.py prepare --db_url "mssql+pyodbc://sa:123456@127.0.0.1:1433/bench_temp_db?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no"
```

### then burn the database
```shell
python main.py burn --db_url "mssql+pyodbc://sa:123456@127.0.0.1:1433/bench_temp_db?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no"
```
