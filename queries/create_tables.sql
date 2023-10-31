
create table `transactions.transactions_raw` (
    time	TIMESTAMP NULLABLE,		
    status	STRING NULLABLE,		
    count	INT64 NULLABLE,
);

create table `transactions.anomalies_raw` (
time	TIMESTAMP	NULLABLE,			
status	BOOLEAN	NULLABLE,			
model_name	STRING	NULLABLE,				
model_type	STRING	NULLABLE,	

);

	
