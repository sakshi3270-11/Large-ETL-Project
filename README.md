\# Sales ETL Pipeline



A Python-based ETL pipeline that extracts sales transaction data from CSV,

validates and transforms the data, and incrementally loads it into Microsoft

SQL Server.



\## Project Overview



The pipeline processes 100,000+ sales transaction records and performs:



\- Data extraction from CSV

\- Data quality validation

\- Rejected record handling

\- Data transformation

\- Incremental loading

\- Batch insertion

\- SQL Server integration

\- ETL audit logging

\- Application logging

\- Error handling and rollback

\- Environment-based configuration



\## ETL Flow



CSV File

&#x20;  ↓

Extract

&#x20;  ↓

Validate

&#x20;  ├── Valid Records → Transform → Incremental Load → SQL Server

&#x20;  │

&#x20;  └── Rejected Records → Rejected Files + Error Details

&#x20;                       



\## Technologies Used



\- Python

\- Pandas

\- PyODBC

\- Microsoft SQL Server

\- Python-dotenv

\- Git / GitHub



\## Project Structure



```text

Large\_ETL\_Project/

│

├── Data/

├── Rejected/

├── logs/

│

├── audit.py

├── config.py

├── extract.py

├── load.py

├── main.py

├── transform.py

├── validate.py

│

├── requirements.txt

├── .gitignore

└── README.md

