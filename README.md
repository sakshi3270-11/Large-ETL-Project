\# Large ETL Pipeline



\## 📌 Project Overview



This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using Python and Microsoft SQL Server.



The pipeline processes sales transaction data, performs data quality validation, transforms records, supports incremental loading, handles rejected records, enables reprocessing, and maintains audit information for pipeline executions.



The project simulates a real-world data engineering workflow.



\---



\## 🚀 Key Features



\* Extracts sales transaction data from CSV files

\* Validates required columns

\* Detects missing values

\* Detects duplicate transaction IDs

\* Validates quantity and unit price

\* Validates transaction status

\* Validates and recalculates total amount

\* Converts transaction dates

\* Standardizes text and status values

\* Performs incremental loading

\* Skips records already present in the database

\* Stores rejected records separately

\* Supports rejected-record reprocessing

\* Maintains ETL audit information

\* Uses separate configuration for database connectivity

\* Uses `.gitignore` to protect environment files and local data



\---



\## 🛠️ Technologies Used



| Technology           | Purpose                        |

| -------------------- | ------------------------------ |

| Python               | ETL development                |

| Pandas               | Data processing                |

| Microsoft SQL Server | Target database                |

| PyODBC               | Python-SQL Server connectivity |

| Git                  | Version control                |

| GitHub               | Source code management         |

| Jupyter Notebook     | Development and testing        |

| PowerShell           | Execution and administration   |



\---



\## 📂 Project Structure



```text

Large\_ETL\_Project/

│

├── main.py

├── extract.py

├── validate.py

├── transform.py

├── load.py

├── audit.py

├── config.py

├── reprocess.py

├── requirements.txt

├── README.md

├── .gitignore

│

├── Data/

│   └── sales\_transactions\_100000.csv

│

└── Reprocess/

&#x20;   └── corrected\_records.csv

```



> `Data/` and `Reprocess/` are excluded from GitHub using `.gitignore`.



\---



\## 🔄 ETL Workflow



\### 1. Extract



The extraction stage reads sales transaction data from a CSV file using Pandas.



The pipeline identifies the available columns and loads the dataset into a DataFrame for further processing.



Example:



```text

Records extracted: 100005

Columns found: 12

```



\---



\### 2. Validate



The validation stage checks the quality and integrity of incoming data.



Validation includes:



\* Required column validation

\* Missing value detection

\* Duplicate transaction ID detection

\* Quantity validation

\* Unit price validation

\* Transaction status validation

\* Total amount validation



Example validation output:



```text

Required columns check: PASSED

Total missing values: 0

Duplicate transaction IDs: 0

Invalid quantity records: 0

Invalid unit price records: 0

Invalid total amount records: 0

```



Invalid records can be separated from valid records for investigation or reprocessing.



\---



\### 3. Transform



The transformation stage prepares validated data for database loading.



Transformations include:



\* Converting `transaction\_date` to datetime

\* Cleaning text fields

\* Standardizing transaction status

\* Recalculating `total\_amount`

\* Creating `transaction\_year`

\* Creating `transaction\_month`



The total amount is calculated as:



```text

total\_amount = quantity × unit\_price

```



\---



\### 4. Incremental Loading



The pipeline checks which transaction IDs already exist in SQL Server before inserting records.



Existing records are skipped, while new records are inserted.



Example:



```text

CSV records:              100005

Existing database records: 100000

New records detected:           5

Existing records skipped:  100000

New records inserted:           5

```



This prevents duplicate transaction records and allows the pipeline to run repeatedly.



\---



\### 5. Audit Logging



Each ETL execution is recorded in an audit table.



Audit information can include:



\* Run ID

\* Start time

\* End time

\* Records extracted

\* Records validated

\* Records rejected

\* Records inserted

\* Records skipped

\* Pipeline status



Example:



```text

Run ID: 4

Records extracted: 100006

Records validated: 100005

Records rejected: 1

Records inserted: 0

Records skipped: 100005

Status: SUCCESS

```



\---



\### 6. Error Handling and Reprocessing



Invalid records are separated from successfully processed records.



The project includes `reprocess.py` to support correction and reprocessing of rejected records.



Workflow:



```text

Invalid Record

&#x20;     ↓

Rejected Records

&#x20;     ↓

Correction

&#x20;     ↓

reprocess.py

&#x20;     ↓

Validation

&#x20;     ↓

Database Load

```



This allows failed records to be corrected and processed without rerunning the entire dataset.



\---



\## ⚙️ Configuration



Database configuration is maintained separately from the ETL logic.



Sensitive configuration such as database credentials should be stored in environment variables instead of being written directly in Python source code.



Example:



```text

.env

```



The `.env` file is excluded from GitHub using `.gitignore`.



\---



\## 📦 Installation



Clone the repository:



```bash

git clone https://github.com/sakshi3270-11/Large-ETL-Project.git

```



Move into the project directory:



```bash

cd Large-ETL-Project

```



Install the required Python packages:



```bash

pip install -r requirements.txt

```



\---



\## ▶️ Running the Pipeline



Run the main ETL pipeline:



```bash

python main.py

```



The pipeline performs:



```text

Extract

&#x20;  ↓

Validate

&#x20;  ↓

Transform

&#x20;  ↓

Incremental Load

&#x20;  ↓

Audit

```



For rejected-record reprocessing:



```bash

python reprocess.py

```



\---



\## 📊 Example Pipeline Execution



```text

==================================================

SALES ETL PIPELINE STARTED

==================================================



Starting Extract step...

Records extracted: 100005

Columns found: 12



Starting Validation step...

Required columns check: PASSED

Total missing values: 0

Duplicate transaction IDs: 0



Starting Transform step...

Transformation completed successfully.



Starting Load step...

New records detected: 5

Existing records skipped: 100000

New records inserted: 5



Audit run created successfully.



==================================================

SALES ETL PIPELINE COMPLETED

==================================================

```



\---



\## 🎯 Learning Outcomes



Through this project, I practiced:



\* ETL pipeline development

\* Data extraction using Python

\* Data quality validation

\* Data transformation using Pandas

\* SQL Server integration

\* Incremental data loading

\* Duplicate prevention

\* Error handling

\* Rejected-record processing

\* Reprocessing failed records

\* Audit logging

\* Configuration management

\* Git and GitHub version control



\---



\## 🔮 Future Improvements



Potential future enhancements include:



\* Automated email notifications for failed pipelines

\* ETL job scheduling

\* Improved application logging

\* Pipeline monitoring with Splunk or Datadog

\* Unit testing

\* CI/CD integration using GitHub Actions

\* Cloud deployment using Azure Data Factory

\* Data warehouse integration

\* Performance optimization for very large datasets



\---



\## 👩‍💻 Author



\*\*Sakshi Jadhav\*\*



GitHub: https://github.com/sakshi3270-11



