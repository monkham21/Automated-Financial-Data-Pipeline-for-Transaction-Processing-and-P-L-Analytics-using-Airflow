finance-data-pipeline/
│
├── dags/
│   └── finance_pipeline_dag.py
│
├── src/
│   ├── extract/
│   │   └── api_client.py
│   │
│   ├── transform/
│   │   └── transform.py
│   │
│   ├── load/
│   │   └── load_to_db.py
│   │
│   └── utils/
│       └── logger.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── sql/
│   ├── create_tables.sql
│   └── pnl_transformation.sql
│
├── tests/
│   └── test_transform.py
│
├── .env
├── requirements.txt
├── docker-compose.yml   (optional but 🔥 for interviews)
└── README.md