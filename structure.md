# Project structure: Roster ROI & EPA Arbitrage

```text
.
├── README.md
├── LICENSE
├── structure.md
├── requirements.txt
├── .gitignore
├── .github/
│   └── workflows/
│       └── update.yml
├── infra/
│   └── supabase/
│       └── ddl.sql
├── etl/
│   ├── etl.py
│   ├── database.py
│   ├── utils.py
│   ├── config.py
│   └── tests/
│       └── test_etl.py
├── src/
│   ├── analysis.py
│   └── stats_helpers.py       # shrinkage functions
├── notebooks/
│   └── exploratory.ipynb
├── streamlit_app/
│   ├── app.py                 # Streamlit app
│   ├── requirements.txt
│   └── static/
├── docs/
│   ├── methodology.md         # metric definitions, caveats, decisions
│   └── data_dictionary.md     # column definitions, sample rules
└── tests/
    └── integration_test.py    # simple E2E on synthetic data
