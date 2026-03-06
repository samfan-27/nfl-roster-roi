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
│   └── stats_helpers.py
├── notebooks/
│   └── exploratory.ipynb
├── streamlit_app/
│   ├── app.py                 # navigation + page router
│   ├── requirements.txt
│   ├── .streamlit/
│   │   └── config.toml
│   ├── components/
│   │   ├── __init__.py
│   │   ├── data_utils.py       # all DB queries + helpers (no UI)
│   │   └── charts.py           # plotly chart builders
│   ├── pages/                  # multipage-style modules
│   │   ├── home.py
│   │   ├── by_position.py
│   │   ├── team.py
│   │   └── player.py
│   ├── utils/
│   │   └── fmt.py              # small UI formatting
│   └── static/
├── docs/
│   ├── methodology.md
│   └── data_dictionary.md
└── tests/
    └── integration_test.py
