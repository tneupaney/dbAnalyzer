# Dynamic Database Health and Performance Analyzer

This tool dynamically analyzes the health and performance of relational databases, currently supporting SQLite and MySQL. It identifies potential bottlenecks, integrity issues, security vulnerabilities, and provides optimization suggestions.

## Table of Contents

* [Features](#features)
* [Installation](#installation)
* [Usage](#usage)
* [Supported Databases](#supported-databases)
* [License](#license)
* [Contributing](#contributing)

## Features

* **Dynamic Schema Discovery:** Automatically inspects and understands the structure of any given relational database (tables, columns, primary keys, foreign keys, indexes, triggers).
* **Query Performance Analysis:** Generates and executes synthetic queries based on the discovered schema to identify slow queries and suggest indexing improvements.
* **Index Analysis:** Detects missing indexes on foreign keys and frequently queried columns, as well as identifies potentially redundant indexes.
* **Data Integrity Checks:** Verifies foreign key constraints and unique constraints to flag orphaned records or duplicate entries.
* **Security Vulnerability Scan:** Heuristically checks for sensitive data (e.g., plaintext passwords, emails, SSNs, credit card numbers) in text-based columns.
* **Trigger Performance Analysis:** Measures the overhead introduced by database triggers by simulating data insertions.
* **Relationship (JOIN) Performance Analysis:** Evaluates the efficiency of JOIN operations based on discovered foreign key relationships.
* **Comprehensive HTML Report:** Generates an easy-to-read, interactive HTML report with collapsible sections, D3.js visualizations for query performance, and actionable SQL optimization suggestions.
* **Modular Design:** Easily extensible to support additional database types through a pluggable handler system.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/your-username/your-repo-name.git](https://github.com/your-username/your-repo-name.git)
    cd your-repo-name
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate # On Windows: `venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    ```bash
    pip install pandas sqlalchemy pymysql # pymysql is for MySQL support
    ```
    *Note: `matplotlib` is no longer directly used for the main plot, but `pandas` might have it as a dependency for other plotting features if you expand the tool.*

## Usage

To run the analysis tool, execute the `main.py` script:

```bash
python3 main.py