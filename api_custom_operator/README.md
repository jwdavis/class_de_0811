# Simple Shopping List Operator - Quick Setup

## What This Demonstrates
- **Custom Operator**: Extends `BaseOperator` for reusable API-to-GCS functionality
- **API Integration**: HTTP requests with error handling
- **Cloud Storage**: GCS upload using Airflow hooks
- **Templating**: Dynamic file names with `{{ ds }}` (execution date)
- **XCom**: Passing data between tasks

## Quick Setup

### 1. File Structure
```
dags/
├── shopping_list_dag.py
└── operators/
    └── shopping_list_operator.py
```