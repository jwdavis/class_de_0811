import sys

from airflow.models.dagbag import DagBag

# Replace with your local DAG file path
DAG_FILE = "<dag_name>.py"

dag_bag = DagBag(dag_folder=DAG_FILE, include_examples=False)

errors = dag_bag.import_errors

if errors:
    print("❌ DAG import errors:")
    for f, err in errors.items():
        if DAG_FILE in f:
            print(f"\nFile: {f}\nError:\n{err}")
    sys.exit(1)
else:
    print("✅ DAG parsed successfully.")
    sys.exit(0)
