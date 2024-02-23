from airflow import DAG
from airflow.operators.papermill_operator import PapermillOperator
from datetime import datetime, timedelta
import os
from pathlib import Path

BASE_PATH = os.path.dirname(Path(os.path.dirname(os.path.abspath(__file__))))
loc = BASE_PATH



# Ustawienia dla DAG-a
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definicja DAG-a
dag = DAG(
    'execute_notebooks',
    default_args=default_args,
    description='DAG do wykonania notebooków',
    schedule_interval=None,  # None oznacza, że nie ma harmonogramu
)

# Ścieżka do katalogu, gdzie znajdują się notebooki
notebooks_path = os.path.join(loc, 'notebooks')

# Lista notebooków do wykonania
extract_notebook = 'extract.ipynb'
transform_notebok = 'transform.ipynb'
load_notebook = 'load.ipynb'



# Tworzenie operatora dla każdego notebooka
extract_operator = PapermillOperator(
        task_id=f'extract_ipynb',
        input_nb=os.path.join(notebooks_path , extract_notebook),
        output_nb=os.path.join(notebooks_path, 'output', 'extracted_notebook.ipynb'),
        parameters={"count": 20},
        dag=dag
    )
transform_operator = PapermillOperator(
        task_id=f'transform_ipynb',
        input_nb=os.path.join(notebooks_path , transform_notebok),
        output_nb=os.path.join(notebooks_path, 'output', 'transformed_notebook.ipynb'),
        parameters={},
        dag=dag
    )
load_operator = PapermillOperator(
        task_id=f'load_ipynb',
        input_nb=os.path.join(notebooks_path , load_notebook),
        output_nb=os.path.join(notebooks_path, 'output', 'loaded_notebook.ipynb'),
        parameters={"table_name": "Papermill_test"},
        dag=dag
    )
    
# Definicja kolejności wykonywania zadań
extract_operator >> transform_operator >> load_operator