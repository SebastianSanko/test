from airflow import DAG
from airflow.operators.papermill_operator import PapermillOperator
from datetime import datetime, timedelta

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
notebooks_path = "../notebooks/"

# Lista notebooków do wykonania
extract_notebook = 'extract.ipynb'
transform_notebok = 'transform.ipynb'
load_notebook = 'load.ipynb'



# Tworzenie operatora dla każdego notebooka
extract_operator = PapermillOperator(
        task_id=f'extract_ipynb',
        input_nb=notebooks_path + extract_notebook,
        output_nb=notebooks_path + 'output/'+ f'extracted_notebook.ipynb',
        parameters={"count": 5},
        dag=dag
    )
transform_operator = PapermillOperator(
        task_id=f'transform_ipynb',
        input_nb=notebooks_path + transform_notebok,
        output_nb=notebooks_path + 'output/'+ f'transformed_notebook.ipynb',
        parameters={},
        dag=dag
    )
load_operator = PapermillOperator(
        task_id=f'load_ipynb',
        input_nb=notebooks_path + load_notebook,
        output_nb=notebooks_path + 'output/'+ f'loaded_notebook.ipynb',
        parameters={"table_name": "Papermill_test"},
        dag=dag
    )
    
# Definicja kolejności wykonywania zadań
extract_operator >> transform_operator >> load_operator