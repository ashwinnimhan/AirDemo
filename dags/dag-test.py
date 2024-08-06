from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'math_operations_dag_taskflow',
    default_args=default_args,
    description='A simple DAG with math operations using TaskFlow API',
    schedule_interval=timedelta(days=1),
)

@task
def add(x, y):
    return x + y

@task
def subtract(x, y):
    return x - y

@task
def multiply(x, y):
    return x * y

@task
def divide(x, y):
    if y != 0:
        return x / y
    else:
        raise ValueError("Cannot divide by zero")

@task
def print_result(result):
    print(f"The result is: {result}")

with dag:
    # Task 1: Addition
    add_result = add(10, 5)

    # Task 2: Subtraction
    subtract_result = subtract(20, 7)

    # Task 3: Multiplication (depends on addition)
    multiply_result = multiply(add_result, 4)

    # Task 4: Division (depends on subtraction)
    divide_result = divide(subtract_result, 8)

    # Task 5: Print result of multiplication
    print_multiply_result = print_result(multiply_result)

    # Task 6: Print result of division
    print_divide_result = print_result(divide_result)
