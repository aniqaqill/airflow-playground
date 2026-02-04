from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(schedule="@daily", catchup=False)
def user_processing():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                firstname VARCHAR(255),
                lastname VARCHAR(255),
                email VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    @task.sensor(poke_interval=30, timeout=3000)
    def is_api_available() -> PokeReturnValue:
        import requests
        response = requests.get("https://dummyuser.vercel.app/users")
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

    @task
    def extract_user(fake_user):
        user = fake_user[0]
        return {
            "id": user["id"],
            "firstname": user["first_name"],
            "lastname": user["last_name"],
            "email": user["email"],
        }
    
    @task
    def store_user(user_info):
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.run(
            "INSERT INTO users (firstname, lastname, email) VALUES (%s, %s, %s)",
            parameters=(user_info["firstname"], user_info["lastname"], user_info["email"])
        )

    # Define Dependencies
    fake_user_data = is_api_available()
    create_table >> fake_user_data
    
    user_data = extract_user(fake_user_data)
    store_user(user_data)

user_processing()