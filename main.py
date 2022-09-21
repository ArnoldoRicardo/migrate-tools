import json
import os

import psycopg
import strconv
import typer
from dotenv import load_dotenv

from files import load_file, open_csv, save_file
from gdown import download_file_from_google_drive, get_file_id

app = typer.Typer()
load_dotenv()


DATABASE_TYPES = {
    'int': 'INT',
    'str': 'TEXT',
    'date': 'DATE',
    'bool': 'BOLEAN',
    'float': 'DECIMAL',
}


@app.command()
def dowload_from_drive(url: str, file: str):
    file_id = get_file_id(url)
    download_file_from_google_drive(file_id, file)


@app.command()
def create_schema_from_csv(file: str):
    name = file.split('.')[0]
    data = open_csv(file, lines=2)

    columns = ''
    headers = []
    for key, value in zip(*data):
        columns += f', {key} {DATABASE_TYPES[strconv.infer(value)]}\n'
        headers.append(key)

    sql = """
        --sql
        CREATE TABLE IF NOT EXISTS {name} (
            id serial NOT NULL PRIMARY KEY
            {columns}
    );
    """.format(name=name.capitalize(), columns=columns)
    save_file(name=name, data=sql, ext='sql')
    save_file(name=name, data=json.dumps(headers), ext='json')


@app.command()
def create_table(schema: str):
    name = schema.split('.')[0]
    headers = load_file(file=f'{name}.json')[1:-1]

    data = open_csv(f'{name}.csv', headers=False)
    values = [tuple(i) for i in data]

    sql = """
        --sql
        INSERT INTO {name} ({headers})
        VALUES
        {values}
        ;
    """.format(headers=headers, values=str(values)[1:-1], name=name.capitalize())

    sql_create_table = load_file(file=schema)
    with psycopg.connect(os.environ['DB_URL']) as conn:
        with conn.cursor() as cur:
            create = cur.execute(sql_create_table)
            insert = cur.execute(sql)
            print(create)
            print(insert)

        conn.commit()


@app.command()
def update_table(file_csv: str, table_name: str):
    print("check what could the pivot o key")


if __name__ == "__main__":
    app()
