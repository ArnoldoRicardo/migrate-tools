import csv
import json
import os
from typing import Generator

import psycopg
import strconv
import typer
from dotenv import load_dotenv

app = typer.Typer()
load_dotenv()


@app.command()
def dowload_from_drive(url: str):
    print("Hello World")


def open_csv(file: str, lines: int = None, headers: bool = True) -> Generator:
    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for index, row in enumerate(csv_reader):
            if not headers:
                if index == 0:
                    continue
            if index == lines:
                break
            yield row


DATABASE_TYPES = {
    'int': 'INT',
    'str': 'TEXT',
    'date': 'DATE',
    'bool': 'BOLEAN',
    'float': 'DECIMAL',
}


def save_file(name: str, data: str, ext: str):
    with open(f'{name}.{ext}', 'w') as file:
        file.write(data)
    print(f'archivo {name}.{ext} creado')


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


def load_file(file: str):
    with open(file, "r") as f:
        lines = f.readlines()
        return ''.join([line for line in lines])


@app.command()
def create_table(schema: str):
    name = schema.split('.')[0]
    headers = load_file(file=f'{name}.json')[1:-1]

    data = open_csv(f'{name}.csv', headers=False)
    values = [tuple(i) for i in data]

    sql = """
        --sql
        INSERT INTO table_name ({headers})
        VALUES
        {values}
        ;
    """.format(headers=headers, values=values)

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
    print("Hello World")


if __name__ == "__main__":
    app()
