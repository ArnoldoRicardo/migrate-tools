import csv
from typing import Generator

import strconv
import typer

app = typer.Typer()


def open_csv(file: str, index: int = None, headers: bool = True) -> Generator:
    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if not headers:
                if line_count == 0:
                    continue
            yield row
            if line_count == index:
                break
            line_count += 1


DATABASE_TYPES = {
    'int': 'INT',
    'str': 'TEXT',
    'date': 'DATE',
    'bool': 'BOLEAN',
    'float': 'DECIMAL',
}


def save_schema(name: str, query: str):
    with open(f'{name}.sql', 'w') as file:
        file.write(query)
    print(f'archivo {name}.sql creado')


@app.command()
def dowload_from_drive(url: str):
    print("Hello World")


@app.command()
def create_schema_from_csv(file: str):
    name = file.split('.')[0]
    data = open_csv(file, index=1)

    columns = ''
    for key, value in zip(*data):
        columns += f', {key} {DATABASE_TYPES[strconv.infer(value)]}\n'

    sql = """
        --sql
        CREATE TABLE IF NOT EXISTS {name} (
            id serial NOT NULL PRIMARY KEY
            {columns}
    );
    """.format(name=name.capitalize(), columns=columns)
    save_schema(name=name, query=sql)


@app.command()
def create_table(schema: str):
    print("Hello World")


@app.command()
def update_table(file_csv: str, table_name: str):
    print("Hello World")


if __name__ == "__main__":
    app()
