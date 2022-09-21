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
    data = open_csv(file, end=2)

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

    save_file(name=f'{name}-last-entry', data=json.dumps(values[-1]), ext='json')

    sql_create_table = load_file(file=schema)
    with psycopg.connect(os.environ['DB_URL']) as conn:
        with conn.cursor() as cur:
            create = cur.execute(sql_create_table)
            insert = cur.execute(sql)
            print(create)
            print(insert)

        conn.commit()


@app.command()
def sync_table(file: str):
    name = file.split('.')[0]
    raw_headers = load_file(file=f'{name}.json')
    headers = load_file(file=f'{name}.json')[1:-1]
    data = open_csv(f'{name}.csv', headers=False)
    values = [tuple([key, *i]) for key, i in enumerate(data)]
    updates = ''.join([f', {name} = EXCLUDED.{name}' for name in json.loads(raw_headers)])

    sql = '''
        --sql
        INSERT INTO {name} (id, {headers})
        VALUES
        {values}
        ON CONFLICT (id) DO UPDATE SET
        id = EXCLUDED.id
        {updates}
    '''.format(name=name.capitalize(), headers=headers, values=str(values)[1:-1], updates=updates)
    with psycopg.connect(os.environ['DB_URL']) as conn:
        with conn.cursor() as cur:
            insert_or_update = cur.execute(sql)
            print(insert_or_update)
        conn.commit()
    print('todo actualizado')


@app.command()
def update_table(file: str):
    name = file.split('.')[0]
    headers = load_file(file=f'{name}.json')[1:-1]
    raw_last_entry = json.loads(load_file(file=f'{name}-last-entry.json'))
    last_entry = tuple((strconv.convert(column) for column in raw_last_entry))

    sql = f"SELECT * FROM {name} ORDER BY ID DESC LIMIT 1"
    with psycopg.connect(os.environ['DB_URL']) as conn:
        with conn.cursor() as curs:
            row = curs.execute(sql).fetchone()
        if not row[1:] == last_entry:
            data = open_csv(file, start=(row[0] + 1))
            values = [tuple(i) for i in data]
            sql_insert = """
                --sql
                INSERT INTO {name} ({headers})
                VALUES
                {values}
                ;
            """.format(headers=headers, values=str(values)[1:-1], name=name.capitalize())
            with conn.cursor() as curs:
                insert = curs.execute(sql_insert)
                print(insert)
                print('columnas nuevas agregadas')


if __name__ == "__main__":
    app()
