import csv
from typing import Generator


def open_csv(file: str, start: int = 0, end: int = None, headers: bool = True) -> Generator:
    if not headers:
        start = 1
    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for index, row in enumerate(csv_reader):
            if index < start:
                continue
            if index == end:
                break
            yield row


def save_file(name: str, data: str, ext: str):
    with open(f'{name}.{ext}', 'w') as file:
        file.write(data)
    print(f'archivo {name}.{ext} creado')


def load_file(file: str):
    with open(file, "r") as f:
        lines = f.readlines()
        return ''.join([line for line in lines])
