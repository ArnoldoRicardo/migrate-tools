# migrate-tools

esta herramienta te ayudara a crear tablas en postgres apartir de archivos csv

los comandos disponibles son

- create-schema-from-csv
    `python main.py create-schema-from-csv actual.csv`

    crea las configuraciones y deteccion de tipos de datos para crear las
    tablas

- create-table
    `python main.py create-table actual.sql`

    crea la tabla y inserta los datos del csv en la bd

- dowload-from-drive
    `python main.py download-from-drive https://drive.google.com/drive/u/0/folders/14NlPgBKElxnqRJjD9nZmtH3WkCUEsS2G actual.csv`

    descarga archivos publicos de google drive

- update-table
    `python main.py update-table actual-last-insert.json`

    actualizar datos de las tablas

    Nota: en proceso
