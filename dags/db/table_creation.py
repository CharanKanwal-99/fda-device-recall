import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import os


def create_table():
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = 'redshift-cluster-1.c09jeylcxwnb.us-east-2.redshift.amazonaws.com'
    DB_PORT = 5432
    
    connection = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
    )
    
    db = 'openfda'
    create_db_query = f'CREATE DATABASE {db};'

    with connection.cursor() as cursor:
        cursor.execute(create_db_query)
        connection.close()
    
    db_url = 'postgresql+psycopg2://aws_charan:Kewaldotuniv1!@redshift-cluster-1.c09jeylcxwnb.us-east-2.redshift.amazonaws.com:5439/openfda'
    engine = create_engine(db_url)

# SQLAlchemy metadata object
    metadata = MetaData()

# Define tables
    firm_table = Table(
    'firm',
    metadata,
    Column('firm_id', Integer, primary_key=True),
    Column('firm', String),
    Column('state', String),
    Column('city', String),
    Column('address_1', String),
    Column('address_2', String),
    Column('postal_code', String)

)

    status_table = Table(
    'status',
    metadata,
    Column('status_id', Integer, primary_key=True),
    Column('status', String)
)
    status_table = Table(
    'cause',
    metadata,
    Column('cause_id', Integer, primary_key=True),
    Column('cause', String)

    )

    recall_table = Table(
    'recall',
    metadata,
    Column('recall_id', Integer, primary_key=True),
    Column('firm_id', Integer),
    Column('status_id', Integer),
    Column('cause_id', Integer),
    Column('product_quantity', String),
    Column('distribution_pattern', String),
    Column('product_code', String),
    Column('device_class', Integer)


)



# Create tables
    metadata.create_all(engine)
