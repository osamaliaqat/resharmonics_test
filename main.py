import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, exc
import sys
import logging
import petl
logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
sh.setFormatter(formatter)
logger.addHandler(sh)
from dotenv import load_dotenv
import os

env_path = os.path.join('.env')
load_dotenv(env_path)

username_mysql = os.getenv('username_mysql')
password_mysql = os.getenv('password_mysql')
host_mysql = os.getenv('host_mysql')
port_mysql = os.getenv('port_mysql')
DB_NAME_mysql = os.getenv('DB_NAME_mysql')

username_staging_psql = os.getenv('user_staging_psql')
password_staging_psql = os.getenv('pass_staging_psql')
host_staging_psql = os.getenv('host_staging_psql')
port_staging_psql = os.getenv('port_staging_psql')
DB_NAME_staging_psql = os.getenv('DB_staging_psql')

username_prod_psql = os.getenv('user_prod_psql')
password_prod_psql = os.getenv('pass_prod_psql')
host_prod_psql = os.getenv('host_prod_psql')
port_prod_psql = os.getenv('port_prod_psql')
DB_NAME_prod_psql = os.getenv('DB_prod_psql')


sql_engine = create_engine(f"mysql://{username_mysql}:{password_mysql}@{host_mysql}:{port_mysql}/{DB_NAME_mysql}")
staging_pg_engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{username_staging_psql}:{password_staging_psql}@{host_staging_psql}:{port_staging_psql}/{DB_NAME_staging_psql}")
staging_pg_conn = staging_pg_engine.connect()
prod_pg_engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{username_prod_psql}:{password_prod_psql}@{host_prod_psql}:{port_prod_psql}/{DB_NAME_prod_psql}")
prod_pg_conn = prod_pg_engine.connect()


def extract():
    try:
        print('----EXCTRACTING----')
        xls = pd.ExcelFile('Technical Test Data.xlsx')
        sheets = xls.sheet_names
        ex_op = open('Technical Test Data.xlsx', 'rb')
        for i in sheets:
            table_name = i
            df = pd.read_excel(ex_op, sheet_name=i)
            df = df.fillna(0)
            df.to_sql(con=sql_engine, name=table_name, if_exists='replace', schema=None)
            print('----EXTRACTION SUCCESSFULL----')
        ex_op.close()
    except Exception as e:
        print(e)


def load():
    try:
        print('----LOADING FROM MYSQL TO PG----')
        contacts = petl.fromdb(sql_engine, 'SELECT id, first_name, last_name, email, telephone_number FROM contact')
        bookings = petl.fromdb(sql_engine, 'SELECT id, contact_id, booking_reference, start_date, end_date, booking_amount, notes FROM booking')

        staging_pg_engine.execute("DROP TABLE IF EXISTS contact")
        staging_pg_engine.execute("CREATE TABLE IF NOT EXISTS  contact ( id INT PRIMARY KEY, first_name VARCHAR(45), last_name VARCHAR(45), email VARCHAR(50),telephone_number VARCHAR(50) ) ")

        staging_pg_engine.execute("DROP TABLE IF EXISTS booking")
        staging_pg_engine.execute("CREATE TABLE IF NOT EXISTS booking ( id INT PRIMARY KEY,  contact_id INT, booking_reference VARCHAR(45), start_date DATE, end_date DATE, booking_amount INT, notes DOUBLE PRECISION ,index INT ) ")


        petl.todb(contacts, staging_pg_conn, 'contact')
        petl.todb(bookings, staging_pg_conn, 'booking')
        print('----LOADING SUCCESSFULL----')

    except exc.SQLAlchemyError as err:
        print("Unexpected error: {0}".format(err))
        raise


def transform():
    try:
        print('----TRANSFORMING PG TO PRODUCTION----')
        prod_pg_engine.execute("DROP TABLE IF EXISTS BookingDaily")
        prod_pg_engine.execute(
            "CREATE TABLE IF NOT EXISTS BookingDaily ( id INT PRIMARY KEY,  contact_id INT, booking_reference VARCHAR(45), date DATE,  booking_daily_amout INT ) ")


        contacts = petl.fromdb(staging_pg_engine, 'SELECT id, first_name, last_name, email, telephone_number FROM contact')
        bookings = petl.fromdb(staging_pg_engine,
                               'SELECT id, contact_id, booking_reference, start_date, end_date, booking_amount, notes FROM booking')

        prod_pg_engine.execute("DROP TABLE IF EXISTS contact")
        prod_pg_engine.execute(
            "CREATE TABLE IF NOT EXISTS  contact ( id INT PRIMARY KEY, first_name VARCHAR(45), last_name VARCHAR(45), email VARCHAR(50),telephone_number VARCHAR(50) ) ")

        prod_pg_engine.execute("DROP TABLE IF EXISTS booking")
        prod_pg_engine.execute(
            "CREATE TABLE IF NOT EXISTS booking ( id INT PRIMARY KEY,  contact_id INT, booking_reference VARCHAR(45), start_date DATE, end_date DATE, booking_amount INT, notes DOUBLE PRECISION ,index INT ) ")

        petl.todb(contacts, prod_pg_conn, 'contact')
        petl.todb(bookings, prod_pg_conn, 'booking')
        print('----TRANSFORMING SUCCESSFULL----')

    except exc.SQLAlchemyError as err:
        print("Unexpected error: {0}".format(err))
        raise


extract()
load()
transform()
