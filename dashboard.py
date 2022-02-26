import streamlit as st
from sqlalchemy import create_engine, exc
from dotenv import load_dotenv
import os

env_path = os.path.join('.env')
load_dotenv(env_path)

username_mysql = os.getenv('username_mysql')
password_mysql = os.getenv('password_mysql')
host_mysql = os.getenv('host_mysql')
port_mysql = os.getenv('port_mysql')
DB_NAME_mysql = os.getenv('DB_NAME_mysql')

engine = create_engine(f"mysql://{username_mysql}:{password_mysql}@{host_mysql}:{port_mysql}/{DB_NAME_mysql}")


def update_record_contact(field, id, new_value, engine):
    try:
        engine.execute("UPDATE contact SET %s = '%s' WHERE id = '%s'" % (field, new_value, id))
    except exc.SQLAlchemyError as err:
        print("Unexpected error: {0}".format(err))
        raise


def update_record_booking(field, id, new_value, engine):
    try:
        engine.execute("UPDATE booking SET %s = '%s' WHERE id = '%s'" % (field, new_value, id))
    except exc.SQLAlchemyError as err:
        print("Unexpected error: {0}".format(err))
        raise


def delete_record_contact(id, engine):
    try:
        engine.execute("DELETE FROM contact  WHERE id = '%s'" % (id))
    except exc.SQLAlchemyError as err:
        print("Unexpected error: {0}".format(err))
        raise


def delete_record_booking(id, engine):
    try:
        engine.execute("DELETE FROM booking WHERE id = '%s'" % (id))
    except exc.SQLAlchemyError as err:
        print("Unexpected error: {0}".format(err))
        raise

st.title('Update and Delete Records')
column_1, column_2 = st.columns(2)

with column_1:
    st.header('Update Records Contact')
    field = st.selectbox('Please select field to update', ('first_name', 'last_name', 'email', 'telephone_number'))
    name_key = st.text_input('Please enter ID of contact to be updated')
    if field == 'first_name':
        updated_first_name = st.text_input('Please enter updated '+field+ ' ')
        if st.button('Update Contact Detail'):
            update_record_contact(field, name_key, updated_first_name, engine)
            st.info('Updated name to **%s** in record **%s**' % (updated_first_name, name_key))
    elif field == 'last_name':
        updated_last_name = st.text_input('Please enter updated '+field+ ' ')
        if st.button('Update Contact Detail'):
            update_record_contact(field, name_key, updated_last_name, engine)
            st.info('Updated details to  **%s** in record **%s**' % (updated_last_name, name_key))
    elif field == 'email':
        updated_email = st.text_input('Please enter updated '+field+  ' ')
        if st.button('Update Contact Detail'):
            update_record_contact(field, name_key, updated_email, engine)
            st.info('Updated details to  **%s** in record **%s**' % (updated_email, name_key))
    elif field == 'telephone_number':
        updated_telephone_numbner = st.text_input('Please enter updated '+field+ ' ')
        if st.button('Update Contact Detail'):
            update_record_contact(field, name_key, updated_telephone_numbner, engine)
            st.info('Updated details to  **%s** in record **%s**' % (updated_telephone_numbner, name_key))

    st.header('Delete Record From Contact')
    name_key = st.text_input('Please enter ID of contact to be deleted')
    if st.button('Delete contact'):
        delete_record_contact(name_key, engine)
        st.info('Delete Record Contact id : **%s**' % (name_key))

with column_2:
    st.header('Update Records Booking')
    field = st.selectbox('Please select field to update', ('booking_reference', 'booking_amount', 'notes'))
    name_key = st.text_input('Please enter ID of Booking to be updated')
    if field == 'booking_reference':
        updated_booking_reference = st.text_input('Please enter updated ' + field + ' ')
        if st.button('Update Booking Detail'):
            update_record_booking(field, name_key, updated_booking_reference, engine)
            st.info('Updated name to **%s** in record **%s**' % (updated_booking_reference, name_key))
    elif field == 'booking_amount':
        updated_booking_amount = st.text_input('Please enter updated ' + field + ' ')
        if st.button('Update Booking Detail'):
            update_record_booking(field, name_key, updated_booking_amount, engine)
            st.info('Updated details to  **%s** in record **%s**' % (updated_booking_amount, name_key))
    elif field == 'notes':
        updated_notes = st.text_input('Please enter updated ' + field + ' ')
        if st.button('Update Booking Detail'):
            update_record_booking(field, name_key, updated_notes, engine)
            st.info('Updated details to  **%s** in record **%s**' % (updated_notes, name_key))

    st.header('Delete Record From Booking')
    name_key = st.text_input('Please enter ID of booking to be deleted')
    if st.button('Delete Booking'):
        delete_record_booking(name_key, engine)
        st.info('Deletd Record Booking id : **%s**' % (name_key))