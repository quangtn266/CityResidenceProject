import logging

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor':'1'};
    """)

    print("Keyspace created successfully")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            firstname TEXT,
            lastname TEXT,
            gender TEXT,
            address TEXT,
            postcode TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)

    print("Table is created succesfully")

def insert_data(session, **kwargs):
    print("inserting data.....")

    user_id = kwargs.get('id')
    firstname = kwargs.get('firstname')
    lastname = kwargs.get('lastname')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture  = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_user(id, firstname, lastname, gender,
                address, postcode, email, username, dob, registered_date, phone,
                picture)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, firstname, lastname, gender, address, postcode, email,
              username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {firstname} {lastname}")

    except Exception as e:
        logging.error(f"could not insert data due to {e}")



