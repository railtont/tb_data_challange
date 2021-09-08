
def get_create_logins_query():
    sql = """
        CREATE TABLE IF NOT EXISTS logins
        (
            id bigserial NOT NULL,
            user_id varchar(20),
            login_ts timestamp,
            primary key(user_id, login_ts)
        )
    """
    return sql

def get_create_user_info_query():
    ql = """
        CREATE TABLE IF NOT EXISTS user_info
        (
            user_id varchar(20) not null,
            android boolean not null default false,
            ios boolean not null default false,
            other boolean not null default false,
            last_login timestamp not null,
            primary key(user_id)
        )
    """
    return sql

def get_create_user_country_query():
    sql = """
        CREATE TABLE IF NOT EXISTS user_country
        (
            user_id varchar(20) not null,
            continent varchar(3),
            country varchar(3)
            primary key(user_id)
        )
    """
    return sql

def get_create_users_login_info_query():
    sql = """
        CREATE TABLE IF NOT EXISTS users_login_info
        (
            user_id varchar(20) not null,
            login_ts timestamp,
            last_login timestamp not null,
            first_login timestamp not null,
            android boolean not null default false,
            ios boolean not null default false,
            other boolean not null default false,
            continent varchar(3),
            country varchar(3),
            primary key(user_id, login_ts)
        )
    """
    return sql

def get_create_logins_date_summarize_query():
    sql = """
        CREATE TABLE IF NOT EXISTS logins_date_summarize
        (
            users_count LONG not null,
            logins_count LONG not null,
            login_ts timestamp not null,
            android boolean not null,
            ios boolean not null,
            other boolean not null,
            continent varchar(3),
            country varchar(3)
            primary key(continent, country, android, ios, other, login_ts)
        )
    """


def get_create_first_login_summarize_query():
    sql = """
        CREATE TABLE IF NOT EXISTS first_login_summarize
        (
            users_count LONG not null,
            logins_count LONG not null,
            first_login timestamp not null,
            android boolean not null,
            ios boolean not null,
            other boolean not null,
            continent varchar(3),
            country varchar(3)
            primary key(continent, country, android, ios, other, login_ts)
        )
    """
    return sql