def get_calculate_first_login_summarize_query():
    sql = """
        insert into first_login_summarize (
            select 
                count(*) as users_count, 
                logins_count, 
                first_login,
                android, ios, other, continent, country
            from (
                select 
                    count(*) logins_count, 
                    user_id, 
                    first_login,
                    android,
                    ios,
                    other,
                    continent,
                    country
                from users_login_info
                group by 2, 3, 4, 5, 6, 7, 8
            )
            group by 2, 3, 4, 5, 6, 7, 8
        )
    """
    return sql

def get_calculate_login_date_summarize_query():
    sql = """
        insert into login_date_summarize (
            select 
                count(*) as users_count, 
                logins_count, 
                login_ds,
                android, ios, other, continent, country
            from (
                select 
                    count(*) logins_count, 
                    user_id, 
                    login_ds,
                    android,
                    ios,
                    other,
                    continent,
                    country
                from users_login_info
                group by 2, 3, 4, 5, 6, 7, 8
            )
            group by 2, 3, 4, 5, 6, 7, 8
        )
    """
    return sql

def get_calculate_users_login_info_query():
    sql = """
        insert into users_login_info (
            select 
                logins.user_id,
                logins.login_ts,
                user_info.android,
                user_info.ios,
                user_info.other,
                user_info.last_login,
                first_login.first_login,
                user_country.continent,
                user_country.country
            from logins left join user_info
            on logins.user_id = user_info.user_id
            left join user_country
            on logins.user_id = user_country.user_id
            left join (
                with 
                    ordered_events as (
                        select
                            user_id, 
                            login_ts, 
                            row_number() over (
                                partition by user_id
                                order by login_ts
                            ) as rn
                        from logins
                    )
                select user_id, login_ts as first_login
                from ordered_events
                where rn = 1
            ) first_login
            on logins.user_id = first_login.user_id
        ) 
    """
    return sql