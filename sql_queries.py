import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#------------------------------------------------------------------------------
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

#------------------------------------------------------------------------------
# CREATE TABLES - STAGING

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        event_key           INTEGER          IDENTITY(0,1),
        artist              VARCHAR(MAX),
        auth                VARCHAR(MAX),
        firstName           VARCHAR(MAX),
        gender              VARCHAR(MAX),
        itemInSession       INTEGER,
        lastName            VARCHAR(MAX),
        length              DOUBLE PRECISION,
        level               VARCHAR(MAX),
        location            VARCHAR(MAX),
        method              VARCHAR(MAX),
        page                VARCHAR(MAX),
        registration        DECIMAL(15,1),
        sessionId           INTEGER,
        song                VARCHAR(MAX),
        status              INTEGER,
        ts                  BIGINT,
        userAgent           VARCHAR(MAX),
        userId              VARCHAR(22)
    )
    SORTKEY( ts, userId );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        song_id            VARCHAR(MAX),
        artist_id          VARCHAR(MAX),
        artist_latitude    DOUBLE PRECISION,
        artist_longitude   DOUBLE PRECISION,
        artist_location    VARCHAR(MAX),
        artist_name        VARCHAR(MAX),
        title              VARCHAR(MAX)           SORTKEY,
        duration           DOUBLE PRECISION,
        year               INTEGER
    );
""")

#------------------------------------------------------------------------------
# CREATE TABLES - ANALYTICS

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        sp_songplay_id      INTEGER             IDENTITY(0,1)   PRIMARY KEY,
        sp_start_time       TIMESTAMP           NOT NULL,
        sp_user_id          INTEGER             NOT NULL,
        sp_level            VARCHAR(8)          NOT NULL,
        sp_song_id          VARCHAR(22)         NOT NULL,
        sp_artist_id        VARCHAR(22)         NOT NULL        DISTKEY,
        sp_session_id       INTEGER             NOT NULL,
        sp_location         VARCHAR(MAX),
        sp_user_agent       VARCHAR(MAX)
    )
    SORTKEY( sp_start_time, sp_user_id );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        u_user_id       INTEGER         NOT NULL    SORTKEY     PRIMARY KEY,
        u_first_name    VARCHAR(MAX)    NOT NULL,
        u_last_name     VARCHAR(MAX)    NOT NULL,
        u_gender        VARCHAR(8),
        u_level         VARCHAR(8)      NOT NULL
    )
    DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        s_song_id       VARCHAR(22)     NOT NULL    SORTKEY     PRIMARY KEY,
        s_title         VARCHAR(MAX)    NOT NULL,
        s_artist_id     VARCHAR(22)     NOT NULL    DISTKEY,
        s_year          INTEGER,
        s_duration      DOUBLE PRECISION
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        a_artist_id     VARCHAR(22)     NOT NULL    SORTKEY     DISTKEY     PRIMARY KEY,
        a_name          VARCHAR(MAX)    NOT NULL,
        a_location      VARCHAR(MAX),
        a_latitude      DOUBLE PRECISION,
        a_longitude     DOUBLE PRECISION
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        t_start_time    TIMESTAMP       NOT NULL    SORTKEY     PRIMARY KEY,
        t_hour          SMALLINT        NOT NULL,
        t_day           SMALLINT        NOT NULL,
        t_week          SMALLINT        NOT NULL,
        t_month         SMALLINT        NOT NULL,
        t_year          SMALLINT        NOT NULL,
        t_weekday       SMALLINT        NOT NULL
    )
    DISTSTYLE AUTO;
""")

#------------------------------------------------------------------------------
# STAGING TABLES

staging_events_copy = ("""
    COPY     staging_events
    FROM     {}
    IAM_ROLE {}
    JSON     {}
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH']
           )

staging_songs_copy = ("""
    COPY     staging_songs
    FROM     {}
    IAM_ROLE {}
    JSON     'auto'
""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['ARN']
           )

#------------------------------------------------------------------------------
# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays
    (
        sp_start_time, sp_user_id, sp_level, sp_song_id, 
        sp_artist_id, sp_session_id, sp_location, sp_user_agent
    )
    (
        WITH e  AS (
                SELECT  *
                FROM    staging_events
                WHERE   page = 'NextSong'
        )

        SELECT  TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second',
                e.userId::INTEGER,
                e.level, 
                s.song_id, s.artist_id,
                e.sessionId, e.location, e.userAgent
        FROM    e,
                staging_songs AS s
        WHERE   e.song = s.title  AND
                e.artist = s.artist_name
    )
""")

user_table_insert = ("""
    INSERT INTO users
    (
        u_user_id, u_first_name, u_last_name, u_gender, u_level
    )
    SELECT  DISTINCT userId::INTEGER,
            firstName, lastName, gender, level
    FROM    staging_events
    WHERE   page = 'NextSong' AND
            userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs
    (
        s_song_id, s_title, s_artist_id, s_year, s_duration
    )
    SELECT  DISTINCT song_id,
            title, artist_id, year, duration
    FROM    staging_songs
    WHERE   song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists
    (
        a_artist_id, a_name, a_location, a_latitude, a_longitude
    )
    SELECT  DISTINCT artist_id,
            artist_name, artist_location,
            artist_latitude, artist_longitude
    FROM    staging_songs
    WHERE   artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time
    (
        t_start_time, t_hour, t_day, t_week, t_month, t_year, t_weekday   
    )
    (
        SELECT  DISTINCT sp_start_time          AS date,
                DATE_PART(hour, date)::INT      AS hour,
                DATE_PART(day, date)::INT       AS day,
                DATE_PART(week, date)::INT      AS week,
                DATE_PART(month, date)::INT     AS month,
                DATE_PART(year, date)::INT      AS year,
                DATE_PART(weekday, date)::INT   AS weekday
        FROM    songplays
    );
""")

#------------------------------------------------------------------------------
# COUNT TABLE ROWS

staging_events_table_count = ("""
    SELECT COUNT(*) FROM staging_events
""")

staging_songs_table_count = ("""
    SELECT COUNT(*) FROM staging_songs
""")

songplay_table_count = ("""
    SELECT COUNT(*) FROM songplays
""")

user_table_count = ("""
    SELECT COUNT(*) FROM users
""")

song_table_count = ("""
    SELECT COUNT(*) FROM songs
""")

artist_table_count = ("""
    SELECT COUNT(*) FROM artists
""")

time_table_count = ("""
    SELECT COUNT(*) FROM time
""")

#------------------------------------------------------------------------------
# SAMPLE QUERIES

top_10_songs = ("""
    WITH songplays_ext  AS (
             SELECT *
             FROM   songplays
             JOIN   songs
             ON     sp_song_id   = s_song_id
             JOIN   artists
             ON     sp_artist_id = a_artist_id
    )

    SELECT   s_title    AS "song title",
             a_name     AS "artist name",
             COUNT(*)   AS count
    FROM     songplays_ext
    GROUP BY s_title, a_name
    ORDER BY count DESC, s_title, a_name
    LIMIT    10;
""")

top_10_users = ("""
    WITH songplays_ext AS (
             SELECT sp_songplay_id, u_first_name, u_last_name, u_user_id
             FROM   songplays
             JOIN   users
             ON     sp_user_id  = u_user_id  AND
                    sp_level    = u_level
        )

    SELECT   DISTINCT( u_first_name || ' ' || u_last_name ) AS "user name",
             u_user_id                                      AS "user ID",
             COUNT(*)                                       AS "song count"
    FROM     songplays_ext
    GROUP BY "user ID", "user name"
    ORDER BY "song count" DESC, "user name"
    LIMIT    10;
""")

top_user_id = ("""
    WITH songplays_ext AS (
            SELECT  sp_session_id, u_user_id
            FROM    songplays
            JOIN    users
            ON      sp_user_id = u_user_id  AND
                    sp_level   = u_level
        ),
        session_counts AS (
            SELECT   u_user_id,
                     COUNT( sp_session_id ) AS count
            FROM     songplays_ext
            GROUP BY u_user_id
        ),
        max_session  AS (
            SELECT   MAX(count) AS max_count
            FROM     session_counts
        )

    SELECT  u_user_id AS top_user
    FROM    session_counts
    WHERE   count = ( 
            SELECT   max_count
            FROM     max_session
    );
""")

top_5_sessions_top_user_49 = ("""
    WITH songplays_user AS (
            SELECT  *
            FROM    songplays
            WHERE   sp_user_id  = 49
        ),
        user_sessions AS (
            SELECT  u_first_name, u_last_name, 
                    sp_session_id, sp_start_time, s_title
            FROM    songplays_user
            JOIN    users
            ON      sp_user_id  = u_user_id  AND
                    sp_level    = u_level
            JOIN    songs
            ON      sp_song_id  = s_song_id
        )

    SELECT   (u_first_name || ' ' || u_last_name)   AS "user name",
             sp_session_id                          AS "session ID",
             (DATE_PART('year', 
                        sp_start_time) || '-' || DATE_PART('month', 
                        sp_start_time) || '-' || DATE_PART('day', 
                        sp_start_time))             AS date,
             COUNT(s_title)                         AS "song count"
    FROM     user_sessions
    GROUP BY sp_session_id, date, "user name"
    ORDER BY "song count" DESC, date
    LIMIT    5;
""")

#------------------------------------------------------------------------------
# QUERY LISTS - SETUP TABLES

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        songplay_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create
                       ]
drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop,
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop
                     ]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy
                     ]

insert_table_queries = [songplay_table_insert, 
                        user_table_insert,
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert
                       ]

#------------------------------------------------------------------------------
# QUERY LISTS - SETUP - dimensional tables only; leaves staging tables alone

create_db_table_queries  = [songplay_table_create, 
                            user_table_create, 
                            song_table_create, 
                            artist_table_create, 
                            time_table_create
                           ]
drop_db_table_queries  = [songplay_table_drop,
                          user_table_drop, 
                          song_table_drop, 
                          artist_table_drop, 
                          time_table_drop
                         ]

#------------------------------------------------------------------------------
# QUERY LISTS - ANALYTICS

count_table_queries = [staging_events_table_count, 
                       staging_songs_table_count, 
                       songplay_table_count, 
                       user_table_count, 
                       song_table_count, 
                       artist_table_count, 
                       time_table_count
                      ]

sample_queries = [top_10_songs,
                  top_10_users,
                  top_user_id,
                  top_5_sessions_top_user_49
                 ]
