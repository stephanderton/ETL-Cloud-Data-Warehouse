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
# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        stg_e_event_key         INTEGER          IDENTITY(0,1),
        stg_e_artist            VARCHAR,
        stg_e_auth              VARCHAR,
        stg_e_first_name        VARCHAR,
        stg_e_gender            VARCHAR,
        stg_e_item_in_session   INTEGER,
        stg_e_last_name         VARCHAR,
        stg_e_length            DOUBLE PRECISION,
        stg_e_level             VARCHAR,
        stg_e_location          VARCHAR,
        stg_e_method            VARCHAR,
        stg_e_page              VARCHAR,
        stg_e_registration      VARCHAR,
        stg_e_session_id        INTEGER,
        stg_e_song              VARCHAR,
        stg_e_status            VARCHAR,
        stg_e_ts                VARCHAR,
        stg_e_user_agent        VARCHAR,
        stg_e_user_id           INTEGER
    )
    SORTKEY( stg_e_ts, stg_e_userId );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        stg_s_song_id            VARCHAR,
        stg_s_artist_id          VARCHAR,
        stg_s_artist_latitude    DOUBLE PRECISION,
        stg_s_artist_longitude   DOUBLE PRECISION,
        stg_s_artist_location    VARCHAR,
        stg_s_artist_name        VARCHAR,
        stg_s_title              VARCHAR            SORTKEY,
        stg_s_duration           DOUBLE PRECISION,
        stg_s_year               INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        sp_songplay_id  INTEGER         IDENTITY(0,1),
        sp_start_time   TIMESTAMP       NOT NULL,
        sp_user_id      INTEGER         NOT NULL,
        sp_level        VARCHAR(8)      NOT NULL,
        sp_song_id      VARCHAR(22)     NOT NULL,
        sp_artist_id    VARCHAR(22)     NOT NULL    DISTKEY,
        sp_session_id   INTEGER         NOT NULL,
        sp_location     VARCHAR,
        sp_user_agent   VARCHAR
    )
    SORTKEY( sp_start_time, sp_user_id );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        u_user_id       INTEGER         NOT NULL    SORTKEY,
        u_first_name    VARCHAR         NOT NULL,
        u_last_name     VARCHAR         NOT NULL,
        u_gender        VARCHAR(1),
        u_level         VARCHAR(8)      NOT NULL
    )
    DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        s_song_id       VARCHAR(22)     NOT NULL    SORTKEY,
        s_title         VARCHAR         NOT NULL,
        s_artist_id     VARCHAR(22)     NOT NULL    DISTKEY,
        s_year          INTEGER,
        s_duration      DOUBLE PRECISION
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        a_artist_id     VARCHAR(22)     NOT NULL    SORTKEY     DISTKEY,
        a_name          VARCHAR         NOT NULL,
        a_location      VARCHAR,
        a_latitude      DOUBLE PRECISION,
        a_longitude     DOUBLE PRECISION
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        t_start_time    TIMESTAMP       NOT NULL    SORTKEY,
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
    INSERT INTO songplays (
        sp_start_time, sp_user_id, sp_level, sp_song_id, sp_artist_id,
        sp_session_id, sp_location, sp_user_agent
    )
    SELECT      stg_e_ts, stg_e_user_id, stg_e_level, stg_s_song_id, stg_s_artist_id,
                stg_e_session_id, stg_e_location, stg_e_user_agent
    FROM        staging_events
    JOIN        staging_songs
    ON          stg_e_song = stg_s_title
    WHERE       stg_e_page = 'NextSong'
    ORDER BY    (sp_start_time, sp_user_id)

    DO UPDATE SET
        sp_level = EXCLUDED.level,
        sp_song_id = EXCLUDED.song_id,
        sp_artist_id = EXCLUDED.artist_id,
        sp_session_id = EXCLUDED.session_id,
        sp_location = EXCLUDED.location,
        sp_user_agent = EXCLUDED.user_agent
""")

user_table_insert = ("""
    INSERT INTO users (
        u_userId, u_firstName, u_lastName, u_gender, u_level
    )

    WHERE   stg_e_page == 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (
        s_song_id, s_title, s_artist_id, s_year, s_duration
    )

    WHERE   stg_e_page == 'NextSong'
""")

artist_table_insert = ("""
    INSERT INTO artists (
        a_artist_id, a_name, a_location, a_latitude, a_longitude
    )

    WHERE   stg_e_page == 'NextSong'
""")

time_table_insert = ("""
    INSERT INTO times(
        t_start_time, t_hour, t_day, t_week, t_month, t_year, t_weekday   
    )

    WHERE   stg_e_page == 'NextSong'
""")

#------------------------------------------------------------------------------
# QUERY LISTS

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
