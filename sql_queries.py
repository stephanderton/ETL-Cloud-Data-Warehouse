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
        stg_e_eventkey        INTEGER          IDENTITY(0,1)
        stg_e_artist          VARCHAR(45)      NOT NULL,
        stg_e_firstName       VARCHAR(25)      NOT NULL,
        stg_e_gender          VARCHAR(1)       NOT NULL,
        stg_e_itemInSession   INTEGER          NOT NULL,
        stg_e_lastName        VARCHAR(25)      NOT NULL,
        stg_e_length          DOUBLE PRECISION NOT NULL,
        stg_e_level           VARCHAR(1)       NOT NULL,
        stg_e_location        VARCHAR(45)      NOT NULL,
        stg_e_sessionId       INTEGER          NOT NULL,
        stg_e_song            VARCHAR(45)      NOT NULL,
        stg_e_userId          INTEGER          NOT NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        stg_s_song_id            VARCHAR(22)      NOT NULL,
        stg_s_artist_id          VARCHAR(22)      NOT NULL,
        stg_s_artist_latitude    DOUBLE PRECISION NOT NULL,
        stg_s_artist_longitude   DOUBLE PRECISION NOT NULL,
        stg_s_artist_location    VARCHAR(45)      NOT NULL,
        stg_s_artist_name        VARCHAR(45)      NOT NULL,
        stg_s_title              VARCHAR(45)      NOT NULL,
        stg_s_duration           DOUBLE PRECISION NOT NULL,
        stg_s_year               INTEGER          NOT NULL
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
    (
        sp_songplay_id  IDENTITY(0,1),
        sp_start_time TIMESTAMP NOT NULL,  \
        sp_user_id INT NOT NULL,  \
        sp_level VARCHAR NOT NULL,  \
        sp_song_id VARCHAR,  \
        sp_artist_id VARCHAR,  \
        sp_session_id INT,  \
        sp_location VARCHAR,  \
        sp_user_agent VARCHAR,  \
    )
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
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
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN']
           )

#------------------------------------------------------------------------------
# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
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
