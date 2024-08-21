class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT  start_time
        ,       extract(hour from start_time) as hour
        ,       extract(day from start_time) as day
        ,       extract(week from start_time) as week
        ,       extract(month from start_time) as month
        ,       extract(year from start_time) as year
        ,       extract(dayofweek from start_time) as dayofweek
        FROM    songplay
    """)

    CREATE_STAGING_EVENTS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(100),
    auth VARCHAR(100),
    firstName VARCHAR(100),
    gender VARCHAR(100),
    itemInSession VARCHAR(100),
    lastName VARCHAR(100),
    length VARCHAR(100),
    level VARCHAR(100),
    location VARCHAR(100),
    method VARCHAR(100),
    page VARCHAR(100),
    registration VARCHAR(100),
    sessionId INTEGER,
    song VARCHAR(100),
    status VARCHAR(100),
    ts VARCHAR(100),
    userAgent VARCHAR(250),
    userId INTEGER,
    PRIMARY KEY(registration, ts))
    DISTSTYLE ALL;
    """

    CREATE_STAGING_SONGS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR(100),
    num_songs VARCHAR(100),
    title VARCHAR(100),
    artist_name VARCHAR(100),
    artist_latitude VARCHAR(100),
    year VARCHAR(100),
    duration VARCHAR(100),
    artist_id VARCHAR(100),
    artist_longitude VARCHAR(100),
    artist_location VARCHAR(100),
    PRIMARY KEY(song_id, artist_id))
    DISTSTYLE ALL;
    """

