class SqlQueries:
    songplay_table_insert = ("""
        CREATE TABLE IF NOT EXISTS public.songplays (
            songplay_id varchar(226) NOT NULL,
            start_time timestamp NOT NULL,
            user_id int4 NOT NULL,
            "level" varchar(256),
            song_id varchar(256),
            artist_id varchar(256),
            session_id int4,
            location varchar(256),
            user_agent varchar(256)
        );
        INSERT INTO public.songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
                md5(events.sessionId || events.start_time) songplay_id,
                events.start_time, 
                events.userId, 
                events.level, 
                songs.song_id,
                songs.artist_id, 
                events.sessionId, 
                events.location, 
                events.userAgent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        CREATE TABLE IF NOT EXISTS public.users (
            user_id int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT users_pkey PRIMARY KEY (user_id)
        );
        INSERT INTO public.users (user_id, first_name, last_name, gender, level)
        SELECT distinct userId, firstName, lastName, gender, level
        FROM staging_events
        WHERE page='NextSong';
    """)

    song_table_insert = ("""
        CREATE TABLE IF NOT EXISTS public.songs (
            song_id varchar(256) NOT NULL,
            title varchar(256),
            artist_id varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (song_id)
        );
        INSERT INTO public.songs (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        CREATE TABLE IF NOT EXISTS public.artists (
            artist_id varchar(256) NOT NULL,
            name varchar(256),
            location varchar(256),
            latitude numeric(18,0),
            longitude numeric(18,0)
        );
        INSERT INTO public.artists (artist_id, name, location, latitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
    CREATE TABLE IF NOT EXISTS public.time 
    (
        start_time timestamp NOT NULL,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int,
        CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );
    INSERT INTO public.time (start_time, hour, day, week, month, year, weekday)
        SELECT 
            start_time, extract(hour from start_time), 
            extract(day from start_time), 
            extract(week from start_time), 
            extract(month from start_time), 
            extract(year from start_time), 
            extract(dayofweek from start_time)
        FROM songplays
    """)
