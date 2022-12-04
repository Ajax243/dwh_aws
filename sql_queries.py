import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES



staging_events_table_create= ("""  CREATE TABLE IF NOT EXISTS staging_events
							( artist TEXT ,
						    auth TEXT,
						    first_name TEXT,
						    gender TEXT,
						    item_in_session INTEGER,
						    last_name TEXT,
						    length FLOAT,
						    level TEXT,
						    location TEXT,
						    method TEXT,
						    page TEXT,
						    registration FLOAT,
						    session_id INT,
						    song TEXT,
						    status INTEGER,
						    ts BIGINT,
						    user_agent TEXT,
						    user_id TEXT )""")

staging_songs_table_create = ("""  CREATE TABLE IF NOT EXISTS staging_songs
									( artist_id TEXT ,
									artist_latitude FLOAT ,
									artist_location TEXT ,
									artist_longitude FLOAT,
									artist_name TEXT,
									duration FLOAT,									
									num_songs INT,
									song_id TEXT,
									title TEXT ,
									year INT); """)

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays
    ( songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    	start_time TIMESTAMP SORTKEY,
    	user_id TEXT DISTKEY,
    	level TEXT,
    	song_id TEXT,
    	artist_id TEXT ,
    	session_id INT,
    	location TEXT,
    	user_agent TEXT) diststyle key;""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
	(user_id TEXT PRIMARY KEY SORTKEY,
	first_name TEXT,
	last_name TEXT,
	gender TEXT,
	LEVEL TEXT) DISTSTYLE ALL;""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs
	(song_id TEXT PRIMARY KEY SORTKEY,
		title TEXT,
		artist_id TEXT DISTKEY, 
		year INT,
		duration FLOAT) DISTSTYLE KEY;""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists
	(artist_id TEXT PRIMARY KEY SORTKEY,
	 artist_name TEXT,
	 artist_location TEXT,
	 artist_latitude FLOAT,
	 artist_longitude FLOAT) DISTSTYLE ALL;""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS times 
	(start_time TIMESTAMP PRIMARY KEY SORTKEY,
	hour INT, 
	day INT,
	week INT, 
	month INT, 
	year INT DISTKEY, 
	weekday INT)
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {} IAM_ROLE '{}' JSON {} region '{}';
	""").format(config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['DWH']['REGION'])

staging_songs_copy = (""" COPY staging_songs FROM {} IAM_ROLE '{}'  region '{}';
""").format(config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['DWH']['REGION'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO
 songplays( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
 SELECT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS ts,e.user_id, e.level, s.song_id,s.artist_id, e.session_id, e.location, e.user_agent
 FROM staging_events e 
 JOIN staging_songs s 
 ON e.song =s.title AND
 e.artist = s.artist_name""")

user_table_insert = (""" INSERT INTO 
	users (user_id, first_name, last_name, gender, level)
	SELECT user_id,first_name, last_name, gender,level
	FROM staging_events
""")

song_table_insert = ("""INSERT INTO 
	songs(song_id, title, artist_id, year, duration)
	SELECT song_id, title,artist_id,year,duration
	FROM staging_songs

""")

artist_table_insert = ("""INSERT INTO 
	artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
	SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
	FROM staging_songs
""")

time_table_insert = ("""INSERT INTO
	times (start_time, hour, day, week, month, year, weekday)
	WITH proper_time AS(SELECT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' as ts FROM staging_events)
	SELECT ts, extract(hour from ts),extract(day from ts),extract(week from ts),extract(month from ts),extract(year from ts),extract(weekday from ts)
	FROM proper_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
