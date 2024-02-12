import re
import logging
import json
from urllib.parse import quote
from urllib.request import urlopen


class Extraction:
    def __init__(self, spotify, client):
        self.spotify = spotify
        self.client = client
        # self.raw_tophits = []
        # self.tophits = []
        # self.lyrics = []
        logging.root.setLevel(logging.INFO)

    def __validate_track(self, raw_tophits) -> list:
        '''Select tracks which doesn't already have lyrics data in lyrics table'''

        sql_query = 'SELECT DISTINCT sid FROM lyrics-summarisation.projectdb.lyrics'
        query_job = self.client.query(sql_query)

        sid = [row.sid for row in query_job.result()]
        valid_tracks = [track for track in raw_tophits if track['sid'] not in sid]
        return valid_tracks

    def __get_lyrics(self, title, artists):
        logging.info(f'Searching lyrics for {title} by {artists}')
        pattern = re.compile('^[^\n\(\-]+')
        song_name = pattern.search(title).group(0).strip()
        artist = artists[0]

        try:
            url = "https://api.lyrics.ovh/v1/" + quote(f"{artist}/{song_name}")
            body = urlopen(url).read()
            lyrics = json.loads(body)['lyrics']
        except:
            raise Exception(f'Cannot find {title} lyrics by {artists}')
        return lyrics

    def get_playlist_songs(self, link, limit, execution_timestamp) -> list:
        album = self.spotify.playlist_tracks(link, limit=limit)
        raw_tophits = []
        tophits = []
        for idx, item in enumerate(album['items']):
            sid = item['track']['id']
            title = item['track']['name']
            artists = [artist['name'] for artist in item['track']['artists']]
            rank = idx + 1
            on_chart_date = execution_timestamp[:10]
            
            raw_tophits.append({'sid': sid, 'title': title, 'artists': artists})
            tophits.append({'sid': sid, 'rank': rank, 'on_chart_date': on_chart_date})
        return raw_tophits, tophits
    
    def get_playlist_lyrics(self, album):
        to_search_lyrics = self.__validate_track(album)

        if len(to_search_lyrics) == 0:
            logging.info('All tracks on top chart already existed in a database. Exit with no insertion.')
            return

        data = []
        for track in to_search_lyrics:
            try:
                lyrics = self.__get_lyrics(track['title'], track['artists'])
                temp = {
                    'sid': track['sid'],
                    'title': track['title'],
                    'artists': track['artists'],
                    'lyrics': lyrics
                }
                data.append(temp)
            except:
                logging.exception(f"Failed to retrieve lyrics for {track['title']} by {track['artists']}")
        return data

    def ingest_into_bigquery(self, table_id, json):
        if len(json) == 0:
            logging.info("Data with 0 row will not be inserted. Exit with no insertion.")
            return
            
        errors = self.client.insert_rows_json(table_id, json)

        if errors == []:
            logging.info("Data inserted successfully.")
        else:
            logging.error(f"Encountered errors while inserting data: {errors}")