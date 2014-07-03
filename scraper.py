import subprocess

from mysql import connector

import config


class SCPError(Exception):
    pass


class LocalFileError(SCPError):
    pass


class RemoteFileError(SCPError):
    pass


class ConnectionError(SCPError):
    pass


class MultiKeyDic:
    """
    Dictionary to simulate 'case 10, 20, 30: ...'-style switch
    """

    def __getitem__(self, key):
        for my_key in self.data:
            if key in my_key:
                return self.data[key]

        raise KeyError


class SCP:
    """Manages interaction with SCP to download files. Factored out into
    a separate class so that later we can do this differently if we
    want.

    The interface here is:

    download(remote : string, local : string) -> void / BadFileTransferError
    """

    def __init__(password=config.PASSWORD,
                 user=config.USER,
                 scp_fmt=config.SCP_COMMAND):

        self._password = password
        self._user = user
        self._scp_fmt = scp

    def _build_query(remote, local):
        return self._scp_fmt.format(password=self._password,
                                    user=self._user,
                                    remote=remote,
                                    local=local)

    def download(remote, local):
        try:
            subprocess.call(self._build_query(remote, local))

        except subprocess.SubprocessError as err:
            resp = MultiKeyDict({
                (1, 4, 5, 8, 65, 67, 71, 72, 73, 74, 75, 76, 79): \
                ConnectionError,
                (2, 3, 7, 10, 70): RemoteFileError,
                (6): LocalFileError
            })

            try:
                raise resp[err.returncode]("Error code: {}".format(
                    err.returncode))
            except KeyError:
                raise err


class Database:
    HOST = "wit.uchicago.edu"
    DATA_DB = "docent"
    MEDIA_DB = "docent_media"

    def __init__(self, username=config.DB_USERNAME,
                 password=config.DB_PASSWORD):

        self._username = username
        self._password = password

        self._data_cx = connector.connect(user=username,
                                          password=password,
                                          host=self.HOST,
                                          database=self.DATA_DB)

        self._media_cx = connector.connect(user=username,
                                           password=password,
                                           host=self.HOST,
                                           database=self.DATA_DB)

        self._dcur = self._data_cx.cursor()
        self._mcur = self._media_cx.cursor()

    def _execute(cursor, query_string, **kwargs):
        '''
        Execute a query and return the full result set.
        '''
        cursor.execute(query_string.format(**kwargs))
        return cursor.fetchall()

    def _dex(query_string, **kwargs):
        '''
        Query the main database.
        '''
        return self._execute(self._dcur, query_string, **kwargs)

    def _mex(query_string, **kwargs):
        '''
        Query the media database.
        '''
        return self._execute(self._mcur, query_string, **kwargs)

    def tour_to_sections_and_titles(self, tour_id):
        '''
        Get a list of tuples containing section ids and titles.
        '''
        QUERY_FMT = "SELECT n_tour_section_id, s_section FROM t_tour_section t"\
                    "INNER JOIN t_section s ON t.n_section_id = s.n_section_id"\
                    "WHERE n_tour_id = {tour_id} ORDER BY n_sequence"

        return self._dex(QUERY_FMT, tour_id=tour_id)

    def section_to_pages(self, tour_id, section_index):
        '''
        Get a list of page ids from a tour id and the index of the
        desired section, counting from 1.
        '''
        QUERY_FMT = "SELECT n_section_page_id, x.n_tour_section_id FROM"\
                    "t_section_page s INNER JOIN t_page p ON"\
                    "s.n_page_id = p.n_page_id INNER  JOIN t_tour_section x ON"\
                    "s.n_tour_section_id = x.n_tour_section_id WHERE"\
                    "n_tour_id = {tour_id} AND x.n_sequence = {section_index}"\
                    "ORDER BY s.n_sequence"

        return self._dex(QUERY_FMT, tour_id=tour_id, section_id=section_id)

    def page_to_body_text(self, page_id):
        '''
        Get the main body text of a given page. Note that the body is HTML.
        '''
        QUERY_FMT = "SELECT s_text FROM t_text t INNER JOIN t_page_text p ON"\
                    "t.n_text_id = p.n_text_id WHERE n_section_page_id = "\
                    "{page_id}"

        # it's wrapped in a tuple in a list.
        return self._dex(QUERY_FMT, page_id=page_id)[0][0]

class Scraper:
    """
    Grabs info from the remote SQL database.
    """

    def __init__(self,
