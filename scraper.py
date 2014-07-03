import subprocess
import collections
import requests
import re
import logging

from mysql import connector
from sys import argv

import easylogger
import config

LOG = easylogger.EasyLogger(logging.getLogger(__name__))

class SCPError(Exception):
    pass


class LocalFileError(SCPError):
    pass


class RemoteFileError(SCPError):
    pass


class SCPConnectionError(SCPError):
    pass


class MultiKeyDict:
    """
    Dictionary to simulate 'case 10, 20, 30: ...'-style switch
    """

    def __getitem__(self, key):
        for my_key in self.data:
            if key in my_key:
                return self.data[key]

        raise KeyError

Note = collections.namedtuple("Note", ["text", "date", "first_name",
                                       "last_name"])

class SCP:
    """
    Manages interaction with SCP to download files. Factored out into
    a separate class so that later we can do this differently if we
    want.

    The interface here is:

    download(remote : string, local : string) -> void / BadFileTransferError
    """

    def __init__(self, password=config.SCP_PASSWORD, user=config.SCP_USERNAME,
                 scp_fmt=config.SCP_COMMAND):

        self._password = password
        self._user = user
        self._scp_fmt = scp_fmt

    def _build_query(self, remote, local):
        return self._scp_fmt.format(password=self._password,
                                    user=self._user,
                                    remote=remote,
                                    local=local)

    def download(self, remote, local):
        try:
            subprocess.call(self._build_query(remote, local))

        except subprocess.SubprocessError as err:
            resp = MultiKeyDict({
                (1, 4, 5, 8, 65, 67, 71, 72, 73, 74, 75, 76, 79): \
                SCPConnectionError,
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

    def _execute(self, cursor, query_string, **kwargs):
        '''
        Execute a query and return the full result set.
        '''
        query = query_string.format(**kwargs)
        LOG.debug("Sending query to database: ", query)
        cursor.execute(query)
        res = cursor.fetchall()
        LOG.debug("Got results: ", res)
        return res

    def _dex(self, query_string, **kwargs):
        '''
        Query the main database.
        '''
        LOG.debug("Querying docent")
        return self._execute(self._dcur, query_string, **kwargs)

    def _mex(self, query_string, **kwargs):
        '''
        Query the media database.
        '''
        LOG.debug("Querying docent_media")
        return self._execute(self._mcur, query_string, **kwargs)

    def tour_to_sections_and_titles(self, tour_id):
        '''
        Get a list of tuples containing section ids and titles.
        '''
        QUERY_FMT = "SELECT n_tour_section_id, s_section FROM t_tour_section t "\
                    "INNER JOIN t_section s ON t.n_section_id = s.n_section_id "\
                    "WHERE n_tour_id = {tour_id} ORDER BY n_sequence"

        return self._dex(QUERY_FMT, tour_id=tour_id)

    def section_to_pages(self, tour_id, section_index):
        '''
        Get a list of page ids from a tour id and the index of the
        desired section, counting from 1.
        '''
        QUERY_FMT = "SELECT n_section_page_id FROM "\
                    "t_section_page s INNER JOIN t_page p ON "\
                    "s.n_page_id = p.n_page_id INNER  JOIN t_tour_section x ON "\
                    "s.n_tour_section_id = x.n_tour_section_id WHERE "\
                    "n_tour_id = {tour_id} AND x.n_sequence = {section_index} "\
                    "ORDER BY s.n_sequence"
        # strip out unnecessary tuples
        res = [r[0] for r in
               self._dex(QUERY_FMT, tour_id=tour_id,
                         section_index=section_index)]
        # self._dex(QUERY_FMT, tour_id=tour_id,
        #                  section_index=section_index)
        return res

    def page_to_body_text(self, page_id):
        '''
        Get the main body text of a given page. Note that the body is HTML.
        '''
        QUERY_FMT = "SELECT s_text FROM t_text t INNER JOIN t_page_text p ON "\
                    "t.n_text_id = p.n_text_id WHERE n_section_page_id = "\
                    "{page_id}"

        # it's wrapped in a tuple in a list.
        return self._dex(QUERY_FMT, page_id=page_id)[0][0]

    def page_to_media_info(self, page_id):
        '''
        Get a list of the files associated with the given page id, tagged
        with a directory and a type.
        '''
        ID_QUERY_FMT = "SELECT n_media_id FROM t_page_media WHERE "\
                       "n_section_page_id = {page_id} AND "\
                       "s_mode IS NULL"

        INFO_QUERY_FMT = "SELECT s_file, s_file_name, s_file_location FROM "\
                         "t_file f INNER JOIN t_file_subtype fs ON " \
                         "f.n_file_id = fs.n_file_id INNER JOIN "\
                         "t_media_subtype ms ON fs.n_file_subtype_id "\
                         "= ms.n_file_subtype_id WHERE ms.n_media_id = "\
                         "{media_id}"

        media_ids = self._dex(ID_QUERY_FMT, page_id=page_id)

        LOG.debug("got ids: ", media_ids)


        file_infos = []

        for media_id in media_ids:
            file_infos.extend(self._mex(INFO_QUERY_FMT, media_id=media_id[0]))

        LOG.debug("got infos: ", file_infos)

        return file_infos

    def page_to_questions(self, tour_id, page_id):
        '''
        Get a list of the journal questions on the page.
        '''
        QUERY_FMT = "SELECT DISTINCT s_word FROM t_page_term p INNER JOIN "\
                    "t_word w ON p.n_word_id = w.n_word_id INNER JOIN "\
                    "t_tour_term t ON p.n_tour_term_id = t.n_tour_term_id "\
                    "WHERE n_tour_id = {tour_id} AND n_section_page_id = "\
                    "{page_id} ORDER BY s_word"

        # Wrapped in a tuple
        return [question[0] for question in self._dex(QUERY_FMT,
                                                      tour_id=tour_id,
                                                      page_id=page_id)]

    def page_to_words(self, tour_id, page_id):
        '''
        Get a list of the dictionary words on the page.
        '''
        QUERY_FMT = "SELECT DISTINCT s_word FROM t_page_term p INNER JOIN "\
                    "t_word w ON p.n_word_id = w.n_word_id INNER JOIN "\
                    "t_tour_term t ON p.n_tour_term_id = t.n_tour_term_id "\
                    "WHERE n_tour_id = {tour_id} AND n_section_page_id = "\
                    "{page_id} ORDER BY s_word"

        return [word[0] for word in self._dex(QUERY_FMT,
                                              tour_id=tour_id,
                                              page_id=page_id)]

    def page_to_notes(self, page_id):
        '''
        Get a list of Note objects for each note on the page.
        '''

        NOTE_QUERY_FMT = "SELECT t_notes, n_user_access_id, t_timestamp FROM "\
                         "t_notes n INNER JOIN t_page_notes p ON n.n_notes_id "\
                         "= p.n_notes_id WHERE n_section_page_id = {page_id} "\
                         "ORDER BY t_timestamp"

        FIRST_QUERY_FMT = "SELECT s_first_name FROM t_user u INNER JOIN "\
                          "t_user_access a ON u.n_user_id = a.n_user_id WHERE "\
                          "n_user_access_id = {user_id}"

        LAST_QUERY_FMT = "SELECT s_last_name FROM t_user u INNER JOIN "\
                          "t_user_access a ON u.n_user_id = a.n_user_id WHERE "\
                          "n_user_access_id = {user_id}"

        ret = []

        for text, user_id, date in self._dex(NOTE_QUERY_FMT, page_id=page_id):
            first_name = self._dex(FIRST_QUERY_FMT, user_id=user_id)
            last_name = self._dex(LAST_QUERY_FMT, user_id=user_id)

            ret.append(Note(text, date, first_name, last_name))

        return ret


class DBBuilder:
    def __init__(self, db):
        self._db = db

class PrintableMixin:
    def __repr__(self):
        name = "{}({})"
        els = []
        for el in self.__dict__:
            if el[0] != "_":
                els.append("{}: {}".format(el, self.__dict__[el]))

        return name.format(self.__class__.split(".")[1],
                           "\n".join(els))

class SectionBuilder(DBBuilder):
    def __init__(self, db, page_builder):
        super().__init__(db)
        self.page_builder = page_builder

    def for_tour(self, tour_id):
        sections = []
        for index, (section, title) in \
            enumerate(self._db.tour_to_sections_and_titles(tour_id)):
            section = Section()
            section.title = title

            section.pages = self.page_builder.for_section(tour_id, index + 1)
            sections.append(section)

        return sections


class PageBuilder(DBBuilder):
    LOGFILE_FMT = "http://new.web-docent.org/modules/media/{}/log.txt"
    LOGFILE_REGEX = re.compile("^::Archive:(.*)$", re.MULTILINE)
    BASE_MEDIA_DIR = "/var/www/vhosts/cwd/modules/media/"
    BASE_ARC_DIR = "/data/cmap/med_arc"
    # Note that the base dir for archives in the log files does not exist

    def for_section(self, tour_id, section_index):
        pages = []
        for page_id in self._db.section_to_pages(tour_id, section_index):
            page = Page()
            page.body = self._db.page_to_body_text(page_id)

            media_infos = self._db.page_to_media_info(page_id)
            image_dir, arc_image_dir, \
                other_media = self._process_media(media_infos or [])

            page.image_dirs = image_dir
            page.arc_image_paths = arc_image_dir
            page.other_media_paths = other_media

            page.questions = self._db.page_to_questions(tour_id, page_id)

            page.dictionary_words = self._db.page_to_words(tour_id, page_id)

            page.notes = self._db.page_to_notes(page_id)

    def _process_media(self, media_infos):
        image_dirs = set()
        arc_image_paths = set()
        other_media_paths = set()

        for file_type, file_name, file_path in media_infos:
            media_dir = self.BASE_MEDIA_DIR.format(file_path)
            if file_type == "image" and media_dir not in image_dirs:
                arc_image_paths.append(self._process_logfile(file_path))
                image_dirs.append(media_dir)
            elif file_path not in other_media_paths:
                other_media_paths.append("{}{}".format(media_dir, file_name))

        return image_dirs, arc_image_paths, other_media_paths

    def _process_logfile(self, file_path):
        logtext = requests.get(self.LOGFILE_FMT.format(file_path)).text
        return self.LOGFILE_REGEX.search(logtext).group(0)


class Section(PrintableMixin):
    def __init__(self):
        self.title = None
        self.pages = None

class Page(PrintableMixin):
    def __init__(self):
        self.body = None
        self.image_dirs = set()
        self.arc_image_paths = set()
        self.other_media_paths = set()
        self.questions = []
        self.dictionary_words = []
        self.notes = []

def main(tour_id):
    db = Database()
    page_builder = PageBuilder(db)
    section_builder = SectionBuilder(db, page_builder)

    sections = section_builder.for_tour(tour_id)

    print(sections)

if __name__ == '__main__':
    main(argv[1])
