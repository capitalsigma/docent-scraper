#pylint: disable=line-too-long, invalid-name, too-few-public-methods
import subprocess
import collections
import requests
import re
import logging
import math
import ftfy
import argparse
import os
import gzip
import shutil
import glob

from mysql import connector
from sys import argv

import easylogger
import config

# LOG = easylogger.LOG
LOG = easylogger.EasyLogger()
LOG.setLevel(logging.INFO)

class SCPError(Exception):
    pass


class LocalFileError(SCPError):
    pass


class RemoteFileError(SCPError):
    pass


class SCPConnectionError(SCPError):
    pass

class BadArgumentsError(Exception):
    pass

class MultiKeyDict(collections.UserDict):
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

class AbstractGetter:
    @staticmethod
    def _build_unzipped_name(name):
        file_name = os.path.basename(name)
        # strip off .gz
        LOG.debug("about to return file_name: {}".format(file_name))
        return file_name[:-3]

    def get(self, remote, local):
        LOG.debug("Getting remote, {} and local, {}".format(
            remote, local))
        self._get(remote, local)
        return gzip.open(local), self._build_unzipped_name(local)


class SCPGetter(AbstractGetter):
    """
    Manages interaction with SCP to download files. Factored out into
    a separate class so that later we can do this differently if we
    want.

    The interface here is:

    get(remote : string, local : string) -> void / BadFileTransferError
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
                                    local=local).split(" ")

    def _get(self, remote, local):
        subprocess.check_call(self._build_query(remote, local))

        # except subprocess.CalledProcessError as err:
        #     resp = MultiKeyDict({
        #         (1, 4, 5, 8, 65, 67, 71, 72, 73, 74, 75, 76, 79): \
        #         SCPConnectionError,
        #         (2, 3, 7, 10, 70): RemoteFileError,
        #         (6,): LocalFileError
        #     })

        #     try:
        #         raise resp[err.returncode]("Error code: {}".format(
        #             err.returncode))
        #     except KeyError:
        #         raise err

class LocalGetter(AbstractGetter):
    def _get(self, remote, local):
        for f in glob.glob(remote):
            shutil.copy(f, local)
        # return gzip.open(local)

class AbstractDownloader:
    '''Wrapper around one of SCP, Local and NoOp to manage download
    behavior
    '''

    def __init__(self, getter=None):
        self._getter = getter

    def get(self, remote, section_id, page_id):
        raise NotImplementedError

class NoOpDownloader(AbstractDownloader):
    def get(self, remote, section_id, page_id):
        return None

class RealDownloader(AbstractDownloader):
    def __init__(self, getter, tid):
        super().__init__(getter)

        self._root_dir = os.path.join(os.getcwd(), "tour-{}-images".format(tid))

        try:
            os.mkdir(self._root_dir)
        except OSError:
            LOG.info("WARNING: Directory {} already exists.".format(
                self._root_dir))

    def get(self, remote, section_id, page_id):
        LOG.debug("Getting remote", remote)

        new_dir = os.path.join(self._root_dir,
                               "section-{}".format(section_id),
                               "page-{}".format(page_id))
        filename = os.path.basename(remote).strip("*")

        try:
            os.makedirs(new_dir)
        except OSError:
            # dir already exists
            pass

        try:
            new_gzipped, unzipped_name = self._getter.get(
                remote, os.path.join(new_dir, filename))

            unzipped_path = os.path.join(new_dir, unzipped_name)

            with open(unzipped_path, "wb") as new_file:
                new_file.write(new_gzipped.read())

            return unzipped_path

        except subprocess.CalledProcessError as err:
            LOG.error("Something went wrong trying to download the image",
                      remote, ". Skipping.")

            return None

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
                                           database=self.MEDIA_DB)

        self._dcur = self._data_cx.cursor()
        self._mcur = self._media_cx.cursor()

    def _execute(self, cursor, query_string, **kwargs):
        '''
        Execute a query and return the full result set.
        '''
        query = query_string.format(**kwargs)
        #LOG.debug("Sending query to database: ", query)
        cursor.execute(query)
        res = cursor.fetchall()
        #LOG.debug("Got results: ", res)
        return res

    def _dex(self, query_string, **kwargs):
        '''
        Query the main database.
        '''
        #LOG.debug("Querying docent")
        return self._execute(self._dcur, query_string, **kwargs)

    def _mex(self, query_string, **kwargs):
        '''
        Query the media database.
        '''
        #LOG.debug("Querying docent_media")
        return self._execute(self._mcur, query_string, **kwargs)

    def tour_to_tour_title(self, tour_id):
        '''
        Get the title of a tour.
        '''
        QUERY_FMT = "SELECT s_tour FROM t_tour WHERE n_tour_id = {tour_id}"

        return self._dex(QUERY_FMT, tour_id=tour_id)[0][0]

    def tour_to_module_title(self, tour_id):
        '''
        Get the title of a module, given a tour.
        '''
        QUERY_FMT = "SELECT s_module FROM t_module INNER JOIN t_module_tour "\
                    "ON t_module_tour.n_module_id=t_module.n_module_id WHERE "\
                    "n_tour_id = {tour_id}"

        return self._dex(QUERY_FMT, tour_id=tour_id)[0][0]

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

    def media_id_to_title_and_caption(self, media_id, page_id):
        QUERY_FMT = "SELECT s_title, s_caption FROM t_page_media "\
                    "WHERE n_media_id = {media_id} AND "\
                    "n_section_page_id = {page_id}"
        resp = self._dex(QUERY_FMT, media_id=media_id, page_id=page_id)

        LOG.debug("got resp:", resp)

        return resp[0]



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

        media_ids = [x[0] for x in self._dex(ID_QUERY_FMT, page_id=page_id)]


        #LOG.debug("got ids: ", media_ids)


        file_infos = []

        for media_id in media_ids:
            for info in self._mex(INFO_QUERY_FMT, media_id=media_id):
                file_infos.append((info, media_id))

        #LOG.debug("got infos: ", file_infos)

        return file_infos

    def page_to_questions(self, tour_id, page_id):
        '''
        Get a list of the journal questions on the page.
        '''
        QUERY_FMT = "SELECT t_body FROM t_page_quiz p INNER JOIN "\
                    "t_quiz_question qq ON p.n_page_quiz_id = "\
                    "qq.n_page_quiz_id INNER JOIN t_ques_body q ON "\
                    "qq.n_quiz_ques_id = q.n_quiz_ques_id INNER JOIN t_body "\
                    "b ON q.n_body_id = b.n_body_id WHERE n_section_page_id = "\
                    "{page_id} ORDER BY n_sequence"

        # Wrapped in a tuple
        return [question[0] for question in self._dex(QUERY_FMT,
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

        return name.format(self.__class__.__name__, "\n".join(els))

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

class MediaBuilder(DBBuilder):
    LOGFILE_FMT = "http://new.web-docent.org/modules/media/{}/log.txt"
    LOGFILE_REGEX = re.compile("^::Archive:(.*)$", re.MULTILINE)

    BASE_MEDIA_DIR = "/var/www/vhosts/cwd/modules/media/{}"
    BASE_ARC_DIR = "/data/cmap/med_arc/*{}"
    # Note that the base dir for archives in the log files does not exist

    def __init__(self, db, downloader):
        super().__init__(db)
        self._downloader = downloader

    def for_page(self, media_infos, section_id, page_id, infos_to_ids):
        media = []

        image_dirs, arc_media_paths, \
            other_media_paths = self._process_media(media_infos)

        LOG.debug("got image dirs: ", image_dirs)
        LOG.debug("got arc media paths: ", arc_media_paths)

        for image_dir, arc_media_path in zip(image_dirs, arc_media_paths):
            LOG.debug("processing image_dir:", image_dir, "and arc_media_path:",
                      arc_media_path)
            media_item = self._build_image(image_dir,
                                           arc_media_path,
                                           infos_to_ids,
                                           page_id)
            # media_item.remote_path = self.IMAGE_FMT.format(image_dir)

            media.append(media_item)

        for media_path in other_media_paths:
            media_item = Media()
            media_item.remote_path = media_path
            media_item.media_type = "other"

            media.append(media_item)
        LOG.debug("built media: ", media)
        for media_item in media:
            local_path = self._downloader.get(media_item.arc_path,
                                              section_id,
                                              page_id)
            media_item.local_path = local_path


        return media

    def _build_image(self, image_dir, arc_media_path, infos_to_ids, page_id):
        media_item = Media()
        media_item.remote_path = image_dir
        media_item.arc_path = arc_media_path
        media_item.media_type = "image"

        title = None
        caption = None

        LOG.debug("building for image_dir: ", image_dir)
        LOG.debug("with infos_to_ids: ", infos_to_ids)

        try:
            short_image_dir = image_dir.split("/")[-1]

            media_id = [value for key, value in infos_to_ids.items()
                        if short_image_dir in key][0]

            LOG.debug("media_id: {}".format(media_id))

            title, caption = self._db.media_id_to_title_and_caption(
                media_id, page_id)



        except (ValueError, IndexError) as e:
        # except BadArgumentsError as e:
            LOG.debug("Caught exception: {}".format(e))
            pass

        media_item.title = title
        media_item.caption = caption

        return media_item


    def _process_logfile(self, file_path):
        logtext = requests.get(self.LOGFILE_FMT.format(
            file_path.strip("/"))).text
        arc_old = self.LOGFILE_REGEX.search(logtext).group(0)
        file_name = arc_old.split("med_arc")[1].strip("/")

        LOG.debug("Got arc_old: ", arc_old)

        return self.BASE_ARC_DIR.format(file_name)

    def _process_media(self, media_infos):
        image_dirs = set()
        arc_image_paths = set()
        other_media_paths = set()

        LOG.debug("got media_infos: ", media_infos)

        for file_type, file_name, file_path in media_infos:
            media_dir = self.BASE_MEDIA_DIR.format(file_path)
            LOG.debug("file type: ", file_type)
            #LOG.debug("file type == image: ", file_type == "image")
            if file_type == "image" and media_dir not in image_dirs:
                arc_image_paths.add(self._process_logfile(file_path))
                image_dirs.add(media_dir)
            elif file_type != "image" and file_path not in other_media_paths:
                other_media_paths.add("{}{}".format(media_dir, file_name))

        return image_dirs, arc_image_paths, other_media_paths

class PageBuilder(DBBuilder):

    def __init__(self, db, media_builder):
        super().__init__(db)
        self._media_builder = media_builder

    def for_section(self, tour_id, section_index):
        pages = []

        for page_id in self._db.section_to_pages(tour_id, section_index):
            page = Page()
            page.page_id = page_id
            page.body = self._db.page_to_body_text(page_id)

            media_infos_and_ids = self._db.page_to_media_info(page_id)
            LOG.debug("Got media_infos_and_ids:", media_infos_and_ids)

            media_infos_to_ids = {"".join(infos): page_id for
                                  infos, page_id in media_infos_and_ids}

            LOG.debug("Got media_infos_to_ids:", media_infos_to_ids)

            file_infos = [x[0] for x in media_infos_and_ids]
            # image_dir, arc_image_dir, \
            #     other_media = self._process_media(media_infos or [])
            page.media = self._media_builder.for_page(file_infos,
                                                      section_index,
                                                      page_id,
                                                      media_infos_to_ids)



            # page.image_dirs = image_dir
            # page.arc_image_paths = arc_image_dir
            # page.other_media_paths = other_media

            page.questions = self._db.page_to_questions(tour_id, page_id)

            page.dictionary_words = self._db.page_to_words(tour_id, page_id)

            page.notes = self._db.page_to_notes(page_id)

            pages.append(page)
        return pages


class Section(PrintableMixin):
    def __init__(self):
        self.title = None
        self.pages = None

class Page(PrintableMixin):
    def __init__(self):
        self.body = None
        self.page_id = None
        self.image_dirs = set()
        self.arc_image_paths = set()
        self.other_media_paths = set()
        self.questions = []
        self.dictionary_words = []
        self.notes = []
        self.media = []


class Media:
    def __init__(self):
        self.remote_path = None
        self.local_path = None
        self.arc_path = None
        self.media_type = None
        self.title = None
        self.caption = None

class Printer:
    SEP = "-" * 25

    def __init__(self, indentation=4):
        self._indentation = indentation
        self._current_level = 0
        self._pages_so_far = 0
        self._bodies = []

    def _print(self, string):
        try:
            to_print = string.decode()
        except AttributeError:
            to_print = string
        to_print = self._fix_unicode(to_print)
        print("{}{}".format(
            " " * (math.floor(self._indentation * self._current_level)),
            to_print))

    def _fix_unicode(self, to_fix):
        bar_fixed = to_fix.replace("|", "'")
        return ftfy.fix_text(bar_fixed)

    def _with_inc_indent(self, fun, args):
        self._current_level += 1
        fun(*args)
        self._current_level -= 1

    def _split_lines(self, left, right, rhs_multiline=False):
        self._print(left)
        self._current_level += .5
        if rhs_multiline:
            for el in right:
                self._print(el)
        else:
            self._print(right)
        self._current_level -= .5

    def _print_note(self, note):
        self._print("Text: {}".format(note.text))
        self._print("Date: {}".format(note.date))
        self._print("Submitted by: {} {}".format(
            note.first_name[0][0], note.last_name[0][0]))

    def _print_notes(self, notes):
        self._print("Notes:")
        for note in notes:
            self._with_inc_indent(self._print_note, (note,))

    def _print_page(self, page):
        self._split_lines("Body:", page.body)
        # self._split_lines("Archived image paths: ", page.arc_image_paths, True)
        self._split_lines("Questions: ", page.questions, True)
        self._split_lines("Dictionary words: ", page.dictionary_words, True)
        # self._split_lines("Paths to non-image media: ", page.other_media_paths, True)
        # self._split_lines("Notes: ", page.notes, True)
        self._print_media(page.media)
        self._print_notes(page.notes)

        self._print_sep()


    def _print_sep(self):
        temp_level = self._current_level
        self._current_level = 0
        self._print(self.SEP)

        self._current_level = temp_level

    def _print_media(self, media):
        self._print("Media:")
        self._current_level += 1
        for index, media_element in enumerate(media, 1):
            self._print("Element #{}".format(index))

            self._current_level += .5

            self._print("Media type: {}".format(media_element.media_type))
            self._print("Remote path: {}".format(media_element.remote_path))
            self._print("Archive path: {}".format(media_element.arc_path))
            self._print("Local path: {}".format(media_element.local_path))
            self._print("Title: {}".format(media_element.title))
            self._print("Caption: {}".format(media_element.caption))

            self._current_level -= .5

        self._current_level -= 1


    def _print_pages(self, pages):
        index = 0
        for index, page in enumerate(pages, 1):
            self._print("Page #{} (id {}, {} in section):".format(
                self._pages_so_far + index,
                page.page_id,
                index))
            self._bodies.append("Page {}: {}".format(page.page_id,
                                                     page.body))

            self._with_inc_indent(self._print_page, (page,))
        self._pages_so_far += index

    def print_sections(self, sections):
        for index, section in enumerate(sections, 1):
            self._print("Section #{}, title: {}".format(index, section.title))
            self._print("Pages: ")
            self._with_inc_indent(self._print_pages, (section.pages,))

    def write_body(self, out_path):
        body_el_sep = "\n{}\n".format(self.SEP)
        if self._bodies:
            with open(out_path, "w") as f:
                f.write(body_el_sep.join([self._fix_unicode(s) for s in
                                          self._bodies]))
        else:
            raise BadArgumentsError


# @easylogger.log_at(new_level=logging.ERROR)
def main():
    arg_parser = argparse.ArgumentParser(description="Download web docent content.")
    arg_parser.add_argument(
        "-i", "--imagefiles",
        dest="imagefiles",
        action="store",
        default="no",
        help="specify download behavior (default: do not download)")
    arg_parser.add_argument(
        "tour_id",
        metavar="tour id",
        nargs=1,
        help="tour id to process",)

    args = arg_parser.parse_args()

    LOG.debug("got args: ", args)

    tour_id = args.tour_id[0]

    def raise_error():
        raise BadArgumentsError

    downloader = collections.defaultdict(raise_error, {
        "yes": lambda: RealDownloader(SCPGetter(), tour_id),
        "local": lambda: RealDownloader(LocalGetter(), tour_id),
        "no": lambda: NoOpDownloader()
    })[args.imagefiles.lower()]()

    db = Database()
    media_builder = MediaBuilder(db, downloader)
    page_builder = PageBuilder(db, media_builder)
    section_builder = SectionBuilder(db, page_builder)



    sections = section_builder.for_tour(tour_id)
    printer = Printer()
    print("CONTENT FOR TOUR ID {}".format(tour_id))
    print("MODULE TITLE: {}".format(db.tour_to_module_title(tour_id)))
    print("TOUR TITLE: {}".format(db.tour_to_tour_title(tour_id)))
    printer.print_sections(sections)
    printer.write_body("summary-tour-{}.txt".format(tour_id))


    return sections

if __name__ == '__main__':
    final_res = main()
