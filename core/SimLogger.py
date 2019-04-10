import datetime
import inspect
import logging
import os
import sys

from core import Constants

if "utils" not in sys.path: sys.path.append("utils")
from utils import AISQLiteUtils, SimUtils


def setup_logging(logging_handler, logging_level=logging.DEBUG):
    """
    Adds a logging handler to core and utils packages.
    Multiple handlers can be added, each with different logging_level.

    If no handler is added before starting the simulator, no logging output is generated.

    Args:
        logging_handler (logging.handler): a logging.handler instance.
        logging_level (int): the numeric value for logging level

    e.g.
    setup_logging(logging.StreamHandler(sys.stdout))  # logs to stdout
    setup_logging(logging.FileHandler('out'))  # logs to file 'out'
    Both calls above can be used at the same time.

    Any logging.handlers can be added, see https://docs.python.org/2/library/logging.handlers.html for more.
    """

    class ContextFilter(logging.Filter):
        """This is a filter which injects contextual information into the log message."""

        def filter(self, record):
            if 'func' not in record.__dict__:
                # walks just above logging frame
                frame = inspect.currentframe().f_back.f_back.f_back.f_back.f_back.f_back.f_back
                record.func = frame.f_code.co_name
                if 'ts_now' not in record.__dict__:
                    record.ts_now = '0'

            record.func = record.func + '()'

            return True

    log_format = logging.Formatter(
        fmt='%(levelname)-6s %(func)-33s @%(ts_now)2s: %(message)s',
        datefmt=SimUtils.DATE_FORMAT,
    )

    logging_handler.addFilter(ContextFilter())
    logging_handler.setFormatter(log_format)
    logging_handler.setLevel(logging_level)

    core_logger = logging.getLogger('core')
    core_logger.addHandler(logging_handler)
    core_logger.setLevel(logging.DEBUG)

    utils_logger = logging.getLogger('utils')
    utils_logger.addHandler(logging_handler)
    utils_logger.setLevel(logging.DEBUG)

    return logging_handler


def cleanup_logging(logging_handler=None):
    """
    Removes the handler(s) added in setup_logging() (remove handler(s) for loggers in core and utils packages).
    If logging_handler is omitted, removes all handlers for core and utils packages.
    """

    SimUtils.remove_logger_handlers(logging.getLogger('core'), logging_handler)
    SimUtils.remove_logger_handlers(logging.getLogger('utils'), logging_handler)

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DBLogger(object):
    __metaclass__ = Singleton

    def __init__(self, sim, DBName='log.db3', BufferSize=100):
        """BufferSize -- size of the buffer, in log entries"""

        self.BufferSize = BufferSize
        self.DBLog = AISQLiteUtils.CMySQLConnection(DBName)
        self.sim = sim
        self.config = self.sim.config
        self._logger = logging.getLogger(__name__)
        self._logger.addHandler(logging.NullHandler())

        cursor = self.DBLog.getCursor()
        cursor.execute("""DROP TABLE IF EXISTS `Log`""")
        cursor.execute("""
            CREATE TABLE `Log` (
              `line_no` INTEGER PRIMARY KEY,
              `real_time` varchar(45) default NULL,
              `sim_time` INTEGER unsigned default NULL,
              `message` varchar(5000) default NULL
              )
            """)
        self.DBLog.commit()
        cursor.close()
        self.WriteCursor = self.DBLog.getCursor()
        self.Buffer = []
        self.iLastIndex = 0

    def close(self):
        self.flush()
        self.WriteCursor.close()
        self.DBLog.close()

        DBLogger._instances = {}

    def flush(self):
        if not self.config['simulation']['LoggingEnabled']: return
        if self.iLastIndex > 0:
            self.WriteCursor.executemany(
                "insert into `Log` (`line_no`, `real_time`, `sim_time`, `message`) values (NULL, ?, ?, ?)", self.Buffer)
            self.DBLog.commit()
            self.Buffer = []
            self.iLastIndex = 0

    def db(self, message):
        if not self.config['simulation']['LoggingEnabled']: return

        real_time = datetime.datetime.now().strftime(SimUtils.DATE_FORMAT)
        sim_time = self.sim.ts_now

        self.Buffer.append((real_time, sim_time, str(message)))
        self.iLastIndex += 1
        if self.iLastIndex == self.BufferSize:
            self.flush()

    def log(self, message, log_level='info'):
        if not self.config['simulation']['LoggingEnabled']: return

        frame = inspect.currentframe().f_back
        if frame.f_code.co_name == 'log_and_db':
            frame = frame.f_back

        class_name = ''
        if frame.f_locals.get('self'):
            instance = frame.f_locals['self']
            if (instance.__dict__.get('name')):
                class_name = instance.__dict__['name']
            else:
                class_name = instance.__class__.__name__

        extra = {
            'func': '{0}.{1}'.format(class_name, frame.f_code.co_name),
            'ts_now': self.sim.ts_now
        }

        if isinstance(log_level, basestring):
            getattr(self._logger, log_level)(message, extra=extra)
        else:
            self._logger.log(log_level, message, extra=extra)

    def log_and_db(self, message, log_level='info'):
        if not self.config['simulation']['LoggingEnabled']: return

        self.log(message, log_level)
        self.db(message)


class DBTaskTrace(object):
    #    NO_TABLES = 1
    #    TABLE_FinishedTasks = 0
    #    TABLE_NAMES = {
    #        TABLE_FinishedTasks: 'FinishedTasks',
    #        }
    #    TABLE_INSERT_FORMAT = {
    #        TABLE_FinishedTasks: (
    #            "(`task_id`, `sub_site`, `exec_site`, `user`, `ts_submit`, `ts_start`, `ts_stop`, `result`, `ncpus`, `visited_sites`)",
    #            "(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
    #        }
    def __init__(self, DBName='tasktrace.db3', BufferSize=100):
        self.BufferSize = BufferSize
        self.DB = AISQLiteUtils.CMySQLConnection(DBName)
        cursor = self.DB.getCursor()
        cursor.execute("""DROP TABLE IF EXISTS `FinishedTasks`""")
        cursor.execute("""
            CREATE TABLE `FinishedTasks` (
              `task_id` INTEGER PRIMARY KEY,
              `sub_site` varchar(45) default NULL,
              `exec_site` varchar(45) default NULL,
              `user` varchar(45) default NULL,
              `ts_submit` INTEGER unsigned default NULL,
              `ts_start` INTEGER unsigned default NULL,
              `ts_stop` INTEGER unsigned default NULL,
              `result` INTEGER unsigned default NULL,
              `ncpus` INTEGER unsigned default NULL,
              `visited_sites` varchar(1000) default NULL
              )
            """)
        self.DB.commit()
        cursor.close()

        self.WriteCursor = self.DB.getCursor()
        self.Buffer = []
        self.iLastIndex = 0

    def close(self):
        self.flush()
        self.WriteCursor.close()
        self.DB.close()

    def flush(self):
        if self.iLastIndex > 0:
            self.WriteCursor.executemany("""
                insert into `FinishedTasks`(`task_id`, `sub_site`, `exec_site`, `user`, `ts_submit`, `ts_start`, `ts_stop`, `result`, `ncpus`, `visited_sites`)
                values (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, self.Buffer)
            self.DB.commit()
            self.Buffer = []
            self.iLastIndex = 0

    def addFinishedTask(self, sub_site, exec_site, user, ts_submit, ts_start, ts_stop, result, ncpus, visited_sites):
        self.Buffer.append((sub_site, exec_site, user, ts_submit, ts_start, ts_stop, result, ncpus, visited_sites))
        self.iLastIndex += 1
        if self.iLastIndex == self.BufferSize:
            self.flush()


class DBStats(object):
    NO_TABLES = 5
    TABLE_NoMessages = 0
    TABLE_SiteStats = 1
    TABLE_UserStats = 2
    TABLE_SystemSitesStats = 3
    TABLE_SystemUsersStats = 4
    TABLE_NAMES = {
        TABLE_NoMessages: 'NoMessages',
        TABLE_SiteStats: 'SiteStats',
        TABLE_UserStats: 'UserStats',
        TABLE_SystemSitesStats: 'SystemSitesStats',
        TABLE_SystemUsersStats: 'SystemUsersStats',
    }

    TABLE_INSERT_FORMAT = {
        TABLE_NoMessages: ("(`id`, `sim_time`, `id_message_type`, `no_messages`)", "(NULL, ?, ?, ?)"),
        TABLE_SiteStats: (
            "(`id`, `sim_time`, `id_stat_type`, `id_source`, `ivalue`, `fvalue`, `svalue`)",
            "(NULL, ?, ?, ?, ?, ?, ?)"),
        TABLE_UserStats: (
            "(`id`, `sim_time`, `id_stat_type`, `id_source`, `ivalue`, `fvalue`, `svalue`)",
            "(NULL, ?, ?, ?, ?, ?, ?)"),
        TABLE_SystemSitesStats: (
            "(`id`, `sim_time`, `id_stat_type`, `ivalue`, `fvalue`, `svalue`)", "(NULL, ?, ?, ?, ?, ?)"),
        TABLE_SystemUsersStats: (
            "(`id`, `sim_time`, `id_stat_type`, `ivalue`, `fvalue`, `svalue`)", "(NULL, ?, ?, ?, ?, ?)"),
    }

    def __init__(self, DBName='stats.db3', BufferSize=10):
        self.BufferSize = BufferSize
        self.DB = AISQLiteUtils.CMySQLConnection(DBName)
        cursor = self.DB.getCursor()
        cursor.execute("""DROP TABLE IF EXISTS `""" + str(self.TABLE_NAMES[self.TABLE_NoMessages]) + """`""")
        cursor.execute("""
            CREATE TABLE `""" + str(self.TABLE_NAMES[self.TABLE_NoMessages]) + """` (
              `id` INTEGER PRIMARY KEY,
              `sim_time` INTEGER unsigned default NULL,
              `id_message_type` INTEGER unsigned default NULL,
              `no_messages` INTEGER unsigned default NULL
              )
            """)
        cursor.execute("""DROP TABLE IF EXISTS `""" + str(self.TABLE_NAMES[self.TABLE_SiteStats]) + """`""")
        cursor.execute("""
            CREATE TABLE `""" + str(self.TABLE_NAMES[self.TABLE_SiteStats]) + """` (
              `id` INTEGER PRIMARY KEY,
              `sim_time` INTEGER unsigned default NULL,
              `id_stat_type` INTEGER unsigned default NULL,
              `id_source` INTEGER usigned default NULL,
              `ivalue` INTEGER unsigned default NULL,
              `fvalue` FLOAT default NULL,
              `svalue` VARCHAR(100) default NULL
              )
            """)
        cursor.execute("""DROP TABLE IF EXISTS `""" + str(self.TABLE_NAMES[self.TABLE_UserStats]) + """`""")
        cursor.execute("""
            CREATE TABLE `""" + str(self.TABLE_NAMES[self.TABLE_UserStats]) + """` (
              `id` INTEGER PRIMARY KEY,
              `sim_time` INTEGER unsigned default NULL,
              `id_stat_type` INTEGER unsigned default NULL,
              `id_source` INTEGER usigned default NULL,
              `ivalue` INTEGER unsigned default NULL,
              `fvalue` FLOAT default NULL,
              `svalue` VARCHAR(100) default NULL
              )
            """)
        cursor.execute("""DROP TABLE IF EXISTS `""" + str(self.TABLE_NAMES[self.TABLE_SystemSitesStats]) + """`""")
        cursor.execute("""
            CREATE TABLE `""" + str(self.TABLE_NAMES[self.TABLE_SystemSitesStats]) + """` (
              `id` INTEGER PRIMARY KEY,
              `sim_time` INTEGER unsigned default NULL,
              `id_stat_type` INTEGER unsigned default NULL,
              `ivalue` INTEGER unsigned default NULL,
              `fvalue` FLOAT default NULL,
              `svalue` VARCHAR(100) default NULL
              )
            """)
        cursor.execute("""DROP TABLE IF EXISTS `""" + str(self.TABLE_NAMES[self.TABLE_SystemUsersStats]) + """`""")
        cursor.execute("""
            CREATE TABLE `""" + str(self.TABLE_NAMES[self.TABLE_SystemUsersStats]) + """` (
              `id` INTEGER PRIMARY KEY,
              `sim_time` INTEGER unsigned default NULL,
              `id_stat_type` INTEGER unsigned default NULL,
              `ivalue` INTEGER unsigned default NULL,
              `fvalue` FLOAT default NULL,
              `svalue` VARCHAR(100) default NULL
              )
            """)
        self.DB.commit()
        cursor.close()

        self.WriteCursor = {}
        self.Buffer = {}
        self.iLastIndex = {}
        for Table in self.TABLE_NAMES:
            self.WriteCursor[Table] = self.DB.getCursor()
            self.Buffer[Table] = []
            self.iLastIndex[Table] = 0

    def close(self):
        for Table in self.TABLE_NAMES:
            self.flush(Table)
            self.WriteCursor[Table].close()
            self.DB.close()

    def flush(self, Table):
        if self.iLastIndex[Table] > 0:
            self.WriteCursor[Table].executemany("""
                insert into `""" + str(self.TABLE_NAMES[Table]) + """`""" + str(self.TABLE_INSERT_FORMAT[Table][0]) + """
                values """ + str(self.TABLE_INSERT_FORMAT[Table][1]) + """
                """, self.Buffer[Table])
            self.DB.commit()
            self.Buffer[Table] = []
            self.iLastIndex[Table] = 0

    def flushall(self):
        for Table in self.TABLE_NAMES:
            self.flush(Table)

    def addNoMessages(self, sim_time, id_message_type, no_messages):
        Table = self.TABLE_NoMessages
        self.Buffer[Table].append((sim_time, id_message_type, no_messages))
        self.iLastIndex[Table] += 1
        if self.iLastIndex[Table] == self.BufferSize:
            self.flush(Table)

    def addSiteStats(self, sim_time, id_stat_type, id_source, ivalue=None, fvalue=None, svalue=None):
        Table = self.TABLE_SiteStats
        self.Buffer[Table].append((sim_time, id_stat_type, id_source, ivalue, fvalue, svalue))
        self.iLastIndex[Table] += 1
        if self.iLastIndex[Table] == self.BufferSize:
            self.flush(Table)

    def addUserStats(self, sim_time, id_stat_type, id_source, ivalue=None, fvalue=None, svalue=None):
        Table = self.TABLE_UserStats
        self.Buffer[Table].append((sim_time, id_stat_type, id_source, ivalue, fvalue, svalue))
        self.iLastIndex[Table] += 1
        if self.iLastIndex[Table] == self.BufferSize:
            self.flush(Table)

    def addSystemSitesStats(self, sim_time, id_stat_type, ivalue=None, fvalue=None, svalue=None):
        Table = self.TABLE_SystemSitesStats
        self.Buffer[Table].append((sim_time, id_stat_type, ivalue, fvalue, svalue))
        self.iLastIndex[Table] += 1
        if self.iLastIndex[Table] == self.BufferSize:
            self.flush(Table)

    def addSystemUsersStats(self, sim_time, id_stat_type, ivalue=None, fvalue=None, svalue=None):
        Table = self.TABLE_SystemUsersStats
        self.Buffer[Table].append((sim_time, id_stat_type, ivalue, fvalue, svalue))
        self.iLastIndex[Table] += 1
        if self.iLastIndex[Table] == self.BufferSize:
            self.flush(Table)
