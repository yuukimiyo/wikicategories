#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Wikipedia category utility for wiki's dummped data.
"""

import argparse
import logging
import sqlite3
import sys
from contextlib import closing

# Define Log settings
LOG_LINE_FM = '%(asctime)s(%(levelname)s) %(name)s: %(message)s'
LOG_DATE_FM = '%H:%M:%S'

# Defines
DEFAULT_DB_NAME = './wikipedia.db'

# SQL
SQL_SELECT_SUBCATEGORIES = '''
SELECT
    p.page_id, p.page_title
FROM
    categorylinks AS c
JOIN
    page AS p ON c.cl_from = p.page_id
WHERE
    c.cl_type = "subcat" AND c.cl_to = "{}";
'''

SQL_SELECT_CONTAIN_PAGES = '''
SELECT
    p.page_id, p.page_title
FROM
    categorylinks AS c
JOIN
    page AS p ON c.cl_from = p.page_id
WHERE
    c.cl_type = "page" AND c.cl_to = "{}";
'''

def get_categories(conf, conn, target, depth=1, ancestor=[], categories=[]):
    """ Get list of subcategories from wikipedia db.
    """
    log = logging.getLogger(sys._getframe().f_code.co_name)

    cur = conn.cursor()
    for cat_num, row in enumerate(cur.execute(SQL_SELECT_SUBCATEGORIES.format(target))):
        subcategory = row[1]

        # unique属性がTrueで、(処理全体を通して)既出のカテゴリが現れた場合、そのカテゴリはリストに追加しない
        if conf.unique and subcategory in categories:
            continue

        # exclude(除外リスト)に存在するカテゴリの場合、リストに追加しない
        if subcategory in conf.exclude:
            continue

        # カテゴリをリストに追加する
        categories.append(subcategory)
        log.debug("[{}] {}".format(depth, " - ".join(ancestor + [target] + [subcategory])))

        # 探索limitを超えた場合、再帰は行わない
        if depth >= conf.limit:
            continue

        # loop属性がFalse(default)で、検索対象が親カテゴリリストに存在した場合、再帰は行わない
        if not conf.loop and subcategory in ancestor:
            continue

        # 追加したカテゴリについて、再帰的にサブカテゴリを探索
        categories = get_categories(conf, conn, subcategory, depth + 1, ancestor + [target], categories)

    return categories

class Conf():
    def __init__(self, unique, limit, loop, exclude):
        self.unique = unique
        self.limit = limit
        self.loop = loop

        if isinstance(exclude, list):
            self.exclude = exclude
        else:
            self.exclude = []

if __name__ == '__main__':

    #
    # Setup Argparse.
    #
    parser = argparse.ArgumentParser(description="Wikipedia category utility for wiki's dummped data.")
    parser.add_argument("dbname", help="Name of SQLite dbfile.")
    parser.add_argument("-t", "--target", default=DEFAULT_DB_NAME, help="Target classname of parent category.")
    parser.add_argument("-l", "--limit", default=1, type=int, help="Limit of tree depth.")
    parser.add_argument("-p", "--loop", default=False, action='store_true', help="Arrow loop category.")
    parser.add_argument("-u", "--unique", default=False, action='store_true', help="Enable uniq list(Not add duplicate name to the list).")
    parser.add_argument("-d", "--dev", default=False, action='store_true', help="Run as debug mode(with debug messages).")
    parser.add_argument("-q", "--quiet", default=False, action='store_true', help="No output.")
    parser.add_argument("-e", "--exclude", nargs='*', help="Exclude the specified category")
    args = parser.parse_args()

    #
    # Setup Logger
    #

    # For Default
    loglevel = logging.INFO

    # For Quiet
    if args.quiet:
        loglevel = logging.ERROR

    # For Develop
    if args.dev:
        loglevel = logging.DEBUG

    logging.basicConfig(level=loglevel, format=LOG_LINE_FM, datefmt=LOG_DATE_FM)

    #
    # Main
    #

    log = logging.getLogger(__name__)

    conn = sqlite3.connect(args.dbname)
    conf = Conf(args.unique, args.limit, args.loop, args.exclude)
    categories = get_categories(conf, conn, args.target, 1, [], [])
    conn.close()

    if not args.quiet:
        for c in categories:
           print(c)

    log.info("end")
