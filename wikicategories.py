#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Wikipedia category utility for wiki's dummped data.
"""

import argparse
import logging
import sqlite3
import sys
from tqdm import tqdm
from contextlib import closing

# Define Log settings
LOG_LINE_FM = '%(asctime)s(%(levelname)s): %(message)s'
LOG_DATE_FM = '%H:%M:%S'

# Defines
DB_NAME = './wikipedia.db'

# SQL
SQL_SELECT_SUBCATEGORIES = '''
SELECT
    c.cl_from, p.page_title
FROM
    categorylinks AS c
JOIN
    page AS p ON c.cl_from = p.page_id
WHERE
    c.cl_type = "subcat" AND c.cl_to = "{}";
'''

def get_categories(log, dbname, targets, unique=False, depth_limit=1, current_depth=1, categories=[]):
    """ Get list of subcategories from wikipedia db.
    """

    with closing(sqlite3.connect(dbname)) as conn:
        c = conn.cursor()

        for cat_num, row in enumerate(c.execute(SQL_SELECT_SUBCATEGORIES.format(targets[-1]))):

            # unique属性がTrueの場合、重複値は追加せず再帰を止める
            if unique and row[1] in categories:
                continue

            categories.append(row[1])
            log.debug("[{}] {} - {}".format(current_depth, " - ".join(targets), row[1]))

            # 探索limitを超えた場合、再帰を止める
            if current_depth >= depth_limit or row[1] in targets:
                continue

            categories = get_categories(log, dbname, targets + [row[1]], unique, depth_limit, current_depth+1, categories)

    return categories

if __name__ == '__main__':

    # Setup Argparse.
    parser = argparse.ArgumentParser(description="Wikipedia category utility for wiki's dummped data.")
    parser.add_argument("dbname", help="Name of SQLite dbfile.")
    parser.add_argument("-t", "--target", default=DB_NAME, help="Target classname of parent category.")
    parser.add_argument("-m", "--limit", default=1, type=int, help="Limit of tree depth.")
    # parser.add_argument("-l", "--loop", default=False, action='store_true', help="Arrow loop category.")
    parser.add_argument("-u", "--unique", default=False, action='store_true', help="Enable uniq list(Not add duplicate name to the list).")
    parser.add_argument("-d", "--dev", default=False, action='store_true', help="Run as debug mode(with debug messages).")
    parser.add_argument("-q", "--quiet", default=False, action='store_true', help="No message without errors.")
    args = parser.parse_args()

    # Setup Logger
    if not args.dev:
        if not args.quiet:
            # For Default
            logging.basicConfig(level=logging.INFO, format=LOG_LINE_FM, datefmt=LOG_DATE_FM)
        else:
            # For Quiet
            logging.basicConfig(level=logging.ERROR, format=LOG_LINE_FM, datefmt=LOG_DATE_FM)
    else:
        # For Develop
        logging.basicConfig(level=logging.DEBUG, format=LOG_LINE_FM, datefmt=LOG_DATE_FM)
    log = logging.getLogger(__name__)

    get_categories(log, args.dbname, [args.target], args.unique, args.limit, 1, [])
