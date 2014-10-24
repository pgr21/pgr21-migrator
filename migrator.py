#!/usr/bin/env python3

import pymysql
import psycopg2
import traceback
import datetime
import re
import sys

cfg = {}
with open('cfg.py') as fp:
    exec(fp.read(), None, cfg)

user_id2id = {}
post_id2id = {}
post_child_cnts = {}
comm_id2id = {}
comm_sort_keys = {}
comm_child_cnts = {}

def get_comm_sort_code(idx):
    return chr(idx)

def main():
    db_to = psycopg2.connect(dbname=cfg['PGSQL_DB'])
    cur_to = db_to.cursor()

    db_from = pymysql.connect(user=cfg['MYSQL_USERNAME'], passwd=cfg['MYSQL_PASSWORD'], database=cfg['MYSQL_DB'])
    cur_from = db_from.cursor()
    cur_from.execute('SET names utf8')
    cur_from.execute('SET wait_timeout = 3600')

    cur_to.execute('DELETE FROM comm')
    cur_to.execute('DELETE FROM post')
    cur_to.execute('DELETE FROM "user"')

    cur_from.execute('SELECT no, user_id, password, name FROM pbb_member_table ORDER BY no')
    for row in cur_from:
        id = row[0]
        username = row[1]
        password = row[2]
        name = row[3]

        cur_to.execute('SAVEPOINT tmp')
        try:
            cur_to.execute('''
                INSERT INTO "user"
                (username, password, name, ts)
                VALUES (%s, %s, %s, now())
                RETURNING id
            ''', [username, password, name])
        except:
            cur_to.execute('ROLLBACK TO tmp')

            # FIXME
            print('Unable to insert a user: #{} {}'.format(id, repr(username)), file=sys.stderr)
        else:
            user_id2id[id] = cur_to.fetchone()[0]
        finally:
            cur_to.execute('RELEASE tmp')

    cur_from.execute('SELECT ismember, memo, subject, reg_date, no FROM pbb_board_freedom ORDER BY no')
    for row in cur_from:
        try: user_id = user_id2id[row[0]]
        except KeyError:
            #print('Unable to find a user\'s id: #{}'.format(user_id), file=sys.stderr)
            continue # FIXME
        text = row[1]
        name = row[2]
        ts = datetime.datetime.fromtimestamp(row[3])
        id = row[4]

        cur_to.execute('''
            INSERT INTO post
            (user_id, name, text, ts, board_id)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        ''', [user_id, name, text, ts, 1]) # FIXME

        new_id = cur_to.fetchone()[0]
        post_id2id[id] = new_id
        post_child_cnts[new_id] = 0

    cur_from.execute('SELECT parent, ismember, name, memo, reg_date, no FROM pbb_board_comment_freedom ORDER BY no')
    for row in cur_from:
        try:
            post_id = post_id2id[row[0]]
            user_id = user_id2id[row[1]]
            user_name = row[2]
            text = row[3]
            ts = datetime.datetime.fromtimestamp(row[4])
            id = row[5]

            text = text.strip()

            mat = re.match(r'(?:&lt;|<)!--(.*?)\|(.*?)--(?:&gt;|>)(.*)$', text, flags=re.S)
            if mat:
                parent_id, level, text = mat.groups()
                try: parent_id = int(parent_id)
                except ValueError:
                    print('Unable to convert a comment\'s parent_id: #{} {}'.format(id, repr(parent_id)), file=sys.stderr)
                    continue
                parent_id = comm_id2id[parent_id]
            else:
                parent_id, level = None, 0
        except KeyError:
            #print('Unable to find an id', file=sys.stderr)
            continue # FIXME

        if parent_id:
            comm_child_cnts[parent_id] += 1
            sort_key = comm_sort_keys[parent_id] + '.' + get_comm_sort_code(comm_child_cnts[parent_id])

            cur_to.execute('''
                UPDATE comm
                SET child_cnt = child_cnt + 1
                WHERE id = %s
            ''', [parent_id])
        else:
            post_child_cnts[post_id] += 1
            sort_key = get_comm_sort_code(post_child_cnts[post_id])

            cur_to.execute('''
                UPDATE post
                SET child_cnt = child_cnt + 1
                WHERE id = %s
            ''', [post_id])

        cur_to.execute('''
            INSERT INTO comm
            (text, ts, user_id, post_id, parent_id, level, sort_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', [text, ts, user_id, post_id, parent_id, level, sort_key])

        new_id = cur_to.fetchone()[0]
        comm_id2id[id] = new_id
        comm_sort_keys[new_id] = sort_key
        comm_child_cnts[new_id] = 0

    db_to.commit()

if __name__ == '__main__':
    main()
