#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Checks vk wall for changes.

Downloads several last posts,
compares them with local copy,
extracts changes like new post, editions in post, new comments,
send all via email.

Requires local SMTP server.
Recommended for use with cron.

Using: vk_wall_checker.py <owner-ID> <access-token> <from-email> <to-email>
"""

import sys
import argparse
import vk
import time
import json
import bz2
from urllib.request import urlopen
from urllib.parse import urlparse
from html.parser import HTMLParser
from datetime import datetime, date, time
import re
import os
import difflib
import mail

working_directory = os.path.expanduser("~/.vk_group_checker")
posts_count = 12
datetimeformat = '%Y.%m.%d-%H.%M.%S'
html_template = """\
<html>
  <head></head>
  <body>
    {}
  </body>
</html>
"""

vk_doc_types = ("текстовый документ", "архив", "gif", "изображение",
                "аудио", "видео", "электронная книга", "неизвестно")


def vk_timeout(last_call_time):
    time.sleep(vk.MIN_PAUSE_BETWEEN_CALLS - (time.time() - last_call_time))


def extended_data_processing(data):
    # Function has side effect: "data" changed
    # turns list of dicts into dict of dicts with id as key.
    d = {d_['id']: d_ for d_ in data}
    for d_ in data:
        for key in d_.keys():
            if key not in ('first_name', 'last_name', 'name', 'photo_50'):
                del d_[key]
    return d


def attachments_processing(attachments):
    for attachment in attachments:
        if attachment['type'] == 'photo':
            pass
        elif attachment['type'] == 'video':
            pass
        elif attachment['type'] == 'audio':
            pass
        elif attachment['type'] == 'doc':
            for key in attachment.keys():
                if key not in ('id', 'title', 'size',
                               'url', 'photo_100', 'type'):
                    del attachment[key]

        elif attachment['type'] == 'video':
            pass
        elif attachment['type'] == 'link':
            pass
        elif attachment['type'] == 'poll':
            pass
        elif attachment['type'] == 'page':
            pass
        elif attachment['type'] == 'album':
            pass
        elif attachment['type'] == 'photos_list':
            pass


def response_processing(response):
    items = response['items']
    profiles = extended_data_processing(response['profiles'])
    groups = extended_data_processing(response['groups'])

    for item in items:
        for key in item.keys():
            if key not in ('id', 'owner_id', 'from_id', 'date', 'text',
                           'attachments', 'signer_id', 'copy_history',
                           'is_pinned', 'reply_to_user', 'reply_to_comment'):
                del item[key]
        if 'attachments' in item:
            attachments_processing(item['attachments'])


    return (items, profiles, groups)


def add_new_extended_data(data, new_data):
    for id in new_data.keys():
        if id not in data:
            data[id] = new_data[id]


def get_new_dump(app_id, access_token, group_id, comments=False):
    session = vk.Session(client_id = vk_app_id, access_token = vk_token)
    response = session.wall.get(owner_id = group_id, count = posts_count,
                                extended = 1)
    t = time.time()
    posts, profiles, groups = response_processing(response)


    if comments:
        for post in posts:
            vk_timeout(t)
            response = session.wall.getComments(owner_id=post['owner_id'],
                                   post_id=post['id'], count=100, sort='asc',
                                   preview_length=0, extended=1)
            t = time.time()
            post['comments'], profiles_, groups_= response_processing(response)

            add_new_extended_data(profiles, profiles_)
            add_new_extended_data(groups, groups_)


def get_last_dump(wall_path):
    names = os.listdir(wall_path)
    last = max(names,
        key=lambda n: datetime.strptime(n.split('.')[0], datetimeformat))
    path = os.path.join(wall_path, last)
    with bz2.open(path, 'rt', encoding='utf-8') as file:
        dump = json.load(file)
    return dump


def save_dump(dump, wall_path):
    now_str = datetime.now().strftime(datetimeformat)
    path = os.path.join(wall_path, now_str+'.json.bz2')
    with bz2.open(path, 'wt', encoding='utf-8') as file:
        json.dump(dump, file, ensure_ascii=False, indent='    ',sort_keys=True)


def compare_dumps(old, new):
    return (new_posts, deleted_posts, changed_posts,
            new_comments, deleted_comments, changed_comments)


def create_argparser():
    parser = argparse.ArgumentParser(description='Checks vk wall for changes')

    # To do: args checking, help strings
    wall = parser.add_mutually_exclusive_group(required=True)
    wall.add_argument('-u', '--user-id', type=int)
    wall.add_argument('-g', '--group-id', type=int)

    parser.add_argument('-f', '--from-email', required=True)
    parser.add_argument('-t', '--to-email', required=True)

    parser.add_argument('-p', '--app-id', type=int, required=True)
    parser.add_argument('-a', '--access-token', required=True)

    parser.add_argument('-c', '--comments', action='store_true')
    return parser

if __name__ == '__main__':
    argparser = create_argparser()
    args = argparser.parse_args(sys.argv)
    if args.group_id:
        owner = -abs(args.group_id)
    elif args.user_id:
        if args.user_id < 0:
            args.error('User ID must be positive')
        owner = args.user_id

    wall_path = os.path.append(working_directory, str(owner))
    new_dump = get_new_dump(args.app_id, args.access_token,
                            owner, args.comments)
    try:
        last_dump = get_last_dump(wall_path)
    except FileNotFoundError:
        # No dir — first run for this wall
        os.makedirs(wall_path, 0o700, True)
        save_dump(new_dump, wall_path)
        subject = 'Запуск отслеживания для стены '+str(owner)
        text = ('Для стены -'+str(owner)+' запущено отслеживание.\n')
        msg = mail.make(args.from_email, args.to_email, subject, text)
        mail.send(msg)
        exit()
    except ValueError:
        # No previous dumps — dumps was deleted
        save_dump(new_dump, wall_path)
        subject = 'Перезапуск отслеживания для стены -'+str(owner)
        text = ('Для стены -'+str(owner)+' перезапущено отслеживание.\n'
                'Вероятно, предыдущие дампы для стены были удалены.')
        msg = mail.make(args.from_email, args.to_email, subject, text)
        mail.send(msg)
        exit()

    diff = compare_dumps(last_dump, new_dump)

    html = build_html_diff(diff)
    subject = build_subject(diff)
    msg = mail.make(args.from_email, args.to_email, subject, html)
    mail.send(msg)