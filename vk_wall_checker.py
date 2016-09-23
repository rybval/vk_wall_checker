#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Checks vk wall for changes.

Downloads several last posts,
compares them with local copy,
extracts changes like new post, editions in post, new comments,
send all via email.

Requires local SMTP server.
Recommended for use with cron.
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

WORKING_DIR = os.path.expanduser("~/.vk_group_checker")
POSTS_COUNT = 12
DATETIME_FORMAT = '%Y.%m.%d-%H.%M.%S'
TEMPLATE = {
'message': """<html>
  <head></head>
  <body>
    {}
  </body>
</html>""",
'section': """<p><b>{}</b></p>{}""",
'post': """<p>id: {id}</p>
<p>Дата публикации: {date}</p>
<p>Автор: {author}</p>
<p>Подписано: {signer}</p>
<p>Текст: {text}</p>
{attachments}""",
'attachments': """{}""",
'comment': """ """,
'photo': """<img src="{link}">""",
'document': """Документ: <a href="{link}">{name}</a>""",
'audio': """ """,
'video': """ """}

template['post'].format(posts[5])

def extended_data_processing(data):
    # Function has side effect: "data" changed
    # turns list of dicts into dict of dicts with id as key.
    d = {d_['id']: d_ for d_ in data}
    for d_ in data:
        for key in tuple(d_):
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
            for key in tuple(attachment):
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
        for key in tuple(item):
            if key not in ('id', 'owner_id', 'from_id', 'date', 'text',
                           'attachments', 'signer_id', 'copy_history',
                           'is_pinned', 'reply_to_user', 'reply_to_comment'):
                del item[key]
        if 'attachments' in item:
            attachments_processing(item['attachments'])

    return (items, profiles, groups)


def add_new_extended_data(data, new_data):
    for id in tuple(new_data):
        if id not in data:
            data[id] = new_data[id]


def get_new_dump(app_id, access_token, owner, comments=False, 
                 count = POSTS_COUNT):
    session = vk.Session(client_id = app_id, access_token = access_token)
    response = session.wall.get(owner_id = owner, count = count,
                                extended = 1)
    t = time.time()
    posts, profiles, groups = response_processing(response)

    if comments:
        for post in posts:
            vk.timeout(t)
            response = session.wall.getComments(owner_id=post['owner_id'],
                                   post_id=post['id'], count=100, sort='asc',
                                   preview_length=0, extended=1)
            t = time.time()
            post['comments'], profiles_, groups_= response_processing(response)

            add_new_extended_data(profiles, profiles_)
            add_new_extended_data(groups, groups_)
            
    dump = posts
    out = dump, {'profiles': profiles, 'groups': groups}
    return out


def get_last_dump(wall_path):
    names = os.listdir(wall_path)
    last = max(names,
        key=lambda n: datetime.strptime(n.split('.')[0], DATETIME_FORMAT))
    path = os.path.join(wall_path, last)
    with bz2.open(path, 'rt', encoding='utf-8') as file:
        dump = json.load(file)
    return dump


def save_dump(dump, wall_path):
    now_str = datetime.now().strftime(DATETIME_FORMAT)
    path = os.path.join(wall_path, now_str+'.json.bz2')
    with bz2.open(path, 'wt', encoding='utf-8') as file:
        json.dump(dump, file, ensure_ascii=False, indent='    ',sort_keys=True)


def extract_ids(items):
    ids = set()
    for item in items:
        ids.add(item['id'])
    return ids


def compare_dumps(old, new):
    old_ids = extract_ids(old)
    new_ids = extract_ids(new)
    appeared_ids = new_ids - old_ids 
    disappeared_ids = old_ids - new_ids
    newest_old = max(old_ids)
    oldest_new = min(new_ids)

    new_posts = []
    for post in new:
        if post['id'] in appeared_ids and post['id'] > newest_old:
            new_posts.append(post)

    deleted_posts = []
    for post in old:
        if post['id'] in disappeared_ids and post['id'] > oldest_new:
            deleted_posts.append(post)

    changed_posts = None
    new_comments = None
    deleted_comments = None
    changed_comments = None
    
    return (new_posts, deleted_posts, changed_posts,
            new_comments, deleted_comments, changed_comments)


def build_part_subject(new, deleted, changed):
    part = ''
    if new_posts or deleted_posts or changed_posts:
        changes = []
        if new_posts:
            changes.append('создание')
        if deleted_posts:
            changes.append('удаление')
        if changed_posts:
            changes.append('изменение')
        part = '(' + ', '.join(changes) + ')'
    return part


def build_subject(new_posts, deleted_posts, changed_posts,
                  new_comments, deleted_comments, changed_comments):
    p = build_part_subject(new_posts, deleted_posts, changed_posts)
    c = build_part_subject(new_comments, deleted_comments, changed_comments)
    if p:
        p = 'Посты ' + p
    if c:
        c = 'Комментарии ' + c
        
    if p and c:
        subject = '. '.join(p, c)
    else:
        subject = p + c
    
    return subject

def build_html(new_posts, deleted_posts, changed_posts,
               new_comments, deleted_comments, changed_comments, 
               extended):
    sections = []
    if new_posts:
        new_posts_html = []
        for post in new_posts:
            author = get_name_by_id(post['from_id'], extended)
            if 'signer_id' in post:
                signer = get_name_by_id(post['signer_id'], extended)
            else:
                signer = author
            dt = datetime.fromtimestamp(post['date']).strftime(DATETIME_FORMAT)
            
            new_posts_html.append(TEMPLATE['post'].format(
            id = post['id'],
            text = post['text']),
            date = dt,
            author = author,
            signer = signer)
            
        sections.append(
            TEMPLATE['section'].format('Новые посты', ''.join(new_posts_html)))

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

    wall_path = os.path.append(WORKING_DIR, str(owner))
    new_dump, extended = get_new_dump(args.app_id, args.access_token,
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

    subject = build_subject(*diff)
    html = build_html(*diff, extended)
    msg = mail.make(args.from_email, args.to_email, subject, html)
    mail.send(msg)