#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Checks vk wall for changes.

Downloads several last posts,
compares them with local copy,
extracts changes like new post, editions in post, new comments,
send all via email.

Requires local SMTP server, mail.py.
Recommended for use with cron.
"""

import os
import sys
import argparse
import time
import json
import bz2
from datetime import datetime
import difflib
import copy

import vk
import mail

VK_URL = 'https://vk.com'
WORKING_DIR = os.path.expanduser("~/.vk_group_checker")
EXTENSION = '.json.bz2'
POSTS_COUNT = 12
DATETIME_FORMAT = '%Y.%m.%d-%H.%M.%S'
TEMPLATE = {
'message': """<html>
<head></head>
<body>
<p><b><a href="{target_link}">{target_name}</a></b></p>
{sections}
</body>
</html>""",
'section': """<hr>
<p> </p>
<p><b>{name}</b></p>
{content}
""",
'post': """<p><a href="{link}"><font color="#808080">{id}, {date}</font></a></p>
<p><a href="{author_link}"><font color="#808080">{author_name}</font></a></p>
{sign}
<p> </p>
{text}
{attachments}
<p>------------------------</p>
<p> </p>""",
'sign': ('<p><font color="#808080">Подписано:</font> '
'<a href="{signer_link}"><font color="#808080">{signer_name}</font></a></p>'),
'attachments': '{attachments_joined}',
'comment': '' ,
'photo': '<img src="{link}">',
'document': 'Документ: <a href="{link}">{title}</a>',
'audio': ('Аудиозапись: <a href="{link}">{artist} — {title}</a> '
'({lenght}, {size} Мб)'),
'video': 'Видео: <a href="{link}">{title}</a>'}


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

    return items, profiles, groups


def add_new_extended_data(data, new_data):
    for ID in tuple(new_data):
        if ID not in data:
            data[ID] = new_data[ID]


def get_new_dump(app_id, access_token, owner,
                 comments=False,
                 count=POSTS_COUNT):
    session = vk.Session(client_id=app_id, access_token=access_token)
    response = session.wall.get(owner_id=owner, count=count, extended=1)
    t = time.time()
    posts, profiles, groups = response_processing(response)

    if comments:
        for post in posts:
            vk.timeout(t)
            response = session.wall.getComments(owner_id=post['owner_id'],
                                                post_id=post['id'],
                                                count=100,
                                                sort='asc',
                                                preview_length=0,
                                                extended=1)
            t = time.time()
            post['comments'], profiles_, groups_= response_processing(response)

            add_new_extended_data(profiles, profiles_)
            add_new_extended_data(groups, groups_)

    dump = posts
    out = dump, {'profiles': profiles, 'groups': groups}
    return out


def get_last_dump(wall_path):
    names = [fn for fn in os.listdir(wall_path) if fn.endswith(EXTENSION)]
    last = max(names,
               key=lambda n: datetime.strptime(n.split(EXTENSION)[0],
                                               DATETIME_FORMAT))
    path = os.path.join(wall_path, last)
    with bz2.BZ2File(path) as file:
        data = file.read()
    datas = data.decode(encoding='utf-8')
    dump = json.loads(datas)
    return dump


def save_dump(dump, wall_path):
    now_str = datetime.now().strftime(DATETIME_FORMAT)
    path = os.path.join(wall_path, now_str+EXTENSION)
    datas = json.dumps(dump, ensure_ascii=False, indent='    ', sort_keys=True)
    data = datas.encode(encoding='utf-8')
    with bz2.BZ2File(path, 'w') as file:
        file.write(data)


def extract_ids(items):
    ids = set()
    for item in items:
        ids.add(item['id'])
    return ids


def compare_texts(old, new):
    if old == new:
        return None

    # does not detect changes in line endings
    old_words = old.split()
    new_words = new.split()
    differ = difflib.Differ()
    diff_words = tuple(differ.compare(old_words, new_words))

    for word in diff_words:
        if word.startswith('+ ') or word.startswith('- '):
            break
    else:
        return None

    diff_marked_text = ''
    for word in diff_words:
        if word.startswith('+ '):
            diff_marked_text += '<ADDED>' + word[2:] + '</ADDED>' + ' '
        elif word.startswith('- '):
            diff_marked_text += '<DELETED>' + word[2:] + '</DELETED>' + ' '
        elif word.startswith('  '):
            diff_marked_text += word[2:] + ' '
    return diff_marked_text


def compare_posts(old, new):
    if old == new:
        return None

    diff_marked_post = copy.deepcopy(old)

    # detect only changes in text
    text_diff = compare_texts(old['text'], new['text'])

    if not text_diff:
        return None

    diff_marked_post['text'] = text_diff

    return diff_marked_post


def get_post_by_id(posts, post_id):
    for post in posts:
        if post['id'] == post_id:
            return post
    return None


def compare_dumps(old, new):
    old_ids = extract_ids(old)
    new_ids = extract_ids(new)
    appeared_ids = new_ids - old_ids
    disappeared_ids = old_ids - new_ids
    still_ids = old_ids & new_ids
    oldest_old = min(old_ids)
    oldest_new = min(new_ids)

    new_posts = []
    for post in new:
        if post['id'] in appeared_ids and post['id'] > oldest_old:
            new_posts.append(post)
    new_posts = tuple(sorted(new_posts, key=lambda d: -int(d['id'])))

    deleted_posts = []
    for post in old:
        if post['id'] in disappeared_ids and post['id'] > oldest_new:
            deleted_posts.append(post)
    deleted_posts = tuple(sorted(deleted_posts, key=lambda d: -int(d['id'])))

    changed_posts = []
    for post in new:
        if post['id'] in still_ids:
            diff_post = compare_posts(get_post_by_id(old, post['id']), post)
            if diff_post:
                changed_posts.append(diff_post)
    changed_posts = tuple(sorted(changed_posts, key=lambda d: -int(d['id'])))

    new_comments = None
    deleted_comments = None
    changed_comments = None

    out = (new_posts, deleted_posts, changed_posts,
           new_comments, deleted_comments, changed_comments)
    if not (new_posts or deleted_posts or changed_posts or
            new_comments or deleted_comments or changed_comments):
        out = None
    return out


def build_part_subject(new, deleted, changed):
    part = ''
    if new or deleted or changed:
        changes = []
        if new:
            changes.append('создание')
        if deleted:
            changes.append('удаление')
        if changed:
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
        subject = '. '.join((p, c))
    else:
        subject = p + c

    return subject


def get_name_by_id(owner_id, extended):
    if owner_id > 0:
        profile = extended['profiles'][owner_id]
        name = profile['first_name'] + ' ' + profile['last_name']
    else:
        name = extended['groups'][abs(int(owner_id))]['name']
    return name


def get_link_by_id(owner_id, extended):
    if owner_id > 0:
        link = VK_URL+'/id{}'.format(owner_id)
    else:
        # to do detection 'public', 'event' and other group types
        link = VK_URL+'/club{}'.format(abs(int(owner_id)))
    return link


def build_attachment_html(attachment, template):
    # to do attachments processing
    if attachment['type'] == 'photo': pass
    elif attachment['type'] == 'audio': pass
    elif attachment['type'] == 'video': pass
    elif attachment['type'] == 'doc': pass
    elif attachment['type'] == 'link': pass
    elif attachment['type'] == 'poll': pass
    elif attachment['type'] == 'album': pass
    else: pass

    return '<p>'+attachment['type']+'</p>\n'


def build_text_html(text):
    text = text.replace('<DELETED>',
                        '<strike><span style="background-color:pink">')
    text = text.replace('</DELETED>', '</span></strike>')
    text = text.replace('<ADDED>',
                        '<span style="background-color:palegreen">')
    text = text.replace('</ADDED>', '</span>')

    html = '<p>' + text.replace('\n', '</p>\n<p>') + '</p>'
    return html


def build_post_html(post, template):
    # to do refactoring
    author_name = get_name_by_id(post['from_id'], extended)
    author_link = get_link_by_id(post['from_id'], extended)

    if 'signer_id' in post:
        signer_name = get_name_by_id(post['signer_id'], extended)
        signer_link = get_link_by_id(post['signer_id'], extended)
        sign = template['sign'].format(signer_name=signer_name,
                                       signer_link=signer_link)
    else:
        sign = ''

    attachment_html_list = []
    if 'attachments' in post:
        for attachment in post['attachments']:
            attachment_html = build_attachment_html(attachment, template)
            attachment_html_list.append(attachment_html)
    attachments_html = ''.join(attachment_html_list)

    dt = datetime.fromtimestamp(post['date']).strftime(DATETIME_FORMAT)

    link = 'https://vk.com/wall{owner}_{id}'.format(owner=post['owner_id'],
                                                    id=post['id'])

    post_html = template['post'].format(id=post['id'],
                                        link=link,
                                        text=build_text_html(post['text']),
                                        date=dt,
                                        author_link=author_link,
                                        author_name=author_name,
                                        sign=sign,
                                        attachments=attachments_html)
    return post_html


def build_section_html(name, posts, template):
    post_htmls = []
    for post in posts:
        post_htmls.append(build_post_html(post, template))
    posts_html = ''.join(post_htmls)
    section = template['section'].format(name=name, content=posts_html)
    return section


def build_html(target_id, extended, template,
               new_posts, deleted_posts, changed_posts,
               new_comments, deleted_comments, changed_comments,):
    sections = []
    if new_posts:
        sect = build_section_html('Новые посты', new_posts, template)
        sections.append(sect)
    if deleted_posts:
        sect = build_section_html('Удалённые посты', deleted_posts, template)
        sections.append(sect)
    if changed_posts:
        sect = build_section_html('Изменённые посты', changed_posts, template)
        sections.append(sect)
    html = template['message'].format(
                             target_link=get_link_by_id(target_id, extended),
                             target_name=get_name_by_id(target_id, extended),
                             sections=''.join(sections))
    return html
    # to do processing of rest diff parts


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


def process_dump_fetch_fail(exception, directory):
    # email with exception sends once
    description = str(exception)
    path = os.path.join(directory, 'exception.json')
    try:
        with open(path, encoding='utf-8') as file:
            descriptions = json.load(file)
    except IOError:
        file_exists = False
        new_exception = True
        descriptions = [description]
    else:
        file_exists = True
        if description in descriptions:
            new_exception = False
        else:
            new_exception = True
            descriptions.append(description)

    if not file_exists or new_exception:
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(descriptions, file,
                      ensure_ascii=False,
                      indent='    ',
                      sort_keys=True)

    if new_exception:
        msg = mail.make(args.from_email, args.to_email,
                        'Ошибка при получении нового дампа',
                        description)
        mail.send(msg)


def dump_fetch_ok(directory):
    path = os.path.join(directory, 'exception.json')
    try:
        os.remove(path)
    except:
        pass


if __name__ == '__main__':
    argparser = create_argparser()
    args = argparser.parse_args(sys.argv[1:])
    if args.group_id:
        owner = -abs(args.group_id)
    else:
        if args.user_id < 0:
            args.error('User ID must be positive')
        owner = args.user_id

    wall_path = os.path.join(WORKING_DIR, str(owner))
    try:
        new_dump, extended = get_new_dump(args.app_id, args.access_token,
                                          owner, args.comments)
    except Exception as e:
        process_dump_fetch_fail(e, wall_path)
    else:
        dump_fetch_ok(wall_path)
        try:
            last_dump = get_last_dump(wall_path)
        except OSError:
            # No dir — first run for this wall
            os.makedirs(wall_path, 0o700, True)
            save_dump(new_dump, wall_path)
            subject = 'Запуск отслеживания для стены '+str(owner)
            text = ('Для стены '+str(owner)+' запущено отслеживание.\n')
            msg = mail.make(args.from_email, args.to_email, subject, text)
            mail.send(msg)
        except ValueError:
            # No previous dumps — dumps was deleted
            save_dump(new_dump, wall_path)
            subject = 'Перезапуск отслеживания для стены '+str(owner)
            text = ('Для стены '+str(owner)+' перезапущено отслеживание.\n'
                    'Вероятно, предыдущие дампы для стены были удалены.')
            msg = mail.make(args.from_email, args.to_email, subject, text)
            mail.send(msg)
        else:
            diff = compare_dumps(last_dump, new_dump)

            if diff:
                save_dump(new_dump, wall_path)
                subject = build_subject(*diff)
                html = build_html(owner, extended, TEMPLATE, *diff)
                msg = mail.make(args.from_email, args.to_email,
                                subject, html=html)
                mail.send(msg)
