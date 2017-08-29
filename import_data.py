import MySQLdb
import urllib2
import json
import datetime
import re


_CLOSE_COMMENT_KEYWORDS = [r'{{(atop|quote box|consensus|Archive(-?)( ?)top|Discussion( ?)top|(closed.*?)?rfc top)',
                           r'\|result=', r"={2,3}( )?Clos(e|ing)( comment(s?)|( RFC)?)( )?={2,3}",
                           'The following discussion is an archived discussion of the proposal',
                           'A summary of the debate may be found at the bottom of the discussion',
                           'A summary of the conclusions reached follows']
_CLOSE_COMMENT_RE = re.compile(r'|'.join(_CLOSE_COMMENT_KEYWORDS), re.IGNORECASE | re.DOTALL)

_DOMAIN = "https://en.wikipedia.org"

def get_article(url, source_id, rfc_DB):
    cmd = 'select id, disqus_id, section_index, title from website_article where url = %s'
    article_result = rfc_DB.fetch_one(cmd, (urllib2.unquote(url),))

    if article_result is not None:
        article_id, disqus_id, section_index, title = article_result
        return article_id, disqus_id, section_index, title
    else:
        if 'wikipedia.org/wiki/' in url:
            url_parts = url.split('/wiki/')
            wiki_sub = url_parts[1].split(':')
            wiki_parts = ':'.join(wiki_sub[1:]).split('#')
            wiki_page = wiki_parts[0]
            section = None
            if len(wiki_parts) > 1:
                section = wiki_parts[1]

            from wikitools import wiki, api
            site = wiki.Wiki(_DOMAIN + '/w/api.php')
            page = urllib2.unquote(str(wiki_sub[0]) + ':' + wiki_page.encode('ascii', 'ignore'))
            params = {'action': 'parse', 'prop': 'sections', 'page': page, 'redirects': 'yes'}
            from wikitools import wiki, api
            try:
                request = api.APIRequest(site, params)

                result = request.query()

                disqus_id = str(result['parse']['pageid'])
                section_title = None
                section_index = None

                if section:
                    for s in result['parse']['sections']:
                        if s['anchor'] == section:
                            disqus_id = str(disqus_id) + '#' + str(s['index'])
                            section_title = s['line']
                            section_index = s['index']
                title = result['parse']['title']
                if section_title:
                    title = title + ' - ' + section_title

                link = urllib2.unquote(url)
                article_insert_command = " insert into website_article (disqus_id, title, url, source_id, section_index)\
                                            values (%s, %s, %s, %s, %s)"

                article_id = rfc_DB.insert(article_insert_command, (disqus_id, title, link, source_id, section_index))
                return article_id, disqus_id, section_index, title

            except api.APIError as e:
                print e







def get_wiki_talk_posts(article_id, disqus_id, section_index, original_title, total_count, rfc_DB):
    def get_section(sections, section_title):
        for s in sections:
            heading_title = s.get('heading', '')
            heading_title = re.sub(r'\]', '', heading_title)
            heading_title = re.sub(r'\[', '', heading_title)
            heading_title = re.sub('<[^<]+?>', '', heading_title)
            if heading_title.strip() == str(section_title).strip():
                return s

    def find_outer_section(title, text, id):
        # Check if closing comment is in here, if not look for the outer section.
        # If there is an outer section, choose it only if it has a closing statement,
        if len(title) > 1:
            section_title = title[1].encode('ascii', 'ignore')
            params = {'action': 'query', 'titles': title[0], 'prop': 'revisions', 'rvprop': 'content', 'format': 'json',
                      'redirects': 'yes'}
            result = api.APIRequest(site, params).query()
            whole_text = _clean_wiki_text(result['query']['pages'][id]['revisions'][0]['*'])

            import wikichatter as wc
            parsed_whole_text = wc.parse(whole_text.encode('ascii', 'ignore'))
            sections = parsed_whole_text['sections']

            for outer_section in sections:
                found_subection = get_section(outer_section['subsections'], section_title)
                if found_subection:
                    outer_comments = outer_section['comments']
                    for comment in outer_comments:
                        comment_text = '\n'.join(comment['text_blocks'])
                        if re.search(_CLOSE_COMMENT_RE, comment_text):
                            params = {'action': 'parse', 'prop': 'sections', 'page': title[0], 'redirects': 'yes'}
                            result = api.APIRequest(site, params).query()
                            for s in result['parse']['sections']:
                                if s['line'] == outer_section.get('heading').strip():
                                    section_index = s['index']
                                    params = {'action': 'query', 'titles': title[0], 'prop': 'revisions',
                                              'rvprop': 'content', 'rvsection': section_index, 'format': 'json',
                                              'redirects': 'yes'}
                                    result = api.APIRequest(site, params).query()
                                    final_section_text = result['query']['pages'][id]['revisions'][0]['*']
                                    return final_section_text
        return text

    from wikitools import wiki, api
    site = wiki.Wiki(_DOMAIN + '/w/api.php')
    title = original_title.split(' - ')

    params = {'action': 'query', 'titles': title[0], 'prop': 'revisions', 'rvprop': 'content', 'format': 'json',
              'redirects': 'yes'}
    if section_index:
        params['rvsection'] = section_index

    request = api.APIRequest(site, params)
    result = request.query()
    page_id = disqus_id.split('#')[0]

    if page_id in result['query']['pages']:
        text = result['query']['pages'][page_id]['revisions'][0]['*']

        # If there isn't a closing statement, it means that the RfC could exist as a subsection of another section, with the closing statement in the parent section.
        # Example: https://en.wikipedia.org/wiki/Talk:Alexz_Johnson#Lead_image
        if not re.search(_CLOSE_COMMENT_RE, text):
            text = find_outer_section(title, text, page_id)

        text = _clean_wiki_text(text)

        import wikichatter as wc
        parsed_text = wc.parse(text.encode('ascii', 'ignore'))

        start_sections = parsed_text['sections']
        if len(title) > 1:
            section_title = title[1].encode('ascii', 'ignore')
            sections = parsed_text['sections']
            found_section = get_section(sections, section_title)
            if found_section:
                start_sections = found_section['subsections']
                start_comments = found_section['comments']
                total_count = import_wiki_talk_posts(start_comments, article_id, None, total_count, rfc_DB)

        total_count = import_wiki_sessions(start_sections, article_id, None, total_count, rfc_DB)


def import_wiki_sessions(sections, article_id, reply_to, total_count, rfc_DB):
    for section in sections:
        disqus_id = reply_to
        if len(section['comments']) > 0:
            total_count = import_wiki_talk_posts(section['comments'], article_id, disqus_id, total_count, rfc_DB)
        if len(section['subsections']) > 0:
            total_count = import_wiki_sessions(section['subsections'], article_id, disqus_id, total_count, rfc_DB)
    return total_count



def import_wiki_talk_posts(comments, article_id, reply_to, total_count, rfc_DB):
    for comment in comments:
        text = '\n'.join(comment['text_blocks'])
        # text = "hi"
        author = comment.get('author')

        if author:
            comment_author_id = import_wiki_authors([author], rfc_DB)[0]
        else:
            comment_author_id = rfc_DB.get_anonymous_id()

        command = "select id from website_comment where text = %s  and article_id= %s and author_id = %s"
        comment_result = rfc_DB.fetch_one(command, (text, article_id, comment_author_id))

        if comment_result is not None:
            (comment_id, )  = comment_result
        else:
            # time = None

            timestamp = comment.get('time_stamp')

            cosigners = [sign['author'] for sign in comment['cosigners']]
            comment_cosigners = import_wiki_authors(cosigners, rfc_DB)

            insert_command = " insert into website_comment (article_id, author_id, text, reply_to_disqus, text_len)\
                                values (%s, %s, %s, %s, %s)"

            comment_id = rfc_DB.insert(insert_command, (article_id, comment_author_id, text, reply_to, len(text)))

            if timestamp:
                update_command = "update website_comment set created_at = %s  where id = %s"
                rfc_DB.update(update_command, (timestamp, comment_id))

            #for comments the id is equal to disqus_id
            disqus_update_command = "update website_comment set disqus_id = %s where id = %s"
            rfc_DB.update(disqus_update_command, (comment_id, comment_id))

            for signer_id in comment_cosigners:
                insert_command = " insert into website_comment_cosigners (comment_id, commentauthor_id)\
                                    values (%s, %s)"

                rfc_DB.insert(insert_command, (comment_id, signer_id))

        total_count += 1

        replies = comment['comments']
        total_count = import_wiki_talk_posts(replies, article_id, comment_id, total_count, rfc_DB)

    return total_count

def import_wiki_authors(authors, rfc_DB):
    found_authors = set()
    anonymous_exist = False
    for author in authors:
        if author:
            found_authors.add(author)
        else:
            anonymous_exist = True
    authors_list = '|'.join(found_authors)

    from wikitools import wiki, api
    site = wiki.Wiki(_DOMAIN + '/w/api.php')
    params = {'action': 'query', 'list': 'users', 'ususers': authors_list,
              'usprop': 'blockinfo|groups|editcount|registration|emailable|gender', 'format': 'json'}

    request = api.APIRequest(site, params)
    result = request.query()
    comment_authors = []
    for user in result['query']['users']:
        try:
            author_id = user['userid']
            command= "select id from website_commentauthor where disqus_id = %s"
            (comment_author_id, )= rfc_DB.fetch_one(command, (author_id,))
            if comment_author_id is None:
                author_insert_command = " insert into website_commentauthor (username, disqus_id, joined_at, edit_count, gender, groups, is_wikipedia)\
                        values (%s, %s, %s, %s, %s, %s, %s)"

                params = (user['name'], author_id, user['registration'], user['editcount'], user['gender'], ','.join(user['groups']), 1)
                comment_author_id = rfc_DB.insert(author_insert_command, params)


        except Exception:
            command = " insert into website_commentauthor (username, is_wikipedia)\
                        values (%s, %s)"

            comment_author_id = rfc_DB.insert(command, (user['name'], 1))

        if comment_author_id:
            comment_authors.append(comment_author_id)

    if anonymous_exist:
        anonymous_id = rfc_DB.get_anonymous_id()
        comment_authors.append(anonymous_id)

    return comment_authors



def count_replies(article_id, rfc_DB):
    command = "select id, disqus_id from website_comment where article_id = %s"
    comments = rfc_DB.fetch_all(command, (article_id,))

    for comment in comments:
        if comment is not None:
            (id, disqus_id) = comment
            command = "select count(*) from website_comment where reply_to_disqus = %s and article_id = %s"
            (num_replies, )= rfc_DB.fetch_one(command, (disqus_id, article_id))

            update_command = "update website_comment set num_replies = %s  where id= %s"
            rfc_DB.update(update_command, (num_replies, id))



def _correct_signature_before_parse(text):
    _user_re = "(\(?\[\[\W*user\W*:(.*?)\|[^\]]+\]\]\)?)"
    _user_talk_re = "(\(?\[\[\W*user[_ ]talk\W*:(.*?)\|[^\]]+\]\]\)?)"
    _user_contribs_re = "(\(?\[\[\W*Special:Contributions/(.*?)\|[^\]]+\]\]\)?)"

    # different format from the ones in signatureutils.py. need to divide (UTC) from time
    # 01:52, 20 September 2013
    _timestamp_re_0 = r"[0-9]{2}:[0-9]{2},? [0-9]{1,2} [^\W\d]+ [0-9]{4}"
    # 18:45 Mar 10, 2003
    _timestamp_re_1 = r"[0-9]{2}:[0-9]{2},? [^\W\d]+ [0-9]{1,2},? [0-9]{4}"
    # 01:54:53, 2005-09-08
    _timestamp_re_2 = r"[0-9]{2}:[0-9]{2}:[0-9]{2},? [0-9]{4}-[0-9]{2}-[0-9]{2}"
    _timestamps = [_timestamp_re_0, _timestamp_re_1, _timestamp_re_2]

    # case 1
    # example url: https://en.wikipedia.org/wiki/Talk:Race_and_genetics#RFC
    text = text.replace("(UTC\n", "(UTC)\n")

    # case 2: get rid of user name's italics
    # especially needed when the signature doesn't have timestamp: https://en.wikipedia.org/wiki/Wikipedia_talk:What_Wikipedia_is_not/Archive_49#RfC:_amendment_to_WP:NOTREPOSITORY
    italics_user_re = re.compile(
        r"'+<.*?>(?P<user>(" + '|'.join([_user_re, _user_talk_re, _user_contribs_re]) + "))<.*?>'+", re.I)
    text = re.sub(italics_user_re, '\g<user>', text)

    # case 3: when there are space(s) or new line(s) between user name and time or time and (UTC)
    wrong_sig_re = re.compile(
        r"((\n)*(?P<user>(" + '|'.join([_user_re, _user_talk_re, _user_contribs_re]) + "))( |\n|<.*?>)*"
                                                                                       "(?P<time>(" + r'|'.join(
            _timestamps) + "))( |\n)*(\(UTC\))?)", re.I)
    text = re.sub(wrong_sig_re, '\g<user> \g<time> (UTC)', text)

    # case 4
    text = text.replace("(UTC)}}", "(UTC)}}\n")
    return text


def _clean_wiki_text(text):
    # case 1: correct wrong signature formats
    text = _correct_signature_before_parse(text)

    # case 2 : when ":" and "*" are mixed together, such as in "\n:*::"
    # example: https://en.wikipedia.org/w/api.php?action=query&titles=Talk:God_the_Son&prop=revisions&rvprop=content&format=json
    mixed_indent_re = "(?P<before>\n:)\*(?P<after>:+)"
    text = re.sub(mixed_indent_re, "\g<before>\g<after>", text)

    # case 3
    start = re.compile('<(div|small).*?>', re.DOTALL)
    end = re.compile('</(div|small).*?>', re.DOTALL)
    template = re.compile('<!--.*?-->', re.DOTALL)
    for target in [start, end, template]:
        text = re.sub(target, '', text)

    # case 4: "&nbsp;" breaks parsing
    # example: <span style=\"border:1px solid #329691;background:#228B22;\">'''[[User:Viridiscalculus|<font color=\"#FFCD00\">&nbsp;V</font>]][[User talk:Viridiscalculus|<font style=\"color:#FFCD00\">C&nbsp;</font>]]'''</span> 01:25, 4 January 2012 (UTC)
    text = text.replace("&nbsp;", " ")

    # case 5
    unicode_re = re.compile('\\\\u[0-9a-z]{4}', re.UNICODE | re.IGNORECASE)
    text = re.sub(unicode_re, '', text)

    # case 6: Editors tend to put ':' infront of {{outdent}} for visualization but this breaks parsing properly.
    # example url : https://en.wikipedia.org/wiki/Talk:No%C3%ABl_Coward/Archive_2#RfC:_Should_an_Infobox_be_added_to_the_page.3F
    wrong_outdent_temp = re.compile(":+( )*{{(outdent|od|unindent).*?}}", re.I)
    text = re.sub(wrong_outdent_temp, "{{outdent}}\n", text)
    return text.strip()



