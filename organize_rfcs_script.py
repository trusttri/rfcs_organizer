import os
import sys
import json
import MySQLdb as mdb
from contextlib import closing
from warnings import filterwarnings
filterwarnings('ignore', category = mdb.Warning)

_OLD_DB = "wikum_neural"
_NEW_DB = "wikum_neural_unique"


class DB():
    def __init__(self, host, user, passwd, db):
        try:
            self.conn = mdb.connect(host= host, user=user, passwd = passwd, db=db)
            self.anonymous_id = self.store_anonymous_id()
            self.wiki_source_id = self.store_wiki_source_id()

            self.conn.set_character_set('utf8')

        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit(1)

    def store_anonymous_id(self):
        command = "select id from website_commentauthor where disqus_id= %s and is_wikipedia = %s"
        comment_author_id = self.fetch_one(command, ('anonymous', 1))
        return comment_author_id

    def get_anonymous_id(self):
        return self.anonymous_id

    def store_wiki_source_id(self):
        cmd = 'select id from website_source where source_name = %s'
        (source_id,) = self.fetch_one(cmd, ("Wikipedia Talk Page",))
        return source_id

    def get_wiki_source_id(self):
        return self.wiki_source_id

    def fetch_one(self, cmd, params):
        with closing(self.conn.cursor()) as cur:
            cur.execute('SET NAMES utf8;')
            cur.execute('SET CHARACTER SET utf8;')
            cur.execute('SET character_set_connection=utf8;')
            cur.execute(cmd, params)
            result = cur.fetchone()
            if result:
                return result
            return None

    def fetch_all(self, cmd, params):
        with closing(self.conn.cursor()) as cur:
            cur.execute('SET NAMES utf8;')
            cur.execute('SET CHARACTER SET utf8;')
            cur.execute('SET character_set_connection=utf8;')
            cur.execute(cmd, params)
            rows = cur.fetchall()
            return rows

    def insert(self, cmd, params):
        with closing(self.conn.cursor()) as cur:
            cur.execute('SET NAMES utf8;')
            cur.execute('SET CHARACTER SET utf8;')
            cur.execute('SET character_set_connection=utf8;')
            cur.execute(cmd, params)
            self.conn.commit()
            return cur.lastrowid

    def update(self, command, params):
        with closing(self.conn.cursor()) as cur:
            cur.execute('SET NAMES utf8;')
            cur.execute('SET CHARACTER SET utf8;')
            cur.execute('SET character_set_connection=utf8;')
            cur.execute(command, params)
            self.conn.commit()
            return cur.lastrowid


    def close(self):
        self.conn.close()



def store_authors(old_DB, new_DB):
    authors_cmd = "select username, disqus_id, joined_at, edit_count, gender, groups, is_wikipedia from website_commentauthor"
    authors_result = old_DB.fetch_all(authors_cmd, ())
    for row in authors_result:
        (username, disqus_id, joined_at, edit_count, gender, groups, is_wikipedia) = row

        #no need to exclude disqus_id.
        exist_check_cmd = "select count(*) from website_commentauthor where username=%s"
        (author_count, )= new_DB.fetch_one(exist_check_cmd, (username, ))

        if author_count == 0:
            insert_cmd =  " insert into website_commentauthor (username, disqus_id, joined_at, edit_count, gender, groups, is_wikipedia)\
                            values (%s, %s, %s, %s, %s, %s, %s)"
            author_id = new_DB.insert(insert_cmd, (username, disqus_id, joined_at, edit_count, gender, groups, is_wikipedia))

        else:
            print 'already exists'



def get_username(author_id, db):
    command = "select username from website_commentauthor where id = %s"
    author = db.fetch_one(command, (author_id,))
    if author:
        (username, ) = author
        return username
    return None

def get_user_id(username, db):
    command = "select id from website_commentauthor where username = %s"
    author = db.fetch_one(command, (username,))
    if author:
        (author_id, ) = author
        return author_id
    return None

def get_article_id(url, db):
    article_id_cmd = "select id from website_article where url = %s"
    res = db.fetch_one(article_id_cmd, (url,))
    if res:
        (article_id,) = res
        return article_id
    return None

def store_comments(url, old_DB, new_DB):
    comment_dict = {}
    new_article_id = get_article_id(url, new_DB)
    ### first check from new db(where we are going to insert) ###
    if new_article_id:
        comment_num_cmd = "select count(*) from website_comment where article_id = %s"
        comment_num_result = new_DB.fetch_one(comment_num_cmd, (new_article_id,))
        (comment_num,) = comment_num_result

        #needed when have to run the script all over again
        if comment_num == 0:
            ### first check from old db ###
            # now get all comments from old DB. also need id for cosigners
            old_article_id = get_article_id(url,old_DB)

            comment_cmd = "select id, author_id, text, reply_to_disqus, text_len, created_at from website_comment where article_id = %s"
            # need to set the disqus_id later on
            old_comments_result = old_DB.fetch_all(comment_cmd, (old_article_id,))
            if old_comments_result:
                # store for sorting
                comment_results = [(old_comment_id, old_author_id, text, reply_to_disqus, text_len, created_at) for (old_comment_id, old_author_id, text, reply_to_disqus, text_len, created_at) in old_comments_result]

                sorted_comments = sorted(comment_results, key=lambda x: x[0])

                for row in sorted_comments:
                    (old_comment_id, old_author_id, text, reply_to_disqus, text_len, created_at) = row

                    #get username from old database using old id
                    username = get_username(old_author_id, old_DB)

                    # get the new author id from new database
                    new_author_id = get_user_id(username, new_DB)

                    #check if comment already exists in new db
                    command = "select id from website_comment where text = %s and article_id= %s and author_id = %s"
                    comment_result = new_DB.fetch_one(command, (text, new_article_id, new_author_id))

                    #if it doesn't exist. if it's none
                    # if comment_result is None:
                    if True:
                        # update reply_to
                        if reply_to_disqus is not None:
                            try:
                                new_reply_to_disqus = comment_dict[long(reply_to_disqus)]
                            except:
                                print "old comment id is " + str(old_comment_id) + ", old reply_to is "
                                print 'strangely the older comment is not stored of ' + str(reply_to_disqus)
                                raise Exception

                        insert_command = "insert into website_comment (article_id, author_id, text, reply_to_disqus, text_len, created_at)\
                                                        values (%s, %s, %s, %s, %s, %s)"

                        if reply_to_disqus:
                            new_comment_id = new_DB.insert(insert_command, (new_article_id, new_author_id, text, new_reply_to_disqus, text_len, created_at))

                        else:
                            new_comment_id = new_DB.insert(insert_command, (new_article_id, new_author_id, text, reply_to_disqus, text_len, created_at))


                        # update the comment's disqus_id
                        disqus_update_command = "update website_comment set disqus_id = %s where id = %s"
                        new_DB.update(disqus_update_command, (new_comment_id, new_comment_id))

                        comment_dict[old_comment_id] = new_comment_id
                        # print comment_dict[old_comment_id]
                        #store cosign
                        store_cosigns(old_comment_id, new_comment_id, old_DB, new_DB)

            # no url in old db
            # else:
            #     print 'no url in old db'
            #     raise Exception

    #no url in new db
    else:
        print 'no url in new db'
        raise Exception



# function for moving author information from old database to a new database
def move_author_info(username, old_DB, new_DB):

    exist_check_cmd = "select count(*) from website_commentauthor where username=%s"
    (author_count,) = new_DB.fetch_one(exist_check_cmd, (username,))

    if author_count == 0:
        authors_cmd = "select username, disqus_id, joined_at, edit_count, gender, groups, is_wikipedia from website_commentauthor where username = %s"
        author_result = old_DB.fetch_one(authors_cmd, (username,))
        (username, disqus_id, joined_at, edit_count, gender, groups, is_wikipedia) = author_result
        insert_cmd = " insert into website_commentauthor (username, disqus_id, joined_at, edit_count, gender, groups, is_wikipedia)\
                        values (%s, %s, %s, %s, %s, %s, %s)"
        author_id = new_DB.insert(insert_cmd,
                                  (username, disqus_id, joined_at, edit_count, gender, groups, is_wikipedia))

        return author_id
    else:
        print 'already exists'
        raise Exception


def store_cosigns(old_comment_id, new_comment_id, old_DB, new_DB):
    #using the old_comment_id get cosginer information from old_DB
    cosign_cmd = "select commentauthor_id from website_comment_cosigners where comment_id = %s"
    cosign_result = old_DB.fetch_all(cosign_cmd, (old_comment_id,))
    if cosign_result:
        # get username from old db using old id from db
        for row in cosign_result:
            (old_commentauthor_id, ) = row
            username = get_username(old_commentauthor_id, old_DB)

            # if username, that is the new author, is already in new_DB, just store cosign
            # else, insert the cosigner and retrieve new id
            new_author_id = get_user_id(username, new_DB)
            if new_author_id is None:
                # insert first
                new_author_id = move_author_info(username, old_DB, new_DB)

            insert_command = " insert into website_comment_cosigners (comment_id, commentauthor_id)\
                                            values (%s, %s)"
            new_cosign_id = new_DB.insert(insert_command, (new_comment_id, new_author_id))






if __name__ == "__main__":
    # password
    password = sys.argv[1]
    old_db_name = sys.argv[2]
    new_db_name = sys.argv[3]

    print password

    # make DB object
    # Fixed the name of the database to 'wikum'.
    old_DB = DB('localhost', 'root', password, old_db_name)
    new_DB = DB('localhost', 'root', password, new_db_name)

    existing_urls = old_DB.fetch_all("select url, title, source_id, disqus_id, section_index from website_article", ())
    url_set = set()
    for row in existing_urls:
        (url, title, source_id, disqus_id, section_index) = row
        url_set.add(url)
        #check if it already exits
        check_url  = new_DB.fetch_one("select url from website_article where url = %s", (url,))
        if check_url is None:
            article_insert_command = " insert into website_article (disqus_id, title, url, source_id, section_index)\
                                        values (%s, %s, %s, %s, %s)"
            article_id = new_DB.insert(article_insert_command, (disqus_id, title, url, source_id, section_index))

        # store comments
        print 'url: ' + url
        store_comments(url, old_DB, new_DB)



    old_DB.close()
    new_DB.close()




        #
    # existing_urls = old_DB.fetch_all("select url, title, source_id, disqus_id, section_index from website_article", ())
    # url_set = set()
    #

    #
    #
    # #now store authors
    # store_authors(old_DB, new_DB)
    #
    # #now store comments
    # store_comments(article_id, old_DB, new_DB)
    #
    #
    # print "NUMBER OF RFCS :  "  + str(len(url_set))
    # old_DB.close()
    # new_DB.close()