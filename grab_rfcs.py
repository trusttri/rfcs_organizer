import os
import sys
import json
import MySQLdb as mdb
from contextlib import closing
from import_data import get_article, get_wiki_talk_posts, count_replies
from warnings import filterwarnings
filterwarnings('ignore', category = mdb.Warning)


_DB_NAME = 'rfc_1'

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



if __name__ == "__main__":

    # file name.
    # In this case it would be "rfcs.json"
    file_name = os.path.dirname(os.path.realpath(__file__)) + '/' + sys.argv[1]
    with open(file_name) as file:
        rfcs = json.load(file)

    # password
    password = sys.argv[2]

    # make DB object
    # Fixed the name of the database to 'wikum'.
    rfc_DB = DB('localhost', 'root', password, _DB_NAME)


    for url in rfcs:
        print url
        source_id = None
        if 'wikipedia.org/wiki/' in url:
            source_id = rfc_DB.get_wiki_source_id() #sure to be
            result = get_article(url, source_id, rfc_DB)
            if result:
                article_id, disqus_id, section_index, title = result
                comment_num_cmd = "select count(*) from website_comment where article_id = %s"
                fetch_result = rfc_DB.fetch_one(comment_num_cmd, (article_id,))
                if fetch_result:
                    (comment_num, ) = fetch_result
                    total_count = 0
                    if comment_num == 0:
                        get_wiki_talk_posts(article_id, disqus_id, section_index, title, total_count, rfc_DB)

                        comment_num = rfc_DB.fetch_one(comment_num_cmd, (article_id,))

                        update_command = "update website_article set comment_num = %s where id = %s"
                        rfc_DB.update(update_command, (comment_num, article_id))

                        count_replies(disqus_id, rfc_DB)

                        print '-----------> ingested!'
                    else:
                        print '-----------> already in'
            else:
                print '[ WARNING: Corresponding RfC was not found in the DB and neither did the API retrieve information of url. ]'

    rfc_DB.close()