# Completed Framework w/ stats
# Python 2.7.8 w/ tweepy
# 10/7/14
from __future__ import division
from bisect import bisect_left
from compiler.ast import flatten
from datetime import datetime
from collections import Counter
from json import loads
from string import punctuation, maketrans
from timeit import default_timer
from random import choice
import tweepy
import atexit
import sqlite3
consumer_token = "XXX"
consumer_secret = "XXX"
access_key = "XXX"
access_secret = "XXX"
auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)
utf_map = {
    u"\u2019": u"'",
    u"\u2018": u"'",
    u"\u201c": u'"',
    u"\u201d": u'"',
    u"\u2026": u'.',
    u"\u2013": u'-',
    u"\u2014": u'-',
}
utf_map = dict([(ord(k), ord(v)) for k, v in utf_map.items()])


def create_resource(path):
    word_file = open('resources/%s.txt' % path)
    return [line.strip() for line in word_file]
#english = create_resource("engwords")
english = create_resource("modern_words")
names = create_resource("NAMES")
places = create_resource("PLACES")
exceptions = create_resource("exceptions")
novels = [line.strip().split('|') for line in open('resources/novel.txt')]


def bi_contains(lst, item):
    return (item <= lst[-1]) and (lst[bisect_left(lst, item)] == item)


def clean_up():
    global conn
    conn.close()
atexit.register(clean_up)


def purify_word(word):
    tab = maketrans("", "")
    return word.translate(utf_map).encode('ascii', 'ignore').translate(tab, punctuation)


def is_palindrone(string):
    string = purify_word(string).replace(" ", "").lower()
    leng = len(string)
    if leng < 2:
        return False
    if string[0] == string[-1]:
        if not leng % 2:
            if string[leng // 2:] == string[:leng // 2][::-1]:
                return True
        else:
            string = string[:leng // 2] + string[leng // 2 + 1:]
            if string[leng // 2:] == string[:leng // 2][::-1]:
                return True
    return False


def classify_word(word, hashs=[], ments=[]):
    pure = purify_word(word)
    numbs = ['am', 'pm', 'th', 'nd', 'st']
    if pure == '':
        return 'emoticon'
    if pure[:4] == 'http':
        return 'link'
    if word[0] == '@':
        return 'mention'
        non_ment = word[1:]
        if non_ment.isdigit():
            return 'number'
        if non_ment in ments:
            return 'mention'
        return 'badment'
    if word[0] == '#':
        non_hash = word[1:]
        if non_hash.isdigit():
            return 'number'
        if non_hash in hashs:
            return 'hashtag'
        return 'badhash'
    if word[0] == '$':
        return 'number'
    if pure[0].isdigit() and pure[-2:] in numbs:
        return 'number'
    if '-' in word:
        return [classify_word(sep) for sep in word.split('-')]
    if '/' in word:
        return [classify_word(sep) for sep in word.split('/')]
    if pure.isdigit():
        return 'number'
    if bi_contains(english, pure.lower()):
        return 'english'
    if bi_contains(exceptions, pure.lower()):
        return 'exception'
    if bi_contains(names, pure.lower().capitalize()):
        return 'name'
    if bi_contains(places, pure.lower().capitalize()):
        return 'place'
    if pure[0].isupper() and not bi_contains(english, pure.lower()):
        return 'proper'
    return 'not_english'


def time_format(tstring):
    m = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
         'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    d = tstring.split()
    t = d[3].split(':')
    return datetime(month=m.index(d[1]) + 1, year=int(d[-1]), day=int(d[2]),
                    hour=int(t[0]), minute=int(t[1]))


def tweet_stats(text, hashs=[], ments=[]):
    cats = ['types', 'proper_nouns', 'not_english']
    stats = Counter({k: Counter() for k in cats})
    totals = Counter({
        'words': 0,
        'characters': 0,
    })
    totals['characters'] = len(purify_word(text).replace(" ", ""))
    totals['words'] = len(text.split())
    for word in text.split():
        word_class = classify_word(word, hashs, ments)
        if word_class == 'proper':
            stats['proper_nouns'][purify_word(word)] += 1
        elif word_class == 'not_english':
            stats['not_english'][purify_word(word)] += 1
        elif not isinstance(word_class, list):
            stats['types'][word_class] += 1
        else:
            for thing in flatten(word_class):
                if thing == 'proper':
                    stats['proper_nouns'][purify_word(word)] += 1
                if thing == 'not_english':
                    stats['not_english'][purify_word(word)] += 1
                else:
                    stats['types'][thing] += 1
    return stats, totals


def preferences(text):
    p = [1500, False, False, False]
    for w in text.split():
        if w.isdigit():
            p[0] = int(w)
        if w in ['rt', 'retweet', 'retweets']:
            p[1] = True
        if w in ['following', 'follow', 'book', 'books']:
            p[2] = True
        if w in ['marks', 'markov', 'markovs', 'sentence']:
            p[3] = True
    return p


class tweeter():

    def __init__(self, raw, sample=1500, inc_rts=False, inc_fol=False, fol_sample=50):
        self.time = datetime.now()
        self.start = default_timer()
        self.profile = raw['screen_name'].encode()
        self.sample_size = sample
        self.inc_rts = int(inc_rts)
        self.name = raw['name'].encode()
        #self.lang = raw['lang']
        self.tweet_count = raw['statuses_count']
        if self.tweet_count < self.sample_size:
            self.sample_size = self.tweet_count
        self.scaling_mult = self.tweet_count / self.sample_size
        self.followers = raw['followers_count']
        self.following = raw['friends_count']
        self.verified = int(raw['verified'])
        self.creation = raw['created_at']
        self.fol_sample = fol_sample
        self.log_info()
        self.sums = {k: sum(self.tweets[k].itervalues()) for k in self.tweets}
        self.sums.update(self.totals)
        self.tweets = {k: self.tweets[k].most_common(5) for k in self.tweets}
        self.maxes = {k: self.sums[k] * self.scaling_mult for k in self.sums}
        self.stats = {
            'tpd': self.tweet_count / (datetime.now() - time_format(self.creation)).days,
            'cpt': self.sums['characters'] / self.sample_size,
            'wpt': self.sums['words'] / self.sample_size,
            'hpt': self.sums['hashtags'] / self.sample_size,
            'nept': self.sums['not_english'] / self.sample_size,
        }
        self.runtime = default_timer() - self.start
        if inc_fol:
            self.friend_est = self.following_sample(self.fol_sample)
        del raw

    def log_info(self):
        cats = ['types', 'proper_nouns', 'not_english',
                'mentions', 'hashtags', 'palindrones']
        self.tweets = Counter({k: Counter() for k in cats})
        self.totals = Counter({
            'words': 0,
            'characters': 0,
        })
        print 'STARTING INFO FOR ', self.profile
        for status in tweepy.Cursor(api.user_timeline, id=self.profile, include_rts=self.inc_rts).items(self.sample_size):
            hashtags = [hash[u'text']
                        for hash in status._json[u'entities'][u'hashtags']]
            self.tweets['hashtags'] += Counter(hashtags)
            mentions = [ment[u'screen_name']
                        for ment in status._json[u'entities'][u'user_mentions']]
            self.tweets['mentions'] += Counter(mentions)
            if is_palindrone(status.text):
                self.tweets['palindrones'][status.text] += 1
            temp_stats, temp_totals = tweet_stats(
                status.text, hashtags, mentions)
            self.totals += temp_totals
            self.tweets = temp_stats + self.tweets

    def following_sample(self, s=100):
        scale = sum(
            [api.get_user(id).statuses_count for id in api.friends_ids(self.profile)[:s]])
        # AVG WORDS PER TWEET
        return (scale * (self.following / s)) * self.stats['wpt']

    def sql(self):
        return [(self.profile, self.name, self.verified, self.inc_rts, self.sample_size,
                 self.tweet_count, self.followers, self.following, str(self.time), self.runtime),
                flatten((self.profile, tuple(
                    (self.sums[k] for k in self.sums)))),
                flatten((self.profile, tuple(
                    (self.stats[k] for k in self.stats)))),
                [(self.profile, k, i[0], i[1]) for k, v in self.tweets.items() for i in v[:5]]]

    def novel(self):
        c = choice(novels)
        return (c[0], c[1], self.friend_est / int(c[2]))

    def tweet_format(self):
        s = '''@{} Tweets/Day: {:.3f}, Words/Tweet: {:.3f}, Chars/Tweet: {:.3f}, Hash/Tweet: {:.3f},
Most Used: #{} #{} | @{} @{} | {} {}'''.format(self.profile, self.stats['tpd'],
                                               self.stats['wpt'], self.stats['cpt'], self.stats[
                                                   'hpt'], self.tweets['hashtags'][0][0],
                                               self.tweets['hashtags'][1][0], self.tweets[
                                                   'mentions'][0][0], self.tweets['mentions'][1][0],
                                               self.tweets['not_english'][0][0], self.tweets['not_english'][1][0])
        if len(s) > 140:
            s = '''@{} T/D: {:.3f}, W/T: {:.3f}, C/T: {:.3f}, H/T: {:.3f},
Commons: #{} #{} | @{} @{} | {} {}'''.format(self.profile, self.stats['tpd'],
                                             self.stats['wpt'], self.stats['cpt'], self.stats[
                                                 'hpt'], self.tweets['hashtags'][0][0],
                                             self.tweets['hashtags'][1][0], self.tweets[
                                                 'mentions'][0][0], self.tweets['mentions'][1][0],
                                             self.tweets['not_english'][0][0], self.tweets['not_english'][1][0])
        if len(s) > 140:
            s = '''@{} T/D: {:.3f}, W/T: {:.3f}, H/T: {:.3f},
Commons: #{} #{} | @{} @{}'''.format(self.profile, self.stats['tpd'],
                                     self.stats['wpt'], self.stats['hpt'], self.tweets[
                                         'hashtags'][0][0], self.tweets['hashtags'][1][0],
                                     self.tweets['mentions'][0][0], self.tweets['mentions'][1][0])
        return s.replace('\n', ' ')


class StdOutListener(tweepy.streaming.StreamListener):

    def on_data(self, data):
        # print data
        d = loads(data)
        c.execute("SELECT COUNT(screen) from user WHERE screen='%s'" %
                  d['user']['screen_name'])
        if c.fetchone()[0]:
            print 'ALREADY COMPLETED'
            try:
                raise Exception('OMG I ALREADY DID YOU')
            except Exception:
                self.on_exception(Exception, d['user']['screen_name'])
                return True
        if d['user']['lang'] != 'en':
            print 'NOT ENGLISH'
            try:
                raise Exception('OMG YOUR NOT ENGLISH')
            except Exception:
                self.on_exception(Exception, d['user']['screen_name'])
                return True
        p = preferences(d['text'])
        u = tweeter(d['user'], p[0], p[1], p[2])
        api.update_status(u.tweet_format())
        c.execute(
            "INSERT OR IGNORE INTO user VALUES (?,?,?,?,?,?,?,?,?,?)", u.sql()[0])
        c.execute(
            "INSERT OR IGNORE INTO sums VALUES (?,?,?,?,?,?,?,?,?)", u.sql()[1])
        c.execute(
            "INSERT OR IGNORE INTO stats VALUES (?,?,?,?,?,?)", u.sql()[2])
        c.executemany(
            "INSERT OR IGNORE INTO common VALUES (?,?,?,?)", u.sql()[3])
        # if u.inc_fol:
        #	c.execute("INSERT OR IGNORE INTO readings VALUES (?,?,?,?)", repr(u)[4])
        conn.commit()
        print 'COMPLETED FOR', d['user']['screen_name']
        return True

    def on_error(self, error):
        print 'Sleeping for 30 min due to --'
        print error
        tweepy.streaming.sleep(1800)

    def on_exception(self, status, user=''):
        print status.args
        pass
        # DM Requester?
        # if user:
        #     api.send_direct_message(user, 'Sorry, but UselessBot only works on
        # twitter accounts in english)

    def on_limit(self, track):
        # process code w/ gathered tweets if over an amt?
        print 'Sleeping for an hour due to --'
        print track
        tweepy.streaming.sleep(3600)

if '__main__' == __name__:
    conn = sqlite3.connect('Users.db')
    c = conn.cursor()
    try:
        c.execute('''CREATE TABLE user
				(screen text unique, name text, verified integer, inc_rts integer, sample integer,
				tweet_count integer, followers integer, following integer, date text, runtime real)''')
        c.execute('''CREATE TABLE sums
				(screen text unique, cs integer, nes integer, pns integer, hs integer,
				ps integer, ws integer, ms integer, ts integer)''')
        c.execute('''CREATE TABLE stats
				(screen text unique, wpt real, tpd real, cpt real, hpt real, nept real)''')
        c.execute('''CREATE TABLE common
				(screen text, type text, word text, count real)''')
        c.execute('''CREATE TABLE readings
				(screen text unique, sample integer, sum integer, followings integer)''')
    except:
        pass
    if raw_input("Mode?"):
        l = StdOutListener()
        stream = tweepy.Stream(auth, l)
        stream.filter(track=['@auselessbot'], encoding='ascii')
