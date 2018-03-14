import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import urllib2
import numpy as np
import os,sys
import gzip, json
from dateutil import parser
import os.path
from scipy import misc
from time import strftime,strptime
import re
import math
import pymysql
import MySQLdb

class GnipDataProcessor(object):

    def __init__(self, i_path, collection, chunk_size=50):
        self.path = i_path
        self.chunk = []
        self.chunk_size = chunk_size
        self.collection = collection
        self.total_inserts = 0

    def all_files(self):
        for path, dirs, files in os.walk(self.path):
            for f in files:
                yield os.path.join(path, f)
    def iter_files(self):
        file_generator = self.all_files()

        for f in file_generator:
            try:
                gfile = gzip.open(f)
                for line in gfile:
                    self.process_line(line)
                gfile.close()
            except Exception as e:
                # print e
                pass
            if self.chunk != []:
                yield self.chunk
        
        

    def process_line(self, line):
        try:
            if len(self.chunk) > self.chunk_size:
                self.process_chunk()
                self.chunk = []
            if line.strip() != "":
                data = json.loads(line)
                if 'id' in data:
                    # data['postedTime_mongo'] = parser.parse(data['postedTime'])
                    self.chunk.append(line)

        except Exception as e:
            # print "error storing chunk \n"
            # print line, e.msg()
            raise

    def process_chunk(self):
        #for item in self.chunk:
        try:
            #print self.chunk
            self.total_inserts += len(self.chunk)
            # print "Inserted: %d number of docs" % self.total_inserts
        except:
            # print "issue inserting"
            pass

def getimage(url, full_name):

    if os.path.isfile(full_name):
        return mpimg.imread(full_name)
    
    try:
        f = urllib2.urlopen(url)
    except:
        return None
    
    data = f.read()
    with open(full_name, "wb") as code:
        code.write(data)
    return mpimg.imread(full_name)


def getallimages(panda_name, column_name):
    downloaded_images = []
    for row in panda_name.loc[panda_name[column_name].notnull(),column_name]:
        for image in row:
            img = getimage(image['media_url'],image['media_url'].split('/')[-1])
            if not(img is None):
                downloaded_images.append(img)
    return downloaded_images

def klout_getId(screenname):
    url = 'http://api.klout.com/v2/identity.json/twitter?screenName={0}&key=memp3ncn4qvp6c8guzjcc8dp'.format(screenname)    
    try:
        return json.load(urllib2.urlopen(url))
    except:
        return None
    
def klout_getScore(kloutId):
    url = 'http://api.klout.com/v2/user.json/{0}/score?key=memp3ncn4qvp6c8guzjcc8dp'.format(kloutId)
    try:
        return json.load(urllib2.urlopen(url))
    except:
        return None
def extractImageUrl(cell):
    if cell is None:
        return None
    return cell[0]['media_url']

def convertDTToDB(cell):
    '2017-10-04T13:00:00.000Z'
    return strftime('%Y-%m-%d %H:%M:%S', strptime(cell,'%Y-%m-%dT%H:%M:%S.000Z'))
def getOldId(cell):
    return cell.split(':')[2]
def normalizeTweetText(text):
    if text is None or type(text) is float:
        return None
    return re.sub(r'[^\w#:@/\.\-\,]', ' ', text)

## Note that if we may put NULL for any of the attributes
## we should remove the single quotation marks from around them.
def ensureDBNull(cell):
    try:
        return 'NULL' if cell is None else ("'" + cell + "'")
    except:
        print "NULL -- MSG"
        return 'NULL'

def getTweetsInsertQuery(tweets):
    query = ""
    counter = 0
    for tweet in tweets:
        try:
            temp = "REPLACE INTO tweet VALUES('{0}', '{1}', '{2}', '{3}', {4}, '{5}', {6}, {7}, {8});\n".format(tweet[0],
                                     tweet[1],
                                     tweet[2],
                                     tweet[3],
                                     ensureDBNull(tweet[4]),
                                     tweet[5],
                                     ensureDBNull(tweet[6]),
                                     ensureDBNull(tweet[7]),
                                     ensureDBNull(tweet[8]))
        
            query = "{0}{1}".format(query, temp)   
        except Exception as e:
            print "{0} --- {1} ---- {2}".format(e,tweet,counter)
        counter += 1               
    return query

def getUsersInsertQuery(users):
    query = ""
    for user in users:
        temp = "('{0}', '{1}', {2}, {3}, '{4}', '{5}', '{6}', {7}, '{8}')".format(user[0],
                                                   user[1],
                                                   ensureDBNull(user[2]),
                                                   user[3],
                                                   user[4],
                                                   user[5],
                                                   user[6],
                                                   ensureDBNull(user[7]),
                                                   user[8])

        query = "{0},\n{1}".format(query, temp)                
    query = "INSERT INTO user VALUES\n{0}".format(query[2:])
    return query

def getPlacesInsertQuery(places):
    query = ""
    for place in places:
        try:
            temp = "('{0}', '{1}', '{2}', '{3}')".format(normalizeTweetText(place[0]),
                                                         place[1],
                                                         place[2],
                                                         place[3])

            query = "{0},\n{1}".format(query, temp)
        except:
            print place
    query = "INSERT INTO place VALUES\n{0}".format(query[2:])
    return query

def convertNanToNone(text):
    try:
        if str(text) == 'nan':
            return None
    except:
        pass
    return text

def insertIntoDB(mycursor, dataset, table):
    if table == "tweet":
        getInsertionQueries = getTweetsInsertQuery
    elif table == "place":
        getInsertionQueries = getPlacesInsertQuery
    elif table == "user":
        getInsertionQueries = getUsersInsertQuery
        
    first = 0;
    last = 1000;
    end = len(dataset.values)
    if last > end:
        mycursor.execute(getInsertionQueries(dataset.values))
    else:
        while first < end:
            mycursor.execute(getInsertionQueries(dataset.values[first:last]))
            first = last;
            last += 1000;
            if last > len(dataset.values):
                last = end + 1
    mycursor.close()
    mycursor = mydb.cursor()
    
def listToStr(cell):
    if cell is None:
        return None
    mystr = "";
    for i in cell:
        try:
            mystr = mystr + "," + str(i)
        except:
            print i
    mystr = "[{0}]".format(mystr[1:])
    return mystr
def getDBInstance():
    return MySQLdb.connect("uncg.saadmtsa.club",    # your host, usually localhost
                         "root",         # your username
                         "vJnVubg49U",  # your password
                         "geotwitter")
def extractHashTags(hashTags):
    if hashTags is None or hashTags == []:
        return None
    mylist = ''
    for hashtag in hashTags:
        mylist = mylist + ',' + hashtag['text']
    return mylist[1:]

if __name__ == '__main__':
    # if(len(sys.argv) < 2):
    #     print 'No arguments given'
    #     exit(0)
    # start = int(sys.argv[1])
    # end = int(sys.argv[2])
    # if(end > 5730):
    #     end = 5730
    # print start, end
    insrt = GnipDataProcessor("IRMA Data","", chunk_size=1000)
    result = []
    mygen = insrt.iter_files()
    for i in mygen:
        result.append(i)

    ## Get the data and put it in a panda dataframe
    # myfile = open("/home/saed/Desktop/Twitter/abc.json", 'r')
    for start in range(0,5701,100):
        myjson = []
        if start == 5700:
            end = 5730
        else:
            end = start + 100
        print start, end
        for i in result[start:end]:
            for j in i:
                try:
                    myjson.append(json.loads(j))
                except:
                    print j
        mypanda = pd.io.json.json_normalize(myjson)


        mydata = mypanda.copy()
        mydata = mydata.drop_duplicates(subset = ['id'])
        mydata = mydata[['id','postedTime','body','geo.coordinates', 'location.name',
                          'twitter_entities.media','twitter_lang', 'twitter_entities.hashtags','actor.id','actor.preferredUsername',
                          'actor.location.displayName','actor.verified','actor.followersCount','actor.friendsCount',
                          'actor.statusesCount','actor.postedTime', 'location.displayName',
                          'location.twitter_country_code','location.geo.coordinates']]
        mydata = mydata.dropna(axis = 0, how = 'all')
        mydata = mydata.loc[mydata['id'].notnull()]
        mydata = mydata.reset_index(drop = True)
        mydata = mydata.rename(index=str, columns={'id' : 'tweet.id', 'geo.coordinates' : 'tweet.coordinates',
                                          'postedTime' : 'tweet.created_at', 'body' : 'tweet.text', 'twitter_lang' : 'tweet.lang',
                                          'twitter_entities.media' : 'tweet.media', 'actor.id' : 'user.id',
                                          'actor.preferredUsername' : 'user.screen_name', 'actor.location.displayName' : 'user.location',
                                          'actor.verified' : 'user.verified', 'actor.followersCount': 'user.followers_count',
                                          'actor.friendsCount' : 'user.friends_count', 'actor.statusesCount' : 'user.statuses_count',
                                          'actor.postedTime' : 'user.created_at', 'twitter_entities.hashtags' : 'tweet.hashtags'})


        mydata = mydata.applymap(convertNanToNone)
        mydata['tweet.created_at'] = mydata['tweet.created_at'].apply(convertDTToDB)
        mydata['user.created_at'] = mydata['user.created_at'].apply(convertDTToDB)
        mydata['tweet.coordinates'] = mydata['tweet.coordinates'].apply(convertNanToNone)
        mydata['tweet.coordinates'] = mydata['tweet.coordinates'].apply(listToStr)
        mydata['tweet.text'] = mydata['tweet.text'].apply(normalizeTweetText)
        mydata['user.location'] = mydata['user.location'].apply(normalizeTweetText)
        mydata['location.name'] = mydata['location.name'].apply(normalizeTweetText)
        mydata['location.displayName'] = mydata['location.displayName'].apply(normalizeTweetText)
        mydata['tweet.media'] = mydata['tweet.media'].apply(extractImageUrl)
        mydata['tweet.id'] = mydata['tweet.id'].apply(getOldId)
        mydata['user.id'] = mydata['user.id'].apply(getOldId)
        mydata['tweet.hashtags'] = mydata['tweet.hashtags'].apply(extractHashTags)
        mydata['tweet.hashtags'] = mydata['tweet.hashtags'].apply(normalizeTweetText)

        tweets = mydata[['tweet.id','tweet.created_at','tweet.text','user.id','tweet.coordinates','location.name','tweet.media','tweet.lang','tweet.hashtags' ]]

        users = mydata[['user.id','user.screen_name','user.location','user.verified','user.followers_count','user.friends_count','user.statuses_count','user.created_at']]
        users = users.drop_duplicates(subset = ['user.id'])
        users['klout_score'] = [0.0] * len(users)
        places = mydata[['location.name','location.displayName','location.twitter_country_code','location.geo.coordinates']]
        places = places.drop_duplicates(subset = ['location.name'])
        myfile = open('TweetsInsertion.sql', 'a')
        for a in getTweetsInsertQuery(tweets.values).split('\n'):
            myfile.write(a+'\n')
        myfile.close()

        myfile = open('UsersInsertion.sql', 'a')
        for a in getUsersInsertQuery(users.values).split('\n'):
            myfile.write(a+'\n')
        myfile.close()

        myfile = open('PlacesInsertion.sql', 'a')
        for a in getPlacesInsertQuery(places.values).split('\n'):
            myfile.write(a+'\n')
        myfile.close()


        del myjson
        del mypanda
        del mydata