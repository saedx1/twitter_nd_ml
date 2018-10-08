import MySQLdb
import json
import urllib2
import re
from time import strftime,strptime
class Database():
	def __init__(self, db = "geotwitter"):
		self.myDB = MySQLdb.connect("geotwitter.uncg.edu",
                         "admin",
                         "geotwitter",
                         db)

	def convertDateTime(self, cell):
		return strftime('%Y-%m-%d %H:%M:%S', strptime(cell,'%Y-%m-%dT%H:%M:%S.000Z'))

	def normalizeTweetText(self, text):
		if text is None or type(text) is float:
			return None
		return re.sub(r'[^\w#:@/\.\-\,]', ' ', text)

	def ensureDBNull(self,cell):
		return 'NULL' if cell is None else ("'" + cell + "'")

	def getTweetsInsertQuery(self, tweets):
	    query = ""
	    for tweet in tweets:
	        temp = "REPLACE INTO tweet VALUES('{0}', '{1}', '{2}', '{3}', {4}, '{5}', {6}, {7}, {8});\n".format(tweet[0],
	                                     tweet[1],
	                                     tweet[2],
	                                     tweet[3],
	                                     self.ensureDBNull(tweet[4]),
	                                     tweet[5],
	                                     self.ensureDBNull(tweet[6]),
	                                     self.ensureDBNull(tweet[7]),
	                                     self.ensureDBNull(tweet[8]))
	        
	        query = "{0}{1}".format(query, temp)                         
	    return query

	def getUsersInsertQuery(self, getUsersInsertQuery):
	    query = ""
	    for user in users:
	        temp = "REPLACE INTO user VALUES('{0}', '{1}', {2}, {3}, \
	          '{4}', '{5}', '{6}', {7}, '{8}');\n".format(user[0],
	                                                   user[1],
	                                                   self.ensureDBNull(user[2]),
	                                                   user[3],
	                                                   user[4],
	                                                   user[5],
	                                                   user[6],
	                                                   self.ensureDBNull(user[7]),
	                                                   user[8])

	        query = "{0}{1}".format(query, temp)                
	    return query

	def getPlacesInsertQuery(self, places):
	    query = ""
	    for place in places:
	        temp = "REPLACE INTO place VALUES ('{0}', '{1}', '{2}', '{3}');\n".format(place[0],
	                                                     place[1],
	                                                     place[2],
	                                                     place[3])

	        query = "{0}{1}".format(query, temp)
	    return query

	def convertNanToNone(self, text):
	    try:
	        if str(text) == 'nan':
	            return None
	    except:
	        pass
	    return text
