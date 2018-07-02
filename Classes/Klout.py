import json
import urllib2
import MySQLdb

class klout(object):
	kloutKeys = ['juqcepjg3c5efrd4q936mqn2', 'kyykevxxhkpb5xrjbvu5wm4f', 'b95w83cjapt7etbw6657d6jz', 'memp3ncn4qvp6c8guzjcc8dp']
	def klout_getId(self, screenname, keyNum, j = 0):
		url = 'http://api.klout.com/v2/identity.json/twitter?screenName={0}&key={1}'.format(screenname,self.kloutKeys[keyNum])    
		try:
			return json.load(urllib2.urlopen(url)), keyNum
		except urllib2.HTTPError as err:
			if err.code == 404:
				return None, keyNum
			else:
				if j != len(self.kloutKeys):
					return self.klout_getId(screenname, (keyNum + 1) % len(self.kloutKeys), j + 1)
				else :
					return 'LIMIT', keyNum

	def klout_getScore(self, kloutId, keyNum, j = 0):
		url = 'http://api.klout.com/v2/user.json/{0}/score?key={1}'.format(kloutId,self.kloutKeys[keyNum])
		try:
			return json.load(urllib2.urlopen(url))
		except urllib2.HTTPError as err:
			if err.code == 404:
				return None, keyNum
			else:
				if j != len(self.kloutKeys):
					return self.klout_getScore(kloutId, (keyNum + 1) % len(self.kloutKeys), j + 1)
				else :
					return 'LIMIT', keyNum
	def klout_getInfluence(self, kloutId, keyNum, j = 0):
		url = 'http://api.klout.com/v2/user.json/{0}/influence?key={1}'.format(kloutId,self.kloutKeys[keyNum])
		try:
			return json.load(urllib2.urlopen(url)), keyNum
		except urllib2.HTTPError as err:
			if err.code == 404:
				return None, keyNum
			else:
				if j != len(self.kloutKeys):
					return self.klout_getInfluence(kloutId, (keyNum + 1) % len(self.kloutKeys), j + 1)
				else :
					return 'LIMIT', keyNum