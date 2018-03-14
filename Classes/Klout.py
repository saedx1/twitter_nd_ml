import json
import urllib2
import MySQLdb

class klout(object):
    kloutKeys = [
              'juqcepjg3c5efrd4q936mqn2'
             ,'kyykevxxhkpb5xrjbvu5wm4f'
             ,'b95w83cjapt7etbw6657d6jz'
             , 'memp3ncn4qvp6c8guzjcc8dp'
            ]
	def klout_getId(self, screenname, keyNum, j = 0):
	    url = 'http://api.klout.com/v2/identity.json/twitter?screenName={0}&key={1}'.format(screenname,kloutKeys[keyNum])    
	    try:
	        return json.load(urllib2.urlopen(url)), keyNum
	    except urllib2.HTTPError as err:
	        if err.code == 404:
	            return None, keyNum
	        else:
	            if j != len(kloutKeys):
	                print j
	                return klout_getId(screenname, (keyNum + 1) % len(kloutKeys), j + 1)
	            else :
	                return 'LIMIT', keyNum

	def klout_getScore(self, kloutId, keyNum, j = 0):
	    url = 'http://api.klout.com/v2/user.json/{0}/score?key=b95w83cjapt7etbw6657d6jz'.format(kloutId,kloutKeys[keyNum])
	    try:
	        return json.load(urllib2.urlopen(url))
	    except urllib2.HTTPError as err:
	        if err.code == 404:
	            return None, keyNum
	        else:
	            if j != len(kloutKeys):
	                print j
	                return klout_getId(screenname, (keyNum + 1) % len(kloutKeys), j + 1)
	            else :
	                return 'LIMIT', keyNum
    def klout_getInfluence(self, kloutId, keyNum, j = 0):
	    url = 'http://api.klout.com/v2/user.json/{0}/influence?key=b95w83cjapt7etbw6657d6jz'.format(kloutId,kloutKeys[keyNum])
	    try:
	        return json.load(urllib2.urlopen(url)), keyNum
	    except urllib2.HTTPError as err:
	        if err.code == 404:
	            return None, keyNum
	        else:
	            if j != len(kloutKeys):
	                return klout_getInfluence(kloutId, (keyNum + 1) % len(kloutKeys), j + 1)
	            else :
	                return 'LIMIT', keyNum