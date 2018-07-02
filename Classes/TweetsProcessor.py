import os, urllib2
import matplotlib.image as mpimg

class TweetsProcessor(object):
	Data = None
	def __init__(self, data):
		self.Data = data

	def normalizeGnipData(self):
		Data = Data.drop_duplicates(subset = ['id'])
		Data = Data[['id','postedTime','body','geo.coordinates', 'location.name',
		                  'twitter_entities.media','twitter_lang', 'twitter_entities.hashtags','actor.id','actor.preferredUsername',
		                  'actor.location.displayName','actor.verified','actor.followersCount','actor.friendsCount',
		                  'actor.statusesCount','actor.postedTime', 'location.displayName',
		                  'location.twitter_country_code','location.geo.coordinates']]
		Data = Data.dropna(axis = 0, how = 'all')
		Data = Data.loc[Data['id'].notnull()]
		Data = Data.reset_index(drop = True)
		Data = Data.rename(index=str, columns={'id' : 'tweet.id', 'geo.coordinates' : 'tweet.coordinates',
		                                  'postedTime' : 'tweet.created_at', 'body' : 'tweet.text', 'twitter_lang' : 'tweet.lang',
		                                  'twitter_entities.media' : 'tweet.media', 'actor.id' : 'user.id',
		                                  'actor.preferredUsername' : 'user.screen_name', 'actor.location.displayName' : 'user.location',
		                                  'actor.verified' : 'user.verified', 'actor.followersCount': 'user.followers_count',
		                                  'actor.friendsCount' : 'user.friends_count', 'actor.statusesCount' : 'user.statuses_count',
		                                  'actor.postedTime' : 'user.created_at', 'twitter_entities.hashtags' : 'tweet.hashtags'})


		Data = Data.applymap(convertNanToNone)
		Data['tweet.created_at'] = Data['tweet.created_at'].apply(convertDTToDB)
		Data['user.created_at'] = Data['user.created_at'].apply(convertDTToDB)
		Data['tweet.coordinates'] = Data['tweet.coordinates'].apply(convertNanToNone)
		Data['tweet.coordinates'] = Data['tweet.coordinates'].apply(listToStr)
		Data['tweet.text'] = Data['tweet.text'].apply(normalizeTweetText)
		Data['user.location'] = Data['user.location'].apply(normalizeTweetText)
		Data['location.name'] = Data['location.name'].apply(normalizeTweetText)
		Data['tweet.media'] = Data['tweet.media'].apply(extractImageUrl)
		Data['tweet.id'] = Data['tweet.id'].apply(getOldId)
		Data['user.id'] = Data['user.id'].apply(getOldId)
		Data['tweet.hashtags'] = Data['tweet.hashtags'].apply(extractHashTags)
		Data['tweet.hashtags'] = Data['tweet.hashtags'].apply(normalizeTweetText)
		return Data.copy()
	
	def normalizeLiveData(self):
		Data = Data.drop_duplicates(subset = ['id_str'])
		Data = Data[['id_str','created_at','text','coordinates.coordinates'
	                      'entities.media', 'entities.hashtags','lang','user.id_str','user.screen_name', 'user.description',
	                      'user.location','user.verified','user.followers_count','user.friends_count',
	                      'user.statuses_count','user.created_at','place.id','place.full_name',
	                      'place.country_code','place.bounding_box.coordinates']]
		Data = Data.dropna(axis = 0, how = 'all')
		Data = Data.loc[Data['id_str'].notnull()]
		Data = Data.reset_index(drop = True)
		Data = Data.rename(index=str, columns={'id_str' : 'tweet.id', 'coordinates.coordinates' : 'tweet.coordinates',
	                                      'created_at' : 'tweet.created_at', 'text' : 'tweet.text', 'lang' : 'tweet.lang',
	                                      'entities.media' : 'tweet.media', 'user.id_str' : 'user.id',
	                                      'entities.hashtags' : 'tweet.hashtags',
	                                      'place.full_name' : 'place.name', 'place.country_code' : 'place.country',
	                                      'place.bounding_box.coordinates' : 'place.polygon'})
		Data = Data.applymap(convertNanToNone)
		Data['tweet.created_at'] = Data['tweet.created_at'].apply(convertDTToDB)
		Data['user.created_at'] = Data['user.created_at'].apply(convertDTToDB)
		Data['tweet.coordinates'] = Data['tweet.coordinates'].apply(convertNanToNone)
		Data['tweet.text'] = Data['tweet.text'].apply(normalizeTweetText)
		Data['user.location'] = Data['user.location'].apply(normalizeTweetText)
		Data['user.description'] = Data['user.description'].apply(normalizeTweetText)
		Data['tweet.media'] = Data['tweet.media'].apply(extractImageUrl)
		Data['tweet.hashtags'] = Data['tweet.hashtags'].apply(extractHashTags)
		Data['tweet.hashtags'] = Data['tweet.hashtags'].apply(normalizeTweetText)
		return Data.copy()

	def extractHashTags(self, hashTags):
	    if hashTags is None or hashTags == []:
	        return None
	    mylist = ''
	    for hashtag in hashTags:
	        mylist = mylist + ',' + hashtag['text']
	    return mylist[1:]

	def listToStr(self, cell):
	    if cell is None:
	        return None
	    mystr = ""
	    for i in cell:
	        try:
	            mystr = mystr + "," + str(i)
	        except:
	            print i
	    mystr = "[{0}]".format(mystr[1:])
	    return mystr

	def extractImageUrl(self, cell):
	    if cell is None:
	        return None
	    return cell[0]['media_url']
	@staticmethod
	def getImage(url, full_name):
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

	@staticmethod
	def getAllImages(panda_name, column_name, path = ''):
	    downloaded_images = []
	    for row in panda_name.loc[panda_name[column_name].notnull()][column_name].tolist():
			print row
			img = TweetsProcessor.getImage(row,path + '/' + row.split('/')[-1])
			if not(img is None):
				downloaded_images.append(img)
	    return downloaded_images
	
	@staticmethod
	def storeImageWithClass(url, id, cls):
		TweetsProcessor.getImage(url, cls + '/' + id)