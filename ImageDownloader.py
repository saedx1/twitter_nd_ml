import Classes.TweetsProcessor as TP
import Classes.Database as Database
import pandas as pd
import sys
if len(sys.argv) > 1:
    path = sys.argv[1]
else:
    path = ''
db = Database().myDB
data = pd.read_sql(sql = 'select ts.tweet_id as "id", tc.image_url as "image_url", (ts.imageRelated & 1) as "wind", (ts.imageRelated & 2) as "flood", (ts.imageRelated & 4) as "destruction" \
                        from tweetCoords tc join tweetClasses ts on tc.tweet_id = ts.tweet_id', con = db)
for index, row in data.iterrows():
    if row['wind'] > 0:
        TP.storeImageWithClass(row['image_url'], row['id'], 'wind')
    if row['flood'] > 0:
        TP.storeImageWithClass(row['image_url'], row['id'], 'flood')
    if row['destruction'] > 0:
        TP.storeImageWithClass(row['image_url'], row['id'], 'destruction')