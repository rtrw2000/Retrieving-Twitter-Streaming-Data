###########################################################
#  Penambangan data Twitter Kota Serang                   #
#  Penelitian HBW Trip Production Kota Serang             #
#                 SAPPK ITB                               #
#  Script Name: Geographic Location Mining with Python    #
#  Purpose : Find, Save Username, Timestamp and Location  #
#  Programmer: Richard Shiawase                           #
###########################################################


import twitter
import pymongo
import csv
import time
import re
import pandas as pd	
class bcolors:
		HEADER = '\033[95m'
		OKBLUE = '\033[94m'
		OKGREEN = '\033[92m'
		WARNING = '\033[93m'
		FAIL = '\033[91m'
		ENDC = '\033[0m'
		BOLD = '\033[1m'
		UNDERLINE = '\033[4m'
	

num_results = 250
outfile = "Wal160821.csv"
config = {}
exec(open("config.txt").read(), config)
print(config["consumer_key"])
api = twitter.Api(consumer_key=config["consumer_key"],
                  consumer_secret=config["consumer_secret"],
                  access_token_key=config["access_key"],
                  access_token_secret=config["access_secret"],
                  tweet_mode='extended')
     
	# open a file to write (mode "w"), and create a CSV writer object
csvfile = open(outfile,'w')
csvwriter = csv.writer(csvfile)
        
	# add headings to our CSV file
row = ["Name", "Created At","Profile Url", "Latitude", "Longitude","Google Maps","Tweet"]
csvwriter.writerow(row)
result_count = 0
last_id = None


# PYMONGO
client = pymongo.MongoClient("mongodb://localhost:27017/")
database = client["TwStreaming"]
collection = "Walantaka"
mongodb_insert = database[collection]

while result_count < int(num_results):
		
		# perform a search based on latitude and longitude
		# query = api.GetSearch(raw_query="q=richardshiawase%20&result_type=recent&since=2014-07-19&count=1")
		query = api.GetSearch(raw_query="q=&geocode=-6.149243,106.211806,5km&lang=id")
		print([s.place["bounding_box"]["coordinates"][0][0][1] for s in query])
		# print([s.user.location for s in query])
		for result in query:
			# print(result.created_at)
			# only process a result if it has a geolocation
			if result.geo or result.place:
				user = result.user.screen_name
				created_at =  time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(result.created_at,'%a %b %d %H:%M:%S +0000 %Y'))
				text = result.full_text
				text = text.encode('ascii', 'replace')
				# remove hashtags
				# only removing the hash # sign from the word
				text = re.sub(r'#', '', text)

				# remove old style retweet text "RT"
				text = re.sub(r'^RT[\s]+', '', text)

				latitude = result.place["bounding_box"]["coordinates"][0][0][1]
				longitude = result.place["bounding_box"]["coordinates"][0][0][0]
				url = 'https://twitter.com/%s' % user
				gurl = 'https://maps.google.com/?q=' + str(latitude) + ',' + str(longitude)
				
				# now write this row to our CSV file
				row = [user, created_at, url, latitude, longitude,gurl, text]
				print('-----------------------------------------------------------------')
				print (' ')
				print (bcolors.OKGREEN +'Name:    '+ bcolors.ENDC, user)
				print (bcolors.OKGREEN +'Timestamp:   '+ bcolors.ENDC, created_at)
				print (bcolors.OKGREEN +'Profile Url: '+ bcolors.ENDC, url)
				print (bcolors.OKGREEN +'Latitude:    '+ bcolors.ENDC, latitude)
				print (bcolors.OKGREEN +'Longitude:   '+ bcolors.ENDC, longitude)
				print (bcolors.OKGREEN +'Google Maps: '+ bcolors.ENDC, gurl)
				print (bcolors.OKGREEN +'Tweet:       '+ bcolors.ENDC, text)
				print  (' ')
				csvwriter.writerow(row)
				result_count += 1
				# mongodb dictionary
				kamus = {"Name": user, "Created At": str(created_at),
						 "Profile Url": url, "Latitude": str(latitude),
						 "Longitude": str(longitude),
						 "Google Maps": gurl,
						 "Tweet": text}
				# insert to mongodb
				print("Inserting to mongodb..")
				mongodb_insert.insert_one(kamus)
				time.sleep(.35)
			last_id = result.id
			
	# let the user know where we're up to
	
if result_count == 1:
		print (bcolors.WARNING + "Got %d result" % result_count + bcolors.ENDC)
		csvfile.close()
		print (bcolors.WARNING + "Saved to ", outfile + bcolors.ENDC)
elif result_count == 0:
		print (bcolors.WARNING + "Didn't get any results try another Latitude and  Longitude" + bcolors.ENDC)
else:
		print (bcolors.WARNING + "Got %d results" % result_count + bcolors.ENDC)
		csvfile.close()
		print (bcolors.WARNING + "Saved to ", outfile + bcolors.ENDC)
	
	# we're all finished, clean up and go home.
