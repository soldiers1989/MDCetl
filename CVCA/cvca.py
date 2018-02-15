import tweepy
import pandas as pd
import time
import jsonpickle
import os


class EthiopixTweet:

	def __init__(self):
		self.ts_all = []
		self.ts = []
		self.col = None
		self.file_name = '{}_tweet_{}.txt'
		self.api_key = '4zdeH8Fkvh9nhBYXHm0RLFVET'
		self.api_secret = 'LCFHgwBfmFRd9mte4gijeKPA27EeXpeghfISiGYjBrsUlUY7HY'
		self.access_token = '33517809-kMvs9vGLyh021P4nqMpPkKJWyFeQ6lVDddxqOg1Er'
		self.access_token_secret = 'a4hKveAobzeVAAlXOKrUUuRbv95llNl6XI3DxtfFGG8JU'
		self.auth_token = tweepy.OAuthHandler(self.api_key, self.api_secret)
		self.auth_token.set_access_token(self.access_token, self.access_token_secret)
		self.api_token = tweepy.API(self.auth_token, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

		self.auth = tweepy.AppAuthHandler(self.api_key, self.api_secret)
		self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

		self.searchQuery = ''
		self.maxTweets = 10000000
		self.tweetsPerQry = 100
		self.sinceID = None
		self.maxID = -1
		self.topics = ['Ethiopian Reporter', 'Addis Fortune', 'Capitaleth', 'Ethiopia News Agency', 'Tigrai Online',
		               'Oromia Media Network', 'VOA Amharic', 'DW (Amharic)']

	def save_csv(self, lst, col, file_name):
		df = pd.DataFrame(lst, columns=col)
		df.to_csv(file_name)

	def get_tweet_first(self):
		# self.ts.append(tweet._json)
		# if len(self.ts) == 100:
		# 	print('Saving first 100...')
		# 	self.save_csv(self.ts, tweet._json.keys(), self.file_name.format(int(time.time())))
		# 	print('Saving at {}'.format(int(time.time())))
		# 	self.ts_all = self.ts_all + self.ts
		# 	self.ts = []
		tweets = self.api_token.search('Ethiopia')
		for t in tweets:
			self.ts.append(t._json)
			col = t._json.keys()
		df = pd.DataFrame(self.ts, columns=col)
		print(df.head())
		df.to_csv('ethiopic_tweeter.csv')

	def get_tweet_second(self):
		tweets = tweepy.Cursor(self.api_token.search, q='EPRDF', include_entities=True).items()

		# x = tweets.next()
		for t in tweets:
			self.ts.append(t._json)
			if len(self.ts) == 200:
				print('Saving first 100...')
				self.save_csv(self.ts, t._json.keys(), self.file_name.format(int(time.time())))

	def get_tweet_third(self):
		if not self.api:
			print('Authentication Issue occured')
		else:
			os.chdir('All Tweets')
			for topic in self.topics:
				tweetCount = 0
				self.sinceID = None
				self.maxID = -1
				self.searchQuery = topic
				file_name = self.file_name.format(self.searchQuery, int(time.time()))
				with open(file_name, 'w') as file:
					while tweetCount < self.maxTweets:
						try:
							if self.maxID <= 0:
								if not self.sinceID:
									tweets = self.api.search(q=self.searchQuery, count=self.tweetsPerQry)
								else:
									tweets = self.api.search(q=self.searchQuery, count=self.tweetsPerQry, since_id=self.sinceID)
							else:
								if not self.sinceID:
									tweets = self.api.search(q=self.searchQuery, count=self.tweetsPerQry, max_id=str(self.maxID - 1))
								else:
									tweets = self.api.search(q=self.searchQuery, count=self.tweetsPerQry, max_id=str(self.maxID - 1), since_id=self.sinceID)
							if not tweets:
								print('------ALL Tweets downloaded completely-----')
								break
							for tweet in tweets:
								file.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')
								tweetCount += len(tweets)
								print('Download {} tweets'.format(tweetCount))
								self.maxID = tweets[-1].id
						except tweepy.TweepError as e:
							print('Exceptions: ' + str(e))
							break


if __name__ == '__main__':
	twe = EthiopixTweet()
	twe.get_tweet_third()