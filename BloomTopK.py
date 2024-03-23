import redis
import csv
import datetime
from decouple import config
from redis import RedisError

r = redis.Redis(password=config('db_pass'), host=config('endpoint'), port=config('port'), decode_responses=True)

r.flushall()

try:
	r.topk().reserve("topsellers", 10, 50, 4, 0.9) 
	#k, width (number of counters per array), depth (number of arrays), decay (p of reducing a counter)
except RedisError as  e:
	print(e)

itemNames={}

with open("OnlineRetail.csv", encoding='utf-8-sig') as csvf:
	csvReader = csv.DictReader(csvf)

	for rows in csvReader:

		stockCode = rows['StockCode']
		itemNames[stockCode] = rows['Description']
		response = r.topk().add('topsellers', stockCode)
		if (response[0] != None):
			print("\n" + itemNames[stockCode] + " evicted " + itemNames[str(response[0])])

			print("\nNEW TOP TEN ITEMS:\n")
			topTen = r.topk().list('topsellers')
			for item in topTen:
				print(itemNames[str(item)])
