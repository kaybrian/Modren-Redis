import csv
import redis
from decouple import config

r = redis.Redis(password=config('db_pass'), host=config('endpoint'), port=config('port'))

with open("OnlineRetail.csv", encoding='utf-8-sig') as csvf:
	csvReader = csv.DictReader(csvf)

	for row in csvReader:
		r.xadd("orders", row)
