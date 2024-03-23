import redis
import datetime
import time
from decouple import config

r = redis.Redis(password=config('db_pass'), host=config('endpoint'), port=config('port'), decode_responses=True)

while True:
	received = r.xread({"orders": '$'}, None, 0)

	print(received)

	for result in received:
		data = result[1]
		for tuple in data:
			orderDict = tuple[1];
			print(orderDict)

#	time.sleep(1)

