import redis
import csv
import datetime
from redis import RedisError
from decouple import config

r = redis.Redis(password=config('db_pass'), host=config('endpoint'), port=config('port'))

try:
	r.ts().create("quantity", retention_msecs = None)
except RedisError as  e:
	print(e)

with open("OnlineRetail.csv", encoding='utf-8-sig') as csvf:
	csvReader = csv.DictReader(csvf)

	lastTime = 0
	totalQuantity = 0

	for rows in csvReader:
		invoiceTime = datetime.datetime.strptime(rows['InvoiceDate'], '%m/%d/%Y %H:%M')
		invoiceEpoch = int(invoiceTime.timestamp()) * 1000
		quantity = int(rows['Quantity'])
#		print(invoiceEpoch, quantity)
		if (invoiceEpoch > lastTime):
			print("Total orders for " + str(lastTime) + " is " + str(totalQuantity))
			r.ts().add("quantity", lastTime, totalQuantity)
			totalQuantity = 0
			lastTime = invoiceEpoch

		totalQuantity += quantity

