import pymongo

dbclient = pymongo.MongoClient("mongodb://localhost:27017/")
db = dbclient["Congress"]
bills = db["BillText"]

print(bills.find().skip(10).next()["bill_id"])