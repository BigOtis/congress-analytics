import multiprocessing as mp
import time
import pymongo
import nltk
from gensim.summarization import keywords
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords

#############
# functions #
#############

def get_stop_words():

	swords = set(stopwords.words('english'))

	# load in custom stopwords
	f = open('stopwords.txt', "r")
	cuswords = list(f)

	# combine them
	for word in cuswords: 
		swords.add(word[:-1])
		
	return swords

def find_keywords(text, swords):
	text = text.lower()
	text = word_tokenize(text)

	wordsFiltered = []
	for w in text:
		if w.isalpha() and w not in swords:
			wordsFiltered.append(w)

	text = " ".join(wordsFiltered)
	
	try:
		result = keywords(text, words=15, lemmatize=True)
	except:
		print("error!")

	return result.splitlines()
	
def update_bill(bill, billStats, swords):
	id = bill["bill_id"]
	print("Processing bill: " + id)
	
	# generate keywords given bill text
	kws = find_keywords(bill["text"], swords)
	
	# Add the keywords to the BillStats document 
	doc = billStats.find_one({"bill_id" : id})
	if doc == None:
		doc = { "bill_id" : id, "keywords" : kws }
		billStats.insert_one(doc)
	else:
		doc["keywords"] = kws
		billStats.update_one({ "bill_id" : id }, { "$set": { "keywords" : kws } })
	
def process_bills(ids):

	#setup stop words
	swords = get_stop_words() #todo - dont do this for every thread...

	# connect to our MongoDB
	dbclient = pymongo.MongoClient("mongodb://localhost:27017/")
	db = dbclient["Congress"]
	bills = db["BillText"]
	billStats = db["BillStats"]

	#loop over the id's in the chunk and do the calculation with each
	chunk_result_list = []
	for id in ids:
		bill = bills.find().skip(id).next()
		bill_id = bill["bill_id"]
		update_bill(bill, billStats, swords)
		chunk_result_list.append(bill_id)
		
	return chunk_result_list
	
def chunks(l, n):
	for i in range(0, len(l), n):
		yield l[i:i + n]
  
##################
# mainline logic #
##################

def main():
		
	dbclient = pymongo.MongoClient("mongodb://localhost:27017/")
	db = dbclient["Congress"]
	bills = db["BillText"]
	size = bills.count()

	ids = chunks(range(0,size-1),1000)
	time2s = time.time()

	pool = mp.Pool(processes=8)
	result = pool.map(process_bills,ids)
	pool.close()
	pool.join()

	time2f = time.time()
	print(time2f-time2s)
	
	
if __name__ == '__main__':
    main()
	
	
