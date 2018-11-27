import multiprocessing as mp
import time
import pymongo

def chunks(l, n):
	for i in range(0, len(l), n):
		yield l[i:i + n]
		
# Create a list that from the results of the function chunks:
# print(list(chunks(range(1,102), 5)))
def calculate(chunk):
	#define client inside function
	dbclient = pymongo.MongoClient("mongodb://localhost:27017/")
	db = dbclient["Congress"]
	bills = db["BillText"]
	#chunk_result_list = []
	#loop over the id's in the chunk and do the calculation with each
	chunk_result_list = []
	for id in chunk:
		result = bills.find().skip(id).next()["bill_id"]
		chunk_result_list.append(result)
		
	return chunk_result_list

	
def main():
	c = chunks(range(0,101),10)
	time2s = time.time()

	pool = mp.Pool(processes=5)
	result = pool.map(calculate,c)
	pool.close()
	pool.join()

	time2f = time.time()
	print(result)
	
	
if __name__ == '__main__':
    main()