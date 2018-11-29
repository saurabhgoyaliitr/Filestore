import rpyc
import hashlib
import os
import sys
from os import listdir
from os.path import isfile, join
from metastore import ErrorResponse
import time
"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""

class SurfStoreClient():

	"""
	Initialize the client and set up connections to the block stores and
	metadata store using the config file
	"""
	def __init__(self, config):
		f = open(config,"r")
		self.conn_meta = None
		self.conn_serv = []
		self.numBlockStores = None
		self.method = None

		while 1:
			line = f.readline()
			if not line:
				break
			line = line.split(": ") 	
			
			if(line[0]=="B"):	
				self.numBlockStores = int(line[1])

			elif(line[0]=="metadata"):
				line = line[1].split(":")
				self.conn_meta = rpyc.connect(line[0],int(line[1]))

			elif(line[0]=="met"):
				self.method = int(line[1])	
			else:
				line = line[1].split(":")
				self.conn_serv.append(rpyc.connect(line[0],int(line[1])))




		# conn = rpyc.connect('localhost', 5000)

	"""
	upload(filepath) : Reads the local file, creates a set of 
	hashed blocks and uploads them onto the MetadataStore 
	(and potentially the BlockStore if they were not already present there).
	"""

	def findServer(self,h):
		if self.method == 1:
			return int(h,16) % self.numBlockStores
		minn_diff = None 
		retser = None
		for i in range(len(self.conn_serv)):
			start_time = time.time()
			self.conn_serv[i].root.ping()
			end_time = time.time()
			if(minn_diff is None or end_time-start_time<minn_diff):
				retser = i 
				minn_diff = end_time-start_time
		return retser		
 
	def do_hash(self,data):
		return hashlib.sha256(data).hexdigest()

	def upload(self, filepath):
		try:
			file = open(filepath, "rb")
		except:
			print ("Not Found")
			return

		
		hashlist = []
		server_list = []
		data = []

		while True:
			try:
				temp = file.read(4096)
				#print(temp)
				if temp == b'':
					break
				data.append(temp)
				# print '--',len(data[-1])
				# print self.do_hash(temp)
				hashlist.append(self.do_hash(temp))
				server_list.append(self.findServer(hashlist[-1]))
				# upload_list.append(self.findServer(hashlist[-1]))
				# print upload_list[-1]
			except:
				break	
		file.close()					

		filename = filepath.split("/")[-1]
		
		
		v, hl = self.conn_meta.root.read_file(filename)
		v+=1
		# print(v)

		while True:
			try:
				# print len(server_list) 
				self.conn_meta.root.modify_file(filename,v,hashlist, server_list)
				print ("OK")
				break

			except Exception as e:
				if e.error_type == 1: 	
					missing = list(eval(e.missing_blocks))
					for h in missing:
						index = hashlist.index(h)
						curr_data = data[index]
						self.conn_serv[self.findServer(h)].root.store_block(h,curr_data)

				else:
					v = e.current_version +1 


	"""
	delete(filename) : Signals the MetadataStore to delete a file.
	"""
	def delete(self, filename):
		v, hl = self.conn_meta.root.read_file(filename)
		if v==0:
			print ("Not Found")
			return 
		v+=1		
		
		while True:
			try: 
				self.conn_meta.root.delete_file(filename,v)	
				print ("OK")
				break
			except Exception as e:
				if e.error_type ==2:
					v = e.current_version+1
				else:
					print ("Not Found")
					break	
	"""
        download(filename, dst) : Downloads a file (f) from SurfStore and saves
        it to (dst) folder. Ensures not to download unnecessary blocks.
	"""
	def download(self, filename, location):
		
		if location[-1]!='/':
			location+='/'
 
		v, hl = self.conn_meta.root.read_file(filename)
		# print v
		
		if v==0 or hl is None:
			print  ("Not Found")
			return 

		files = [join(location, f) for f in listdir(location) if isfile(join(location, f))]
		missing = []
		data = {}
		finaldata = b''
		hl = list(eval(hl))
		
		for f in files:
			r = open(f,"rb")
			
			while True: 	
				try:
					curr_data = r.read(4096)
					if curr_data==b'':
						break
					data[self.do_hash(curr_data)] = curr_data
				except:
					break
			r.close()			
						
		for h in hl:
			if h[0] not in data:
				finaldata+=self.conn_serv[h[1]].root.get_block(h[0])
			else:
				finaldata+=data[h[0]]
		
		# print len(finaldata)
		final_file = location
		final_file += filename	
			
		f = open(final_file,"wb")
		f.write(finaldata)		
		f.close()	
		print ("OK")	

	"""
	 Use eprint to print debug messages to stderr
	 E.g - 
	 self.eprint("This is a debug message")
	"""
	# def eprint(*args, **kwargs):
	# 	print(*args, file=sys.stderr, **kwargs)



if __name__ == '__main__':
	client = SurfStoreClient(sys.argv[1])
	operation = sys.argv[2]
	if operation == 'upload':
		client.upload(sys.argv[3])
	elif operation == 'download':
		client.download(sys.argv[3], sys.argv[4])
	elif operation == 'delete':
		client.delete(sys.argv[3])
	else:
		print("Invalid operation")
		
