import rpyc
import sys


'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues - 
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''
class ErrorResponse(Exception):
	def __init__(self, message):
		super(ErrorResponse, self).__init__(message)
		self.error = message

	def missing_blocks(self, hashlist):
		self.error_type = 1
		self.missing_blocks = hashlist

	def wrong_version_error(self, version):
		self.error_type = 2
		self.current_version = version

	def file_not_found(self):
		self.error_type = 3



'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''
class MetadataStore(rpyc.Service):
	

	"""
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
	"""
	def __init__(self, config):
		f = open(config,"r")
		self.conn_serv = []
		self.numBlockStores = None
		self.version = {}
		self.is_deleted = {}
		self.myhashlist = {}

		while 1:
			line = f.readline()
			if not line:
				break
			line = line.split(": ") 	
			
			if(line[0]=="B"):	
				self.numBlockStores = int(line[1])

			elif(line[0]!="metadata" and line[0]!="met"):
				line = line[1].split(":")
				self.conn_serv.append(rpyc.connect(line[0],int(line[1])))	


	'''
        ModifyFile(f,v,hl): Modifies file f so that it now contains the
        contents refered to by the hashlist hl.  The version provided, v, must
        be exactly one larger than the current version that the MetadataStore
        maintains.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	'''
	
	def findServer(self,h):
		return int(h,16) % self.numBlockStores

	def do_hash(self, data):
		return hashlib.sha256(data).hexdigest()

	def exposed_modify_file(self, filename, version, hashlist, server_list):
		# print "hello"
		if filename not in self.version:
			v = 0
		else:
			v= self.version[filename]

		if (version!=v+1):
			ex = ErrorResponse("Mismatch version")
			ex.wrong_version_error(v)
			raise ex 

		missing = []
		mylist  = []
		i=0
		for h in hashlist:
			mylist.append(h)
			server =  server_list[i]
			i+=1
			if not self.conn_serv[server].root.has_block(h):
				missing.append(h)

		if(len(missing)==0):
			
			self.version[filename] = version
			# print filename
			# print self.version[filename]
			self.myhashlist[filename] = []
			for i in range (len(mylist)):
				self.myhashlist[filename].append((mylist[i],server_list[i]))
			if filename in self.is_deleted:
				self.is_deleted.pop(filename)
			# print len(mylist)
		else:			
			ex = ErrorResponse("Missing files")
			ex.missing_blocks(missing)
			raise ex 
	
	'''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	'''
	def exposed_delete_file(self, filename, version):
			if filename not in self.version:
				ex = ErrorResponse ("File Not Found")
				ex.file_not_found()
				raise ex 
 
			v = self.version[filename]
			if(v+1!=version):
				ex = ErrorResponse("Mismatch version")
				ex.wrong_version_error(v)
				raise ex 

			self.version[filename]=v+1
			self.is_deleted[filename] =1 
				

	'''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	'''
	def exposed_read_file(self, filename):
		if filename not in self.version:
			# print filename
			# print "hello"
			return 0, str([]) 

		if filename in self.is_deleted:
			return self.version[filename], None

		return self.version[filename], str(self.myhashlist[filename])	



if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(MetadataStore(sys.argv[1]), port = 6000)
	server.start()

