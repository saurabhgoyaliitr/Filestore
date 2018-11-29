import rpyc
import sys


class BlockStore(rpyc.Service):


	"""
	Initialize any datastructures you may need.
	"""
	def __init__(self):
		self.dict = {}

	"""
        store_block(h, b) : Stores block b in the key-value store, indexed by
        hash value h
	
        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	"""
	def exposed_store_block(self, h, block):
			self.dict[h] = block 
	"""
	b = get_block(h) : Retrieves a block indexed by hash value h
	
        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	"""
	def exposed_get_block(self, h):
		return self.dict[h]

	"""
        rue/False = has_block(h) : Signals whether block indexed by h exists
        in the BlockStore service

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	"""
	def exposed_has_block(self, h):
		if( h in self.dict):
			return True
		return False

	def exposed_ping(self):
		return True	
		
if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	port = int(sys.argv[1])
	server = ThreadPoolServer(BlockStore(), port=port)
	server.start()
