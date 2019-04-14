import rpyc
import time
import threading
import random
import os


incoming_lb_conns = {}
outgoing_lb_conns = {}

load_balancer_set = {}  #TODO

for load_balancer in loadbalancer_set:
	try:
		outgoing_lb_conns[ip_addr] = rpyc.connect(ip_addr, PORT)

def get_ip(conn):
    return conn._channel.stream.getpeername()[0]

class LB2SNService(rpyc.Service):
	def on_connect(self, conn):
        ip_addr = get_ip(conn)
        incoming_lb_conns[ip_addr] = conn
        print("[SN2LB] LoadBalancer node connected! IP:", IP_addr)

    def on_disconnect(self, conn):
        ip_addr = get_ip(conn)
        del incoming_lb_conns[ip_addr]
        del outgoing_lb_conns[ip_addr]
        #abort

    def exposed_write_abort( log_id, record_id ):
    	folder_name = "/home/"+ log_id.str()
		file_name = folder_name + "/" + record_id.str()
		if ( os.path.exist(file_name) ):
			os.remove(file_name)

	def exposed_write_commit_request( IP_ADDR, record ):
		try:
			conn = outgoing_lb_conns[ IP_ADDR ]
			try:
				folder_name = "/home/"+ record.log_id.str()
				file_name = folder_name + "/" + record.record_id.str()
				if( not os.path.isdir( folder_name ) ):
					os.mkdir( folder_name )
				f = open( file_name ,"w+")
				f.write( record )
				conn.root.write_agreed( record.log_id )
			except:
				conn.root.write_abort( record )

	def exposed_read( log_id, record_id, ip_addr ):
		folder_name = "/home/"+ log_id.str()
		file_name = folder_name + "/" + record_id.str()
		if ( os.path.exist(file_name) ):
			f = open( file_name ,"r+")
			record = f.read( )
			conn = rpyc.connect(ip_addr, PORT) #todo
			conn.root.read_receive( record )
