import rpyc
import time
import threading
import random
import os

SN2LB_PORT = 88045
SN2CLIENT_PORT = 88888

incoming_lb_conns = {}
outgoing_lb_conns = {}
outgoing_client_conns = {}

load_balancer_set = []  #TODO

for ip_addr in load_balancer_set:
	try:
		outgoing_lb_conns[ip_addr] = rpyc.connect(ip_addr, SN2LB_PORT)
	except:
		print("error")

def get_ip(conn):
	return conn._channel.stream.getpeername()[0]

def check_SN2CLIENT_conn( ip_addr ):
	if(ip_addr in outgoing_lb_conns.keys())
		return outgoing_lb_conns[ip_addr]
	else:
		conn = rpyc.connect(ip_addr, SN2CLIENT_PORT)
		outgoing_client_conns[ip_addr] = conn
		return conn


class LB2SNService(rpyc.Service):
	def on_connect(self, conn):
		ip_addr = get_ip(conn)
		incoming_lb_conns[ip_addr] = conn
		print("[SN2LB] LoadBalancer node connected! IP:", ip_addr)

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

	def exposed_write_commit( IP_ADDR, record_id )
		conn = check_SN2CLIENT_conn(IP_ADDR)
		conn.root.write_receive( record_id )

	def exposed_read( log_id, record_id, ip_addr ):
		folder_name = "/home/"+ log_id.str()
		file_name = folder_name + "/" + record_id.str()
		if ( os.path.exist(file_name) ):
			f = open( file_name ,"r+")
			record = f.read( )
			conn = check_SN2CLIENT_conn(IP_ADDR)
			conn.root.read_receive( record )
