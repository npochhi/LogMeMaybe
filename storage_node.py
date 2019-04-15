import rpyc
import time
import threading
import random
import os
import pickle
from record_class import Record

SN2LB_PORT = 88045
SN2CLIENT_PORT = 88888
SN2SN_PORT = 12356

DEBUG = True

incoming_lb_conns = {}
outgoing_lb_conns = {}
outgoing_client_conns = {}
incoming_sn_conns = {}
outgoing_sn_conns = {}

def check_debug( msg ):
	if(DEBUG == True):
		print(msg)

load_balancer_set = []  #TODO

for ip_addr in load_balancer_set:
	try:
		outgoing_lb_conns[ip_addr] = rpyc.connect(ip_addr, SN2LB_PORT)
		check_debug("[SN2LB] LoadBalancer node connected! IP: " + ip_addr)
	except:
		check_debug("[SN2LB] Error connecting to LoadBalancer! IP: " + ip_addr)

def get_ip(conn):
	return conn._channel.stream.getpeername()[0]


def check_SN2CLIENT_conn(ip_addr):
	if(ip_addr in outgoing_lb_conns.keys()):
		return outgoing_lb_conns[ip_addr]
	else:
		try:
			conn = rpyc.connect(ip_addr, SN2CLIENT_PORT)
			check_debug("[SN2CLIENT] Client node connected! IP: " + ip_addr)
			outgoing_client_conns[ip_addr] = conn
			return conn
		except:
			check_debug("[SN2CLIENT] Error connecting to Client Node! IP: " + ip_addr)

def check_SN2SN_conn(ip_addr):
	if(ip_addr in outgoing_sn_conns.keys()):
		return outgoing_sn_conns[ip_addr]
	else:
		try:
			conn = rpyc.connect(ip_addr, SN2SN_PORT)
			check_debug("[SN2SN] Storage node connected! IP: " + ip_addr)
			outgoing_sn_conns[ip_addr] = conn
			return conn 
		except:
			check_debug("[SN2SN] Error connecting to Storage Node! IP: " + ip_addr)


class SN2SNService(rpyc.Service):
	def on_connect(self, conn):
		ip_addr = get_ip(conn)
		incoming_sn_conns[ip_addr] = conn
		check_debug("[SN2SN] Storage node connected! IP: ", ip_addr)

	def on_disconnect(self, conn):
		ip_addr = get_ip(conn)
		del incoming_sn_conns[ip_addr]
		del outgoing_sn_conns[ip_addr]
		check_debug("[SN2SN] Storage node disconnected! IP: ", ip_addr)

	def exposed_replicate_receive( record ):
		file_name = "./home/" + record.log_id.str() + "/" + ecord.record_id.str()
		if ( not os.path.exist(file_name) ):
			pickle.dump(record, open(file_name, "wb"))
			check_debug("[REPLICATE] Record added! File: " + file_name)



class LB2SNService(rpyc.Service):
	def on_connect(self, conn):
		ip_addr = get_ip(conn)
		incoming_lb_conns[ip_addr] = conn
		check_debug("[SN2LB] LoadBalancer node connected! IP: ", ip_addr)

	def on_disconnect(self, conn):
		ip_addr = get_ip(conn)
		del incoming_lb_conns[ip_addr]
		del outgoing_lb_conns[ip_addr]
		check_debug("[SN2LB] LoadBalancer node disconnected! IP: ", ip_addr)
		#abort

	def exposed_write_abort( log_id, record_id ):
		folder_name = "./home/"+ log_id.str()
		file_name = folder_name + "/" + record_id.str()
		if ( os.path.exist(file_name) ):
			os.remove(file_name)
			check_debug("[WRITE ABORT] Record deleted! File: " + file_name)

	def exposed_write_commit_request( IP_ADDR, record ):
		conn = outgoing_lb_conns[ IP_ADDR ]
		try:
			folder_name = "./home/"+ record.log_id.str()
			file_name = folder_name + "/" + record.record_id.str()
			if( not os.path.isdir( folder_name ) ):
				os.mkdir( folder_name )
			pickle.dump(record, open(file_name, "wb"))
			check_debug("[WRITE COMMIT] Record added! File: " + file_name)
			conn.root.write_agreed( record.log_id )
		except:
			check_debug("[WRITE COMMIT] Error in commit! File: " + file_name)
			conn.root.write_abort( record )

	def exposed_write_commit( IP_ADDR, record_id ):
		conn = check_SN2CLIENT_conn(IP_ADDR)
		conn.root.write_receive( record_id )

	def exposed_read( IP_ADDR, record_id, log_id ):
		folder_name = "./home/"+ log_id.str()
		file_name = folder_name + "/" + record_id.str()
		if ( os.path.exist(file_name) ):
			record = pickle.load(open(file_name, "rb"))
			conn = check_SN2CLIENT_conn(IP_ADDR)
			check_debug("[READ] Record Send to Client! File: " + file_name + " CLIENT_IP: " + IP_ADDR)
			conn.root.read_receive( record )

	def exposed_replicate( old_ip, new_ip ):
		dirc = "./home/"
		logs = os.walk(dirc)[1]
		records = [  ]
		for log in logs:
			logdirc = './home' + log
			records += [ logdirc + x for x in os.walk(logdirc)[2] ]
		for record in records:
			record_path = './home/' + record
			record_obj = pickle.load(open(record_path, "rb"))
			if(old_ip in record_obj.copy_set):
				record_obj.remove(old_ip)
				record_obj.add(new_ip)
				conn = check_SN2SN_conn(new_ip)
				check_debug("[REPLICATE] Record Send to Storage Node! File: " + file_name + " CLIENT_IP: " + IP_ADDR)
				conn.root.replicate_receive(record_obj)

