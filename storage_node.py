import rpyc
import time
import threading
import random
import os
import pickle
import shutil

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

SN2LB_PORT = 50001
SN2CLIENT_PORT = 50005
SN2SN_PORT = 50004
LB2SN_PORT = 50003

DEBUG = True

incoming_lb_conns = {}
outgoing_lb_conns = {}
outgoing_client_conns = {}
incoming_sn_conns = {}
outgoing_sn_conns = {}
is_commit_received = {}

def check_debug( msg ):
	if(DEBUG == True):
		print(msg + "\n")

load_balancer_set = ['10.145.219.216']  #TODO

def get_ip(conn):
	return conn._channel.stream.sock.getpeername()[0]

def get_key(val, fun, my_dict): 
	for key, value in my_dict.items(): 
		 if val == value: 
			 return key
	check_debug("[ERROR] On_disconnect error! Service: " + fun)
	exit(0) 
	return None

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

def start_service_thread(service):
	service.start()


#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
class Record:
	def __init__(self, log_id, record_id, copy_set, data):
		self.log_id = log_id
		self.record_id = record_id
		self.copy_set = copy_set
		self.data = data

	def set_copy_set(self, copy_set):
		self.copy_set = copy_set

	def set_record_id(self, record_id):
		self.record_id = record_id

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
class SN2SNService(rpyc.Service):
	def on_connect(self, conn):
		ip_addr = get_ip(conn)
		incoming_sn_conns[ip_addr] = conn
		check_debug("[SN2SN] Storage node connected! IP: " +  ip_addr)

	def on_disconnect(self, conn):
		ip_addr = get_key(conn, "SN2SN", incoming_sn_conns)
		del incoming_sn_conns[ip_addr]
		del outgoing_sn_conns[ip_addr]
		check_debug("[SN2SN] Storage node disconnected! IP: " + ip_addr)

	def exposed_replicate_receive( self, log_id, record_id, copy_set, data ):
		record = Record(log_id, record_id, copy_set, data)
		file_name = "./home/" + str(record.log_id) + "/" + str(record.record_id)
		if ( not os.path.exists(file_name) ):
			pickle.dump(record, open(file_name, "wb"))
			check_debug("[REPLICATE] Record added! File: " + file_name)

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class LB2SNService(rpyc.Service):
	def on_connect(self, conn):
		ip_addr = get_ip(conn)
		incoming_lb_conns[ip_addr] = conn
		check_debug("[LB2SN] LoadBalancer node connected! IP: " + ip_addr)

	def on_disconnect(self, conn):
		ip_addr = get_key(conn, "LB2SN", incoming_lb_conns)
		del incoming_lb_conns[ip_addr]
		del outgoing_lb_conns[ip_addr]
		check_debug("[LB2SN] LoadBalancer node disconnected! IP: " + ip_addr)
		#abort

	def abort_waiting(self, log_id, record_id):
		time.sleep(1)
		if is_commit_received[log_id] == False:
			self.exposed_write_abort(log_id, record_id)

	def exposed_write_abort( self, log_id, record_id ):
		folder_name = "./home/"+ str(log_id)
		file_name = folder_name + "/" + str(record_id)
		if ( os.path.exists(file_name) ):
			os.remove(file_name)
			check_debug("[WRITE ABORT] Record deleted! File: " + file_name)

	def exposed_write_commit_request( self, IP_ADDR, log_id, record_id, copy_set, data ):
		record = Record(log_id, record_id, copy_set, data)
		conn = outgoing_lb_conns[ IP_ADDR ]
		folder_name = "./home/"+ str(record.log_id)
		file_name = folder_name + "/" + str(record.record_id)
		try:
			if( not os.path.isdir( folder_name ) ):
				os.mkdir( folder_name )
			pickle.dump(record, open(file_name, "wb"))
			check_debug("[WRITE COMMIT REQUEST] Record added! File: " + file_name)
			is_commit_received[log_id] = False
			conn.root.write_agreed( record.log_id )
			t1 = threading.Thread(target=self.abort_waiting, args = (log_id, record_id))
			t1.start()
		except:
			check_debug("[WRITE COMMIT REQUEST] Error in commit! File: " + file_name)

	def exposed_write_commit( self, IP_ADDR, log_id, record_id ):
		check_debug("[WRITE COMMIT] Commit received log_id, record_id: " + str(log_id)+ ", " + str(record_id))
		is_commit_received[log_id] = True
		conn = check_SN2CLIENT_conn(IP_ADDR)
		conn.root.write_receive( record_id )

	def exposed_read( self, IP_ADDR, record_id, log_id ):
		folder_name = "./home/"+ str(log_id)
		file_name = folder_name + "/" + str(record_id)
		try:
			if ( os.path.exists(file_name) ):
				record = pickle.load(open(file_name, "rb"))
				conn = check_SN2CLIENT_conn(IP_ADDR)
				conn.root.read_receive( record.log_id, record.record_id, record.data )
				check_debug("[READ] Record Send to Client! File: " + file_name + " CLIENT_IP: " + IP_ADDR)
		except:
			check_debug("[READ] Failed to send record to Client! File: " + file_name + " CLIENT_IP: " + IP_ADDR)

	def exposed_replicate( self, old_ip, new_ip ):
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
				conn.root.replicate_receive(record_obj.log_id, record_obj.record_id, record_obj.copy_set, record_obj.data)

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

if __name__ == "__main__":
	try:
		# if os.path.exists("./home"):
		# 	shutil.rmtree('./home')
		# os.mkdir("./home")
			
		sn2sn = SN2SNService()
		lb2sn = LB2SNService()
		from rpyc.utils.server import ThreadedServer
		sn2sn_service = ThreadedServer(sn2sn, port=SN2SN_PORT, protocol_config = rpyc.core.protocol.DEFAULT_CONFIG)
		lb2sn_service = ThreadedServer(lb2sn, port=LB2SN_PORT, protocol_config = rpyc.core.protocol.DEFAULT_CONFIG)
		t1 = threading.Thread(target=start_service_thread, args = (sn2sn_service,)) 
		t2 = threading.Thread(target=start_service_thread, args = (lb2sn_service,))
		t1.start()
		t2.start()

		for ip_addr in load_balancer_set:
			try:
				outgoing_lb_conns[ip_addr] = rpyc.connect(ip_addr, SN2LB_PORT)
				check_debug("[SN2LB] LoadBalancer node connected! IP: " + ip_addr)
			except:
				check_debug("[SN2LB] Error connecting to LoadBalancer! IP: " + ip_addr)

		t1.join()
		t2.join()
	except KeyboardInterrupt:
		print("[EXIT] Server Closing\n")
		exit(0)