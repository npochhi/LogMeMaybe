import rpyc
import time
import threading
import random
import os

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

Client2LB_PORT = 50002
SN2CL_PORT = 50005
LB2CL_PORT = 50006

DEBUG = True

outgoing_lb_conns = {}
incoming_sn_conns = {}
incoming_lb_conns = {}
self_ipaddr = "10.145.180.194"
ip_addr1 = "10.145.219.216"
ip_addr2 = "localhost"

load_balancer_set = [ip_addr1,ip_addr2]  

def get_ip(conn):
	return conn._channel.stream.sock.getpeername()[0]

def get_key(val, fun, my_dict): 
	for key, value in my_dict.items(): 
		 if val == value: 
			 return key
	if DEBUG:
		print("[ERROR] On_disconnect error! Service: " + fun)
	exit(0) 
	return None

class SN2ClientService(rpyc.Service):
	def on_connect(self, conn):
		incoming_sn_conns[get_ip(conn)] = conn
		if DEBUG:
			print("[SN2Cl] Storage node connected with ip addr : " + get_ip(conn))

	def on_disconnect(self, conn):
		ip_addr = get_key(conn, "SN2ClientService", incoming_sn_conns)
		del incoming_sn_conns[ip_addr]
		if DEBUG:
			print("[SN2Cl] Storage node disconnected with ip addr : " + ip_addr)

	def exposed_read_receive(self, log_id, record_id, data):
		print("[SN2Cl] log_id : " + str(log_id) + "\t record_id : " + str(record_id))
		print(data)

	def exposed_write_receive(self, record_id):
		print("[SN2Cl] record successfully written with record id : " + str(record_id))


class LB2ClientService(rpyc.Service):
	def on_connect(self, conn):
		incoming_lb_conns[get_ip(conn)] = conn
		if DEBUG:
			print("[LB2Cl] Load Balancer connected with ip addr : " + get_ip(conn))

	def on_disconnect(self, conn):
		ip_addr = get_key(conn, "LB2ClientService", incoming_lb_conns)
		del incoming_lb_conns[ip_addr]
		del outgoing_lb_conns[ip_addr]
		if DEBUG:
			print("[LB2Cl] Load Balancer disconnected with ip addr : " + ip_addr)
		temp = load_balancer_set[0]
		del load_balancer_set[0]
		load_balancer_set.append(temp)
		try:
			if DEBUG:
				print("[LB2Cl] Connecting load balancer with ip addr : " + load_balancer_set[0])
			outgoing_lb_conns[load_balancer_set[0]] = rpyc.connect(load_balancer_set[0], Client2LB_PORT)
			if DEBUG:
				print("[LB2Cl] Connection successfull with load balancer having ip addr : " + load_balancer_set[0])
		except:
			print("[LB2Cl] error connecting load balancer")	
			exit(0)	

	def exposed_commit(self, msg):
		print(msg)


def server_start(service):
	service.start()


if __name__ == "__main__":
	sn2cl = SN2ClientService()
	lb2cl = LB2ClientService()
	from rpyc.utils.server import ThreadedServer
	sn2cl_service = ThreadedServer(sn2cl, port=SN2CL_PORT, protocol_config = rpyc.core.protocol.DEFAULT_CONFIG)
	lb2cl_service = ThreadedServer(lb2cl, port=LB2CL_PORT, protocol_config = rpyc.core.protocol.DEFAULT_CONFIG)
	t1 = threading.Thread(target=server_start, args=(sn2cl_service,))
	t2 = threading.Thread(target=server_start, args=(lb2cl_service,))
	t1.start()
	t2.start()
	try:
		if DEBUG:
			print("Connecting load balancer with ip addr : " + load_balancer_set[0])
		outgoing_lb_conns[load_balancer_set[0]] = rpyc.connect(load_balancer_set[0], Client2LB_PORT)
		if DEBUG:
			print("Connection successfull with load balancer having ip addr : " + load_balancer_set[0])
	except:
		print("error connecting load balancer")
		exit(0)
	while True:
		print("Enter 1 to read, 2 for writing:")
		x = int(input())
		if x == 1:
			print("Enter log_id and record_id : ")
			log_id, record_id = map(int, input().split())
			if DEBUG:
				print("Sending read request to Load Balancer with ip addr : " + load_balancer_set[0])
			outgoing_lb_conns[load_balancer_set[0]].root.read(self_ipaddr, record_id, log_id)
		else:
			print("Enter log_id : ")
			log_id = int(input())
			print("Enter record data : ")
			data = input()
			if DEBUG:
				print("Sending write request to Load Balancer with ip addr : " + load_balancer_set[0])
			outgoing_lb_conns[load_balancer_set[0]].root.write(self_ipaddr, log_id, data)