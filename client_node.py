import rpyc
import time
import threading
import random
import os
from record_class import Record

Client2LB_PORT = 65458
Client2SN_PORT = 95648

DEBUG = True

outgoing_lb_conns = {}
incoming_sn_conns = {}
self_ipaddr = ""
ip_addr1 = ""
ip_addr2 = ""

load_balancer_set = [ip_addr1,ip_addr2]  #TODO

try:
	if DEBUG:
		print("Connecting load balancer with ip addr : " + load_balancer_set[0])

	outgoing_lb_conns[load_balancer_set[0]] = rpyc.connect(load_balancer_set[0], Client2LB_PORT)

	if DEBUG:
		print("Connection successfull with load balancer having ip addr : " + load_balancer_set[0])

except:
	print("error connecting load balancer")
	#exit(0)

def get_ip(conn):
	return conn._channel.stream.getpeername()[0]

class SN2ClientService(rpyc.Service):
	def on_connect(self, conn):
		if DEBUG:
			print("Storage node connected with ip addr : " + get_ip(conn))
		pass

	def on_disconnect(self, conn):
		if DEBUG:
			print("Storage node disconnected with ip addr : " + get_ip(conn))
		pass

	def exposed_read_receive(self, record):
		print("Record data : " + record.data)

	def exposed_write_receive(self, record_id):
		print("record successfully written with record id : " + record_id)


class LB2ClientService(rpyc.Service):
	def on_connect(self, conn):
		if DEBUG:
			print("Load Balancer connected with ip addr : " + get_ip(conn))
		pass

	def on_disconnect(self, conn):
		if DEBUG:
			print("Load Balancer disconnected with ip addr : " + get_ip(conn))

		temp = load_balancer_set[0]
		del load_balancer_set[0]
		load_balancer_set.append(temp)
		try:
			if DEBUG:
				print("Connecting load balancer with ip addr : " + load_balancer_set[0])

			outgoing_lb_conns[load_balancer_set[0]] = rpyc.connect(load_balancer_set[0], Client2LB_PORT)

			if DEBUG:
				print("Connection successfull with load balancer having ip addr : " + load_balancer_set[0])

		except:
			print("error connecting load balancer")	
			exit(0)	

	
		def exposed_commit(self, msg):
			print(msg)




while True:
	print("Enter 1 to read, 2 for writing:")
	x = int(input())
	if x == 1:
		print("Enter log_id and record_id : ")
		log_id, record_id = map(int, input().split())
		if DEBUG:
			print("Sending read request to Load Balancer with ip addr : " + load_balancer_set[0])
		outgoing_lb_conns[load_balancer_set[0]].root.read(self_ipaddr, log_id, record_id)
		
	else:
		new_record = Record()
		print("Enter log_id : ")
		new_record.log_id = int(input())
		print("Enter record data : ")
		new_record.data = input()
		if DEBUG:
			print("Sending write request to Load Balancer with ip addr : " + load_balancer_set[0])
		outgoing_lb_conns.conns[load_balancer_set[0]].root.write(self_ipaddr, new_record)