import rpyc
import time
import threading
import random
import os

Client2LB_PORT = 65458
Client2SN_PORT = 95648

outgoing_lb_conns = {}
incoming_sn_conns = {}
self_ipaddr = ""
ip_addr1 = ""
ip_addr2 = ""

load_balancer_set = [ip_addr1,ip_addr2]  #TODO

try:
	outgoing_lb_conns[ip_addr1] = rpyc.connect(ip_addr1, Client2LB_PORT)
except:
	print("error connecting load balancer")
	exit(0)

def get_ip(conn):
	return conn._channel.stream.getpeername()[0]

class record:
	def __init__(self):
		self.log_id = -1
		self.record_id = -1
		self.copy_set = []
		self.data = ""



class SN2ClientService(rpyc.Service):
	def on_connect(self, conn):
		pass

	def on_disconnect(self, conn):
		pass

	def exposed_read_receive(self, record):
		print(record.data)

	def exposed_write_receive(self, record_id):
		print("record successfully written with record id : " + record_id)


while True:
	print("Enter 1 to read, 2 for writing:")
	x = int(input())
	if x == 1:
		print("Enter log_id and record_id : ")
		log_id, record_id = int(input().split())
		try:
			outgoing_lb_conns[load_balancer_set[0]].root.read(self_ipaddr, log_id, record_id)
		except:
			temp = load_balancer_set[0]
			del load_balancer_set[0]
			load_balancer_set.append(temp)
			outgoing_lb_conns[load_balancer_set[0]].root.read(self_ipaddr, log_id, record_id)

	else:
		new_record = record()
		print("Enter log_id : ")
		new_record.log_id = int(input())
		print("Enter record data : ")
		new_record.data = input()
		outgoing_lb_conns.conns[load_balancer_set[0]].root.write(self_ipaddr, new_record)