import rpyc
import time
import threading
import random

ns_sem = threading.Semaphore()

incoming_sn_conns = {}
incoming_lb_conns = {}
outgoing_sn_conns = {}
outgoing_lb_conns = {}

node_set = {}
log_sem = {}
log_counter = {}

num_nodeset = 2
write_set = 2

def get_ip(conn):
	return conn._channel.stream.getpeername()[0]

class SN2LBService(rpyc.Service):
	def on_connect(self, conn):
		ip_addr = get_ip(conn)
		incoming_sn_conns[conn] = ip_addr
		print("[SN2LB] Storage node connected! IP:", ip_addr)

	def on_disconnect(self, conn):
		ip_addr = incoming_sn_conns[conn]
		del incoming_sn_conns[conn]
		ns_sem.acquire()
		for l_id, nodes in node_set:
			if conn in nodes:
				node_set[l_id].remove(conn)
		for out_obj, ip in outgoing_sn_conns:
			if ip == ip_addr:
				del outgoing_sn_conns[out_obj]
				break
		ns_sem.release()
		print("[SN2LB] Storage node disconnected! IP:", sn_conns[conn])

	def exposed_get_all_sn(self):
		return incoming_sn_conns.values()

	def exposed_get_all_lb(self):
		return incoming_lb_conns.values()

class LB2LBService(rpyc.Service):
	def __init__(self, **kwargs):
		self.token = 0
		super(LB2LBService, self).__init__(**kwargs)

	def on_connect(self, conn):
		ip_addr = get_ip(conn)
		incoming_lb_conns[conn] = ip_addr
		out_obj = rpyc.connect(ip_addr, "") # TODO: add port
		outgoing_lb_conns[out_obj] = ip_addr
		print("[LB2LB] Load balancer connected! IP:", ip_addr)

	def on_disconnect(self, conn):
		ip_addr = lb_conns[conn]
		del incoming_lb_conns[conn]
		for out_obj, ip in outgoing_lb_conns:
			if ip == ip_addr:
				del outgoing_lb_conns[out_obj]
				break
		print("[LB2LB] Load balancer disconnected! IP:", ip_addr)

	def exposed_get_all_sn(self):
		return incoming_sn_conns.values()

	def exposed_get_all_lb(self):
		return incoming_lb_conns.values()

	def exposed_get_stamp(self):
		pass

	def exposed_get_token(self):
		pass

class Client2LBService(rpyc.Service):
	def on_connect(self, conn):
		print(conn._channel.stream.sock.getpeername())
		print("Connected!")

	def on_disconnect(self, conn):
		print("Disconnect!")

	def init_log(self, log_id):
		log_counter[log_id] = 0
		log_sem[log_id] = threading.Semaphore()
		node_set[log_id] = random.choices(incoming_sn_conns.values(), num_nodeset)
 
	def exposed_read(self, ip_addr, record_id, log_id):
		for node in node_set[log_id]:
			if node not in outgoing_sn_conns.values():
				c = rpyc.connect(node, 40000)
				outgoing_sn_conns[c] = node
				c.root.read(ip_addr, record_id, log_id)
			else:
				for ip in outgoing_sn_conns.values():
					if ip == node:
						c = outgoing_sn_conns[node]
						c.root.read(ip_addr, record_id, log_id)


	def exposed_write(self, ip_addr, record, log_id):
		# record counter
		if log_id in node_set.keys():
			for node in random.choices(node_set[log_id], write_set):
				if node not in outgoing_sn_conns.values():
					c = rpyc.connect(node, 40000)
					outgoing_sn_conns[c] = node
					c.root.write(ip_addr, record_id, log_id)
				else:
					for ip in outgoing_sn_conns.values():
						if ip == node:
							c = outgoing_sn_conns[node]
							c.root.write(ip_addr, record_id, log_id)

		else:
			self.init_log(log_id)
