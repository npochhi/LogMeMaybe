import rpyc
import time
import threading
import random

IP_ADDR = "10.145.219.216" # TODO: Set this

incoming_sn_conns = {}
incoming_lb_conns = {}
outgoing_sn_conns = {}
outgoing_lb_conns = {}
outgoing_client_conns = {}

node_set = {}
log_sem = {}
log_counter = {}

num_nodeset = 2
write_set = 2

NUM_NODESET = 2
NUM_COPYSET = 2

init_log_agreed = {}
init_log_prev_nodeset = {}
init_log_commit_recv = {}

def get_ip(conn):
    return conn._channel.stream.getpeername()[0]

class SN2LBService(rpyc.Service):
    def on_connect(self, conn):
        ip_addr = get_ip(conn)
        incoming_sn_conns[conn] = ip_addr
        outgoing_sn_conns[rpyc.connect(ip_addr, 50003)] = ip_addr # TODO: Add port
        print("[SN2LB] Storage node connected! IP:", IP_addr)

    def on_disconnect(self, conn):
        ip_addr = incoming_sn_conns[conn]
        del incoming_sn_conns[conn]
        for l_id, nodes in node_set:
            if conn in nodes:
                node_set[l_id].remove(conn)
        for out_obj, ip in outgoing_sn_conns:
            if ip == ip_addr:
                del outgoing_sn_conns[out_obj]
                break
        print("[SN2LB] Storage node disconnected! IP:", sn_conns[conn])

    def exposed_get_all_sn(self):
        return incoming_sn_conns.values()

    def exposed_get_all_lb(self):
        return incoming_lb_conns.values()

    def exposed_write_commit_reqest(IP_ADDR, log_id):
        counter_semaphore[log_id].acquire()
        log_counter[log_id] = log_counter[log_id] + 1
        counter_semaphore[log_id].release()
        c = outgoing_lb_conns[ip]
        c.root.write_agreed(log_id)

    def exposed_write_agreed(log_id):
        write_agreed_count[log_id] = write_agreed_count[log_id] + 1

    def exposed_write_commit(log_id):
        write_commit_received[log_id] = true

    def exposed_write_abort(record):
        counter_semaphore[log_id].acquire()
        log_counter[log_id] = log_counter[log_id] - 1
        counter_semaphore[log_id].release()

class LB2LBService(rpyc.Service):
    def __init__(self, **kwargs):
        self.token = 0
        super(LB2LBService, self).__init__(**kwargs)

    def on_connect(self, conn):
        ip_addr = get_ip(conn)
        incoming_lb_conns[conn] = ip_addr
        out_obj = rpyc.connect(ip_addr, 50000) # TODO: add port
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

    def exposed_init_log_agree(self, log_id):
        init_log_agreed[log_id] += 1

    def exposed_init_log_commit(self, log_id):
        init_log_commit_recv[log_id] = True

    def exposed_init_log_abort(self, log_id):
        node_set[log_id] = init_log_prev_nodeset[log_id]

    def exposed_init_log_2pc(self, log_id, new_nodes, ip_addr):
        try:
            init_log_commit_recv[log_id] = False
            coor_conn = None
            node_set[log_id] = new_nodes
            init_log_prev_nodeset
            for conn, conn_addr in outgoing_lb_conns.items():
                if conn_addr == ip_addr:
                    coor_conn = outgoing_lb_conns[conn_addr]
            coor_conn.init_log_agree(log_id)
            time.sleep(0.5)
            if init_log_commit_recv[log_id] == False:
                self.exposed_init_log_abort(log_id)
        except:
            self.exposed_init_log_abort(log_id)

    def exposed_write_commit_reqest(ip, log_id):
        try:
            write_commit_received[log_id] = False
            log_counter[log_id] = log_counter[log_id] + 1
            coor_conn = None
            for conn, conn_ip in outgoing_lb_conns.items():
                if ip == conn_ip:
                    coor_conn = conn
            c = outgoing_lb_conns[coor_conn]
            c.root.write_agreed(log_id)
            time.sleep(0.5)
            if write_commit_received[log_id] == False:
                self.exposed_write_abort(log_id)
        except:
            self.exposed_write_abort(log_id)


    def exposed_write_agreed(log_id):
        write_agreed_count[log_id] = write_agreed_count[log_id] + 1

    def exposed_write_commit(log_id):
        write_commit_received[log_id] = True

    def exposed_write_abort(log_id):
        log_counter[log_id] = log_counter[log_id] - 1

class Client2LBService(rpyc.Service):
    def on_connect(self, conn):
        ip_addr = get_ip(conn)
        outgoing_client_conns[rpyc.connect(ip_addr, 50005)] = ip_addr # TODO: add port
        print("[Client2LB] Client connected! IP:", conn._channel.stream.sock.getpeername())

    def on_disconnect(self, conn):
        print("[Client2LB] Client disconnected!")

    def exposed_init_log(self, log_id, ip_addr):
        client_conn = None
        for client, client_ip in outgoing_client_conns:
            if ip_addr == client_ip:
                client_conn = client
        try:
            if log_id in node_set:
                print("[LoadBalancer] Log with this log_id already exists!")
                client_conn.root.commit("[Client] Log with this log_id already exists!")

            init_log_agreed[log_id] = 0

            new_nodes = random.choices(outgoing_sn_conns.values(), num_nodeset)
            total_lbs = len(outgoing_lb_conns)

            for conn in outgoing_lb_conns:
                conn.root.init_log_2pc(log_id, new_nodes, IP_ADDR)

            time.sleep(0.5)

            if init_log_agreed != total_lbs:
                for conn in outgoing_lb_conns:
                    conn.root.init_log_abort(log_id)
            else:
                for conn in outgoing_lb_conns:
                    conn.root.init_log_commit(log_id)
                client_conn.root.commit("[Client] A log with log id", log_id, "has been created successfully!")
        except:
            try:
                print("[LoadBalancer] Log creation failed!")
                client_conn.root.commit("[Client] Log creation failed!")
                for conn in outgoing_lb_conns:
                    try:
                        conn.root.init_log_abort(log_id)
                    except:
                        pass
            except:
                pass
        init_log_agreed[log_id] = 0

    def write_func(client_ip, record):
        try:
            # local transaction
            copy_set = random.choices(node_set[log_id], write_set)

            record.set_copy_set(copy_set)
            log_counter[log_id] += 1
            record.set_id(log_counter[log_id])

            out_conn = len(outgoing_lb_conns) + len(copy_set)

            # 2 phase commit
            for conn in outgoing_lb_conns:
                conn.root.write_commit_reqest(IP_ADDR, record.log_id)
            for ip in copy_set:
                copy_conn = None
                for node_ip, node_conn in outgoing_lb_conns.items():
                    if node_ip == ip:
                        copy_conn = node_conn
                copy_conn.root.write_commit_reqest(IP_ADDR, record)

            time.sleep(0.5)
            if write_agreed_count[log] == out_conn:
                for conn in outgoing_lb_conns:
                    conn.root.write_commit(record.log_id)
                for copy_ip in copy_set:
                    copy_conn = None
                    for node_ip, node_conn in outgoing_lb_conns.items():
                        if node_ip == ip:
                            copy_conn = node_conn
                    copy_conn.root.write_commit(client_ip, record)
                    client_conn = None
                    for client, client_ip in outgoing_client_conns:
                        if ip_addr == client_ip:
                            client_conn = client
                    client_conn.root.commit("[Client] Record successfully written! Record ID:", record.log_id, "Log ID:", log_id)
            else:
                for conn in outgoing_lb_conns:
                    conn.root.write_abort(record.log_id)
                for ip in copy_set:
                    copy_conn = None
                    for copy_ip, node_conn in outgoing_lb_conns.items():
                        if copy_ip == ip:
                            copy_conn = node_conn
                    copy_conn.root.write_abort(record)
        except:
            print("[LoadBalancer] Write failed!")
            client_conn = None
            for client, client_ip in outgoing_client_conns:
                if ip_addr == client_ip:
                    client_conn = client
            client_conn.root.commit("[Client] Write failed!")
            for conn in outgoing_lb_conns:
                try:
                    conn.root.write_abort(log_id)
                except:
                    pass
        write_agreed_count[log_id] = 0
       
    def exposed_read(self, client_ip, record_id, log_id):
        try:
            for node in node_set[log_id]:
                for ip in outgoing_sn_conns.values():
                    if ip == node:
                        c = outgoing_sn_conns[node]
                        c.root.read(client_ip, record_id, log_id) 
        except:
            print("[LoadBalancer] Read failed!")
            client_conn = None
            for client, c_ip in outgoing_client_conns.items():
                if c_ip == client_ip:
                    client_conn = client
            client_conn.root.commit("[Client] Read successful!")


    def exposed_write(self, client_ip, record):
        if log_id in node_set.keys():
            write_func(client_ip, record) 
        else:
            self.init_log(log_id)
            write_func(client_ip, record)

if __name__ == "__main__":
    lb2lb = LB2LBService()
    sn2lb = SN2LBService()
    client2lb = Client2LB()
    from rpyc.utils.server import ThreadedServer
    lb2lb_service = ThreadedServer(lb2lb, port=50000)
    sn2lb_service = ThreadedServer(sn2lb, port=50001)
    client2lb_service = ThreadedServer(client2lb, port=50002)
    lb2lb.start()
    sn2lb.start()
    client2lb.start()
