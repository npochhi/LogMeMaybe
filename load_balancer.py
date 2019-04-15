import rpyc
import time
import threading
import random
IP_ADDR = "10.109.56.13" # TODO: Set this

incoming_sn_conns = {}
incoming_lb_conns = {}
outgoing_sn_conns = {}
outgoing_lb_conns = {}
outgoing_client_conns = {}

node_set = {}
log_sem = {}
log_counter = {}

num_nodeset = 1
write_set = 1

lb_ips = ["10.145.219.216"]

init_log_agreed = {}
init_log_prev_nodeset = {}
init_log_commit_recv = {}

write_agreed_count = {}
write_commit_received = {}

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True


def get_ip(conn):
    return conn._channel.stream.sock.getpeername()[0]

class SN2LBService(rpyc.Service):
    def on_connect(self, conn):
        ip_addr = get_ip(conn)
        incoming_sn_conns[conn] = ip_addr
        outgoing_sn_conns[rpyc.connect(ip_addr, 50003)] = ip_addr # TODO: Add port
        print("[SN2LB] Storage node connected! IP:", ip_addr)

    def on_disconnect(self, conn):
        ip_addr = incoming_sn_conns[conn]
        del incoming_sn_conns[conn]
        for l_id, nodes in node_set.items():
            if conn in nodes:
                node_set[l_id].remove(conn)
        for out_obj, ip in outgoing_sn_conns.items():
            if ip == ip_addr:
                del outgoing_sn_conns[out_obj]
                break
        print("[SN2LB] Storage node disconnected! IP:", ip_addr)

    def exposed_get_all_sn(self ):
        return incoming_sn_conns.values()

    def exposed_get_all_lb(self):
        return incoming_lb_conns.values()

    def exposed_write_commit_request(self, IP_ADDR, log_id):
        log_counter[log_id] = log_counter[log_id] + 1
        c = outgoing_lb_conns[ip]
        c.root.write_agreed(log_id)

    def exposed_write_agreed(self, log_id):
        write_agreed_count[log_id] = write_agreed_count[log_id] + 1


class LB2LBService(rpyc.Service):
    def on_connect(self, conn):
        ip_addr = get_ip(conn)
        incoming_lb_conns[conn] = ip_addr
        if ip_addr not in outgoing_lb_conns.values():
            out_obj = rpyc.connect(ip_addr, 50000) # TODO: add port
            outgoing_lb_conns[out_obj] = ip_addr
        print("[LB2LB] Load balancer connected! IP:", ip_addr)

    def on_disconnect(self, conn):
        ip_addr = incoming_lb_conns[conn]
        del incoming_lb_conns[conn]
        for out_obj, ip in outgoing_lb_conns.items():
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

    def abort_waiting(self, log_id, dic, fun):
        time.sleep(1)
        if dic[log_id] == False:
            fun(log_id)

    def exposed_init_log_2pc(self, log_id, new_nodes, ip_addr):
        try:
            init_log_commit_recv[log_id] = False
            coor_conn = None
            init_log_prev_nodeset[log_id] = None
            node_set[log_id] = new_nodes
            for conn, conn_addr in outgoing_lb_conns.items():
                if conn_addr == ip_addr:
                    coor_conn = conn
            coor_conn.root.init_log_agree(log_id)
            t1 = threading.Thread(target=self.abort_waiting, args = (log_id, init_log_commit_recv, self.exposed_init_log_abort))
            t1.start()
        except:
            self.exposed_init_log_abort(log_id)

    def exposed_write_commit_request(self, ip, log_id):
        try:
            write_commit_received[log_id] = False
            log_counter[log_id] = log_counter[log_id] + 1
            coor_conn = None
            for conn, conn_ip in outgoing_lb_conns.items():
                if ip == conn_ip:
                    coor_conn = conn
            c = coor_conn
            c.root.write_agreed(log_id)
            t1 = threading.Thread(target=self.abort_waiting, args = (log_id, write_commit_received, self.exposed_write_abort))
            t1.start()
        except:
            self.exposed_write_abort(log_id)


    def exposed_write_agreed(self, log_id):
        write_agreed_count[log_id] = write_agreed_count[log_id] + 1

    def exposed_write_commit(self, log_id):
        write_commit_received[log_id] = True

    def exposed_write_abort(self, log_id):
        log_counter[log_id] = log_counter[log_id] - 1

class Client2LBService(rpyc.Service):
    def on_connect(self, conn):
        ip_addr = get_ip(conn)
        outgoing_client_conns[rpyc.connect(ip_addr, 50006)] = ip_addr # TODO: add port
        print("[Client2LB] Client connected! IP:", ip_addr)

    def on_disconnect(self, conn):
        print("[Client2LB] Client disconnected!")

    def exposed_init_log(self, log_id, ip_addr):
        client_conn = None
        for client, client_ip in outgoing_client_conns.items():
            if ip_addr == client_ip:
                client_conn = client       
        try:
            if log_id in node_set:
                print("[LoadBalancer] Log with this log_id already exists!")
                client_conn.root.commit("[Client] Log with this log_id already exists!")
                return
            print("[LoadBalancer] Log does not exist, creating....")
            init_log_agreed[log_id] = 0
            print(outgoing_sn_conns)
            new_nodes = random.choices(list(outgoing_sn_conns.values()), k=num_nodeset)
            node_set[log_id] = new_nodes
            total_lbs = len(outgoing_lb_conns)

            print(node_set[log_id])

            for conn in outgoing_lb_conns:
                conn.root.init_log_2pc(log_id, new_nodes, IP_ADDR)

            time.sleep(0.5)
            if init_log_agreed[log_id] != total_lbs:
                for conn in outgoing_lb_conns:
                    conn.root.init_log_abort(log_id)
            else:
                for conn in outgoing_lb_conns:
                    conn.root.init_log_commit(log_id)
                client_conn.root.commit("[Client] A log with log id " + str(log_id) + " has been created successfully!")
                log_counter[log_id] = 0
                print("[LoadBalancer] A log with log_id", log_id, "has been created successfully!")

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
        print(node_set)
        init_log_agreed[log_id] = 0

    def write_func(self, client_ip, log_id, data):
        client_conn = None
        for client, c_ip in outgoing_client_conns.items():
            if client_ip == c_ip:
                client_conn = client

            # local transaction
        try:
            copy_set = random.choices(list(node_set[log_id]), k=write_set)
            log_counter[log_id] += 1


            out_conn = len(outgoing_lb_conns) + len(copy_set)

            write_agreed_count[log_id] = 0
            # 2 phase commit
            for conn in outgoing_lb_conns:
                conn.root.write_commit_request(IP_ADDR, log_id)
            print(copy_set)
            print(outgoing_sn_conns.values())
            for ip in copy_set:
                copy_conn = None
                for node_conn, node_ip in outgoing_sn_conns.items():
                    if node_ip == ip:
                        copy_conn = node_conn
                copy_conn.root.write_commit_request(IP_ADDR, log_id, log_counter[log_id], copy_set, data)

            time.sleep(0.5)
            if write_agreed_count[log_id] == out_conn:
                for conn in outgoing_lb_conns:
                    conn.root.write_commit(log_id)
                for copy_ip in copy_set:
                    copy_conn = None
                    for node_conn, node_ip in outgoing_sn_conns.items():
                        if node_ip == ip:
                            copy_conn = node_conn
                    copy_conn.root.write_commit(client_ip, log_id, log_counter[log_id])
                client_conn.root.commit("[Client] Record successfully written! Record ID: " + str(log_counter[log_id]) + " Log ID: " + str(log_id))
                print("[LoadBalancer] Record successfully written! Record ID: " + str(log_counter[log_id]) + " Log ID: " + str(log_id))
            else:
                for conn in outgoing_lb_conns:
                    conn.root.write_abort(log_id)
                for ip in copy_set:
                    copy_conn = None
                    for node_conn, copy_ip in outgoing_sn_conns.items():
                        if copy_ip == ip:
                            copy_conn = node_conn
                    copy_conn.root.write_abort(log_id, log_counter[log_id])
                print("[LoadBalancer] Write failed!")
        except:
            print("[LoadBalancer] Write failed!")
            client_conn.root.commit("[Client] Write failed!")
            for conn in outgoing_lb_conns:
                try:
                    conn.root.write_abort(log_id)
                except:
                    pass
       
    def exposed_read(self, client_ip, record_id, log_id):
        try:
            for node in node_set[log_id]:
                for conn, ip in outgoing_sn_conns.items():
                    if ip == node:
                        conn.root.read(client_ip, record_id, log_id) 
            print("Read successfull !")
        except:
            print("[LoadBalancer] Read failed!")
            client_conn = None
            for client, c_ip in outgoing_client_conns.items():
                if c_ip == client_ip:
                    client_conn = client
            client_conn.root.commit("[Client] Read failed!")


    def exposed_write(self, client_ip, log_id, data):
        if log_id in node_set.keys():
            self.write_func(client_ip, log_id, data) 
        else:
            self.exposed_init_log(log_id, client_ip)
            self.write_func(client_ip, log_id, data)

from rpyc.utils.server import ThreadedServer
import threading

def server_start(service):
    service.start()

if __name__ == "__main__":
    lb2lb = LB2LBService()
    sn2lb = SN2LBService()
    client2lb = Client2LBService()
    lb2lb_service = ThreadedServer(lb2lb, port=50000, protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    sn2lb_service = ThreadedServer(sn2lb, port=50001, protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    client2lb_service = ThreadedServer(client2lb, port=50002, protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t1 = threading.Thread(target=server_start, args=(lb2lb_service,))
    t2 = threading.Thread(target=server_start, args=(sn2lb_service,))
    t3 = threading.Thread(target=server_start, args=(client2lb_service,))
    t1.start()
    t2.start()
    t3.start()
    for ip in lb_ips:
        try:
            outgoing_lb_conns[rpyc.connect(ip, 50000)] = ip
        except:
            pass
