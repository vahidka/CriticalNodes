""" Distributed Algorithm Simulator

Base functions and classes for the Distributed Algorithm Simulator
"""

__author__ = 'C. U. Ileri'

import simpy
import networkx as nx
#import matplotlib.pyplot as plt
import logging
from sys import maxsize
from random import seed, randint, gauss
#from mwmatching import *
#from termcolor import colored
from math import log, ceil, pi, sqrt

seed(456)

class MsgManager(object):
    """ Message Manager

    Manages sending and receiving of all messages.
    Nodes send and receive messages via MsgManager.
    """
    
    delayTime = 1
    
    def __init__(self, env, nodes):
        self.id = "ddd"
        self.env = env
        self.action = env.process(self.run())
        self.messageBox = simpy.Container(env, capacity=100000, init=0)
        self.messages = {}
        self.nodes = nodes       
            
    def add_to_message_queue(self, msg):
        """ Sends message
        
        Actually puts message to the message box
        where it will be forwarded to the receiver
        when the time comes.
        """
        
        timestamp = self.env.now + MsgManager.delayTime;
        
        if not timestamp in self.messages:
            self.messages[timestamp] = []
            
        self.messages[timestamp].append(msg)
        self.messageBox.put(1)
    
    def run(self):
        """ Simulation function
        
        At each timestep, checks messageBox and forwards 
        any message to its receiver.        
        """
        logging.debug("Simulation function starts")
        while True:            
            
            msgToBeSent = []

            # Find messages to be sent NOW
            if self.env.now in self.messages:
                msgToBeSent = self.messages[self.env.now]

            # Put messages to the mailbox of the receiver
            for msg in msgToBeSent:                
                if 'Radio' in msg[2]:
                    # Forward the 'radio' message to all neighbors
                    for n in self.nodes[msg[0]].neighbor.keys():
                        self.forward_message(n,msg)
                        #self.nodes[n].messages.append(msg)
                        #self.nodes[n].mailbox.put(1)                        
                else:
                    # Direct messages are sent only to its receiver
                    # self.nodes[msg[2]['receiver']].messages.append(msg)
                    # self.nodes[msg[2]['receiver']].mailbox.put(1)
                    self.forward_message(msg[2]['receiver'],msg)

            # Delete SENT messages
            if self.env.now in self.messages:
                del(self.messages[self.env.now])
                
            yield self.env.timeout(1) # Go to the next time step

    def forward_message(self, n, msg):
        self.nodes[n].messages.append(msg)
        self.nodes[n].mailbox.put(1)                        


class Synchronizer(object):
    """ Process which checks termination of nodes """
        
    def __init__(self, env, n_nodes):
        self.env = env
        self.action = env.process(self.run())
        self.completedProcesses = simpy.Container(env, capacity = n_nodes, init = 0)
        self.nextRound = simpy.Container(env, capacity = n_nodes, init = 0)
        self.nNodes = n_nodes
        self.nRound = 30 # How many times the nodes will be restarted
        
    def run(self):
        logging.debug("SYNCHRONIZER starts running")
        logging.debug(self.completedProcesses.capacity)
        logging.debug(self.completedProcesses.level)
        
        for i in range(self.nRound):
            with self.completedProcesses.get(self.nNodes) as fin:
                yield fin                
                for i in range(10):
                    logging.info("****000*****")
                    
                # Put tokens to the nextRound container
                self.nextRound.put(self.nNodes)
            

class Node(object):
    """ Node object
    
    Simulates a node on the distributed system
    """
    
    def __init__(self, env, id, msgManager, synchronizer):
        
        # Basic Node variables
        self.id = id
        self.env = env
        self.msgManager = msgManager
        self.messages = [] # Message box
        self.mailbox = simpy.Container(env, capacity=1000, init=0)
        self.synchronizer = synchronizer
        self.neighbor = {} # Neighbors        

        # Additional variables and sets
        self.packetCounter = 0 
        self.lastSentPacket = {} # Holds last sent packet to each node
        self.messageCounter = {} # Counter for each type of message
        self.lastMoveAt = 0
        self.lastMsgTime = 0

        # Set simulation function
        self.action = env.process(self.run())
        
    def run(self):
        """ Simulation function
        
        Distributed algorithm will be written here
        """       
        
        yield self.env.timeout(1)
        
        logging.info('Node %d starts running at %d' % (self.id, self.env.now))

        # Update lastmove variable
        self.lastMoveAt = self.env.now

        # Inform synchronizer about termination
        self.synchronizer.completedProcesses.put(1)
        logging.info("Node %d puts into sync. Current Level: %d" % (self.id, self.synchronizer.completedProcesses.level))        
        return
           
    def add_neighbor(self, node, w):
        self.neighbor[node.id] = (node, w)

    def sync(self, clearMessages = False):
        yield self.env.timeout(1)
        self.synchronizer.completedProcesses.put(1)
                    
        logging.debug("### Node %d waits for sync message" % self.id )
        yield self.synchronizer.nextRound.get(1)
        if clearMessages:
            while self.mailbox.level > 0:
                self.mailbox.get(1)                
                self.receive_message()
        yield self.env.timeout(1)

    def send_message(self, _id, msgdict):
        """ For sending message

        Prepares packet and forward it to the message manager
        """
        
        msgdict['sender'] = self.id
        msgdict['receiver'] = _id
        msgdict['packet'] = self.packetCounter
        
        self.lastSentPacket[_id] = self.packetCounter
        self.packetCounter = self.packetCounter + 1
        
        msg = (self.id, _id, msgdict) # The packet
        
        if self.id != -1: # If not the deamon
            logging.info("-- Node %d sends message to Node %d at time %s. Msg: %s " % (self.id, _id, self.env.now, str(msgdict)))
        # Increase message counter
        if msgdict['type'] in self.messageCounter:
            self.messageCounter[msgdict['type']] += 1
        else:
            self.messageCounter[msgdict['type']] = 1

        self.lastMsgTime = self.env.now

        # Forward the message to the message manager
        self.msgManager.add_to_message_queue(msg)
        if self.mailbox.level != len(self.messages):
            raise Exception("ERROR")
        return msgdict

    def send_radio_message(self, _id, msgdict):
        """ For sending radio message
        
        Adds 'Radio' sign to the packet so that
        the message manager understands that it 
        is a radio message that is to be received
        by all neighbors.
        """
        msgdict['Radio'] = 0        
        return self.send_message(_id, msgdict)

    def receive_message(self):
        """ Reads message on top of the message box.  """
        msg = self.messages[0]
        self.messages.pop(0)        
        return msg

    def delay_message(self, msg):
        """ Neglects message FOR NOW and puts it to the end of the message queue """
        self.messages.append(msg)
        self.mailbox.put(1)

    def get_weight(self, _id):        
        return self.neighbor[_id][1]

class Root(Node):
    """ Dummy central deamon

    Central dummy node which controls synchronization among the system 
    """
    
    def __init__(self, env, id, msgManager,
                 synchronizer, nNodes, system = 'TDMA',
                 roundtime = 10):
        
        Node.__init__(self, env, id, msgManager, synchronizer)
        self.nNodes = nNodes
        self.system = system # TDMA or else

        if self.system == 'TDMA':
            # In case of TDMA: Time-division for a node
            logging.debug("Number of nodes %d" % nNodes)
            self.roundtime = nNodes
        elif self.system == 'TimeSync':
            # In case of TimeSync: Duration of a single round
            self.roundtime = roundtime

    def run(self):
        if self.system == 'TDMA':
            yield self.env.process(self.TDMA())
        elif self.system == 'TimeSync':
            yield self.env.process(self.TimeSync())
 
    def TDMA(self):
        """ TDMA 
        
        Nodes are priviledged one by one starting from 
        node 0 to the last node.
        A single node is priviledged at any time step.
        """
        i = r = 0 # r: round
        while True:
            if i == 0: r += 1
            yield self.env.timeout(1) # Secures  transmission of messages before the next round
            self.send_message(i,{'type': 'ROUND', 'round': r})
            i = (i+1) % self.nNodes
            
            
    def TimeSync(self):
        """ Time synchronization
        
        A round message is send to the nodes after 
        each 'roundtime' time steps
        """
        
        i = r = 0
        while True:
            r += 1
            for i in range(self.nNodes):
                self.send_message(i, {'type': 'ROUND', 'round': r})
            yield self.env.timeout(self.roundtime)        

class Graph(object):
    """ Graph object

    Constructs, holds and controls the whole network: 
    nodes, the message manager, the synchronizer, the deamon
    """
    def __init__(self, env, nodeCount=None, nodeObject = Node, system = 'SYNC',  syncsystem = 'TDMA', capacity = -1):
        """ Constructor
        
        A graph can be initiated as empty (nodeCount == None) or
        with 'nodeCount' nodes.

        If an empty graph is initialized, nodes are to be added 
        one by one or by a loading function such as loadNX.

        If 'nodeCount' is specified, a graph is initialized with 
        'nodeCount' nodes whose ids are from 0 to (nodeCount-1)
        """
        self.env = env
        self.nodes = []
        self.nodeCount = 0
        self.msgManager = MsgManager(self.env, self.nodes)
        self.synchronizer = None
        self.system = system
        self.syncsystem = syncsystem
        self.NodeObject = nodeObject
        self.capacity = capacity
        
        if(nodeCount!=None):            
            self.nodeCount = nodeCount
            self.nodes = []
            self.synchronizer = Synchronizer(self.env, nodeCount)
            for i in range(nodeCount):
                logging.debug("Node %d created by constructor" % i)                
                n = self.NodeObject(self.env, i, self.msgManager, self.synchronizer)
                self.nodes.append(n)
        self.msgManager.nodes = self.nodes

        if self.system == 'ASYNC':
            self.root = None
        else:
            self.root = Root(self.env, -1, self.msgManager, self.synchronizer, self.nodeCount, system = self.syncsystem)
            
        maxround = 0

    def setSystem(self, system, roundTime = 100):
        """ Set the system (TDMA, timesync or else...) """
        if system == 'SYNC': system = 'TimeSync'
        self.root.system = system
        self.root.roundtime = roundTime

    def deliver_message(self, msg):
        """ Takes the packet 'msg' and delivers to the receiver """
        self.nodes[msg['receiver']].messages.append(msg)
        self.nodes[msg['receiver']].mailbox.put(1)                    

    def get_node_list(self):
        """ Returns node list """
        return self.nodes

    def get_node_ids(self):
        """ Returns the list of node ids """        
        #for n in self.nodes:            
        #    l.append(n.id)
        #return l

        return [n.id for n in self.nodes]

    def add_edge(self, n1, n2, w = 1):
        """ Add edge manually """
        self.nodes[n1].add_neighbor(self.nodes[n2], w)
        self.nodes[n2].add_neighbor(self.nodes[n1], w)    

    def get_edge_list_nx(self):
        """ Returns the edge list according to NetworkX format """
        # l = []
        # for n in self.nodes:            
        #     for e in n.neighbor.keys():
        #         if(n.id < e):
        #             l.append((n.id, e, {'weight':n.neighbor[e][1]}))
        l = [(n.id, e, {'weight': n.neighbor[e][1]}) for n in self.nodes for e in n.neighbor.keys()  if n.id < e]
        return l

    def edgeList(self):
        """ Returns the edge list as list of tuples """
        # l = []
        # for n in self.nodes:            
        #     for e in n.neighbor.keys():
        #         if(n.id < e):
        #             l.append((n.id, e, n.neighbor[e][1]))

        l = [(n.id, e, n.neighbor[e][1]) for e in n.neighbor.keys() for n in self.nodes if n.id < e]
                    
        return l

    def load_graph_from_nx(self, nxg):
        """ Gets a NetworkX instance and loads edges from it 

        Usage on empty graphs is strongly recommended
        """
        
        self.synchronizer = Synchronizer(self.env, nxg.number_of_nodes())        

        # Add nodes
        for n in nxg.nodes():            
            logging.debug("Node %d is created from nx" % n)            
            node = self.NodeObject(self.env, n, self.msgManager, self.synchronizer)
            self.nodes.append(node)
            self.nodeCount += 1
	
        # Add edges
        for e in nxg.edges():
            if not 'weight' in nxg[e[0]][e[1]]:
               self.add_edge(e[0], e[1], 1)
            else:
               self.add_edge(e[0], e[1], nxg[e[0]][e[1]]['weight'])
            

        # Set the root, if any
        if self.system == 'SYNC':
            self.root.nNodes = self.nodeCount

        # Update message manager's variables
        self.msgManager.nodes = self.nodes
        self.msgManager.id = "manager" # can be neglected

        logging.info("Graph loaded successfully from networkx instance!")

    def migrate_to_NX(self):
        nxInstance = nx.Graph()
        nxInstance.add_nodes_from(self.get_node_ids())
        nxInstance.add_edges_from(self.get_edge_list_nx())
        return nxInstance
    
# TO BE REMOVED !!!!!!
def writeGraph(hr, N, delta, minw, maxw, iseed):
    """ Write graph according to a specific format """
    
    f = open('.\graphs\graph%d_%d_%d_%d_%d.dat' % (N, delta, minw, maxw, iseed), 'w')
    f.write(str(hr.number_of_nodes()))
    f.write('\n')
    f.write(str(hr.number_of_edges()))
    f.write('\n')
    a = ""
    
    for e in hr.edges():
        f.write("%d %d %d\n" %(e[0], e[1], hr[e[0]][e[1]]['weight']))
    f.close()    
 

