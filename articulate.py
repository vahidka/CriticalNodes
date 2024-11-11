
#this version has claim problem when a claimed node send message to previously claimed node.
#this happen because of rule 3

from distSimulator import *
from array import *
import timeit
import sys
import collections
import os
import networkx as nx

sentbytes=0;
receivedbytes=0;
CurrentCompSize=0;
k=0;
SentHello=0

NODECOUNT = 250
UNDISCOVERED = False
DISCOVERD = True
UNVISITED = 0
VISITED = 1
SON = 2
FORWARD = 3
FATHER = 4
BRIDGE_FATHER = 5
BACKTRACKED = 6
CLOSED = 7
BRIDGE_SON = 8
INFORM = 9
HELLO = 10
MAXVAL = 32000
packetSize=4;
class DNode(Node):
    def __init__(self, *args):
        Node.__init__(self, *args)
        self.state=UNDISCOVERED;
        self.SenderBusy=self.articulation=self.root=False;
        self.low=self.depth=MAXVAL;
        self.links=[]
        self.block_ids=[]
        self.critical=False;
        self.firstHello=True;
       

    def broadcast(self,typ,value):
        global sentbytes;
        sentbytes+=packetSize;
        for i in self.neighbor.keys():
            self.send_message(i, {'type': typ, 'value':value});
		
	
    def unicast(self,target,typ,value):
        global sentbytes;
        sentbytes+=packetSize;
        self.send_message(target, {'type': typ, 'value':value});
        
    def multiCast(self, typ, d,destype):
        global sentbytes;
        for tuple_item in self.links:
            target = tuple_item[0]
            state = tuple_item[1]
            if state==destype:
                sentbytes+=packetSize;
                self.send_message(target, {'type': typ, 'value':d});

    def receive_visited_message(self,l,p_depth):
        t=self.links[l]
        if t[1]==UNVISITED or t[1]==SON:
            if p_depth!=-1 and p_depth<self.low: self.low=p_depth;
            self.restart(l);


    def receive_forward_message(self,l,p_depth):
        if self.state == UNDISCOVERED:
            self.state=DISCOVERD;
            t=self.links[l]
            self.links[l]=(t[0],FATHER)
            self.depth=p_depth+1;
            if self.low == p_depth: self.low=self.depth;
            self.search();
            self.multiCast('VISITED',-1,VISITED);
            self.multiCast('VISITED',self.depth,UNVISITED);
        else:
            
            self.restart(l);

    def receive_inform_message(self,b_id):
       if b_id in self.block_ids: 
            self.block_ids.append(b_id);
            self.multiCast('INFORM',b_id,BACKTRACKED);
      
    def receive_backward_message(self,l,son_low):
        global sentbytes;
        t=self.links[l]
        if t[1] == SON:
            if self.depth <= son_low:
                if self.depth<son_low:
                    self.links[l]=(t[0],BRIDGE_SON)
                else:
                    self.links[l]=(t[0],CLOSED)
                self.articulation=True;
                self.AlgFinish=1;
                self.block_ids.append(self.id);
                if self.root==False or len(self.block_ids)>1:
                    self.send_message(t[0], {'type': 'INFORM', 'value':self.id});
                    sentbytes+=packetSize;

                else: self.links[l]=(t[0],BACKTRACKED)
            if son_low<self.low: self.low=son_low;
            self.search();

    def restart(self,l):
        target = self.links[l][0]
        state = self.links[l][1]
        if state==UNVISITED: 
            self.links[l]=(target,VISITED)
        elif state==SON: 
            self.links[l]=(target,VISITED)
            self.search();

    
    def search(self):
        global sentbytes;
        for index, (target,state) in enumerate(self.links):
            if state==UNVISITED:
                self.links[index]=(target,SON)
                self.send_message(target, {'type': 'FORWARD', 'value':self.depth});
                sentbytes+=packetSize;
                return;
        if self.root==True:
            if len(self.block_ids)==1:
                self.articulation=False;
            self.AlgFinish=1;
        else:
    	    for index, (target,state)  in enumerate(self.links):
                if state==FATHER: 
                    if self.depth< self.low:  self.low=self.depth;
                    if self.low== self.depth:
                        self.links[index]=(target,BRIDGE_FATHER)
                    self.send_message(target, {'type': 'BACKTRACKED', 'value':self.low});
                    sentbytes+=packetSize;
                    





    def run(self):
        global receivedbytes,sentbytes,totalPath,k,g,SentHello,n;
        if self.id==0:
            self.root=True;
            self.firstHello=False;
            self.broadcast('HELLO',0);
            SentHello=1
        while(True):
            yield self.mailbox.get(1)
            [v, u, msg] = self.receive_message()
            receivedbytes+=5;          
            #print(v,"-->",u,msg)
            if msg['type'] == 'HELLO':
                self.links.append((v,UNVISITED));
                if self.firstHello==True: 
                    self.broadcast('HELLO',0);
                    SentHello=SentHello+1
                    self.firstHello=False;
                    if n==SentHello:
                        self.send_message(0, {'type': 'START', 'value':0});
            index = next((i for i, t in enumerate(self.links) if t[0] == v), -1)
            if msg['type'] == 'FORWARD':
                self.receive_forward_message(index,msg['value']);
            elif msg['type'] == 'BACKTRACKED': self.receive_backward_message(index,msg['value']);
            elif msg['type'] == 'VISITED': self.receive_visited_message(index,msg['value']);
            elif msg['type'] == 'INFORM': self.receive_inform_message(msg['value']);
            elif msg['type'] == 'START':
                self.state=DISCOVERD;
                self.depth=self.low=0;
                self.search();
                self.multiCast('VISITED',self.depth,UNVISITED);
                
for filename in os.listdir(os.getcwd()+"/topo_final/"):
	for k in range(5,21,5):
		with open(os.path.join(os.getcwd()+"/topo_final/", filename), 'r') as f: # open in readonly mode
			line=f.readline();
			filenames=filename.split('_');
			print(filename+","+str(k)+",",end='')
			n=int(line.split()[0])
			optlist=f.readline(); # read critical nodes list
			env = simpy.Environment()
			g = Graph(env, nodeCount =n, nodeObject = DNode)
			G2= nx.path_graph(3)
			sentbytes=0;
			receivedbytes=0;

			g.setSystem('ASYNC') 
			for line in f: # read rest of lines
				x= int(line.split()[0])
				y= int(line.split()[1])
				g.add_edge(x,y)
				G2.add_edge(x,y)
			
			env.run(500000)
			result=[];
			for x in g.nodes:
				if x.articulation==True:
					result.append(x.id)
			result.sort();
			crticset=result[0:min(k,len(result))]
			print(" ".join(str(i) for i in crticset),end='');
			print(","+str(sentbytes)+","+str(receivedbytes),end='');
			for node in crticset:
				G2.remove_node(node) 
			num_components = nx.number_connected_components(G2)
			print(','+str(num_components))
			f.close();