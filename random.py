
#this version has claim problem when a claimed node send message to previously claimed node.
#this happen because of rule 3

from distSimulator import *
from array import *
import timeit
import sys
import collections
import os
import numpy as np

sentbytes=0;
receivedbytes=0;
CurrentCompSize=0;
k=0;


class DNode(Node):
	def __init__(self, *args):
		Node.__init__(self, *args)
		self.pu = -1;
		self.au =1000;
		self.ru =0;
		self.ku =0;
		self.lambdau =1;
		self.Cu=dict();
		self.Tu=set();
		self.Bu=dict();

	def neighbors_in(self,stat):
			return set(key for key in self.Su.keys() if 1 in [self.Su[key].get(lab) for lab in stat]);
	def neighbors_notin(self,stat):
			return set(key for key in self.Su.keys() if 1 not in [self.Su[key].get(lab) for lab in stat]);




	def broadcast(self,type,value):
		global sentbytes;
		if isinstance(value,int): sz=2;
		else: sz=1+len(value[1]);
		sentbytes+=sz*2;
		
		#print("Node %d broadcast %s(%s).  size:%d" % (self.id, type,value,sz));
		for i in self.neighbor.keys():
			self.send_message(i, {'type': type, 'value':value});
		
	
	def unicast(self,target,type,value):
		global sentbytes;
		if isinstance(value,int): sz=2;
		else: sz=len(value)*2;
		sentbytes+=sz;
		
		#print("Node %d send %s(%s) to %d. size:%d" % (self.id, type,value,target,sz));
		self.send_message(target, {'type': type, 'value':value});
			
		
	def Progress(self):
		if len(self.neighbor.keys())==len(self.Tu):
			self.Done();
		else:
			for w in self.neighbor.keys(): 
				if w not in self.Tu:
					#print("BRDC Next: "+str(self.id)+" "+str((self.ku,w,self.ru))+"    pu:"+str(self.pu)+"  au"+str(self.au)+"   ru"+str(self.ru)+"   lambdau"+str(self.lambdau)+"   Cu"+str(self.Cu)+"   Tu"+str(self.Tu)+"   Bu"+str(self.Bu));
					self.broadcast('Join',w);
					break;

	def Done(self):
			#print("BRDC Return: "+str(self.id)+" "+str((self.au,self.lambdau,self.Bu))+"    pu:"+str(self.pu)+"  au"+str(self.au)+"   ru"+str(self.ru)+"   lambdau"+str(self.lambdau)+"   Cu"+str(self.Cu)+"   Tu"+str(self.Tu)+"   Bu"+str(self.Bu));
			if self.pu!=self.id:
				self.broadcast('Token',(self.pu,self.Bu));
			else:
				for v in self.Bu.keys():
					#print(str(v)+":"+str(self.Bu[v])+"  ",end = '');
					print(str(v)+"  ",end = '');

			
		
	""" This method describes what a node does"""
	def run(self):
		global receivedbytes,sentbytes,totalPath,k,g;
		# random deger girilecek
		self.Bu[self.id]=np.random.randint(0,10001);#len(self.neighbor.keys());
		
		
		if self.id == 0:
			self.pu=self.id;
			self.ku =k;
			self.Progress();
			
			
			
		while(True):
			yield self.mailbox.get(1)
			[v, u, msg] = self.receive_message()	
#-----------------------------------------------------------  Discovering Minimum Cut Size
			if msg['type'] == 'Join' :
				self.Tu.add(v);
				w=msg['value']
				receivedbytes=receivedbytes+2;
				if w==u:
					self.pu=v;
					self.ku=k;
					#print("RECV Next from "+str(v)+" in "+str(u)+" : "+str(msg['value'])+"    pu:"+str(self.pu)+"  au"+str(self.au)+"   ru"+str(self.ru)+"   lambdau"+str(self.lambdau)+"   Cu"+str(self.Cu)+"   Tu"+str(self.Tu)+"   Bu"+str(self.Bu));
					self.Progress();
				#else:
					#print("RECV Next from "+str(v)+" in "+str(u)+" : "+str(msg['value'])+"    pu:"+str(self.pu)+"  au"+str(self.au)+"   ru"+str(self.ru)+"   lambdau"+str(self.lambdau)+"   Cu"+str(self.Cu)+"   Tu"+str(self.Tu)+"   Bu"+str(self.Bu));
			
							
			elif msg['type'] == 'Token' :
				self.Tu.add(v);
				(w,B)=msg['value']
				receivedbytes=receivedbytes+2+len(B);
				if w==self.id:
					self.Bu.update(B);
					while len(self.Bu)>self.ku :
						sp= min(self.Bu.values());
						t=list(self.Bu.keys())[list(self.Bu.values()).index(sp)]
						self.Bu.pop(t, None)
					self.Progress();
				
	


#outfile=open("dfsout.txt", 'w');
for filename in os.listdir(os.getcwd()+"/topo_final/"):
	for k in range(5,21,5):
		with open(os.path.join(os.getcwd()+"/topo_final/", filename), 'r') as f: # open in readonly mode
			line=f.readline();
			print(filename+" , "+str(k)+" , ", end = '')
			nn=int(line.split()[0])
			optlist=f.readline(); # read critical nodes list
			env = simpy.Environment()
			g = Graph(env, nodeCount =nn, nodeObject = DNode)
			# Sistemin asenkron calisacagini belirtiyoruz:
			g.setSystem('ASYNC') 
			for line in f: # read rest of lines
				x= int(line.split()[0])
				y= int(line.split()[1])
				g.add_edge(x,y)
			#print("n:%d   d:%d  MinCutSize:%d   CutCount:%d    MyPath:%d     TradPath:%d" % (n,fileinfoint[1],graph.c, len(graph.mincut), graph.pathcount,fileinfoint[6]))
			#outfile.write("%d,%d,%d,%d,%d,%d\n" % (nn,fileinfoint[1],graph.c, len(graph.mincut), graph.pathcount,fileinfoint[6]));
			#outfile.flush();
			receivedbytes=0;
			sentbytes=0;
			env.run(500000)
			print(' ,  S:'+str(sentbytes)+' ,  R:'+str(receivedbytes))
			f.close();
#outfile.close();		


"""print("Cut Size: : %d"%(g.nodes[0].c))
print("Total Path : %d"%(totalPath))
print("Total Sent Bytes : %d"%(sentbytes*0.95))"""
