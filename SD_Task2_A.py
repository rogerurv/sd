from pyactive.controller import init_host,launch,start_controller, sleep, serve_forever, interval_host, later
from pyactive.exception import TimeoutError
from socket import *
import os

class Sensor:
    _sync={}
    _async=['start','subscribe','start_interval','stop_interval','update']
    _parallel=[]
    _ref=['subscribe']

    # funcio d'inici de sensor

    def __init__(self):
		self.subscribers=[]		# llista de Streetlights
		self.list_SL=[]			# llista de llista de valors
		self.i=0				# contadors de valors enviats
	
	
	# funcio per afegir Streetlight
	# param: sl (Streetlight)
		
    def subscribe(self, sl):
        self.subscribers.append(sl)		# afegir Streetlight a llista

   
	# funcio per comencar a enviar valors
	# param: nom d'arxiu a llegir dades
	
    def start(self,filename):
		f=open(filename,'r')		# obrir fitxer parametre
		for line in f:				# anar llegint totes les files de fitxer
			llista=[]				# llistas per guardar valors
			characters=line.split()	# variable character= valors separats
			for ch in characters:	# bucle per a tots caracters
				llista.append(ch)	# afegir caracters un a un a llista
			self.list_SL.append(llista)	# afegir llista a llista total
		f.close()

	# funcio per enviar valors a Streetlight
	
    def update(self):
		
		if (self.i<len(self.list_SL[0])):		# enviar tants valors com tamany de llista
			print "Index d'informacio:" +str(self.i) # feedfack de quin valor ha enviat
		   
			self.subscribers[0].input_data(self.list_SL[0][self.i],0,self.i)	# enviar valor index al Streetlight 0
			self.subscribers[1].input_data(self.list_SL[1][self.i],1,self.i)	# enviar valor index al Streetlight 1
			self.subscribers[2].input_data(self.list_SL[2][self.i],2,self.i)	# enviar valor index al Streetlight 2
		
			self.i=self.i+1		# incrementar nombre de valor
		
	# funcio per enviar valors a Streetlight cada segon
		
    def start_interval(self):
        self.interval1=interval_host(self.host,1,self.update)
       


class Streetlight:
	_sync={}
	_async=['input_data','switch_on','switch_off','is_on','set_queue','log','set_log']
	_parallel=[]
	_ref=[]

	# funcio d'inici de Streetlight
	
	def __init__(self):
		self.on=True			# boolea per saber si Streetlight obert o no
		self.index=0			# index Streetlight
		self.data=0				# valor actual rebut
		self.resultats=[]		# llista a escriure a fitxer de sortida
		self.cont=0
	
	# funcio per conectar Streetlight i cua
	# param: queue(cua), index(index Streetlight)
		
	def set_queue(self,queue,index):
		self.queue=queue
		self.resultats.append("Streetlight" + str(index+1) +":")
		self.resultats.append("ON")
	
	def set_log(self,write):
		self.write=write
		
		
	# funcio per rebre i enviar dades de sensor a cua
	# param: data (valor actual), index (index Streetlight)	
		
	def input_data(self,data,index,lamport):
		self.data=data
		self.index=index
		self.queue.send(self.data,self.index,self.on,self.cont,lamport)	# envia dada a cua
		
	# funcio per conectar el Streetlight		
		
	def switch_on(self):
		self.on=True
		self.resultats.append("ON")
	
	# funcio per desconectar el Streetlight
	
	def switch_off(self):
		self.on=False
		self.resultats.append("OFF")
	
	# funcio per afegir valor actual a fitxer
		
	def is_on(self,cont):
		self.cont=cont
		self.resultats.append(self.data)

	# funcio per escriure a fitxer
	
	def log(self):

		self.write.log(self.resultats)	# cridem al log de la cua
		
class Queue:
	_sync = {}
	_async = ['connect_queue','add_server','send','switch','write','log']
	_parallel = []
	_ref = ['connect_queue']

	
	# funcio per iniciar la cua

	def __init__(self):
		
		self.subscribers=[]			# llista de subscriptors
		self.servers=[]				# llista de servidors
		self.index_server=0			# index de servidor (inici el 0)
		self.resultats=[]			# llista per a escriure a fitxer
	
	# funcio per a conectar cua i server
	# param: server
		
	def add_server(self,server):
		self.servers.append(server)
	
	# funcio per a conectar cua i Streetlight
	# param: sl(Streetlight)
	
	def connect_queue(self,sl):
		self.subscribers.append(sl)
	
	# rebre i enviar dades de a server
	# param: inf (valor actual), on(estat de Streetlight)
		
	def send(self,inf,index,on,cont,lamport):
		
		
		self.servers[self.index_server].push_data(inf,index,on,cont,lamport,self.index_server)
		self.index_server=self.index_server+1
				
		if (self.index_server==len(self.servers)):
			self.index_server=0	
			
		
		
	
	
	# funcio per conectar, desconectar o mantindre estat d'Streetlight segons la opcio	
	# param: opcio, index (index Streetlight)
	 	
	def switch(self,opcio,index,cont):
		
		
		self.subscribers[index].is_on(cont)
		
		if (opcio==1):
			self.subscribers[index].switch_on()
		
		if (opcio==0): 
			self.subscribers[index].switch_off()
			
		
		
				
		
class Server:
	_sync = {}
	_async = ['set_queue_server','connect_database','push_data']
	_parallel = []
	_ref = []
	
	# funcio d'inici de server
	
	def __init__(self):
		self.inf=[0,0,0]	# llista de valors de cada Streetlight
		self.cont=[0,0,0]	# vector de contadors de cada Streetlight
		self.database=[]
		self.cont_commit=0
	
	# connectem server i databases
		
	def connect_database(self,database):
		self.database.append(database)
	
		 
	# funcio per a conectar server amb cua
	#param: queue(cua)
	
	
	def set_queue_server(self,queue):
		self.queue=queue
	
	# funcio per a operar segons dada rebuda)
	# param: inf (dada actual), index (index Streetlight), cont(contador Streetlight), on(estat actual Streetlight)
		
	def push_data(self,inf,index,on,cont,lamport,index_server):
		
		
		print 'Informacio streetlight ' + str(index+1) +': ' + str(inf) # feedback
		self.opcio=3
		
		self.inf[index]=inf
		self.cont[index]=cont
		
		if (int(self.inf[index])==1):	# si tenim un "1" posa contador a 0
			self.cont[index]=0
			
			
			if (on==False):				# si teniem Streetlight tancat, obre
				self.opcio=1
				
		else:
			self.cont[index]=self.cont[index]+1		# si tenim un "0" augmenta contador
			
			if (self.cont[index]==4):	# si tenim 4 "0"
				if(on==True):			# si el Streetlight esta obert, tanca
					self.opcio=0
					self.cont[index]=0
		
		
		
		
		for i in self.database:	
			if(i.vote(lamport,index)==True):							# contem quantes databases estan a favor de realitzar el commit
				self.cont_commit+=1
		
			
				
		if (self.cont_commit==len(self.database)):						# si totes les databases hi estan a favor
			self.cont_commit=0											# reiniciem contador
			
			for i in self.database:	
				i.commit(lamport,index)									# indiquem a les databases que poden actualizar el seu lamport clock
			print "Lamport "+str(lamport)+" de Streetlight "+str(index+1)+" commited"
			self.queue.switch(self.opcio,index,self.cont[index])		# canviem estat segons opcio
			
		else:
			print "Lamport "+str(lamport)+ " Streetlight "+str(index+1)+ " aborted"
			for i in self.database:
				i.abort(lamport)										# indiquem a les databases que no es realitza el commit
			sleep(1)	
			self.queue.send(self.inf[index],index,on,cont,lamport)		# reenviem les dades a la cua
			
		
			
		
			
			


class Database:
	_sync={'vote':1,'commit':2,'abort':2}
	_async=['set_lamport','connect_server']
	_parallel=[]
	_ref=[]
    
	def __init__(self):
		self.lamp_clock=[0,0,0]	# vector de contadors de cada Streetlight
		
		
	
	
	# votar si es pot actualitzar el fitxer
	# el resultat de la votacio es guarda en un fitxer temporal
	
	def vote(self,data,index):
		
		filename=str(self.id)+".txt"
		f= open(filename,'w')
			
		f.writelines("%d\n" %data)
		
		f.close()
			
		if(data-self.lamp_clock[index]>1):
			return False
		
		else:
			return True	
				
			
	# votacio satisfactoria
	# actualitzar lamport clock de database i esborrar de fitxer
	# el clock temporal						
					
	def commit(self,lamport,index):
		
		#print "Lamport "+str(index)+":"+str(self.lamp_clock[index])
		filename=str(self.id)+".txt"
		f= open(filename,'a+')
		
		for line in f:
			if (line==lamport):
				str(self.id)+".txt".remove(line)
		self.lamp_clock[index]=lamport
		
		f.close()
	
	
	# votacio insatifactoria
	# esborrat de lamport temporal del fitxer 	
		
	def abort(self,lamport):	
		filename=str(self.id)+".txt"
		f= open(filename,'r')	
		
		for line in f:
			if (line==lamport):
				str(self.id)+".txt".remove(line)
		f.close()
		
		
class Write_file():
	
	_sync = {}
	_async = ['log','write']
	_parallel = []
	_ref = ['connect_server']
	
	def __init__(self):
		
		self.resultats=[]			# llista per a escriure a fitxer
		
		
		
	# funcio per a afegit a llista valors per esciure
	# param: llista amb valors
	
	def log(self,llista):

		self.resultats.append(llista)
			
	# funcio per escriure a fitxer
	
	def write(self):

		print 'Escribint resultats al fitxer ResultatsB1.txt'

		f= open('ResultatsB1.txt','w')

		for i in self.resultats:
			f.writelines("%s\n" % i)

		f.close()	



def test():
	
	
	
	
	
	host=init_host()
	sensor=host.spawn_id('1','SD_Task2_A','Sensor',[])
	sl1=host.spawn_id('1','SD_Task2_A','Streetlight',[])
	sl2=host.spawn_id('2','SD_Task2_A','Streetlight',[])
	sl3=host.spawn_id('3','SD_Task2_A','Streetlight',[])
	q= host.spawn_id('1','SD_Task2_A','Queue',[])

	
	l=host.spawn_id('1','SD_Task2_A','Write_file',[])


	sensor.subscribe(sl1)	# connectem clients al sensor
	sensor.subscribe(sl2)
	sensor.subscribe(sl3)
	
	sl1.set_log(l)		# connectem clients amb classe per escriure a arxiu
	sl2.set_log(l)		# connectem clients amb classe per escriure a arxiu
	sl3.set_log(l)		# connectem clients amb classe per escriure a arxiu
	
	
	sl1.set_queue(q,0)		# connectem clients amb cua
	sl2.set_queue(q,1)
	sl3.set_queue(q,2)
	
	q.connect_queue(sl1)	# connectem cua amb clients
	q.connect_queue(sl2)
	q.connect_queue(sl3)
	
	
	s1=host.spawn_id('1','SD_Task2_A','Server',[])
	s2=host.spawn_id('2','SD_Task2_A','Server',[])
	
	
	db1=host.spawn_id('1','SD_Task2_A','Database',[])
	db2=host.spawn_id('2','SD_Task2_A','Database',[])
	db3=host.spawn_id('3','SD_Task2_A','Database',[])
	s1.connect_database(db1)		# connectem servidors amb database
	s1.connect_database(db2)
	s1.connect_database(db3)
	
	s1.set_queue_server(q)			# connectem servidors amb cua
	q.add_server(s1)				# connectem cua amb servidors
	

	
	sensor.start('arxiu.txt')

	sensor.start_interval()

	sleep(27)			# esperar a tenir tots els valors
	sl1.log()
	sl2.log()
	sl3.log()
	

	sleep(3)			# esperar a escriure a llista els resultats
	l.write()
	
if __name__ == '__main__':
	start_controller('pyactive_thread')
	launch(test)
