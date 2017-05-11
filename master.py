# -*- coding: utf-8 -*-
#coding=<utf8>
import sys, os
import re
import traceback
import http.server
import socketserver
import socket
import json
import tempfile
import zipfile
import unicodedata
import os
from os import curdir, sep
import cgi
import threading
from threading import Thread
import time
import calendar
import logging
import datetime
import pam
import errno
cur_thread = threading.current_thread()
cur_thread.name="Master Thread"
if not os.path.exists('ksu-lambda_master.log'):
    f=open('ksu-lambda_master.log', 'w')
    f.close()
#logging.basicConfig(filename='ksu-lambda_master.log',filemode='a',level=logging.DEBUG, format="[%(levelname)s] (%(threadName)-10s) %(message)s",)
logging.basicConfig(level=logging.DEBUG, format="[%(levelname)s] (%(threadName)-10s) %(message)s",)
"""
-listens for minions and updates network table
-listens for client status requests and returns status
-listens for client job submission request and return a machine
-listens for admin status requests and returns status
-listens for admin job submission request and returns a minion
-method for managing queue
-


"""

global myAddress
global minionSchedule
global setMinionSchedule
global networkTable
global networkTableLock
global availalbeTableLock
global busyMinions
global busyMinionsLock
global clientThreads
global clientThreadsLock
global queue
global queueLock
global masterJobHistoryLock
global masterJobHistory
#list of users and locks for their log file
global users
global usersLock
global finishedJobList
global finishedJobListLock

global minionNames
global minionSchedule
global setMinionSchedule
global scheduleLock
global adminJobCount
global web_server_port
adminJobCount = 0

setMinionSchedule = []
users = []
queue = []
clientThreads=[]
finishedJobList=[]
myAddress=None
busyMinions =[]
networkTableLock = threading.Lock()
availableTableLock = threading.Lock()
busyMinionsLock = threading.Lock()
clientThreadsLock = threading.Lock()
masterJobHistoryLock = threading.Lock()
finishedJobListLock = threading.Lock()
scheduleLock = threading.Lock()
queueLock= threading.Lock()
usersLock = threading.Lock()
minion_update_port = 50000
client_request_port = 40000
admin_request_port = 60000
web_server_port = 30000
minion_start_time = 0
minion_stop_time = 6
global local
local = False
def get_master_job_history():
    global masterJobHistory
    masterJobHistory = 'master_job_hisotry_' + str(time.strftime("%m-%d-%Y")) +'.log'
    if not os.path.exists(masterJobHistory):
        with open(masterJobHistory, 'w') as f:
            pass
    return masterJobHistory



class Master():
    def __init__(self):
        logging.log(20,"Starting Master")
        global networkTable
        global myAddress
        global minionSchedule
        self.getAddr()
        networkTable = NetworkTable()
        self.networkTable = networkTable
        minionSchedule =[[],[],[],[],[],[],[]] #7 days of the week
        try:
            self.updateListenThread = UDP_BackgroundProcess("Minon_update",myAddress, minion_update_port, omegaUDP_Handler).listen()
            self.clientListenThread = TCP_BackgroundProcess("Client Listener","127.0.0.1", client_request_port, clientHandler).listen()
            #log("listening for clients with thread: " , self.updateListenThread.getThreadName())
            self.adminListenThread=TCP_BackgroundProcess("Admin Listener","127.0.0.1", admin_request_port, adminHandler).listen()
            webServerThread = TCP_BackgroundProcess("Webserver",myAddress, web_server_port, HTTP_webpageHandler).listen() 
            self.queue_manager_thread = queueManager().start()
            self.alive=True
        except Exception as e:
            logging.log(20, e)
            sys.exit(1)
    def getAddr(self):
        global myAddress
        global local
        # get the IP address of the executing module
        if local: myAddress="127.0.0.1"
        if myAddress : logging.log(20, "myAddress is " + myAddress)
        # get dynamic addr by connecting to internet
        else:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                self.s.connect(("8.8.8.8", 80))
                logging.log(20, "Connected to internet. Using address : " + self.s.getsockname()[0])
                myAddress= self.s.getsockname()[0]
                logging.log(20, "myAddress is " + myAddress)
            except OSError:
                logging.log(20, "Could not connect to internet. Using localhost 127.0.0.1")
                myAddress="127.0.0.1"
            except:
                logging.log(20, "Could not get address.")

class NetworkTable():
    logging.log(20, "Creating Network Table")
    minionNumber=0
    networkTable=None
    global busyMinions
    global busyMinionsLock
    global minionNameLock
    global minionNames
    global minionSchedule
    global setMinionSchedule
    global scheduleLock
    def __init__(self):
        self.networkTable={}
        self.minionNames=[]
        minionNames=self.minionNames
        self.minionNameLock = threading.Lock()
    def updateMinionName(self, entry):
        if len(entry["name"].split('-')) ==1 :
            self.minionNames.append(entry["name"]) #room name
            # returns next available minion number
            minionNumber = 1
            logging.log(20, 'Current Minion Names: ' +str( self.minionNames))
            with self.minionNameLock:
                for name in self.minionNames:
                    if name[0] == entry["name"]:
                        name[1] += 1
                        minionNumber=name[1]
                    self.minionNames.append([entry["name"], 1])
                    break
                entry["name"] = entry["name"] + "-" + str(minionNumber)
                minionSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                entryAddress = (entry['master_port'][0], entry['master_port'][1])
                try:
                    minionSock.connect(entryAddress)
                    minionSock.send(('name\n'+ entry['name']).encode('utf-8'))
                    confirmation=minionSock.recv(1024).decode()
                    if confirmation=='done': logging.log(20,"Minion confirmed name changed to " + entry["name"])
                    else: logging.log(40, "Minion did not confirm name update")
                except Exception as e:
                    logging.log(20, e)
                    logging.log(40, "Minion did not confirm name update")
                    name[1] += 1
                finally:
                    minionSock.close()
    def updateEntry(self, jsonEntry, minionName=None):
        entry = json.loads(str(jsonEntry))
        self.updateMinionName(entry)
        #today = datetime.datetime.today().weekday()
        #if len(minionSchedule[today]) > :
            #scheduler = scheduleChecker(entry, schedule, today).start()
        entry["epoch"]=int(calendar.timegm(time.gmtime()))
        if entry['stop_time']== 'None' or entry['today'] == 'None' or entry['start_time'] =='None':
            classroom = entry['name'].split('-')[0]
            today = datetime.datetime.today().weekday() #returns 0 Monday to 6 Sunday
            todaySchedule = minionSchedule[today]
            if len(todaySchedule) > 0:
                #time at this instance
                hour = datetime.datetime.time(datetime.datetime.today()).hour #returns int 0 - Midnight to int 23 - 11pm
                minutes = datetime.datetime.time(datetime.datetime.today()).minute #returns int 0 - 59
                minutes = (hour*60) + minutes
                for room in todaySchedule:
                    if room[0] == classroom:
                        for i in range(1, len(room), 2): # index 1 is for name
                            startTime = room[i]
                            stopTime = room[i+1]
                            stopTime= stopTime.split(":")
                            startTime = startTime.split(":")
                            startTimeMinutes = (int(startTime[0]) * 60) + int(startTime[1])
                            stopTimeMinutes = (int(stopTime[0]) * 60) + int(stopTime[1]) #time in minutes during the day that the job should stop
                            if stopTimeMinutes > minutes > startTimeMinutes: #current time is within schedule time
                                self.updateMinionTime(entry, str(startTimeMinutes), str(stopTimeMinutes), str(today)) 
        os_version = entry['os_version']
        if os_version == 'Windows' or os_version == 'Linux' or os_version == 'Darwin':
            if os_version not in self.networkTable:
                entry['first_epoch']=entry['epoch']
                self.networkTable[os_version]=[entry]
            else:
                #check if minion is in busyMinions. if yes, set is busy to true so minion cannot be assigned another jobs
                if entry['is_busy'] == 'False':
                    for i in busyMinions:
                        if i['name'] == entry['name']:
                            entry['is_busy'] = 'True'
                            logging.log(20, entry['name'] + ' did not report as busy.  Setting ' + entry['name'] + ' as busy')
                        break
                found = False
                for minion in self.networkTable[os_version]:
                    if minion['name'] == entry['name']:
                        self.networkTable[os_version].remove(minion)
                        self.networkTable[os_version].append(entry)
                        found = True
                        break
                if not found:
                    self.networkTable[os_version].append(entry)
            logging.log(20, 'Name: ' + entry['name'] + '\tOS: ' + entry['os_version'] + '\tmachine: ' + entry['machine'] + '\tGPU:' + entry['gpu'] + '\tRAM:' + entry['avail_ram'] + '\tBusy:' + entry['is_busy'])       
        
        else:
            logging.log(40,'UNKNOWN OS: ' +  'Name: ' + entry['name'] + '\tOS: ' + entry['os_version'] + '\tmachine: ' + entry['machine'] + '\tGPU:' + entry['gpu'] + '\tRAM:' + entry['avail_ram'] + '\tBusy:' + entry['is_busy'])       

                
##        try:
##            for i in self.networkTable:
##                if int(calendar.timegm(time.gmtime()) - i['epoch']) > 1000:
##                    logging.log(30, 'Unhealthy minion:  ' + i['name'] +' found. Checking on  '+ i['name'] +'s status')
##                    self.networkTable.remove(i)
##                if(i["name"] == entry["name"]):
##                    health = entry['epoch'] - i['first_epoch']
##                    if health < 0: continue
##                    entry['first_epoch']=i['first_epoch']
##                    entry['health']=health
##                    self.networkTable.remove(i)
##                    self.networkTable.append(entry) 
##                    return
##            health = 0
##            entry['health']=health
##            entry['first_epoch']=entry['epoch']
##            self.networkTable.append(entry)
##        except Exception as e:
##            logging.log(40, e)
##            logging.log(40, 'Exception while updating netowrk table')
    def getTable(self):
        return json.dumps(self.networkTable)
    def updateMinionTime(self,minion, start_time, stop_time, today):
        time_updater = minionTimeUpdater(minion, start_time, stop_time, today)
        time_updater.start()
    def checkSchedule(self, classroom, wall_time, entry): #string classroom, int wall_time
        today = datetime.datetime.today().weekday() #returns 0 Monday to 6 Sunday
        todaySchedule = minionSchedule[today]
        if len(todaySchedule) > 0:
            #time at this instance
            hour = datetime.datetime.time(datetime.datetime.today()).hour #returns int 0 - Midnight to int 23 - 11pm
            minutes = datetime.datetime.time(datetime.datetime.today()).minute #returns int 0 - 59
            minutes = (hour*60) + minutes
            for room in todaySchedule:
                if room[0] == classroom:
                    for i in range(1, len(room), 2): # index 1 is for name
                        startTime = room[i]
                        stopTime = room[i+1]
                        startTime = startTime.split(":")
                        stopTime= stopTime.split(":")
                        startTimeMinutes = (int(startTime[0]) * 60) + int(startTime[1])
                        stopTimeMinutes = (int(stopTime[0]) * 60) + int(stopTime[1]) #time in minutes during the day that the job should stop
                        if stopTimeMinutes > minutes > startTimeMinutes: #current time is within schedule time
                            if entry['start_time'] != str(startTimeMinutes) or entry['stop_time'] !=str(stopTimeMinutes) or entry['today'] != str(today):
                                    self.updateMinionTime(entry, str(startTimeMinutes), str(stopTimeMinutes), str(today))
                            if (stopTimeMinutes - minutes) >= wall_time:
                                entry['start_time'] = str(startTimeMinutes)
                                entry['stop_time'] = str(stopTimeMinutes)
                                entry['today'] = str(today)
                                return True
                            else:
                                return False
                        else:
                            return False
        if entry['start_time'] != 'None' or entry['stop_time'] !='None' or entry['today'] != str(today):
            self.updateMinionTime(entry, 'None', "None", str(today))
            entry['start_time'] = 'None'
            entry['stop_time'] = 'None'
            entry['today'] = str(today)
        return False
    def getMinion(self,job_string):
        logging.log(20, "Searching network table for minion in table. OS: " + job_string[1] + ' , GPU: ' + job_string[3])
        request={}
        request["os_version"] = job_string[1]
        no_queue = job_string[2]
        request["gpu"] = job_string[3]
        request["high_ram"] = job_string[4]
        keys=list(request.keys())
        high_ram=job_string[4]
        wall_time=job_string[8].split(":")
        wall_time = (int(wall_time[0]) * 60) + int(wall_time[1])
        minions=[] #currently available
        capable=[] #busy but capable minions. Used for queue
        os=request["os_version"].split("_")
        count = 0
        os_keys = self.networkTable.keys()
        for os_version in self.networkTable:
            if os_version in os:
                for minion in self.networkTable[os_version]:#for minions in list keyed by osversion
                    if minion['is_busy'] =='False':
                        if request['gpu'] != 'any':
                            if minion['gpu'] == request['gpu']:
                                minions.append(minion)
                        else:
                            minions.append(minion)
                    else:
                        if request['gpu'] != 'any':
                            if minion['gpu'] == request['gpu']:
                                capable.append(minion)
                        else:
                            capable.append(minion)
        
        for entry in minions:
            classroom = entry["name"].split("-")[0]
            scheduled = self.checkSchedule(classroom, wall_time, entry)
            if not scheduled: minions.remove(entry) #classroom is out of schedule
        for entry in capable:
            classroom = entry["name"].split("-")[0]
            scheduled = self.checkSchedule(classroom, wall_time, entry)
            if not scheduled: capable.remove(entry) #classroom is out of schedule
        for entry in busyMinions:
            for entry2 in minions:
                if entry['name'] == entry2['name']:
                    minions.remove(entry2)
                    logging.log(20, 'removed minion busy minions')
        logging.log(20, "minions found: " + str(len(minions)) + ' capable minons found:' + str(len(capable)))
        ram_max={}
        chosenMinion={}
        minion_reply=[]
        minion_reply.append(capable)
        if len(minions)>0:
            if high_ram =="True":
                if len(minions) >1:
                    ram_max["avail_ram"] = 0.0
                    for entry in minions:
                        if float(entry["avail_ram"]) > ram_max["avail_ram"]: ram_max= entry
                    minion_reply.append(ram_max)
                    logging.log(20, 'Selected high_ram minion'+ ram_max['name'])
                    return minion_reply
                else:
                    chosenMinion=minions[0]
                    logging.log(20, 'Currently, only one minion is available: ' + chosenMinion['name'])
                    with busyMinionsLock: busyMinions.append(entry)
                    minion_reply.append(chosenMinion)
                    return minion_reply
            else:
                chosenMinion=minions[0]
                logging.log(20, 'Taking the first of found minions: ' + chosenMinion['name'])
                with busyMinionsLock:  busyMinions.append(chosenMinion)
                minion_reply.append(chosenMinion)
                return minion_reply
        else:
            logging.log(20, 'No minions found')
            chosenMinion["addr"] = "None"
            minion_reply.append(chosenMinion)
            return minion_reply
    def adminGetMinion(self, job_string):#returns all minions
        job_string = job_string.split('\t')
        gpu = job_string[2]
        os = job_string[1].split('_')
        adminReply =[]
        epoch=int(calendar.timegm(time.gmtime()))
        for os_version in self.networkTable:
            if os_version in os:
                for minion in self.networkTable[os_version]:
                    if minion['os_version'] == os_version:
                        if epoch - minion['epoch'] < 100:
                            if gpu == 'any':
                                adminReply.append(minion)
                            elif minion['gpu'] == gpu:
                                adminReply.append(minion)
        return adminReply
    def check_minion(self, minion):
        minionSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        replied=False
        minionName = minion['name']
        try:
            minionSock.connect((minion['addr'], int(minion['master_port'][1])))
            minionSock.send(('report').encode('utf-8'))
            reply=minionSock.recv(1024)
            reply2=reply
            if len(reply) > 1023:
                reply2 +=reply
                reply=minionSock.recv(1024)
            reply= reply2.decode()
            replied=True
        except Exception as e:
            logging.log(40, e)
        finally:
            if replied: self.updateEntry(reply, minionName)
            else:
                logging.log(40, 'Minion: ' + minion['name'] + ' did not reply')
            minionSock.close()
        return replied
    def update_busy_minions(self, name):
        logging.log(20, 'Remove busy minion')
        global busyMinionsLock
        for entry in busyMinions:
            if entry['name'] == name:
                with busyMinionsLock: busyMinions.remove(entry)
    def table_status(self, web):
        nl ='\n'
        tab = '\t'
        if web=='web':
            nl = '<br>'
            tab= '&emsp;&emsp;&emsp;'
        logging.log(20, 'Checking table status')
        reply=[]
        windows=0
        mac=0
        linux=0
        for os_version in self.networkTable:
            if os_version == 'Windows': windows = len(self.networkTable[os_version])
            elif os_version == 'Darwin': mac = len(self.networkTable[os_version])
            else: linux = len(self.networkTable[os_version])
        reply.append('Total Windows: ' + str(windows) +tab)
        reply.append('Total Mac:     ' + str(mac)+tab)
        reply.append('Total Linux:   ' + str(linux)+ nl)
        windows = mac = linux = 0
        global busyMinions
        for entry in busyMinions:
            if entry['os_version']=='Windows': windows +=1
            elif entry['os_version']=='Darwin': mac +=1
            else: linux+=1
        reply.append('Busy Windows:  ' + str(windows)+tab)
        reply.append('Busy Mac:      ' + str(mac)+tab)
        reply.append('Busy Linux:    ' + str(linux)+ nl + nl)
        if web=='web':
            replystring =''
            for line in reply: replystring += line
            return replystring
        else:
            return reply
    def statusReply(self, minion):
        statusReply =''
        statusReply +='name:' + minion['name'] + '\t'
        statusReply +='busy:' + minion['is_busy'] + '\t'
        statusReply +='os_version:' + minion['os_version'] + '\t'
        statusReply +='machine:' + minion['machine'] + '\t'
        statusReply +='gpu:' + minion['gpu'] + '\t'
        statusReply +='Ram:' + minion['avail_ram'] + '\t'
        statusReply += '\n'
        return statusReply
    def get_all_gpu_minions(self):
        gpuReply=''
        if len(self.networkTable) > 0:
            found=False
            for os_version in self.networkTable:
                if len(self.networkTable[os_version]) > 0:
                    for minion in self.networkTable[OS] :
                        if minion['gpu'] != 'None':
                            gpuReply += self.statusReply(minion)
                    found = True
            if not found:
                gpuReply+='There are no gpu minions at this time\n'
        else: gpuReply += 'There are no miinions in the network table at this time\n'
        return gpuReply
    def search_minions_by_IP(self, IP):
        ipReply=''
        if len(self.networkTable) > 0:
            found = False
            for os_version in self.networkTable:
                if len(self.networkTable[os_version]) > 0:
                    for minion in self.networkTable[os_version] :
                        if minion['addr']==IP or minion['addr'] == socket.gethostbyname(IP):
                            ipReply += self.statusReply(minion)
                            found = True
            if not found:
                ipReply += 'There are no minions with the specified IP address'
        else: ipReply += 'There are no miinions in the network table at this time\n'
        return ipReply
    def search_minions_by_OS(self, OS):
        osReply=''
        if len(self.networkTable) > 0:
            found = False
            if OS in self.networkTable:
                if len(self.networkTable[OS]) > 0:
                    found = True
                    for minion in elf.networkTable[OS]:
                        osReply += self.statusReply(minion)
            if not found:
                osReply += 'There are no minions with the specified OS\n'
        else: osReply += 'There are no miinions in the network table at this time\n'
        return osReply
    def get_all_minions(self):
        allReply=''
        if len(self.networkTable) > 0:
            for os_version in self.networkTable:
                for minion in self.networkTable[os_version]:
                    allReply += self.statusReply(minion)
        else: allReply += 'There are no miinions in the network table at this time\n'
        return allReply


    
class ThreadedUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer):
    pass
    

class UDP_BackgroundProcess:
    listenPort=None
    def __init__(self,name, addr, port, handler):
        self.name = name
        self.addr=addr
        self.handler=handler
        self.listenPort = port
        self.alive=True
    def listen(self ):
        self._startScriptReceival = threading.Thread(name = self.name, target=self._listen).start()
        while ( not self.listenPort ): time.sleep(.5)
        return self
    def _listen(self):
        #self.listeningSocket=ThreadedUDPServer((self.addr, self.port), self.handler)
        #self.listeningSocket.allow_reuse_address = True
        #self.listenPort=self.listeningSocket.server_address
        minionUpdateSocket=socket.socket (socket.AF_INET, socket.SOCK_DGRAM)
        minionUpdateSocket.bind((self.addr, self.listenPort))
        logging.log(20,  " UDP_BackgroundProcess:\t"  + self.name + " listener is running at TCP socket : " + self.addr+":"+str(self.listenPort))
        while self.alive:
            entry, minion = minionUpdateSocket.recvfrom(1024)
            entry = entry.decode()
            global networkTable
            networkTable.updateEntry(entry, None)
    def getListenPort(self):
        return self.listenPort
    def kill(self):
        minionUpdateSocket.close()

#may need to implement lambda machine version of listening.
#This method creates a new thread for each udp packet received.
class omegaUDP_Handler(socketserver.BaseRequestHandler):
    def handle(self):
        entry= self.request[0].decode()
        global networkTableLock
        global networkTable
        with networkTableLock: networkTable.updateEntry(entry, None)
        #logging.log(20,"updated " + str(self.client_address))
        #logging.log(20,str(self.networkTable.getTable()))

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

class TCP_BackgroundProcess:
    listenPort=None
    def __init__(self, name, addr, port, handler):
        self.name = name
        self.addr=addr
        self.handler=handler
        self.port = port
    def listen(self ):
        self._startScriptReceival = threading.Thread(name=self.name, target=self._listen).start()
        while ( not self.listenPort ): time.sleep(.5)
        return self
    def _listen(self):
        self.listeningSocket=ThreadedTCPServer((self.addr, self.port), self.handler)
        self.listeningSocket.allow_reuse_address = True
        self.listenPort=self.listeningSocket.server_address 
        logging.log(20,"TCP_BackgroundProcess:\t"+ self.name +" is running at TCP socket : " + self.listenPort[0]+":"+str(self.listenPort[1]))
        self.listeningSocket.serve_forever()
    def getListenPort(self):
        return self.listenPort[1]
    def kill(self):
        self.listeningSocket.close()

class clientHandler(socketserver.BaseRequestHandler):
    global networkTable
    global clientThreads
    global clientThreadsLock    
    global myAddress
    global users
    global usersLock
    global finishedJjobList
    global finishedJjobListLock
    global client
    def handle(self):
        self.cur_thread=threading.current_thread()
        self.lock = threading.Lock()
        self.minion = 'None'
        self.alive=False
        self.stop=False
        self.queue = False
        self.requestTime=time.strftime("%c")
        self.requestTime = 'Request: ' + self.requestTime
        self.jobID = '--------'
        #              Wed Apr 26 07:53:08 2017
        self.startTime='------------------------'
        self.stopTime='------------------------'
        self.jobStatus='---'
        logging.log(20, self.requestTime)
        self.proceed=True
        self.received = False
        logging.log(20,"clientHandler_thread")
        self.job_string = self.request.recv(1024).decode().strip().split()
        logging.log(20,"job_string " + str(self.job_string))
        #submit_job, status
        if self.job_string[0] == "status":
            self.checkStatus()
        elif self.job_string[0] == "submit_job":
            if len(self.job_string) == 9:
                self.os_version=self.job_string[1]
                self.no_queue=self.job_string[2]
                self.gpu=self.job_string[3]
                self.high_ram=self.job_string[4]
                self.user=self.job_string[5]
                self.cur_thread.name=self.user + '_submit_job_handler'
                self.data_folder=self.job_string[6]
                self.output_folder=self.job_string[7]
                self.wall_time=self.job_string[8]
                with networkTableLock: self.minion_reply = networkTable.getMinion(self.job_string)
                time.sleep(1)
                self.minion=self.minion_reply[1]
                self.capable=self.minion_reply[0]
                self.msg={}
                self.msg['request_time']=self.requestTime
                self.clientThread=[] # self, user, requestTime, jobID, startTime, stopTime, jobStatus
                self.clientThread.append(self)
                self.clientThread.append(self.user)
                self.clientThread.append(self.requestTime)
                self.clientThread.append(self.jobID)
                self.clientThread.append(self.startTime)
                self.clientThread.append(self.stopTime)
                self.clientThread.append(self.jobStatus)
                if self.minion["addr"] != "None":
                    logging.log(20, "Using minion: " +str(self.minion['name']))
                    self.runJob()
                else:
                    if len(self.capable):
                        if self.no_queue =='True':
                            #user requested no queue
                            #job cannot be completed right now but can it be placed in queue
                            self.request.send(('try_queue').encode('utf-8'))
                            self.request.shutdown(socket.SHUT_WR)
                            confirmation= self.request.recv(1024).decode()
                            self.request.close()
                            if confirmation == 'done': logging.log(20, "client received reply: " + 'try_queue')
                            else: logging.log(40, "could not confirm if client received reply")
                        else:
                            self.setQueue()
                    else:
                        #There are no minions that can run this job in the network table
                        self.request.send(('None').encode('utf-8'))
                        self.request.shutdown(socket.SHUT_WR)
                        confirmation= self.request.recv(1024).decode()
                        if confirmation == 'done': logging.log(20, "client received reply: " +  'None')
                        else: logging.log(40, "could not confirm if client received reply")
                        self.request.close()
            else:
                self.request.send(('Client did not send enough parameters for job submission. Please notify system administrator or try again').encode('utf-8'))
                self.request.shutdown(socket.SHUT_WR)
                self.request.close()
                logging.log(40,"client did not send enough parameters for job submission")
        else:
            self.request.send(('Client did not send enough parameters for job submission. Please notify system administrator or try again').encode('utf-8'))
            self.request.shutdown(socket.SHUT_WR)
            self.request.close()
            logging.log(40,"clientHandler does not how to handle the client request")
    def checkStatus(self):
        self.user=self.job_string[1]
        self.cur_thread.name=self.user + '_check_status'
        #check network table status
        with networkTableLock: self.table_status = networkTable.table_status(None)
        self.status='\n-------------------------------------\nCluster Status\n-------------------------------------\n'
        for os in self.table_status: self.status += os
        #check clientThreads list                
        self.status += '\n-------------------------------------\nCurrent Running Jobs\n-------------------------------------\n'
        for thread in clientThreads:
            if thread[1]==self.user:
                for entry in range(2, len(thread)): self.status += thread[entry] + '\t'
                self.status+= '\n'
        if len(clientThreads) == 0: self.status+= 'None\n'
        self.status+='\n-------------------------------------\nClass Schedule\n-------------------------------------\n'
        self.status += '\n' + getSchedule(None)
        logging.log(20, self.status)
        self.request.send(self.status.encode('utf-8'))
        confirmation= self.request.recv(1024).decode()
        if confirmation == "done":
            logging.log(20, "client received reply")
        else:
            logging.log(40, "could not confirm if client received reply")
        self.request.close()
    def runJob(self):
        #notify client
        self.msg['minion_OS']=self.minion['os_version']
        self.request.send(('minion').encode('utf-8'))
        #client thread list: self, startTime, jobID, 
        self.getJobID()
        self.notify_minion()
        self.clientThread[6]='Running'
        self.msg['jobID']=self.jobID
        self.request.send(str(json.dumps(self.msg)).encode('utf-8'))
        self.request.shutdown(socket.SHUT_WR)
        confirmation= self.request.recv(1024).decode()
        if confirmation == "done": logging.log(20, "Notified client of job submission")
        else: logging.log(40, "could not confirm if client received job submission notification")
        self.request.close()
        with clientThreadsLock: clientThreads.append(self.clientThread)
        self.submit_job()
    def setQueue(self):
        #job can and will be placed in queue
        #implement queue if no_queue is False
        logging.log(20, 'Placing job in queue')
        self.request.send(('queue').encode('utf-8'))
        self.queue=True
        self.getJobID()
        self.clientThread[6]='Queue'
        self.msg['jobID']=self.jobID
        self.request.send(str(json.dumps(self.msg)).encode('utf-8'))
        self.request.shutdown(socket.SHUT_WR)
        confirmation= self.request.recv(1024).decode()
        if confirmation == 'done': logging.log(20, "client received reply")
        else: logging.log(40, "could not confirm if client received reply")
        self.request.close()
        with clientThreadsLock: clientThreads.append(self.clientThread)
        with queueLock: queue.append(self.clientThread)
        self.queue_wait()
        self.notify_minion()
        self.submit_job()
    def getJobID(self):
        self.userLock = None
        for user in users:
            if user[0] == self.user:
                self.userLock = user[1]
                break
        if not self.userLock:
            self.userLock = threading.Lock()
            with usersLock:users.append([self.user, self.userLock])
        self.userFinishedJobListLock = None
        for user in finishedJobList:
            if user[0] == self.user:
                self.userFinishedJobListLock = user[1]
        if not self.userFinishedJobListLock:
            self.userFinishedJobListLock = threading.Lock()
            with finishedJobListLock: finishedJobList.append([self.user, self.userFinishedJobListLock, []])
        with self.userLock:
            self.jobCount=1
            if os.path.exists(self.job_string[5]):
                self.userProfile=open(self.user, "r")
                self.jobCountString=self.userProfile.readline()
                self.userProfile=open(self.user, "w")
                self.userProfile.seek(0)
                self.jobCountString = self.jobCountString[:len(self.jobCountString)-1]
                self.jobCountString=self.jobCountString.strip()
                if len(self.jobCountString) == 0:
                    logging.log(40, 'invalid job count for user ' + self.user)
                    self.jobCountString = '1'
                self.jobCount = int(self.jobCountString)
                self.jobCount +=1
                self.jobCount = str(self.jobCount)
                self.jobCountString = self.jobCount
                for i in range (len(self.jobCountString), 100): self.jobCountString +=' '
                self.userProfile.write(str(self.jobCountString) + '\n')
                self.userProfile.close()
            else:
                self.jobCount="1"
                self.jobCountString = self.jobCount
                for i in range (len(self.jobCount), 100): self.jobCountString +=' '
                self.userProfile=open(self.user, "w+")
                self.userProfile.write(str(self.jobCountString) + '\n')
                self.userProfile.close()
        self.jobID = self.user + "-"+ self.jobCount
        self.cur_thread.name=self.jobID
        self.clientThread[3]=self.jobID
        logging.log(20, "Assigned jobID: " + self.jobID)
    def queue_wait(self):
        while not self.alive:
            logging.log(20, 'Waiting in queue')
            time.sleep(15)
    def set_minion(self, minion):
        self.minion=minion
        logging.log(20, 'set minion to ' + self.minion['name'])
        self.alive=True
    def notify_minion(self):
        logging.log(20, 'Notifying ' +self.minion['name'] + ' of incoming job: ' + self.jobID)
        try:
            self.startTime='Start: ' +time.strftime("%c")
            self.clientThread[4]=self.startTime
            if self.queue:
                for entry in clientThreads:
                    if entry[3]==self.jobID:
                        with clientThreadsLock :entry[4]=self.startTime
                        break
            self.minion["jobID"]=self.jobID
            self.minion_ip = self.minion["addr"]
            self.minionStatusPort=int(self.minion["master_port"][1])
            self.minion_client_port=int(self.minion["client_port"][1])
            self.minionNotifySock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.minion_address = (self.minion_ip, self.minionStatusPort)
            self.minionNotifySock.connect(self.minion_address)
            self.minionNotifySock.send(("incoming_client\n" + self.minion["jobID"]).encode("utf-8"))
            self.minionNotifySock.shutdown(socket.SHUT_WR)
            self.confirmation = self.minionNotifySock.recv(1024).decode()
            self.minionNotifySock.close()
            if self.confirmation == "done": logging.log(20, self.minion['name'] + " received job ID")
            else: logging.log(30, 'could not confirm if ' + self.minion['name'] + ' received job ID')
        except Exception as e:
            logging.log(40, e)
            logging.log(40, 'Error while notifying minion')
        
    def submit_job(self):
        try:
            logging.log(20, 'submitting job')
            #self.minion_checker_thread = threading.Thread(name=(self.jobID+'_minion_checker'), target=self.check_minion())
            self.minion_checker_thread = minionChecker(self)
            self.minion_checker_thread.setName(self.jobID+'_minion_checker')
            self.minion_checker_thread.start()
            #create socket object
            self.clientLog=[]
            self.clientSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #connect to provided server address and port
            self.clientSock.connect((self.minion['addr'], int(self.minion_client_port)))
            #steps to send file to minion
            #get folder name based on job ID which is given by master
            self.folderName = self.jobID
            
            #set up receive server and send port to minion
            self.receiveSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.receiveSock.bind((myAddress, 0))
            (self.clientIP, self.clientPort) = self.receiveSock.getsockname()
            self.clientIP = str(self.clientIP)
            self.clientPort = str(self.clientPort)
            self.receiveSockInfo = self.clientIP + ":"  + self.clientPort
            logging.log(20, self.jobID + ' data return socket \t' + self.receiveSockInfo)
            self.receiveSockInfo1 = self.receiveSockInfo
            #only way I found to make it take the ip and port. Might need to look for a better way
            #for i in self.receiveSockInfo: self.receiveSockInfo1 += i
            
            self.info = self.folderName + ' ' + self.receiveSockInfo1
            self.clientSock.send(self.info.encode('utf-8'))
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as t:
                logging.log(20, "Creating zip file")
                #logging.log(20, os.path(t.name))
                self.zipf = zipfile.ZipFile(t, 'w', compression=zipfile.ZIP_DEFLATED, allowZip64=True)
                #folder will be given in parameters
                for root, dirs, files in os.walk(self.data_folder):
                    dirLength = len(self.data_folder)
                    for file in files:
                        fileToAddPath=os.path.join(root, file)
                        self.zipf.write(fileToAddPath, fileToAddPath[dirLength:])
                self.zipInfo=self.zipf.namelist()
                logging.log(20, 'Zip info' + str(self.zipInfo))
                self.zipf.close()
                logging.log(20, "Sending zip file")
                self.zipName = t.name
                t.close()
                try:
                    time.sleep(10)
                    self.fileZip = open(self.zipName, 'rb')
                    self.toSend = self.fileZip.read(1024)
                    while self.toSend:
                        self.clientSock.send(self.toSend)
                        self.toSend = self.fileZip.read(1024)
                    self.clientSock.shutdown(socket.SHUT_WR)
                    logging.log(20, "Completed sending zip file")
                finally:
                    self.fileZip.close()
                    logging.log(20, 'Temp zip file location: ' + str(t.name))
                    os.remove(t.name)
                    self.clientSock.close()

            #steps to recieve file back from minion
            self.received = False
            self.receiveSock.listen(1)
            while self.proceed:
                logging.log(20, 'Waiting for minion to send file back')
                
                (self.clientSock, (self.minionIP, self.minionPort))=self.receiveSock.accept()
                self.minionIP = str(self.minionIP)
                logging.log(20, 'Data return socket: connected to ' + self.minionIP+ " / " + self.minion_ip)
                self.minion_ip = str(self.minion_ip)
                #self.minion_ip_by_addr = socket.gethostbyname(self.minion_ip)
                #self.minion_ip_by_name = socket.gethostbyaddr(self.minion_ip)
                #self.minion_ip_by_name = str(self.minion_ip_by_name)
                #self.minion_ip_by_addr = str(self.minion_ip_by_addr)
                #logging.log(20, self.minion_ip_by_name+ " " + self.minion_ip_by_addr)
                #if received is same as the one we sent out to.
                #if self.minionIP == self.minion_ip_by_name or self.minionIP == self.minion_ip_by_addr:
                if self.minionIP == self.minion_ip:
                    try: 
                        msg = self.clientSock.recv(1024).decode()
                        logging.log(20, 'Data return socket: Received message ' + msg)
                        if msg == 'Minion is ready to send files back':
                            self.received = True
                            self.proceed = False
                        elif msg == 'error':
                            self.proceed = False
                            logging.log(40, self.minion['name'] + ' reported an error')
                            logging.log(40, self.clientSock.recv(1024).decode())
                            self.stopTime= 'Stop_: ' + str(time.strftime("%c"))
                            self.jobStatus += 'Minion reported an error'
                            self.kill_thread()
                        elif msg == 'file received':
                            logging.log(20, self.minion['name'] + ' confirmed receipt of zip file')
                    except Exception as e:
                        logging.log(40, 'Error while receiving message on data return socket')
                else:
                    logging.log(20, 'Data return socket: Unknown client connecting. Closing connection.')
                    self.clientSock.shutdown(socket.SHUT_WR)
                    self.clientSock.close()
            if self.received:
                self.zipFileName=self.folderName +'.zip'
                #change os.getcwd() to method for getting home folder of user in linux
                self.folder = os.path.join(self.output_folder, self.folderName)
                logging.log(20, 'Creating output folder')
                logging.log(20, 'Full path to output folder: '+ self.folder)
                try:
                    os.makedirs(self.folder)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
                    else:
                        logging.log(20, 'Output folder already exists')
                except Exception as e:
                    logging.log(50, 'Unable to create output folder')
                logging.log(20, "Receiving Job and saving to "+ self.zipFileName + ' in current working directory')
                logging.log(20, "currently, the contents in Output folder are ")
                logging.log(20, os.listdir(self.folder))
                #might need to change to w+ for linux and mac
                self.fp = open(self.zipFileName, 'wb+')
                self.clientSock.send(('Client is ready to recieve file back').encode('utf-8'))
                self.data = self.clientSock.recv(1024)
                while self.data:
                    self.fp.write(self.data)
                    self.data = self.clientSock.recv(1024)
                time.sleep(1)
                self.clientSock.send(bytes('done', 'utf8'))
                self.clientSock.shutdown(socket.SHUT_WR)
                self.clientSock.close()
                with networkTableLock: networkTable.update_busy_minions(self.minion['name'])
                logging.log(20, 'unzipping/extracting file contents to ' + self.folder)
                self.fp.seek(0)
                self.rcvFileName = self.fp.name
                with zipfile.ZipFile(self.fp.name,"r") as self.zip_ref:
                    self.zip_ref.extractall(self.folder)
                    self.zip_ref.close()
                    del self.zip_ref
                self.fp.close()
                del self.fp
                logging.log(20, 'deleting zip file: '+ self.zipFileName)
                os.remove(self.zipFileName)
                logging.log (20, 'Output files for job ' + self.jobID + ' are in '+ self.folder)
                logging.log(20, 'Output folder contains ' + str(os.listdir(self.folder)))
            self.receiveSock.close()
                #else:
                    #logging.log(40, 'Minion did not send output files')
            self.stopTime='Stop_: ' + str(time.strftime("%c"))
            self.jobStatus='Completed Successfully'
            self.print_job()
        except Exception as e:
            logging.log(50, e)
            logging.log(50, 'Error while submitting job')
            self.jobStatus = 'Error while submitting job'
            self.kill_thread()
    def kill_thread(self):
        logging.log(40, 'Client thread kill process requested')
        self.proceed=False
        self.receiveSock.close()
        self.clientSock.close()
        global networkTable
        with networkTableLock: networkTable.update_busy_minions(self.minion['name'])
        self.print_job()
    def check_minion(self):
        logging.log(20, 'submit job: check_minion')
        count=0
        while self.proceed and not self.received:
            global networkTable
            with networkTableLock: replied = networkTable.check_minion(self.minion)
            if not replied: self.count +=1
            if count > 20:
                self.proceed = False
                self.kill_thread()
            time.sleep(120) 
    def print_job(self):
        logging.log(20, 'printing ' + self.user + ' log of this job to file')
        for entry in clientThreads:
            if entry[3]==self.jobID:
                with clientThreadsLock:
                    entry[5]=self.stopTime
                    entry[6]=self.jobStatus
                self.clientThread[5]=self.stopTime
                self.clientThread[6]=self.jobStatus
                global masterJobHistoryLock
                with masterJobHistoryLock:
                    with open(get_master_job_history(), 'a') as self.history:
                        self.line = ''
                        for args in range(1, len(entry)): self.line += entry[args] +'\t'
                        self.history.write(self.line + '\n')
                with clientThreadsLock: clientThreads.remove(entry)
                break
        logging.log(20, 'printing ' + self.user + ' log completed')
        self.jobStatus=''
        for entry in range(2,len(self.clientThread)): self.jobStatus += self.clientThread[entry] + '\t'
        with self.userLock:
            with open(self.user, "a") as self.userProfile: self.userProfile.write(self.jobStatus + '\n')
        self.finishedJob=[]
        #Append 
        for i in range(2, len(self.clientThread)):
            self.finishedJob.append(self.clientThread[i])
        
        for user in finishedJobList:
            if user[0] == self.user:
                with finishedJobListLock:
                    with self.userFinishedJobListLock: user[2].append(self.finishedJob)
                break
        logging.log(20, self.jobStatus)
            
class adminHandler(socketserver.BaseRequestHandler):
    global finishedJobList
    global clientThreads
    global queue
    global networkTable
    def handle(self):
        self.cur_thread = threading.current_thread()
        self.cur_thread.name='Admin'
        logging.log(20,"adminHandler_thread")
        requested1 = self.request.recv(1024).decode()
        self.requested = requested1
        while not requested1:
            requested1 = self.request.recv(1024).decode()
            self.requested+= requested1
        #self.requested =self.requested.decode()
        logging.log(20, 'Admin request: ' + self.requested)
        self.entry= self.requested.split('\t')
        self.reply=''
        if self.entry[0] == "status":
            self.checkStatus()
        elif self.entry[0] == 'submit_job':
            #receive jobstring
            self.os_version = self.entry[1]
            self.gpu = self.entry[2]
            self.user = self.entry[4]
            self.cur_thread.name += '-' + self.user
            self.submitJob()
        elif self.entry[0] == 'kill':
            #use clientThreads to find jobID, userID
            # clientThread# self, user, requestTime, jobID, startTime, stopTime, jobStatus
            if self.entry[1]=='jobID':
                self.jobID = self.entry[2]
                self.removed = False
                #start with queue
                
                for job in queue:
                    if job[3] == self.jobID:
                        clientThread= job[0]
                        minion = clientThread.minion
                        with queueLock: queue.remove(job)
                        self.reply += 'Removed job ' + self.jobID + ' from queue\n'
                        logging.log(20, 'Removed job ' + self.jobID + ' from queue')
                        if minion["addr"] != 'None':
                            minionKiller = minionJobKiller(minion)
                            minionKiller.start()
                        self.removed = True
                        break
                
                if not self.removed:
                    for job in clientThreads:
                        if job[3] == self.jobID:
                            clientThread = job[0]
                            clientThread.killThread()
                            self.reply += 'Removed job ' + self.jobID + ' from queue\n'
                            logging.log(20, 'Removed job ' + self.jobID + ' from queue')
                            if minion["addr"] != 'None':
                                minionKiller = minionJobKiller(minion)
                                minionKiller.start()
                            self.removed = True
                            break
                if not self.removed:
                    logging.log(20, 'killer Could not find ' + self.jobID)
                    self.reply += 'killer Could not find ' + self.jobID
            elif self.entry[1] == 'userID':
                self.userID = self.entry[2]
                self.removed = False
                
                for job in queue:
                    if job[1] == self.userID:
                        self.jobID = job[3]
                        clientThread= job[0]
                        minion = clientThread.minion
                        with queueLock: queue.remove(job)
                        self.reply += 'Removed job ' + self.jobID + ' from queue\n'
                        logging.log(20, 'Removed job ' + self.jobID + ' from queue')
                        if minion["addr"] != 'None':
                            minionKiller = minionJobKiller(minion)
                            minionKiller.start()
                        self.removed = True
                
                if not self.removed:
                    for job in clientThreads:
                        if job[1] == self.userID:
                            self.jobID = job[3]
                            clientThread = job[0]
                            clientThread.killThread()
                            self.reply += 'Removed job ' + self.jobID + ' from queue\n'
                            logging.log(20, 'Removed job ' + self.jobID + ' from queue')
                            if minion["addr"] != 'None':
                                minionKiller = minionJobKiller(minion)
                                minionKiller.start()
                            self.removed = True
                if not self.removed:
                    logging.log(20, 'killer Could not find ' + self.jobID)
                    self.reply += 'killer Could not find ' + self.jobID
            else:
                logging.log(40, 'could not decode kill request parameters'+ self.requested)
                self.reply += 'could not decode kill:  request parameters' + self.requested
        elif self.entry[0] == 'schedule':
            self.setSchedule()
        logging.log(20, 'Admin reply: ' + self.reply)
        self.request.send(self.reply.encode('utf-8'))
        self.request.shutdown(socket.SHUT_WR)
        self.confirmation = self.request.recv(1024).decode()
        if self.confirmation =='done': logging.log(20, 'Admin received status')
        else: logging.log(30, 'Admin did not receive status')
    def checkStatus(self):
        if self.entry[1] ==  'jobs':
            self.reply+='\n---------------------------\nJOBS\n---------------------------\n'
            if len(self.entry) > 2:
                if self.entry[2] == 'jobID': self.jobID_search_running_jobs(self.entry[3])
                elif self.entry[2] == 'userID': self.userID_search_jobs(self.entry[3])
                elif self.entry[2] == 'queue': self.get_all_queued_jobs()
                elif self.entry[2]=='all':
                    self.get_all_running_jobs()
                    self.get_all_queued_jobs()
                    self.get_all_completed_jobs()
                else:
                    logging.log(40, 'Could not decode status: jobs request parameters'+ self.requested)
                    self.reply='Could not decode status request: jobs: ' + self.requested
            else:
                logging.log(40, 'Could not decode status: jobs request parameters'+ self.requested)
                self.reply='Could not decode status request: jobs: ' + self.requested
        elif self.entry[1] == 'minions':
            self.reply+='\n---------------------------\nMINIONS\n---------------------------\n'
            if len(self.entry) > 2:
                if self.entry[2] == 'Windows' or self.entry[2] == 'Darwin' or self.entry[2] == 'Linux' or self.entry[2] == 'Linux_Windows' or self.entry[2] == 'Linux_Darwin' or self.entry[2] == 'Darwin_Windows' or self.entry[2] == 'Linux_Darwin_Windows':
                    self.os_version = self.entry[2]
                    if self.os_version == 'Windows' or self.os_version =='Linux_Windows' or self.os_version == 'Darwin_Windows' or self.os_version == 'Linux_Darwin_Windows':
                        with networkTableLock: self.reply += networkTable.search_minions_by_OS('Windows')
                    elif self.os_version == 'Darwin' or self.os_version == 'Darwin_Windows' or self.os_version =='Linux_Darwin' or self.os_version == 'Linux_Darwin_Windows':
                        with networkTableLock: self.reply += networkTable.search_minions_by_OS('Darwin')
                    elif self.os_version == 'Linux' or self.os_version == 'Linux_Windows' or self.os_version == 'Linux_Darwin' or self.os_version == 'Linux_Darwin_Windos':
                        with networkTableLock: self.reply += networkTable.search_minions_by_OS('Linux')
                    else:
                        logging.log(40, 'could not decode status: minions request parameters: ' + self.os_version)
                        self.reply+= 'could not decode status: minions request parameters: ' + self.os_version
                elif self.entry[2] == 'gpu':
                    with networkTableLock: self.reply += networkTable.get_all_gpu_minions()
                elif self.entry[2] == 'all':
                    with networkTableLock: self.reply += networkTable.get_all_minions()
                else:
                    with networkTableLock:
                        if len(self.entry)==3: self.reply += networkTable.search_minions_by_IP(self.entry[2])
                        else:
                            logging.log(40, 'could not decode status: minions request parameters'+ self.requested)
                            self.reply += 'could not decode status: minions request parameters' + self.requested
            else:
                logging.log(40, 'could not decode status: minions request parameters'+ self.requested)
                self.reply += 'could not decode status: minions request parameters' + self.requested
        else:
            logging.log(40, 'could not decode status request parameters'+ self.requested)
            self.reply += 'could not decode status: minions request parameters' + self.requested
            self.reply+='\n---------------------------\nUSERS\n---------------------------\n'
            for user in users: self.reply += user + '\n'
            self.reply+='\n---------------------------\nJOBS\n---------------------------\n'
            self.get_all_running_jobs()
            self.get_all_queued_jobs()
            self.get_all_completed_jobs()
            self.reply+='\n---------------------------\nMINIONS\n---------------------------\n'
            with networkTableLock: self.reply += networkTable.get_all_minions()
    def get_all_running_jobs(self):
        #search running jobs
        if len(clientThreads) > 0:
            reply2 = ''
            for client in clientThreads:
                if client[6] != 'Queue':
                    for i in range(1, len(client)): reply2 += client[i] + '\t'
                    reply2 += '\n'
            if reply2 == '':
                reply2 +='There are no running jobs at this time\n'
            self.reply += reply2
        else: self.reply += 'There are no running Jobs at this time\n'
        time.sleep(1)
    def get_all_completed_jobs(self):
        if len(finishedJobList)>0:
            reply2 = ''
            for user in finishedJobList:
                if len(user[2])>0:
                    for job in user[2]:
                        for entry in job: reply2+= entry + '\t'
                        reply2 += '\n'
            if reply2 == '':
                reply2 +='There are no completed jobs at this time\n'
            self.reply += reply2
        else: self.reply += 'There are no completed jobs at this time\n'
        time.sleep(1)
    def get_all_queued_jobs(self):
        if len(queue) > 0:
            for clientThread in queue:
                for i in range(1, len(clientThread)): self.reply+= clientThread[i] + '\t'
                self.reply+=  '\n'
        else: self.reply += 'There are no queued jobs at this time\n'
        time.sleep(1)
    def jobID_search_running_jobs(self, jobID):
        self.reply += 'jobID\n'
        self.found=False
        #search running jobs
        #job id search running jobs
        if len(clientThreads) > 0:
            for client in clientThreads:
                if client[3] == jobID:
                    for i in range(3, len(client)): self.reply+= client[i] + '\t'
                    self.reply += '\n'
                    self.found=True
                    break
        if not self.found and len(finishedJobList) > 0:
            #search completed jobs
            for user in finishedJobList:
                for job in user[2]:
                    if job[1] == jobID:
                        for entry in job: self.reply+= entry + '\t'
                        self.reply += '\n'
                        self.found=True
                        break
        if not self.found:
            self.reply += 'Could not find a job with jobID: ' + jobID + '\n'
        time.sleep(1)
    def userID_search_jobs(self, userID):
        self.reply +='userID' + userID + '\n'
        self.found = False
        if len(finishedJobList) > 0:
            #search completed jobs
            for user in finishedJobList:
                if user[0] == userID:
                    for job in user[2]:
                        for entry in job: self.reply+= entry + '\t'
                        self.reply += '\n'
                    self.found=True
        if len(clientThreads) > 0:
            #search running jobs
            for client in clientThreads:
                if client[1] == userID:
                    for i in range(3, len(client)): self.reply+= client[i] + '\t'
                    self.reply += '\n'
                    self.found=True
        if not self.found:
            self.reply += 'Could not find a job with userID: '+ userID
        time.sleep(1)
    def submitJob(self):
        global adminJobCount
        with networkTableLock: self.minions = networkTable.adminGetMinion(self.requested)
        self.adminJobCount = 0
        if len(self.minions) ==0:
            logging.log(20, 'no ' + self.entry[1] + ' minions at this time')
            self.reply += 'no ' + self.entry[1] + ' minions at this time'
            return
        else:
            self.adminJobCount = adminJobCount
            adminJobCount += 1
        count = 1
        self.reply += 'jobIDs for jobs executed\n'
        self.file = 'ksu_clusters_admin_'+ str(self.adminJobCount)
        self.file = os.path.join(self.entry[3], self.file)
        self.fileLock = threading.Lock()
        with open(self.file, 'w') as f:
            f.write(self.file + '\n')
        for minion in self.minions:
            jobID = minion['os_version'] +'_' + minion['name'] + '_job_' + str(self.adminJobCount)+'-'+str(count)
            self.reply += jobID + '\n'
            jobHandler= minionAdminJobHandler( minion, self.entry, jobID, self.file, self.fileLock)
            jobHandler.start()
            count += 1
        time.sleep(1)
    def setSchedule(self):
        self.classroom = self.entry[1]
        self.operating_time = self.entry[2].split('_')
        self.day = int(self.entry[3])
        self.time=''
        
        logging.log(20, 'Current minion Schedule ' + str(minionSchedule))
        self.room = [self.classroom]
        for times in self.operating_time:
            self.room.append(times)
            self.time += times + ' '
        logging.log(20, 'Requested room ' + str(self.room))
        day = minionSchedule[self.day]
        logging.log(20, 'Current day ' + str(day))
        with scheduleLock:
            for room in day:
                if room[0] == self.classroom:
                    day.remove(room)
                    day.append(self.room)
                    self.reply = 'Updated classroom: ' + self.classroom + ' schedule: '  + self.time
                    return
            day.append(self.room)
        self.reply = 'Set classroom: ' + self.classroom + ' schedule: '  + self.time
        logging.log(20, self.reply)
        logging.log(20, 'minionSchedule ' + str(minionSchedule))
class minionChecker(Thread):
    def __init__(self,client_handler):
        Thread.__init__(self)
        self.client_handler = client_handler
        logging.log(20,"minionChecker_thread")
    def run(self):
        logging.log(20, 'submit job: check_minion')
        count=0
        while self.client_handler.proceed and not self.client_handler.received:
            global networkTable
            with networkTableLock: self.client_handler.replied = networkTable.check_minion(self.client_handler.minion)
            if not self.client_handler.replied: self.count +=1
            if count > 20: self.client_handler.kill_thread()
            time.sleep(100)

class queueManager(Thread):
    global queue
    global queueLock
    global networkTable
    def __init__(self):
        Thread.__init__(self)
        self.setName('Queue_manager')
    def run(self):
        while True:
            logging.log(20, 'Checking Queue. Queue length ' + str(len(queue)))
            for job in queue:
                self.clientThread=job[0]
                with networkTableLock: self.minion_reply = networkTable.getMinion(self.clientThread.job_string)
                self.minion = self.minion_reply[1]
                if self.minion["addr"] != "None":
                    logging.log(20, 'Removing ' + job[3] + ' from queue')
                    job[0].set_minion(self.minion)
                    with queueLock: queue.remove(job)
            time.sleep(50)

class minionJobKiller(Thread):
    def __init__(self,):
        Thread.__init__(self, minion, name)
        self.setName(name + '_' + minion['name'] + 'killer')
        self.name = name
        self.jobID = jobID
        logging.log(20, "Killing " + jobID)
    def run(self):
        try:
            self.minionSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.minionSock.connect((minion['addr'], int(minion['master_port'][1])))
            self.minionSock.send(('kill').encode('utf-8'))
            self.reply = minionSock.recv(1024).decode()
        finally:
            self.minionSock.close()
        if self.reply=='done': logging.log(20, 'Killed ' + self.name)
        else: logging.log(20, 'Could not confirm killing ' + self.name)

class minionTimeUpdater(Thread):
    global scheduleLock
    def __init__(self, minion, start_time, stop_time, today):
        Thread.__init__(self)
        self.start_time = start_time
        self.stop_time = stop_time
        self.minion = minion
        self.today = today
        self.setName(self.minion['name'] + "-time_updater")
    def run(self):
        try:
            self.minionSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.minionSock.connect((self.minion['addr'], int(self.minion['master_port'][1])))
            logging.log(20, "Setting operating time on " + str(self.minion["name"]) + " for day "+ self.today)
            self.minionSock.send(('set_time\n' + self.start_time + '\n' +self.stop_time + "\n" + self.today).encode('utf-8'))
            self.confirm = self.minionSock.recv(1024).decode()
            time.sleep(1)
            if self.confirm=='done': logging.log(20, str(self.minion["name"]) + ' time updated ' )
            else: logging.log(20, str(self.minion["name"]) +' could not confirm time update ')
        except Exception as e:
            logging.log(30, e)
            logging.log(30, "Error while updating " + str(self.minion["name"]) + " operating time")
        finally:
            self.minionSock.close()
        return
class minionAdminJobHandler(Thread):
    def __init__(self, minion, job_string, jobID, file, fileLock):
        Thread.__init__(self)
        self.minion = minion
        self.job_string = job_string
        self.cur_thread = threading.current_thread()
        self.cur_thread.name = 'Admin-' + jobID
        self.folder = jobID
        self.os_version = self.job_string[1]
        self.gpu = self.job_string[2]
        self.output_folder = self.job_string[3]
        self.user = self.job_string[4]
        self.file = file
        self.fileLock = fileLock
        logging.log(20, self.minion['name'] + '\t' + str(job_string) + '\t' + self.folder )
        if self.minion['os_version'] == 'Windows': self.command = self.job_string[5]
        elif self.minion['os_version'] == 'Darwin': self.command = self.job_string[6]
        else:self.command = self.job_string[7]
    def run(self):
        try:
            self.submitJob()
        except Exception as e:
            logging.log(40, 'Error while submitting job. Exception' + str(e))
    def submitJob(self):
        logging.log(20, 'submitting job')
        #create socket object
        self.minion_ip = self.minion["addr"]
        self.minionStatusPort=int(self.minion["master_port"][1])
        self.minion_client_port=int(self.minion["client_port"][1])
        self.clientSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #connect to provided server address and port
        self.clientSock.connect((self.minion_ip, int(self.minionStatusPort)))
        logging.log(20, "minion connected")
        #steps to send file to minion
        self.clientSock.send(('execute\n' + self.folder + '\n' + self.command).encode('utf-8'))
        self.reply1 = self.clientSock.recv(1024).decode()
        self.reply = self.reply1
        while self.reply1:
            self.reply1 = self.clientSock.recv(1024).decode()
            self.reply += self.reply1
        time.sleep(1)
        logging.log(20, 'writing results to ' + self.file)
        with self.fileLock:
            with open(self.file, 'a') as f: f.write(self.reply + '\n')
        
class HTTP_webpageHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path=="/":
            self.path="/indexLambda.html"
        self.path = self.path[1:len(self.path)]
        self.path = os.getcwd()+  sep +'alpha'+ sep + self.path
        try:
            #Check the file extension required and
            #set the right mime type

            sendReply = False
            if self.path.endswith(".html"):
                mimetype='text/html'
                sendReply = True
            elif self.path.endswith(".jpg"):
                mimetype='image/jpg'
                sendReply = True
            elif self.path.endswith(".gif"):
                mimetype='image/gif'
                sendReply = True
            elif self.path.endswith(".js"):
                mimetype='application/javascript'
                sendReply = True
            elif self.path.endswith(".css"):
                mimetype='text/css'
                sendReply = True
            
            elif self.path.endswith(".png"):
                mimetype='image/png'
                sendReply = True
            elif self.path.endswith(".ico"):
                mimetype='image/ico'
                sendReply = True
        
            if sendReply == True:
                #Open the static file requested and send it
                self.send_response(200)
                self.send_header('Content-type',mimetype)
                self.end_headers()
                with  open(self.path, 'rb')  as f: self.wfile.write(f.read())
            return
        except IOError:
            self.send_error(404,'File Not Found: %s' % self.path)
    def do_POST(self):
        if self.path=="/login":
            form = cgi.FieldStorage(fp=self.rfile,headers=self.headers, environ={'REQUEST_METHOD':'POST','CONTENT_TYPE':self.headers['Content-Type'],})
            self.username=form['username'].value
            logging.log(20, self.username + ' attempting to login')
            if pam.pam().authenticate(self.username, form['password'].value):
                #this is where we authenticate the user
                #right now, assume the user is authenticated
                #and send the check status page
                self.path = os.getcwd() + sep +  'alpha' + sep +'checkStatus.html'
                self.topHalf = """
                        <!DOCTYPE html>
                        <html>
                        <head>
                        <meta charset="UTF-8">
                        <title> Check Status </title>
                        <meta name="viewport" content="width=device-width, user-scalable=no, initial-scale=1.0, maximum-scale=1.0, minimum-scale=1.0">
                        <link rel="stylesheet" type="text/css" href="checkStatus.css">
                        </head>
                        <body>
                                <div class="MainContainer">
                                        <div class="Header">
                                                <img src="KSULambda.png" alt="ksu_logo" />
                                        </div>
                                        <div class= "mainBox">
                                                <form action="/checkStatus" method="POST" >
                                                <button  class="button type="submit" value="checkStatus"  button1">Check Status</button>
                                                </form>
                                                
                                                """
                self.topHalf += '<div class= "main"><h2>' + self.username + '</h2>' + """
                            <h3>Processes running </h3>
                            """
                self.middle = self.processesRunning()
                #self.middle += "<br><h4>Completed Processes</h4>"+ self.finishedProcesses()
                
                self.bottomHalf = """
                                        </div>
                                            </div>
                                    </div>
                            </body>
                            </html>
                            """
                mimetype='text/html'
                try:
                    self.send_response(200)
                    self.send_header('Content-type',mimetype)
                    self.end_headers()
                    self.middle = self.processesRunning()
                    self.wfile.write(bytes(self.topHalf + self.middle + self.bottomHalf, 'utf8'))
                except Exception as e:
                    logging.log(30, e)
            else:
                try:
                    self.path= os.getcwd() +  sep  + 'alpha' + sep + "indexLambda.html"
                    mimetype='text/html'
                    self.send_response(200)
                    self.send_header('Content-type',mimetype)
                    self.end_headers()
                    with open(self.path, 'rb') as f: self.wfile.write(f.read())
                except Exception as e:
                    logging.log(30, e)
            return
        if self.path =="/checkStatus":
    
            self.topHalf="""
                <!DOCTYPE html>
                <html>
                <head>
                <meta charset="UTF-8">
                <title> Check Status </title>
                <meta name="viewport" content="width=device-width, user-scalable=no, initial-scale=1.0, maximum-scale=1.0, minimum-scale=1.0">
                <link rel="stylesheet" type="text/css" href="checkStatus.css">
                </head>
                <body>
                <div class="MainContainer">
                        <div class="Header">
                                <img src="KSULambda.png" alt="ksu_logo" />
                        </div>
                        <div class="Main">
                        <h3>Cluster Status </h3>
                                """
            self.bottomHalf="""
			</div>
                </div>
                </body>

                </html>
                """
            self.status = ''
            mimetype='text/html'
            try:
                self.send_response(200)
                self.send_header('Content-type',mimetype)
                self.end_headers()
                with networkTableLock: self.middle = networkTable.table_status('web')
                self.middle += getSchedule('web')
                self.wfile.write(bytes(self.topHalf + self.middle + self.bottomHalf, 'utf8'))
            except Exception as e:
                    logging.log(30, e)
            return
    def getClusterStatus(self):#return html format of status
        pass
    def processesRunning(self):
        self.processes =''
        self.count =0
        for thread in clientThreads:
            if thread[1]==self.username:
                self.count +=1
                #thread indexes 2-request 3-jobID 4-start 5-stop 6-jobstatus
                #'Request: '
                self.processes += thread[2][8:]+ '&emsp;'
                self.processes += thread[3]+ '&emsp;'
                if thread[4].startswith('Start: '):
                    self.processes += thread[4][6:]+ '&emsp;'
                else:
                    self.processes += thread[4] + '&emsp;'
                if thread[5].startswith('Stop_: '):
                    self.processes += thread[5][6:]+ '&emsp;'
                else:
                    self.processes += thread[5] + '&emsp;'
                self.processes += thread[6]+ '&emsp;'
                #for entry in range(2, len(thread)): self.processes += thread[entry] + '&emsp;'
                self.processes += '<br>'
        if self.count == 0:
            self.processes += '<br>None at this time<br>'
        self.processes += '<br>'
        return self.processes
    def finishedProcesses(self):
        self.finishedJobs =''
        self.count =0
        for i in range(len(finishedJobList)-1, -1, -1):
            if finishedJobList[i][0]==self.username:
                self.count +=1
                for data in finishedJobList[i][2]:
                #thread indexes 0-request 1-jobID 2-start 3-stop 4-jobstatus
                    #'Request: '
                    self.finishedJobs += data[0][8:]+ '&emsp;'
                    self.finishedJobs += data[1]+ '&emsp;'
                    if data[2].startswith('Start: '):
                        self.finishedJobs += data[2][6:]+ '&emsp;'
                    else:
                        self.finishedJobs += data[2]+ '&emsp;'
                    if data[3].startswith('Stop_: '):
                        self.finishedJobs += data[3][6:]+ '&emsp;'
                    else:
                        self.finishedJobs += data[3] + '&emsp;'
                    self.finishedJobs += data[4]+ '&emsp;'
                    #for entry in range(0, len(data)): self.processes += data[entry] + '&emsp;
                    self.finishedJobs += '<br>'
        if self.count == 0:
            self.finishedJobs += '<br>None at this time<br>'
        self.finishedJobs += '<br>'
        return self.finishedJobs
def getSchedule(web):
    nl ='\n'
    tab = '\t'
    schedule = ''
    if web=='web':
        nl = '<br>'
        tab= '&emsp;&emsp;&emsp;'
        schedule +="""</div>
                        <div class= "main">
			<h3>Minion Schedule </h3>
                        """
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    daycount =0
        
    for day in minionSchedule:
        schedule += days[daycount] + nl
        daycount += 1
        if len(day) ==0:
            schedule += 'No schedule set for any minion' + nl + nl
        else:
            for minion in day:
                schedule += minion[0]+nl
                totalTimes = len(minion)
                for i in range(1, len(minion), 2):
                    schedule += minion[i] +'-' + minion[i+1]
                    if i + 2 != totalTimes:
                        schedule += ' , '
                schedule += nl + nl
    return schedule
def main():
    print ('Check log at ' + os.path.dirname(os.path.realpath('__file__')) + '\\ksu-lambda_master.log')
    master = Master()
    #master.getAddr()


main()
