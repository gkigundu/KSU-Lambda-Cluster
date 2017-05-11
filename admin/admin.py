#!/usr/bin/python
# -*- coding: utf-8 -*-
#TCP_client_to_minion_data_transfer.py
#Gabriel Kigundu
#February 1, 2017
docstring="""
This represents the file transfer protocol for the client.
It sends data as zipfile, and waits to receive return data
then unzips return data.

"""

import socket
import time
import tempfile
import zipfile
import os
import unicodedata
import argparse
import sys
from argparse import RawDescriptionHelpFormatter
import getpass
import platform
import json
import logging
import traceback
#logging.basicConfig(filename='ksu-lambda_admin.log',filemode='w',level=logging.DEBUG, format="[%(levelname)s] (%(threadName)-10s) %(message)s",)
logging.basicConfig(level=logging.DEBUG, format="[%(levelname)s] (%(threadName)-10s) %(message)s",)
parser = argparse.ArgumentParser(description=docstring, formatter_class=RawDescriptionHelpFormatter)
parser.add_argument('-stat', '-status', '--check_status', help='Check status of variable', required=False, action='store_true')
parser.add_argument('-sub', '-submit', '--submit_job', help='submit_job', required=False, action='store_true')
parser.add_argument('-j','-job','--job_file', help='Job file name',required=False)
parser.add_argument('-d','-data','--data_folder', help='Data folder name',required=False)
parser.add_argument('-o', '-out', '--output', help='Output file/folder name. File for status, folder for job submission', required=False)
parser.add_argument('-lmw', '--linux_mac_windows', help='Excecutable on any os', required=False, action='store_true')
parser.add_argument('-lm', '--linux_mac', help='Executable only on linux or mac', required=False, action='store_true')
parser.add_argument('-lw', '--linux_windows', help='Excecutable on only linux or windows', required=False, action='store_true')
parser.add_argument('-mw', '--mac_windows', help='Excecutable on only mac or windows', required=False, action='store_true')
parser.add_argument('-l', '--linux', help='Excecutable on only linux', required=False, action='store_true')
parser.add_argument('-mac', '--mac', help='Excecutable on only mac', required=False, action='store_true')
parser.add_argument('-win', '--windows', help='Excecultable on only windows', required=False, action='store_true')
parser.add_argument('-g', '-gpu', '--gpu', help='Get a minion with a powerful GPU', required=False, action='store_true')
parser.add_argument('-jobID','-jobID','--job_ID', help='check_status: jobs: Job ID OR kill: Job_ID',required=False)
parser.add_argument('-userID','-userID','--user_ID', help='check_status: jobs: user ID OR kill: User_ID',required=False)
parser.add_argument('-k', '-kill', '--kill', help='kill', required=False, action='store_true')
parser.add_argument('-jobs','--jobs', help='check_status: jobs',required=False, action='store_true')
parser.add_argument('-minion','--minions', help='check_status: minions',required=False, action='store_true')
parser.add_argument('-ip','-IP', '--minion_IP', help='check_status: minions: IP',required=False)
parser.add_argument('-queue','--queue', help='check_status: queue',required=False, action='store_true')
parser.add_argument('-sched', '--schedule', help='check_status: minions: IP',required=False, action='store_true')
parser.add_argument('-t','-time', '--time', help='schedule time Format: HH:MM_HH:MM ',required=False)
parser.add_argument('-room', '--classroom', help='classroom to be scheduled',required=False)
parser.add_argument('-day', '--day', help='day to be scheduled',required=False)

args = parser.parse_args()
global master_ip
global master_port
global master_address
global minion_ip
global minion_port
global minion_address
global user
global data
global jobID
global myaddress
global output_folder
global schedule
global operating_time
global classroom
#variables for parsing job file
global windows
global mac
global linux
global lamda_windows
global lambda_mac
global lambda_linux
windows=False
mac = False
linux = False

master_ip="127.0.0.1"
master_port=60000
master_address = (master_ip, master_port)
#minon address and port provided from master
minion_ip=None
minion_port=None
minion_address = None
user = getpass.getuser()
data=args.data_folder
jobID=''
output_folder = os.path.expanduser('~')
kill = ''
os_version=None
gpu='any'

def job_parser(job_file):
    global windows
    global mac
    global linux
    global lambda_windows
    global lambda_mac
    global lambda_linux
    file = open(job_file, 'r')
    job_list = []
    for line in file:
        line = line.strip()
        job_list.append(line)
    file.close()
    count = 0
    sections = []
    section = []
    
    for line in job_list:
        if line.startswith('####'):
            count +=1
            if len(section)>0:
                sections.append(section)
                section = []
        elif line.startswith('#'): pass
        else:
            section.append(line)
    sections.append(section)
    job_list=sections
    section_count = 1
    #section 0 parameters, section 1: windows, section 2: mac, section 3: linux
    for section in job_list:
        #print('section ' + str(section_count))
        section_count+=1
        #for line in section: print(line)
    set_arguments(job_list[0])
    lambda_windows = ''
    count = 0
    for line in job_list[1]:
        line = line.strip()
        if len(line) > 0:
            if count ==0:
                    lambda_windows +=  line
                    count += 1
            elif count > 0:
                lambda_windows +=  ' &&  ' + line
    lambda_mac = ''
    count = 0
    for line in job_list[2]:
        line = line.strip()
        if len(line) > 0:
            if count ==0:
                    lambda_mac +=  line 
                    count += 1
            elif count > 1:
                lambda_mac += ' ;  ' + line 
    lambda_linux = ''
    count = 0
    for line in job_list[3]:
        line = line.strip()
        if len(line) > 0:
            if count ==0:
                    lambda_linux +=  line 
                    count += 1
            elif count > 1:
                lambda_linux += ' ; ' + line 
    if linux:
        if mac:
            if windows: args.linux_mac_windows=True
            else: args.linux_mac=True
        elif windows: args.linux_windows=True
        else: args.linux=True
    elif mac:
        if windows: args.mac_windows=True
        else: args.mac=True
    elif windows: args.windows=True
    else: sys.exit('Please input yes or no for an OS')

def set_arguments(parameters):
    global windows
    global mac
    global linux
    count = 0
    for param in parameters:
        count+=1
        param = param.split('=')
        if len(param) == 0: continue
        if len(param) !=2:
            logging.log(50, 'Invalid job file format. All parameters must be set to yes or no')
            print (param)
            sys.exit('Invalid job file format. All parameters must be set to yes or no. Folders must be set to a path as well.')
        if param[0].strip().lower() == 'windows':
            if param[1].strip().lower() == 'yes': windows=True
        elif param[0].strip().lower() == 'mac':
            if param[1].strip().lower() == 'yes': mac = True
        elif param[0].strip().lower() == 'linux':
            if param[1].strip().lower() == 'yes': linux = True
        elif param[0].strip().lower() == 'gpu':
            if param[1].strip().lower() == 'yes': args.gpu=True
        elif param[0].strip().lower() == 'output_folder':
            if len(param[1]) > 0: args.output = param[1].strip()
        elif param[0].strip().lower() == 'schedule':
            if param[1].strip().lower() == 'yes': args.schedule = True
        elif param[0].strip().lower() == 'operating_time': args.time = param[1].strip()
        elif param[0].strip().lower() == 'classroom': args.output = param[1].strip()
        elif param[0].strip().lower() == 'day': args.day = param[1].strip()
        elif param[0].strip().lower() == 'kill':
            if param[1].strip().lower() == 'yes': args.kill = True
        elif param[0].strip().lower() == 'jobid': args.jobID = param[1].strip()
        elif param[0].strip().lower() == 'userid': args.userID = param[1].strip()
        else: sys.exit('unknown parameter in job file: ' + param[0])
    if count != 11:
        sys.exit('Invalid job file format. 8 parameters required. ' + str(count) + ' parameters found')
  



def check_parameters():
    global windows
    global mac
    global linux
    global lambda_windows
    global lambda_mac
    global lambda_linux
    global no_queue
    global gpu
    global os_version
    global output_folder
    global data
    global classroom
    global operating_time
    global day
    if args.check_status:
        if args.submit_job or args.job_file or args.data_folder or  args.output: sys.exit('You entered invalid parameters for checking status')
        checkStatus()
    elif args.submit_job:
        if args.job_file:
            if not os.path.exists(args.job_file): sys.exit ("could not find specified job file")
            job_parser(args.job_file)
        else:
            sys.exit ("You must specify a data folder")
        if args.output:
            if len(args.output) > 0:
                if not os.path.exists(args.output):
                    try:
                        logging.log(20, 'Output data path must be absolute path i.e /home/user/output_file/folder')
                        os.makedirs(args.output)
                        logging.log(20, 'output file/folder: ' + os.path.abspath(args.output))
                    except:
                        stack=traceback.extract_stack()
                        logging.log(20, stack)
                        print(stack)
                        sys.exit('could not create output file/folder')            
                output_folder = args.output
        if args.linux_mac_windows:
            if args.linux_mac or args.linux_windows or args.mac_windows or args.linux or args.mac or args.windows: sys.exit('Please, enter one argument for os version')
            os_version = 'Linux_Darwin_Windows'
        elif args.linux_mac:
            if args.linux_windows or args.mac_windows or args.linux or args.mac or args.windows: sys.exit('Please, enter one argument for os version')
            os_version = 'Linux_Darwin'
        elif args.linux_windows:
            if args.mac_windows or args.linux or args.mac or args.windows: sys.exit('Please, enter one argument for os version')
            os_version = 'Linux_Windows'
        elif args.mac_windows:
            if args.linux or args.mac or args.windows: sys.exit('Please, enter one argument for os version')
            os_version = 'Darwin_Windows'
        elif args.linux:
            if args.mac or args.windows: sys.exit('Please, enter one argument for os version')
            os_version = 'Linux'
        elif args.mac:
            if args.windows: sys.exit('Please, enter one argument for os version')
            os_version = 'Darwin'
        elif args.windows:
            os_version = 'Windows'
        else: sys.exit('You must specify at least one OS that job can execute on')
        if args.gpu: gpu=args.gpu
        else: gpu = 'any'
        submitJob()
    elif args.schedule:
        if args.submit_job or args.data_folder or  args.output or args.linux_mac or args.linux_windows or args.mac_windows or args.linux or args.mac or args.windows or args.linux_mac_windows or args.gpu: sys.exit('When scheduling, select --time and --classroom only')
        if args.classroom: classroom = args.classroom
        else: sys.exit('classroom required when scheduling')
        if args.time:
            temp_time = args.time.split('_')
            if len(temp_time) % 2 != 0: sys.exit('Time must be entered in multiples of two for start and stop: HH:MM_HH:MM_HH:MM_HH:MM')
            else:
                for time in temp_time:
                    timesplit = time.split(':')
                    if len(timesplit) != 2: sys.exit('Time format must be enterd as HH:MM using semicolon as separator')
                    try:
                        int(timesplit[0])
                        int(timesplit[1])
                    except:
                        sys.exit('Please use only numbers to enter time') 
            operating_time = args.time
        if args.day:
            try:
                if int(args.day) >= 0 and int(args.day) <= 6: day= args.day
                else: sys.exit('Day must be between 0 for Monday and 6 for Sunday')
            except:
                sys.exit('Day must be a number between 0 for Monday and 6 for Sunday')
        else: sys.exit('Please enter a day for scheduling')
        schedule()
    elif args.kill:
        if args.submit_job or args.data_folder or  args.output or args.linux_mac or args.linux_windows or args.mac_windows or args.linux or args.mac or args.windows or args.linux_mac_windows or args.gpu or args.schedule or args.time or args.classroom: sys.exit('When using kill, enter all select either jobID or userID')
        if not args.job_ID or not args.user_ID: sys.exit('You must select either a userID or jobID')
        if args.job_ID and args.user_ID: sys.exit('You must select either a userID or jobID but not both')
        killJobs()
    else:
        parser.print_help()
        sys.exit()

#set job_string to send to master    
#def setJobString():
    
#send master the parameters
#
def checkStatus():
    status_string='status'
    if args.jobs:
        if args.minion_IP or args.gpu or args.windows or args.mac or args.linux or args.linux_mac or args.linux_windows or args.mac_windows or args.linux_mac_windows: sys.exit('Parameters for checking status of jobs are: jobID, userID, queue')
        status_string += '\tjobs'
        if args.job_ID: status_string += '\tjobID '+ args.job_ID
        elif args.user_ID: status_string += '\tuserID '+ args.user_ID
        elif args.queue: status_string += '\tqueue'
        else: status_string += '\tall'
    elif args.minions:
        status_string += '\tminions'
        if args.windows: status_string += '\tWindows'
        elif args.mac: status_string += '\tDarwin'
        elif args.linux: status_string += '\tLinux'
        elif args.linux_windows: status_string += '\tLinux_Windows'
        elif args.linux_mac: status_string += '\tLinux_Darwin'
        elif args.mac_windows: status_string += '\tDarwin_Windows'
        elif args.linux_mac_windows: status_string += '\tLinux_Darwin_Windows'
        elif args.gpu: status_string += '\tgpu'
        elif args.minion_IP: status_string += '\t' + args.minion_IP
        else: status_string += '\tall'
    else:
        sys.exit('Please select statas for either minion or jobs. ie -status - minion or -status -jobs') 
    
    masterSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        masterSock.connect(master_address)
        
        masterSock.send(status_string.encode('utf-8'))
        statusRecv =masterSock.recv(1024)
        status = statusRecv
        while statusRecv:
            statusRecv =masterSock.recv(1024)
            status += statusRecv
        masterSock.send(('done').encode('utf-8'))
        masterSock.shutdown(socket.SHUT_WR)
        masterSock.close()
        status=status.decode()
        logging.log(20, status)
        if args.output:
            #write results to output folder
            file=''
            try:
                with open(args.output, 'w') as file: file.write(status + '\n')
            except:
                logging.log(40, repr(traceback.extract_stack()))
                print('Could not write to output folder. Check log file for status')
        else:
            print(status)
    except:
        sys.exit('Error connecting to master')
def submitJob():
    try:
        global output_folder
        if output_folder[len(output_folder)-1] != os.sep: output_folder += os.sep
        job_string = 'submit_job\t' + os_version + '\t' + gpu + '\t' + output_folder + '\t' + user + '\t' + lambda_windows + '\t' + lambda_mac + '\t' + lambda_linux
        masterSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #try:
        logging.log(20, 'Connecting to master' + str(master_address))
        masterSock.connect(master_address)
        #except:
        masterSock.send(job_string.encode('utf-8'))
        logging.log(20, 'sending job parameters to master. Job parameters: ' + job_string)
        reply = masterSock.recv(1024).decode()
        logging.log(20, 'reply '+ reply)
        masterSock.send(('done').encode('utf-8'))
        masterSock.shutdown(socket.SHUT_WR)
        masterSock.close()
        print(reply)
    except Exception as e:
        logging.log(50, e)

def schedule():
    try:
        job_string = 'schedule\t' + classroom + '\t' + operating_time + '\t' + day
        masterSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #try:
        logging.log(20, 'Connecting to master' + str(master_address))
        masterSock.connect(master_address)
        #except:
        masterSock.send(job_string.encode('utf-8'))
        logging.log(20, 'sending job parameters to master. Job parameters: ' + job_string)
        reply = masterSock.recv(1024).decode()
        logging.log(20, 'reply '+ reply)
        masterSock.send(('done').encode('utf-8'))
        masterSock.shutdown(socket.SHUT_WR)
        masterSock.close()
        print(reply)
    except Exception as e:
        logging.log(50, e)
        sys.exit('error')

def killJobs():
    try:
        job_string = 'kill\t'
        if args.job_ID: job_string += 'jobID\t' + args.jobID
        elif args.userID: job_string += 'userID\t' + args.userID
        masterSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #try:
        logging.log(20, 'Connecting to master' + str(master_address))
        masterSock.connect(master_address)
        #except:
        masterSock.send(job_string.encode('utf-8'))
        logging.log(20, 'sending job parameters to master. Job parameters: ' + job_string)
        reply = masterSock.recv(1024).decode()
        logging.log(20, 'reply '+ reply)
        reply=json.loads(reply)
        logging.log(20, reply)
        print(reply)
    except Exception as e:
        logging.log(50, e)
        sys.exit('error')
    
print ('Check log at ' + os.path.dirname(os.path.realpath('__file__')) + '\\ksu-lambda_admin.log')
try:
    check_parameters()
except Exception as e:
    logging.log(50, e)
    sys.exit('error')
