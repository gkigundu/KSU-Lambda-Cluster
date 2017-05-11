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
logging.basicConfig(filename='ksu-lambda_client.log',filemode='a',level=logging.DEBUG, format="[%(levelname)s] (%(threadName)-10s) %(message)s",)
#logging.basicConfig(level=logging.DEBUG, format="[%(levelname)s] (%(threadName)-10s) %(message)s",)
parser = argparse.ArgumentParser(description=docstring, formatter_class=RawDescriptionHelpFormatter)
parser.add_argument('-stat', '-status', '--check_status', help='Check status of variable', required=False, action='store_true')
parser.add_argument('-sub', '-submit', '--submit_job', help='submit_job', required=False, action='store_true')
parser.add_argument('-job','-job','--job_file', help='Job file name',required=False)
parser.add_argument('-d','-data','--data_folder', help='Data folder name',required=False)
parser.add_argument('-o', '-out', '--output', help='Output folder name', required=False)
#parser.add_argument('-k', '-kill', '--kill_process', help='PID to kill', required=False) currently only for admin only
parser.add_argument('-lmw', '--linux_mac_windows', help='Excecutable on any os', required=False, action='store_true')
parser.add_argument('-lm', '--linux_mac', help='Executable only on linux or mac', required=False, action='store_true')
parser.add_argument('-lw', '--linux_windows', help='Excecutable on only linux or windows', required=False, action='store_true')
parser.add_argument('-mw', '--mac_windows', help='Excecutable on only mac or windows', required=False, action='store_true')
parser.add_argument('-l', '--linux', help='Excecutable on only linux', required=False, action='store_true')
parser.add_argument('-m', '--mac', help='Excecutable on only mac', required=False, action='store_true')
parser.add_argument('-w', '-win', '--windows', help='Excecultable on only windows', required=False, action='store_true')
parser.add_argument('-nq', '-no_queue','--no_queue', help='Do not put job in queue', required=False, action='store_true')
parser.add_argument('-r', '-ram','--high_ram', help='Get the minion with the highest available ram', required=False, action='store_true')
parser.add_argument('-g', '-gpu', '--gpu', help='Get a minion with a powerful GPU', required=False)
parser.add_argument('-wall', '--wall_time', help='Expected max time to run the job', required=False)
args = parser.parse_args()

#parser.add_argument('-loc', '-local','--localhost', help='run on localhost', required=False, action='store_true')
#args.locahhost = True
global master_ip
global master_port
global master_address
global minion_ip
global minion_port
global minion_address
global user
global data
global jobID
global output_folder
global wall_time
#variables for parsing job file
global windows
global mac
global linux
global lambda_windows
global lambda_mac
global lambda_linux
windows=False
mac = False
linux = False
lambda_windows = ''
lambda_mac = ''
lambda_linux = ''
output_folder = os.path.expanduser('~')

master_ip="127.0.0.1"
master_port=40000
master_address = (master_ip, master_port)
#minon address and port provided from master
minion_ip=None
minion_port=None
minion_address = None
user = getpass.getuser()
data=None
status_string='status'
job_string = ''
jobID=''


os_version=None
no_queue='False'
gpu='any'
high_ram='False'

logging.log(20, args)

def job_parser(job_file):
    global windows
    global mac
    global linux
    global lambda_windows
    global lambda_mac
    global lambda_linux
    with open(job_file, 'r') as file:
        job_list = []
        for line in file:
            line = line.strip()
            job_list.append(line)
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
    if count != 4:
        sys.exit('Job file must have 4 and only 4 sections even if they are not used. Please check job file format')
    sections.append(section)
    job_list=sections
    section_count = 1
    #section 0 parameters, section 1: windows, section 2: mac, section 3: linux
    #for section in job_list:
        #print('section ' + str(section_count))
        #section_count+=1
        #for line in section: print(line)
    
    set_arguments(job_list[0])
    lambda_windows = job_list[1]
    lambda_mac = job_list[2]
    lambda_linux = job_list[3]
##    lambda_windows = job_list[1]
##    count = 0
##    print(str(job_list))
##    for line in job_list[1]:
##        print('lambda windows ' + line)
##        line = line.strip()
##        print('lambda windows ' + line)
##        if len(line) > 0:
##            if count ==0:
##                lambda_windows +=  line
##                count += 1
##            elif count > 0:
##                lambda_windows +=  ' &&  ' + line
##        print('lambda windows ' + line)
##        print('lambda windows ' + lambda_windows)
##    lambda_mac = ''
##    count = 0
##    for line in job_list[2]:
##        line = line.strip()
##        if len(line) > 0:
##            if count ==0:
##                lambda_mac +=  line 
##                count += 1
##            elif count > 1:
##                lambda_mac += ' ;  ' + line 
##    lambda_linux = ''
##    count = 0
##    for line in job_list[3]:
##        line = line.strip()
##        if len(line) > 0:
##            if count ==0:
##                lambda_linux +=  line 
##                count += 1
##            elif count > 1:
##                lambda_linux += ' ; ' + line 
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
        elif param[0].strip().lower() == 'no_queue':
            if param[1].strip().lower() == 'yes': args.no_queue = True
        elif param[0].strip().lower() == 'gpu': args.gpu=param[1]
        elif param[0].strip().lower() == 'highest_ram':
            if param[1].strip().lower() == 'yes': args.high_ram=True
        elif param[0].strip().lower() == 'data_folder': args.data_folder = param[1].strip()
        elif param[0].strip().lower() == 'output_folder': args.output = param[1].strip()
        elif param[0].strip().lower() == 'wall_time': args.wall_time = param[1].strip()
        else: sys.exit('unknown parameter in job file: ' + param[0])
    if count != 9:
        sys.exit('Invalid job file format. 8 parameters required. ' + str(count) + ' parameters found')
    

def check_arguments():
    global windows
    global mac
    global linux
    global lambda_windows
    global lambda_mac
    global lambda_linux
    global no_queue
    global gpu
    global high_ram
    global os_version
    global output_folder
    global data
    global wall_time
    if args.check_status:
        if args.submit_job or args.output or args.job_file or args.data_folder or args.linux_mac_windows or args.linux_mac or args.linux_windows or args.mac_windows or args.linux or args.mac or args.windows or args.no_queue or args.gpu or args.high_ram: sys.exit('You entered invalid parameters for checking status')
        checkStatus()
    elif args.submit_job:
        if args.job_file:
            if not os.path.exists(args.job_file): sys.exit ("could not find specified job file")
            job_parser(args.job_file)
        if args.data_folder:
            if not os.path.exists(args.data_folder): sys.exit ("could not find specified data folder.\nData folder must be full path i.e /home/user/data")

            data = args.data_folder
            #create files with commands for each OS
            if data[len(data)-1] != os.sep: data += os.sep
            if args.job_file:
                if windows:
                    try:
                        with open(os.path.join(data, 'lambda.bat'), 'w') as file:
                            for line in lambda_windows: file.write(line + '\n')
                    except Exception as e:
                        logging.log(40, e)
                        stack=traceback.extract_stack()
                        logging.log(40, stack)
                        print(stack)
                        sys.exit('unable to write lambda.bat')
                if mac:
                    try:
                        with open(os.path.join(data, 'lambda_mac.sh'), 'w') as file:
                            for line in lambda_mac: file.write(line + '\n')
                    except Exception as e:
                        logging.log(40, e)
                        stack=traceback.extract_stack()
                        logging.log(40, stack)
                        print(stack)
                        sys.exit('unable to write lambda_mac.sh')
                if linux:
                    try:
                        with open(os.path.join(data, 'lambda_linux.sh'), 'w') as file:
                            for line in lambda_windows: file.write(line + '\n')
                    except Exception as e:
                        logging.log(40, e)
                        stack=traceback.extract_stack()
                        logging.log(40, stack)
                        print(stack)
                        sys.exit('unable to write lambda_linux.sh')
                        
            if not os.listdir(args.data_folder): sys.exit ("You entered an empty data folder.\nData folder must be full path i.e /home/user/data")
            
        else: sys.exit ("You must specify a data folder")
        if args.output:
            if not os.path.exists(args.output):
                try:
                    logging.log(20, 'Output data path must be absolute path i.e /home/user/output_folder')
                    os.makedirs(args.output)
                    logging.log(20, 'output folder: ' + os.path.abspath(args.output))
                except:
                    stack=traceback.extract_stack()
                    logging.log(20, stack)
                    print(stack)
                    sys.exit('could not create output folder')
            output_folder = args.output
            if output_folder[len(output_folder)-1] != os.sep: output_folder += os.sep
        if args.linux_mac_windows:
            if args.linux_mac or args.linux_windows or args.mac_windows or args.linux or args.mac or args.windows: sys.exit('Please, enter on argument for os version')
            os_version = 'Linux_Darwin_Windows'
        elif args.linux_mac:
            if args.linux_windows or args.mac_windows or args.linux or args.mac or args.windows: sys.exit('Please, enter on argument for os version')
            os_version = 'Linux_Darwin'
        elif args.linux_windows:
            if args.mac_windows or args.linux or args.mac or args.windows: sys.exit('Please, enter on argument for os version')
            os_version = 'Linux_Windows'
        elif args.mac_windows:
            if args.linux or args.mac or args.windows: sys.exit('Please, enter on argument for os version')
            os_version = 'Darwin_Windows'
        elif args.linux:
            if args.mac or args.windows: sys.exit('Please, enter on argument for os version')
            os_version = 'Linux'
        elif args.mac:
            if args.windows: sys.exit('Please, enter on argument for os version')
            os_version = 'Darwin'
        elif args.windows:
            os_version = 'Windows'
        else:
            sys.exit('You must specify at least one OS that job can execute on')
        if args.no_queue: no_queue='True'
        if args.gpu: gpu=args.gpu
        if args.high_ram: high_ram = 'True'
        if not args.wall_time: sys.exit('You must specify wall_time when submitting a job')
        else:
            try:
                time = args.wall_time.split(":")
                if len(time) != 2:
                    raise IndexError
                else:
                    hours = time[0]
                    minutes=time[1]
                    hours= int(hours)
                    minutes = int(minutes)
            except IndexError: sys.exit("Incorrect format for wall_time. Format must be HH:MM where H is hour and M is minutes")
            except Exception as e:
                logging.log(40, e)
                sys.exit("Incorrect format for wall_time.")
            wall_time=args.wall_time
        submitJob()
    else:
        parser.print_help()
        sys.exit()
       

#send master the info
def checkStatus():
    status_string='status\t' + user 
    masterSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #timeout=masterSock.gettimeout()
    #timeout=timeout*10
    #masterSock.settimeout(timeout)
    try:
        logging.log(20, 'Status: connecting to master at ' + str(master_address))
        masterSock.connect(master_address)
    except:
        logging.log(20,'cannot connect to master at this time')
        sys.exit('cannot connect to master at this time')
    masterSock.send(status_string.encode('utf-8'))
    statusRecv =masterSock.recv(1024)
    status = statusRecv
    if len(status) >1023:
        while statusRecv:
            statusRecv =masterSock.recv(1024)
            status += statusRecv
    status=status.decode()
    logging.log(20, status)
    print(status)
    masterSock.send(('done').encode('utf-8'))
    masterSock.shutdown(socket.SHUT_WR)
    masterSock.close()
    sys.exit()

def submitJob():
    job_string = 'submit_job\t' + os_version + '\t' + no_queue + '\t' + gpu + '\t' + high_ram + '\t' + user + '\t' + data + '\t' + output_folder + '\t' + wall_time
    masterSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        logging.log(20, 'Submit_job: Connecting to master' + str(master_address))
        masterSock.connect(master_address)
    except:
        logging.log(50, 'Unable to connect to master' + str(master_address))
        sys.exit('cannot connect to master at this time')
    logging.log(20, 'sending job parameters to master. Job parameters: ' + job_string)
    masterSock.send(job_string.encode('utf-8'))
    reply = masterSock.recv(1024).decode()
    logging.log(20, 'reply '+ reply)
    if reply == 'minion':
        logging.log(20, 'minion has been assigned. Job will begin execution shortly')
        print('minion has been assigned. Job will begin execution shortly')
        reply = masterSock.recv(1024).decode()
        reply=json.loads(str(reply))
        logging.log(20, str(reply))
        print('minion OS:    ' + reply['minion_OS'] + '\njobID:        ' + reply['jobID'] + '\nRequest Time: ' + reply['request_time'])
        masterSock.send(('done').encode('utf-8'))
        masterSock.shutdown(socket.SHUT_WR)
        masterSock.close()
        sys.exit('\nUse jobID:' + str(reply['jobID']) + ' to track this job\n')
    elif reply == 'queue':
        logging.log(20, 'Minion cannot be assigned at this time. Job has been placed in queue')
        print('Minion cannot be assigned at this time. Job has been placed in queue')
        reply = masterSock.recv(1024).decode()
        reply=json.loads(reply)
        logging.log(20, str(reply))
        
        print('jobID:        ' + reply['jobID'] + '\nRequest Time: ' + reply['request_time'])
        masterSock.send(('done').encode('utf-8'))
        masterSock.shutdown(socket.SHUT_WR)
        masterSock.close()
        sys.exit('\nUse jobID:' + str(reply['jobID']) + ' to track this job\n')
    elif reply=='None':
        masterSock.send(('done').encode('utf-8'))
        masterSock.shutdown(socket.SHUT_WR)
        masterSock.close()  
        checkStatus()
        logging.log(20, 'no minion available with requeted job parameters')
        print('no minion available with requeted job parameters')
        sys.exit('no minion available with requeted job parameters')
    elif reply=='try_queue':
        masterSock.send(('done').encode('utf-8'))
        masterSock.shutdown(socket.SHUT_WR)
        masterSock.close()  
        checkStatus()
        logging.log(20, 'no minion currently available with requeted job parameters.\nConsider placing job in queue')
        sys.exit('no minion currently available with requeted job parameters.\nConsider placing job in queue')
    else:
        masterSock.send(('done').encode('utf-8'))
        masterSock.shutdown(socket.SHUT_WR)
        masterSock.close()
        logging.log(20, reply)
        sys.exit(reply)

check_arguments()
print ('Check log at ' + os.path.dirname(os.path.realpath('__file__')) + '\\ksu-lambda_client.log')

