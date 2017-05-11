def job_parser(job_file):
    file = open(job_file, 'r')
    job = []
    for line in file:
        if len(line)>0:
            job.append(line.strip())
    file.close()
    job_list = get_sections(job)
    section_count = 1
    #the loop below is just to verify the sections are in the list/array. section 1 should be parameters, 2 should be windows, 3 should be mac, 4 should be linux
    #using these sections in this job_list variable(list/array), split the parameters using = and set the corresponding args.'variable' that will be in client.py
    #for example, windows = yes would be args.windows but also if mac=yes and windows=yes then args.mac_windows etc
    #Then create the appropriate lambda.bat(windows), lambda_mac.sh(mac), lambda_linux.sh(linux) in the given data folder
    #for now, create them in the same path as the job_parser.py. This can be done with file=open('lambda.bat', 'w'). The 'w' means write and create file if it doesn't exist
    #it will open in the same path as the job_parser.py unless you specify a full path like f=open('C:/lambda/lambda.bat', 'w'). The folder must exist in this case though or it will crash and burn
    for section in job_list:
        print('section ' + str(section_count))
        section_count+=1
        for line in section:
            print(line)

def get_sections(job_list):
    count = 0
    sections = []
    section = []
    for line in job_list:
        if line.startswith('####'):
            if len(section)>0:
                sections.append(section)
                section = []
        elif line.startswith('#'):
            pass
        else:
            section.append(line)
    sections.append(section)
    return sections

job_parser('sample_job_file.txt')

"""
if args.check_status:
    if args.submit_job or not args.output or not args.job_file or not args.data_folder or args.linux_mac_windows or args.linux_mac or args.linux_windows or args.mac_windows or args.linux or args.mac or args.windows or args.no_queue or args.gpu or args.high_ram:
            sys.exit('You entered invalid parameters for checking status')
    checkStatus()
elif args.submit_job:
    if args.job_file:
        if not os.path.exists(args.job_file):
            sys.exit ("could not find specified job file")
        if len(sys.argv) > 3:
            if not args.output:
                sys.exit("You entered invalid parameters for submitting job.\nEither enter your job parameters on the command line or submit a job file")
            elif len(sys.argv) > 4:
                sys.exit ("You entered invalid parameters for submitting job.\nEither enter your job parameters on the command line or submit a job file")
        job_parser(args.job_file)
    if args.data_folder:
        if not os.path.exists(args.data_folder):
            sys.exit ("could not find specified data folder")
        elif not os.listdir(args.data_folder):
            sys.exit ("You entered an empty data folder")
    else:
        sys.exit ("You must specify a data folder")
    if args.linux_mac_windows:
        if args.linux_mac or args.linux_windows or args.mac_windows or args.linux or args.mac or args.windows:
            sys.exit('Please, enter on argument for os version')
        os_version = 'Linux_Darwin_Windows'
    elif args.linux_mac:
        if args.linux_windows or args.mac_windows or args.linux or args.mac or args.windows:
            sys.exit('Please, enter on argument for os version')
        os_version = 'Linux_Darwin'
    elif args.linux_windows:
        if args.mac_windows or args.linux or args.mac or args.windows:
            sys.exit('Please, enter on argument for os version')
        os_version = 'Linux_Windows'
    elif args.mac_windows:
        if args.linux or args.mac or args.windows:
            sys.exit('Please, enter on argument for os version')
        os_version = 'Darwin_Windows'
    elif args.linux:
        if args.mac or args.windows:
            sys.exit('Please, enter on argument for os version')
        os_version = 'Linux'
    elif args.mac:
        if args.windows:
            sys.exit('Please, enter on argument for os version')
        os_version = 'Darwin'
    elif args.windows:
        os_version = 'Windows'
    else:
        sys.exit('You must specify at least one OS that job can execute on')
    if args.no_queue:
        no_queue='True'
    if args.gpu:
        gpu='True'
    if args.high_ram:
        high_ram = 'True'
    if args.output:
        if not os.path.exists(os.getcwd, args.output):
            sys.exit('specified output folder does not exist.')
"""
