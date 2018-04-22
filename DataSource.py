import sys
import numpy as np
import time
from ZNS import ZNSevaluator
import multiprocessing
import shutil

def generateData(inputStr): 
    versions = ["ipv4", "ipv6"]
    vfracs = [0.5, 0.5]
    if inputStr.pro: 
        protocols = inputStr.pro
        pfracs = inputStr.pro_frac
    else: 
        protocols = ['HTTP', 'SMTP', 'TCP', 'UDP', 'SNMP', 'FTP', 'BGP', 'BGP', 'DHCP', 'SSH']
        pfracs = [0.05, 0, 0.45, 0.2, 0.1, 0.2, 0, 0, 0, 0]
    if inputStr.pack_size: 
        sizes = inputStr.pack_size
        sfracs = inputStr.pack_size_frac
    else: 
        sizes = [100, 200]
        sfracs = [0.7, 0.3]
    if inputStr.ips: 
        ips = inputStr.ips
        ifracs = inputStr.ip_frac
    else: 
        ips = ['172.16.254.1', '255.255.255.128', '192.168.0.0', '172.16.0.0', '172.16.254.1', '255.255.128.0', '216.3.128.12', '24.60.91.16', '1.40.215.65', '23.129.64.104']
        ifracs = [0.1, 0.3, 0.05, 0.05, 0.1, 0.1, 0.04, 0.06, 0.1, 0.1]
    if inputStr.bandwidth: 
        bandwidth = inputStr.bandwidth
    else: 
        bandwidth = 0.5
    idx = 1
    totalsize = 0
    temp = str(int(time.time()))
    f = open("./Temp/Packets" + temp, "w")
    f1 = open("./Log/Logs", "w")
    start = time.time()
    while 1: 
        border = 0.0
        sborder = 0.0
        version = np.random.choice(versions, p = vfracs)
        protocol = np.random.choice(protocols, p = pfracs)
        sour, dest = np.random.choice(ips, 2, p = ifracs)
        pick = np.random.uniform(0.0, 1.0)
        size = 0
        for i in range(len(sizes)): 
            if pick >= border: 
                if pick < border + sfracs[i]: 
                    size = np.random.uniform(sborder, sborder + sizes[i])
                    border += sfracs[i]
                    sborder += sizes[i]
        if(totalsize + size >= bandwidth * 1024): 
            while 1: 
                end = time.time()
                elapsed = end - start
                if elapsed > 1: 
                    f1.write(str(totalsize) + " KB(s) of packets passed through in " + 
                        str(elapsed) + " seconds, flow rate is: " + str(totalsize / elapsed / 1024) + " MB/s. ")
                    totalsize = 0
                    f.close()
                    shutil.move("./Temp/Packets" + temp, "./Data/Packets" + temp)
                    temp = str(int(time.time()))
                    f = open("./Temp/Packets" + temp, "w")
                    start = time.time()
                    break
        totalsize += size
        f.write(str(idx) + ' ' + version + ' ' + sour + ' ' + dest + ' ' + protocol + ' ' + str(size) + '\n')
        idx = idx + 1

def newGen(inputStr): 
    newProc = multiprocessing.Process(target = generateData, args = (inputStr, ))
    newProc.start()
