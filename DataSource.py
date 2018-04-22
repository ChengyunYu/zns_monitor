import sys
import numpy as np
import time
from ZNS import ZNSevaluator

def generateData(inputStr): 
    versions = ["ipv4", "ipv6"]
    protocols = inputStr.pro
    sizes = inputStr.pack_size
    ips = inputStr.ips
    vfracs = [0.5, 0.5]
    pfracs = inputStr.pro_frac
    ifracs = inputStr.ip_frac
    sfracs = inputStr.pack_size_frac
    bandwidth = inputStr.bandwidth
    idx = 1
    totalsize = 0
    f = open("./Data/Packets" + str(int(time.time())), "w")
    start = time.time()
    while 1: 
        border = 0.0
        sborder = 0.0
        version = np.random.choice(versions, p = vfracs)
        protocol = np.random.choice(protocols, p = pfracs)
        sour, dest = np.random.choice(ips, 2, p = ifracs)
        pick = np.random.uniform(0.0, 1.0)
        for i in range(1, len(sizes)): 
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
                    print(str(totalsize) + " KB(s) of packets passed through in " + 
                        str(elapsed) + "seconds, flow rate is: " + str(totalsize / elapsed / 1024) + " MB/s. ")
                    totalsize = 0
                    f.close()
                    f = open("./Data/Packets" + str(int(time.time())), "w")
                    start = time.time()
                    break
        totalsize += size
        f.write(str(idx) + ' ' + version + ' ' + sour + ' ' + dest + ' ' + protocol + ' ' + str(size) + '\n')
        idx = idx + 1
