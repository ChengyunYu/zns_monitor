
# coding: utf-8

# In[ ]:


import socket
import sys
import numpy as np
HOST = ''
PORT = 9999
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print 'Socket created'
try:
    s.bind((HOST, PORT))
except socket.error , msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    sys.exit()
print 'Socket bind complete'
s.listen(10)
print 'Socket now listening'
conn, addr = s.accept()
import numpy as np
versions = ['ipv4', 'ipv6']
protocols = ['HTTP', 'SMTP', 'TCP', 'UDP', 'SNMP', 'FTP', 'BGP', 'BGP', 'DHCP', 'SSH']
ips = ['172.16.254.1', '255.255.255.128', '192.168.0.0', '172.16.0.0', '172.16.254.1', '255.255.128.0', '216.3.128.12', '24.60.91.16', '1.40.215.65', '23.129.64.104']
vfracs = np.array([0.6, 0.4])
pfracs = np.array([0.05, 0, 0.45, 0.2, 0.1, 0.2, 0, 0, 0, 0])
ifracs = np.array([0.1, 0.3, 0.05, 0.05, 0.1, 0.1, 0.04, 0.06, 0.1, 0.1])
idx = 1
while 1:
    version = np.random.choice(versions, p = vfracs)
    protocol = np.random.choice(protocols, p = pfracs)
    sour, dest = np.random.choice(ips, 2, p = ifracs)
    size = np.random.random_sample()
    conn.send(str(idx) + ' ' + version + ' ' + sour + ' ' + dest + ' ' + protocol + ' ' + str(size) + '\n')
    idx = idx + 1
