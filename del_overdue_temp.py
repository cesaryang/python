import pexpect
import time

host = '172.18.99.43'

username = 'root'
password = 'rootroot'

file_input = file('overdue_asset_id_temp_2.txt', 'r')

#login
child = pexpect.spawn('ssh ' + username  + '@' + host)
child.logfile = file('overdue_asset_id_temp_2.log', 'w')
child.expect("id_rsa':")
child.sendline()
child.expect('password:')
child.sendline(password)
child.expect('#')
child.sendline('cd /arroyo/db')
child.expect('#')
child.sendline('./AVSDBUtil')

# del #B# file
for line in file_input:
    child.expect(r'Enter [1/2/3/4/5/6/7/8] or 0? :')
    child.sendline('1')
    child.expect(r'Enter [1/2/0]? :')
    child.sendline('1')
    child.expect('Enter the Content Name:')
    child.sendline(line)
    time.sleep(1)

#logout    
child.expect('#')
child.sendline('exit')
child.expect('#')
child.sendline('exit')


