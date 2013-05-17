import subprocess
from subprocess import call, check_call, check_output, PIPE
hosts = [
'xo-3c-ea-3a.local.'
'xo-0d-58-b2.local.'
'xo-15-4c-93.local.'
'xo-26-7a-e7.local.'


]
command = """cd /root/ers/ers-local/couch; ./couchstop; rm couchdata/*; ./couchstart; exit;"""
try:
	print call(["ssh", "root@" + hosts[0], command])
except subprocess.CalledProcessError as e:
	print e
#check_call(["ls", "-l"], shell=True, stdin=PIPE, stdout=PIPE)

#ssh root@10.20.52.151 "cd /root/ers/ers-local/couch; ./couchstop; rm couchdata/*; ./couchstart"
