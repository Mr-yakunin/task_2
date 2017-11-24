## script for file generating "testfile.txt"
import time
import subprocess
from random import randrange

## you can enter the number of lines in file
count = 0
try:
	count = int(input("Please, enter number of lines: ")) - 1
except ValueError as ex:
## if you wanto to play with script and give him not integer values
## your number of line would be 50000
	print("'%s' cannot be converted to int: %s" % (count, ex))
	print("So, count must be 50.000 (count = 50.000)")
	count = 50000 - 1
## start working
print("Start script for file generation")
start_time = time.time()
file = open("testfile.txt", "w")
i = 0
## generate strings
while i <= count:
	file.write(str(randrange(0,256,1))+", "+str(int(time.time()))+", "+str(randrange(1,500001,1))+"\n")
	i += 1
## upload generated file to HDFS
subprocess.call("hadoop fs -rm -r /app", shell=True)
subprocess.call("hadoop fs -mkdir /app", shell=True)
subprocess.call("hadoop fs -put -f testfile.txt /app/test", shell=True)
## print time of scripting
print("Success: end script, elapsed time: {:.3f} sec".format(time.time() - start_time))