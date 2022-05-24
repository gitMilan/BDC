sometimes the ports are in use, kill them using
kill -9 $(ps -A | grep python | awk '{print $1}')


start server
python3 assignment2.py -s rnaseqfile.fastq -o csvfile.csv

run client
python3 assignment2.py -c --host 127.0.0.1 --port 5381 -n 2