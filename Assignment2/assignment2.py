import argparse as ap
import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import os, sys, time, queue
import argparse as ap
import csv


argparser = ap.ArgumentParser(description="Script voor Opdracht 2 van Big Data Computing;  Calculate PHRED scores over the network.")
mode = argparser.add_mutually_exclusive_group(required=True)
mode.add_argument("-s", action="store_true", help="Run the program in Server mode; see extra options needed below")
mode.add_argument("-c", action="store_true", help="Run the program in Client mode; see extra options needed below")
server_args = argparser.add_argument_group(title="Arguments when run in server mode")
server_args.add_argument("-o", action="store", dest="csvfile", type=ap.FileType('w', encoding='UTF-8'),
                       required=False, help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
server_args.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='*', help="Minstens 1 Illumina Fastq Format file om te verwerken")
server_args.add_argument("--chunks", action="store", type=int, help="Aantal chunks of de fastq file(s) in op te splitsen.")

server_args.add_argument("--host", dest="host",action="store", type=str, help="The hostname where the Server is listening")
server_args.add_argument("--port", dest="port", action="store", type=int, help="The port on which the Server is listening")


client_args = argparser.add_argument_group(title="Arguments when run in client mode")
client_args.add_argument("-n", action="store",
                       dest="n", required=False, type=int,
                       help="Aantal cores om te gebruiken per host.")
# client_args.add_argument("--host", action="store", type=str, help="The hostname where the Server is listening")
# client_args.add_argument("--port", action="store", type=int, help="The port on which the Server is listening")

args = argparser.parse_args()


POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
IP = ''
PORTNUM = 5381
AUTHKEY = b'whathasitgotinitspocketsesss?'
# data = ["Always", "look", "on", "the", "bright", "side", "of", "life!"]

def fastq_reader(fastqfiles):
    lines = []

    for file in fastqfiles:
        file_name = file.name
        with open(file_name, 'r') as fastq:
            line_counter = 0
            for count, line in enumerate(fastq, start=1):
                if count % 4 == 0:
                    line = line.strip()
                    # phred_score = calculate_phred_score(line)
                    line_counter += 1
                    lines.append(line)
    return lines

data = fastq_reader(args.fastq_files)


def make_server_manager(port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=('', port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager


def runserver(fn, data):
    # Start a shared manager server and access its queues
    manager = make_server_manager(PORTNUM, b'whathasitgotinitspocketsesss?')
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not data:
        print("Gimme something to do here!")
        return

    print("Sending data!")
    for d in data:
        shared_job_q.put({'fn' : fn, 'arg' : d})

    time.sleep(2)

    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print("Got result!", result)
            if len(results) == len(data):
                print("Got all results!")
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the server!")
    manager.shutdown()
    print(results)

    if args.csvfile:
        print(f"writing results to: {args.csvfile.name}")
        cur_read = 0
        for result in results:
            write_to_csv([cur_read, result['result']])
            cur_read += 1


def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager


def calculate_phred_score(line):
    ascii_scores = [ord(c) - 33  for c in line]
    phred_score = sum(ascii_scores) / len(ascii_scores)
    return phred_score


def write_to_csv(data):
    with open(args.csvfile.name, 'a', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(data)



def capitalize(word):
    """Capitalizes the word you pass in and returns it"""
    with open(args.csvfile.name, 'a', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow([word.upper()])
    return word.upper()


def runclient(num_processes):
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)



def run_workers(job_q, result_q, num_processes):
    processes = []
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()


def peon(job_q, result_q):
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print("Aaaaaaargh", my_name)
                return
            else:
                try:
                    result = job['fn'](job['arg'])
                    print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                    result_q.put({'job': job, 'result' : result})

                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result' : ERROR})

        except queue.Empty:
            print("sleepytime for", my_name)
            time.sleep(1)



def run_as_server():

    server = mp.Process(target=runserver, args=(calculate_phred_score, data))
    server.start()
    server.join()


def run_as_client():
    client = mp.Process(target=runclient, args=(4,))
    client.start()
    client.join()


def main():
    if args.c is True:
        run_as_client()

    else:
        run_as_server()


if __name__ == '__main__':
    main()