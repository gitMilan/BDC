import itertools
import multiprocessing as mp
import argparse as ap
import csv

argparser = ap.ArgumentParser(description="Script voor Opdracht 1 van Big Data Computing")
argparser.add_argument("-n", action="store",
                       dest="n", required=True, type=int,
                       help="Aantal cores om te gebruiken.")
argparser.add_argument("-o", action="store", dest="csvfile", type=ap.FileType('w', encoding='UTF-8'),
                       required=False, help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
argparser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+', help="Minstens 1 Illumina Fastq Format file om te verwerken")
args = argparser.parse_args()


def calculate_phred_score(line):
    '''calculates the average phred score for a line'''
    line = line.strip()
    ascii_scores = [ord(c) - 33  for c in line]
    phred_score = sum(ascii_scores) / len(ascii_scores)
    return phred_score


def fastq_reader(fastqfile):
    '''reads a fastq file and returns the phred score lines as a list'''
    lines = []
    with open(fastqfile, 'r') as fastq:
        line_counter = 0
        for count, line in enumerate(fastq, start=1):
            if count % 4 == 0:
                line = line.strip()
                # phred_score = calculate_phred_score(line)
                line_counter += 1
                lines.append(line)
    return lines

def keyfunc(row):
    # `row` is one row of the CSV file.
    # replace this with the name column.
    return row

def worker(chunk):
    # `chunk` will be a list of CSV rows all with the same name column
    # replace this with your real computation
    # print(chunk)
    result = calculate_phred_score(chunk)
    return result


def csv_writer(data):
    '''write a list of values in data list, indexed to a csv file'''
    zipped_reads = zip(list(range(0,len(data))), data)
    with open(args.csvfile.name, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        for read in zipped_reads:
            writer.writerow(read)


def main():
    pool = mp.Pool()
    num_chunks = 4
    results = []
    for file in args.fastq_files:
        file_name = file.name
        with open(file_name) as f:
            chunks = itertools.groupby(f, keyfunc)
            while True:
                # make a list of num_chunks chunks
                groups = [list(chunk) for key, chunk in
                        itertools.islice(chunks, num_chunks)]
                if groups:
                    for count, group in enumerate(groups, start=1):
                        if count % 4 == 0:
                            result = pool.map(worker, group)
                            results.extend(result)
                else:
                    break
    pool.close()
    pool.join()

    if args.csvfile:
        csv_writer(results)


if __name__ == "__main__":
    main()
