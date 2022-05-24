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
    '''calculates the phred score for a line'''
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


def main():
    '''Main'''
    cpus = args.n
    with mp.Pool(cpus) as pool:

        for file in args.fastq_files:
            file_name = file.name
            lines = fastq_reader(file_name)
            results = pool.map(calculate_phred_score, lines)
            if args.csvfile is not None:
                zipped_reads = zip(list(range(0,len(results))), results)
                with open(args.csvfile.name, 'w', encoding='UTF8') as f:
                    writer = csv.writer(f)
                    for read in zipped_reads:
                        writer.writerow(read)


if __name__ == "__main__":
    main()
