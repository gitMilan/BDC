#!/bin/bash

#SBATCH --job-name=assignment3

# Define, how many nodes you need. Here, we ask for 1 node.
# Each node has 16 or 20 CPU cores.
#SBATCH --nodes=16

#SBATCH --time=0-01:00:00

#SBATCH --mem-per-cpu=1500MB
##SBATCH --mem=5GB

#SBATCH --partition=assemblix
n=17

TEMP_DIR=/dev/null
FQ_DATA=/data/dataprocessing/MinIONData/all.fq
REFERENCE_INDEX=/data/dataprocessing/MinIONData/all_bacteria.fna
TIME_OUTPUT=timings.txt

source /commons/conda/conda_load.sh

/usr/bin/time -o ${TIME_OUTPUT} --append -f "${n}\t%e" minimap2 -a ${REFERENCE_INDEX} ${FQ_DATA} -t ${n} > ${TEMP_DIR}/minimap2output${n}cores.sam
