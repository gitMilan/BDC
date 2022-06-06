 #!/bin/bash
#SBATCH --time 2:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=16000
module load Bowtie2
export BOWTIE2_INDEXES=/data/p225083/BOWTIE2_INDEXES
export DATA=/data/p225083
bowtie2 -x human -U $DATA/all.fq -p 16 -S ${DATA}/output.sam