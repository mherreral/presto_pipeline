#!/bin/bash

#SBATCH --job-name='PRESTO EXEC'
#SBATCH --output=out-%J.log
#SBATCH --error=err-%J.log
#SBATCH --ntasks=16
#SBATCH -N 1
#SBATCH --partition=longjobs
#SBATCH --nodelist=compute-1-13


source ~/anaconda3/bin/activate # conda activation
source /share/apps/intel/oneapi/setvars.sh # since most of the dependencies are compiled with intel compilers
mkdir -p /root/presto_pipeline/results/{gcc,intel}/{test1,test2}/{serial,parallel}

move () {
    # $1 cosa a mover, $2 compilador, $3 test, $4 tipo, $5 iterador TENER EN CUENTA
    # QUE AL MOVER EL LOG, LE PASAMOS EL NOMBRE COMPLETO Y EL ITERADOR EN $5,
    mkdir /root/presto_pipeline/results/$2/$3/$4/$5
    mv $1 /root/presto_pipeline/results/$2/$3/$4/$5
}

iterate () {
    for i in {1..10}; do
	# $1 script, $2 compilador, $3 datset test, $4 tipo, $i en el que vamos del iterador, $5 nombre dataset
	(time python $1 $5) > log.$2_$3_$4_$i 2>>time.$2_$3_$4
	move subbands $2 $3 $4 $i
	move log.$2_$3_$4_$i $2 $3 $4 $i
    done
    mv time.$2_$3_$4 /root/presto_pipeline/results/$2/$3/$4
}


# GCC
conda activate iprestopy2 # Contains presto GCC
module load presto/3.0.1_gcc-8.3.1


# serial gcc test1
cd /root/presto_pipeline/TestData1
iterate ../fixed_pipeline.py gcc test1 serial GBT_Lband_PSR.fil

# parallel gcc test 1
iterate ../pipeline.py gcc test1 parallel GBT_Lband_PSR.fil

# serial gcc test2
cd /root/presto_pipeline/TestData2
iterate ../fixed_pipeline.py gcc test2 serial 'Dec+1554_arcdrift+23.4-M12_0194.fil'

# parallel gcc test2
iterate ../pipeline.py gcc test2 parallel 'Dec+1554_arcdrift+23.4-M12_0194.fil' 


# INTEL
conda deactivate
conda activate intelprestopy2
module unload presto/3.0.1_gcc-8.3.1
module load presto/3.0.1_intel-2021 

# serial intel test1
cd /root/presto_pipeline/TestData1
iterate ../fixed_pipeline.py intel test1 serial GBT_Lband_PSR.fil

# parallel intel test 1
iterate ../pipeline.py intel test1 parallel GBT_Lband_PSR.fil

# serial intel test2
cd /root/presto_pipeline/TestData2
iterate ../fixed_pipeline.py intel test2 serial 'Dec+1554_arcdrift+23.4-M12_0194.fil'

# parallel intel test2
iterate ../pipeline.py intel test2 parallel 'Dec+1554_arcdrift+23.4-M12_0194.fil'
