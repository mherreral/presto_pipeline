"""
FFT of PRESTO test for ASC20-21 Event

This is an example script for fft process in PRESTO test, 
which has been tested and verfied with PRESTO-v3.0 software.
For PRESTO-v2.0 with Python 2.xx, the name of some importing
packages should be changed.

This test need a wroking directory `fft`, which contains the 
given *.dat files from the `input` subdirectory. The team may
first create a directory and copy the *.dat files, such as
```
    cd B1516+02_300s_2bit
    mkdir -p fft
    cp input/*.dat fft/
    (time python ../source/asc2021_presto_fft.py) > log.fft 2>&1
```

http://www.asc-events.org/ASC20-21/
"""
import os, sys, glob, re
from subprocess import getoutput
import numpy as np
import time

# For multiprocessing (parallelism)
from mpi4py.futures import MPIPoolExecutor
from mpi4py import MPI
import multiprocessing as mp
from functools import partial

#=================== Define Parameter ===================#
Tutorial_Mode = False

rootname = 'Sband'
maxDM = 31 #max DM to search
minDM = 29
Nsub = 32 #32 subbands
Nint = 64 #64 sub integration
Tres = 0.5 #ms
zmax = 200
wmax = 100
ENABLE_MPI = False

def realfft(df): 
    fftcmd = "realfft %s" % df
    stdout = "%s\n" % fftcmd
    return getoutput(fftcmd), stdout

if __name__ == "__main__":
    
    #====================== fft search ======================#

    cwd = os.getcwd()
    working_dir = 'fft'
    if not os.access(working_dir, os.F_OK):
        os.mkdir(working_dir)
        output = getoutput('cp input/*.dat fft/')
        print(output)
    os.chdir(working_dir)

    if ENABLE_MPI:
        comm = MPI.COMM_WORLD
        nprocs = comm.Get_attr(MPI.UNIVERSE_SIZE)
        pool = MPIPoolExecutor(max_workers=nprocs, wdir=working_dir)

    else:
        cores = mp.cpu_count()
        pool = mp.Pool(cores)

    datfiles = glob.glob("*.dat")
    t0 = time.time() #start wall time of fft
    with open('fft.log', 'wt') as logfile:
        result = pool.map(realfft, datfiles)
        output, stdout = zip(*result)
        logfile.writelines(output)
        sys.stdout.writelines(stdout)

        walltime = "wall time = %.2f" % (time.time() - t0) # count wall time
        logfile.write(walltime + '\n')
        logfile.close()
    
    #===========================
    #Since we moved to 'subbands', let's come back
    os.chdir(cwd)
    if ENABLE_MPI:
        pool.shutdown(wait = False)
    else:
        pool.close()
