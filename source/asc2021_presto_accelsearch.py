"""
Accelsearch of PRESTO test for ASC20-21 Event

This is an example script for accelsearch process in PRESTO test, 
which has been tested and verfied with PRESTO-v3.0 software.
For PRESTO-v2.0 with Python 2.xx, the name of some importing
packages should be changed.

This test need a wroking directory `fft`, which contains the 
generated *.fft files from the `fft` test step and the given *.inf 
files from the input subfolder. The team may first copy the *.inf 
files, then run the script with following command:
```
    cd B1516+02_300s_2bit
    cp input/*.inf fft/
    (time python ../source/asc2021_presto_accelsearch.py) > log.accelsearch 2>&1
```

For more details about ASC Event, please refert to: 
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

def accelsearch(fft_file):
    searchcmd = "accelsearch -zmax %d -wmax %d %s"  % (zmax, wmax, fftf)
    stdout = "%s\n" % searchcmd
    return getoutput(searchcmd), stdout

if __name__ == "__main__":

    #====================== fft search ======================#

    cwd = os.getcwd()
    working_dir = 'fft'
    if not os.access(working_dir, os.F_OK):
        os.mkdir(working_dir)
        output = getoutput('cp ../input/*.inf ./')
        print(output)
    os.chdir(working_dir)

    if ENABLE_MPI:
        comm = MPI.COMM_WORLD
        nprocs = comm.Get_attr(MPI.UNIVERSE_SIZE)
        pool = MPIPoolExecutor(max_workers=nprocs, wdir=working_dir)
    else:
        cores = mp.cpu_count()
        pool = mp.Pool(cores)

    t0 = time.time() # start wall time of accelsearch
    fftfiles = glob.glob("*.fft")
    with open('accelsearch.log', 'wt') as logfile:
        result = pool.map(accelsearch, fftfiles)
        output, stdout = zip(*result)
        logfile.writelines(output)
        sys.stdout.writelines(stdout)

        walltime = "wall time = %.2f" % (time.time() - t0) # report wall time
        logfile.write(walltime + "\n")
        logfile.close()

    #===========================
    #Since we moved to 'subbands', let's come back
    os.chdir(cwd)
    if ENABLE_MPI:
        pool.shutdown(wait = False)
    else:
        pool.close()
