"""
Folding of PRESTO test for ASC20-21 Event

This is an example script for dedisperse process in PRESTO test, 
which has been tested and verfied with PRESTO-v3.0 software.
For PRESTO-v2.0 with Python 2.xx, the name of some importing
packages should be changed.

This test read the *.fits and cands.inc file as the input parameter, and 
generate a result folder `fold` as working directory.
Use following commands to run the fold test.
```
    cd B1516+02_300s_2bit
    (time python ../source/asc2021_presto_fold.py B1516+02_300s_2bit.fits) > log.fold 2>&1
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
# Tutorial_Mode = True
Tutorial_Mode = False

rootname = 'Sband'
maxDM = 31 #max DM to search
minDM = 29
Nsub = 32 #32 subbands
Nint = 64 #64 sub integration
Tres = 0.5 #ms
zmax = 200
wmax = 100
Tutorial_Mode = False

def prepfold(filename, cand):
    foldcmd = "prepfold -n %(Nint)d -nsub %(Nsub)d -dm %(dm)f -p %(period)f %(filfile)s -o %(outfile)s -noxwin -nodmsearch" % {
                'Nint':Nint, 'Nsub':Nsub, 'dm':cand["DM"],  'period':cand["p"], 'filfile':filename, 'outfile':rootname+'_DM'+cand["DMstr"]} #full plots
    stdout = "%s\n" % foldcmd
    return getoutput(foldcmd), stdout

if __name__ == "__main__":
    filename = sys.argv[1]

    # print '''

    # ================reading candidates==================

    # '''
    fp = open('cands.inc', 'r')
    lines = fp.readlines()
    cands = []
    for line in lines:
        cand = {}
        line_list = line.split()
        cand['DM'] = float(line_list[1])
        cand['p']  = float(line_list[3])
        cand['DMstr'] = line_list[5]
        cands.append(cand)
        
    fp.close()

    # print '''

    # ================folding candidates==================

    # '''

    cwd = os.getcwd()
    working_dir = 'fold'
    if not os.access(working_dir, os.F_OK):
        os.mkdir(working_dir)
    os.chdir(working_dir)
    os.system('ln -s ../%s %s' % (filename, filename))

    t0 = time.time() #start wall time of fold
    with open('folding.log', 'wt') as logfile:
        function = partial(prepfold, filename)
        result = pool.map(function, cands)
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

