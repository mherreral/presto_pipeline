"""
Dedispersion of PRESTO test for ASC20-21 Event

This is an example script for dedisperse process in PRESTO test, 
which has been tested and verfied with PRESTO-v3.0 software.
For PRESTO-v2.0 with Python 2.xx, the name of some importing
packages should be changed.

This test read the *.fits file as the input parameter, and 
generate a result folder `disperse` as working directory.
Use following commands to run the dedispersion test.

```
    cd B1516+02_300s_2bit
    (time python ../source/asc2021_presto_dedisperse.py B1516+02_300s_2bit.fits) > log.disperse 2>&1
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

#================Define Parameter================#

rootname = 'Sband'
maxDM = 31 #max DM to search
minDM = 29
Nsub = 32 #32 subbands
Nint = 64 #64 sub integration
Tres = 0.5 #ms
zmax = 200
wmax = 100
ENABLE_MPI = False

def prepsubband_f(lowDM, dDM, NDMs, Nout, subdownsamp, datdownsamp, filename dml):
    lodm = dml[0]
    subDM = np.mean(dml)
    prepsubband = "prepsubband -sub -subdm %.2f -nsub %d -downsamp %d -o %s %s" % (subDM, Nsub, subdownsamp, rootname, '../'+filename)
    output = getoutput(prepsubband)

    subnames = rootname+"_DM%.2f.sub[0-9]*" % subDM
    prepsubcmd = "prepsubband -nsub %(Nsub)d -lodm %(lowdm)f -dmstep %(dDM)f -numdms %(NDMs)d -downsamp %(DownSamp)d -o %(root)s %(subfile)s" % {
                'Nsub':Nsub, 'lowdm':lodm, 'dDM':dDM, 'NDMs':NDMs, 'DownSamp':datdownsamp, 'root':rootname, 'subfile':subnames}
    output = ''.join((output, getoutput(prepsubcmd))) # joining both outputs faster than '+='
    stdout = ''.join((prepsubband, '\n', prepsubcmd, '\n'))
    return output, stdout 

if __name__ == "__main__":
    
    filename = sys.argv[1]

    # print '''

    # ====================Read Header======================

    # '''

    readheadercmd = 'readfile %s' % filename
    print (readheadercmd)
    output = getoutput(readheadercmd)
    print (output)
    header = {}
    for line in output.split('\n'):
        items = line.split("=")
        if len(items) > 1:
            header[items[0].strip()] = items[1].strip()

    # print '''

    # ============Generate Dedispersion Plan===============

    # '''

    Nchan = int(header['Number of channels'])
    tsamp = float(header['Sample time (us)']) * 1.e-6
    BandWidth = float(header['Total Bandwidth (MHz)'])
    fcenter = float(header['Central freq (MHz)'])

    ddplancmd = 'DDplan.py -l %(minDM)s -d %(maxDM)s -n %(Nchan)d -b %(BandWidth)s -t %(tsamp)f -f %(fcenter)f -s %(Nsub)s -o DDplan.ps' % {
        'minDM':minDM, 'maxDM':maxDM, 'Nchan':Nchan, 'tsamp':tsamp, 'BandWidth':BandWidth, 'fcenter':fcenter, 'Nsub':Nsub}
    print (ddplancmd)
    ddplanout = getoutput(ddplancmd)
    print (ddplanout)
    planlist = ddplanout.split('\n')
    ddplan = []
    planlist.reverse()
    for plan in planlist:
        if plan == '':
            continue
        elif plan.strip().startswith('Low DM'):
            break
        else:
            ddplan.append(plan)
            ddplan.reverse()
        
    # print '''

    # ================Dedisperse Subbands==================

    # '''

    cwd = os.getcwd()
    working_dir = 'disperse'
    if not os.access(working_dir, os.F_OK):
        os.mkdir(working_dir)
    os.chdir(working_dir)

    #Multiprocessing Pool definition
    #Made this way to have only one pool along the entire script
    if ENABLE_MPI:
        comm = MPI.COMM_WORLD
        nprocs = comm.Get_attr(MPI.UNIVERSE_SIZE)
        pool = MPIPoolExecutor(max_workers=nprocs, wdir=working_dir)
        
    else:
        cores = mp.cpu_count()
        pool = mp.Pool(cores)
    
    logfile = open('disperse.log', 'wt')
    t0 = time.time() #collect start time
    for line in ddplan:
        ddpl = line.split()
        lowDM = float(ddpl[0])
        hiDM = float(ddpl[1])
        dDM = float(ddpl[2])
        DownSamp = int(ddpl[3])
        NDMs = int(ddpl[6])
        calls = int(ddpl[7])
        Nout = Nsamp/DownSamp 
        Nout -= (Nout % 500)
        dmlist = np.split(np.arange(lowDM, hiDM, dDM), calls)

        #copy from $PRESTO/python/Dedisp.py
        subdownsamp = DownSamp/2
        datdownsamp = 2
        if DownSamp < 2: subdownsamp = datdownsamp = 2
        function = partial(prepsubband_f, lowDM, dDM, NDMs, Nout, subdownsamp, datdownsamp, filename) # for passing several params to Executor.map
        result = pool.map(function, dmlist)
        output, stdout = zip(*result)
        logfile.writelines(output)
        sys.stdout.writelines(stdout)

    walltime = "wall time = %.2f\n" % (time.time() - t0) # count wall time
    logfile.write(walltime)
    logfile.close()
    
    #===========================
    #Since we moved to 'subbands', let's come back
    os.chdir(cwd)
    if ENABLE_MPI:
        pool.shutdown(wait = False)
    else:
        pool.close()
