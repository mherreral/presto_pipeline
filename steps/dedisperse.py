"""
A simple pipelien for demostrating presto
Weiwei Zhu
2015-08-14
Max-Plank Institute for Radio Astronomy
zhuwwpku@gmail.com

Modified by EAFIT University's team, ASC20-21
2020-12-29
"""
import os, sys, glob, re
from commands import getoutput
import numpy as np

# Since we are using PRESTO v3.0.1
from presto import sifting
from operator import attrgetter

# For multiprocessing (parallelism)
from mpi4py.futures import MPIPoolExecutor
from mpi4py import MPI
import multiprocessing as mp
from functools import partial

#For profiling
import cProfile, pstats, StringIO
PROFILE = True #Change to False unless you need to find out bottlenecks
ENABLE_MPI = False #In case of running with MPI

#Tutorial_Mode = True
Tutorial_Mode = False

rootname = 'Sband'
maxDM = 80 #max DM to search
Nsub = 32 #32 subbands
Nint = 64 #64 sub integration
Tres = 0.5 #ms
zmax = 0
    
#=====FUNCTION DEFINITIONS=====

def query(question, answer, input_type):
    print "Based on output of the last step, answer the following questions:"
    Ntry = 3
    while not input_type(raw_input("%s:" % question)) == answer and Ntry > 0:
        Ntry -= 1
        print "try again..."
    if Ntry == 0:print "The correct answer is:", answer

    
def prepsubband_f(lowDM, dDM, NDMs, Nout, subdownsamp, datdownsamp, filename, maskfile, dml):
    lodm = dml[0]
    subDM = np.mean(dml)
    if maskfile:
        prepsubband = "prepsubband -sub -subdm %.2f -nsub %d -downsamp %d -mask ../%s -o %s %s" % (subDM, Nsub, subdownsamp, maskfile, rootname, '../'+filename)
    else:
        prepsubband = "prepsubband -sub -subdm %.2f -nsub %d -downsamp %d -o %s %s" % (subDM, Nsub, subdownsamp, rootname, '../'+filename)
    output = getoutput(prepsubband)

    subnames = rootname+"_DM%.2f.sub[0-9]*" % subDM
    prepsubcmd = "prepsubband -nsub %(Nsub)d -lodm %(lowdm)f -dmstep %(dDM)f -numdms %(NDMs)d -numout %(Nout)d -downsamp %(DownSamp)d -o %(root)s %(subfile)s" % {
                'Nsub':Nsub, 'lowdm':lodm, 'dDM':dDM, 'NDMs':NDMs, 'Nout':Nout, 'DownSamp':datdownsamp, 'root':rootname, 'subfile':subnames}
    output = ''.join((output, getoutput(prepsubcmd))) # joining both outputs faster than '+='
    stdout = ''.join((prepsubband, '\n', prepsubcmd, '\n'))
    return output, stdout 



if __name__ == "__main__":

    filename = sys.argv[1]
    if len(sys.argv) > 2:
        maskfile = sys.argv[2]
    else:
        maskfile = None

    
    print '''

    ====================Read Header======================

    '''

    if PROFILE:
        pr = cProfile.Profile()
        pr.enable()

    readheadercmd = 'readfile %s' % filename
    print readheadercmd
    output = getoutput(readheadercmd)
    print output
    header = {}
    for line in output.split('\n'):
        items = line.split("=")
        if len(items) > 1:
            header[items[0].strip()] = items[1].strip()


    if PROFILE:
        pr.disable()
        s = StringIO.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print s.getvalue()

    print '''

    ============Generate Dedispersion Plan===============

    '''
    if PROFILE:
        pr = cProfile.Profile()
        pr.enable()


    try:
        Nchan = int(header['Number of channels'])
        tsamp = float(header['Sample time (us)']) * 1.e-6
        BandWidth = float(header['Total Bandwidth (MHz)'])
        fcenter = float(header['Central freq (MHz)'])
        Nsamp = int(header['Spectra per file'])

        if Tutorial_Mode:
            query("Input file has how many frequency channel?", Nchan, int)
            query("what is the total bandwidth?", BandWidth, float)
            query("what is the size of each time sample in us?", tsamp*1.e6, float)
            query("what's the center frequency?", fcenter, float)
            print 'see how these numbers are used in the next step.'
            print ''

        ddplancmd = 'DDplan.py -d %(maxDM)s -n %(Nchan)d -b %(BandWidth)s -t %(tsamp)f -f %(fcenter)f -s %(Nsub)s -o DDplan.ps' % {
                'maxDM':maxDM, 'Nchan':Nchan, 'tsamp':tsamp, 'BandWidth':BandWidth, 'fcenter':fcenter, 'Nsub':Nsub}
        print ddplancmd
        ddplanout = getoutput(ddplancmd)
        print ddplanout
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
    except:
        print 'failed at generating DDplan.'
        sys.exit(0)


    if Tutorial_Mode:
        calls = 0
        for line in ddplan:
            ddpl = line.split()
            calls += int(ddpl[7])
        query("According to the DDplan, how many times in total do we have to call prepsubband?", calls, int)
        print 'see how these numbers are used in the next step.'
        print ''


    if PROFILE:
        pr.disable()
        s = StringIO.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print s.getvalue()



    #===========================================
    #Changing to 'disperse' where the results are saved
    cwd = os.getcwd()
    working_dir = 'subbands'
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
    
    print '''

    ================Dedisperse Subbands==================

    '''

    if PROFILE:
        pr = cProfile.Profile()
        pr.enable()

    try:
        logfile = open('dedisperse.log', 'wt')
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
            
            subdownsamp = DownSamp/2
            datdownsamp = 2
            if DownSamp < 2: subdownsamp = datdownsamp = 1                
            function = partial(prepsubband_f, lowDM, dDM, NDMs, Nout, subdownsamp, datdownsamp, filename, maskfile) # for passing several params to Executor.map
            result = pool.map(function, dmlist)
            output, stdout = zip(*result)
            logfile.writelines(output)
            sys.stdout.writelines(stdout)
            
        os.system('rm *.sub*')
        logfile.close()

    except Exception as e:
        print 'failed at prepsubband.', e
        os.chdir(cwd)
        sys.exit(0)


    if PROFILE:
        pr.disable()
        s = StringIO.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print s.getvalue()
        
    #===========================
    #Since we moved to 'subbands', let's come back
    os.chdir(cwd)
    if ENABLE_MPI:
        pool.shutdown(wait = False)
    else:
        pool.close()

