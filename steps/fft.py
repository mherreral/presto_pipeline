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


def realfft(df): 
    fftcmd = "realfft %s" % df
    stdout = "%s\n" % fftcmd
    return getoutput(fftcmd), stdout

if __name__ == "__main__":

    #===========================================
    #Changing to 'subbands' where the results are saved
    cwd = os.getcwd()
    working_dir = 'subbands'
    if not os.access(working_dir, os.F_OK):
        os.mkdir(working_dir)
    os.chdir(working_dir)

    if ENABLE_MPI:
        comm = MPI.COMM_WORLD
        nprocs = comm.Get_attr(MPI.UNIVERSE_SIZE)
        pool = MPIPoolExecutor(max_workers=nprocs, wdir=working_dir)

    else:
        cores = mp.cpu_count()
        pool = mp.Pool(cores)

    print '''

    ================fft-search subbands==================

    '''                     

    try:

        if PROFILE:
            pr = cProfile.Profile()
            pr.enable()

        datfiles = glob.glob("*.dat")
        with open('fft.log', 'wt') as logfile:
            result = pool.map(realfft, datfiles)
            output, stdout = zip(*result)
            logfile.writelines(output)
            sys.stdout.writelines(stdout)

        if PROFILE:
            pr.disable()
            s = StringIO.StringIO()
            sortby = 'cumulative'
            ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
            ps.print_stats()
            print s.getvalue()
        
    except Exception as e:
        print 'failed at fft search.', e
        os.chdir(cwd)
        sys.exit(0)

    if ENABLE_MPI:
        pool.shutdown(wait = False)
    else:
        pool.close()

    os.chdir(cwd)
