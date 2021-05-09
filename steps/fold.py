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
ENABLE_MPI = False

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

def ACCEL_sift(zmax):
    '''
    The following code come from PRESTO's ACCEL_sift.py
    '''

    globaccel = "*ACCEL_%d" % zmax
    globinf = "*DM*.inf"
    # In how many DMs must a candidate be detected to be considered "good"
    min_num_DMs = 2
    # Lowest DM to consider as a "real" pulsar
    low_DM_cutoff = 2.0
    # Ignore candidates with a sigma (from incoherent power summation) less than this
    sifting.sigma_threshold = 4.0
    # Ignore candidates with a coherent power less than this
    sifting.c_pow_threshold = 100.0

    # If the birds file works well, the following shouldn't
    # be needed at all...  If they are, add tuples with the bad
    # values and their errors.
    #                (ms, err)
    sifting.known_birds_p = []
    #                (Hz, err)
    sifting.known_birds_f = []

    # The following are all defined in the sifting module.
    # But if we want to override them, uncomment and do it here.
    # You shouldn't need to adjust them for most searches, though.

    # How close a candidate has to be to another candidate to                
    # consider it the same candidate (in Fourier bins)
    sifting.r_err = 1.1
    # Shortest period candidates to consider (s)
    sifting.short_period = 0.0005
    # Longest period candidates to consider (s)
    sifting.long_period = 15.0
    # Ignore any candidates where at least one harmonic does exceed this power
    sifting.harm_pow_cutoff = 8.0

    #--------------------------------------------------------------

    # Try to read the .inf files first, as _if_ they are present, all of
    # them should be there.  (if no candidates are found by accelsearch
    # we get no ACCEL files...
    inffiles = glob.glob(globinf)
    candfiles = glob.glob(globaccel)
    # Check to see if this is from a short search
    if len(re.findall("_[0-9][0-9][0-9]M_" , inffiles[0])):
        dmstrs = [x.split("DM")[-1].split("_")[0] for x in candfiles]
    else:
        dmstrs = [x.split("DM")[-1].split(".inf")[0] for x in inffiles]
    dms = map(float, dmstrs)
    dms.sort()
    dmstrs = ["%.2f"%x for x in dms]

    # Read in all the candidates
    cands = sifting.read_candidates(candfiles)

    # Remove candidates that are duplicated in other ACCEL files
    if len(cands):
        cands = sifting.remove_duplicate_candidates(cands)

    # Remove candidates with DM problems
    if len(cands):
        cands = sifting.remove_DM_problems(cands, min_num_DMs, dmstrs, low_DM_cutoff)

    # Remove candidates that are harmonically related to each other
    # Note:  this includes only a small set of harmonics
    if len(cands):
        cands = sifting.remove_harmonics(cands)

    # Write candidates to STDOUT
    if len(cands):
        #cands.sort(sifting.cmp_sigma)
        cands.sort(key=attrgetter('sigma'), reverse=True) # Changed since we use PRESTO v3.0.1
    return cands


def prepfold(filename, cand):
    foldcmd = "prepfold -n %(Nint)d -nsub %(Nsub)d -dm %(dm)f -p %(period)f %(filfile)s -o %(outfile)s -noxwin -nodmsearch" % {
                'Nint':Nint, 'Nsub':Nsub, 'dm':cand.DM,  'period':cand.p, 'filfile':filename, 'outfile':rootname+'_DM'+cand.DMstr} #full plots
    stdout = "%s\n" % foldcmd
    return getoutput(foldcmd), stdout
                     

if __name__ == "__main__":

    filename = sys.argv[1]
    if len(sys.argv) > 2:
        maskfile = sys.argv[2]
    else:
        maskfile = None

    #===========================================
    #Changing to 'subbands' where the results are saved
    cwd = os.getcwd()
    working_dir = 'subbands'
    if not os.access(working_dir, os.F_OK):
        os.mkdir(working_dir)
    os.chdir(working_dir)
    
    print '''

    ================sifting candidates==================

    '''
    if PROFILE:
        pr = cProfile.Profile()
        pr.enable()
    
    cands = ACCEL_sift(zmax)

    if PROFILE:
        pr.disable()
        s = StringIO.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print s.getvalue()


    print '''

    ================folding candidates==================

    '''
    
    #Multiprocessing Pool definition
    #Made this way to have only one pool along the entire script
    if ENABLE_MPI:
        comm = MPI.COMM_WORLD
        nprocs = comm.Get_attr(MPI.UNIVERSE_SIZE)
        pool = MPIPoolExecutor(max_workers=nprocs, wdir=working_dir)
        
    else:
        cores = mp.cpu_count()
        pool = mp.Pool(cores)

    
    if PROFILE:
        pr = cProfile.Profile()
        pr.enable()
        

    try:
        os.system('ln -s ../%s %s' % (filename, filename))
        with open('folding.log', 'wt') as logfile:
            function = partial(prepfold, filename)
            result = pool.map(function, cands)
            output, stdout = zip(*result)
            logfile.writelines(output)
            sys.stdout.writelines(stdout)
            
    except:
        print 'failed at folding candidates.'
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
