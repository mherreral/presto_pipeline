"""
A simple pipelien for demostrating presto
Weiwei Zhu
2015-08-14
Max-Plank Institute for Radio Astronomy
zhuwwpku@gmail.com
"""
import os, sys, glob, re
import sifting
from commands import getoutput
import numpy as np

#Tutorial_Mode = True
Tutorial_Mode = False

rootname = 'Sband'
maxDM = 80 #max DM to search
Nsub = 32 #32 subbands
Nint = 64 #64 sub integration
Tres = 0.5 #ms
zmax = 0

filename = sys.argv[1]
if len(sys.argv) > 2:
    maskfile = sys.argv[2]
else:
    maskfile = None

def query(question, answer, input_type):
    print "Based on output of the last step, answer the following questions:"
    Ntry = 3
    while not input_type(raw_input("%s:" % question)) == answer and Ntry > 0:
        Ntry -= 1
        print "try again..."
    if Ntry == 0:print "The correct answer is:", answer

#"""

print '''

====================Read Header======================

'''

#try:
#myfil = filterbank(filename)

readheadercmd = 'readfile %s' % filename
print readheadercmd
output = getoutput(readheadercmd)
print output
header = {}
for line in output.split('\n'):
    items = line.split("=")
    if len(items) > 1:
        header[items[0].strip()] = items[1].strip()

#print header
#except:
    #print 'failed at reading file %s.' % filename
    #sys.exit(0)


print '''

============Generate Dedispersion Plan===============

'''

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

print '''

================Dedisperse Subbands==================

'''

cwd = os.getcwd()
try:
    if not os.access('subbands', os.F_OK):
        os.mkdir('subbands')
    os.chdir('subbands')
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

        #copy from $PRESTO/python/Dedisp.py
        subdownsamp = DownSamp/2
        datdownsamp = 2
        if DownSamp < 2: subdownsamp = datdownsamp = 1

        for i, dml in enumerate(dmlist):
            lodm = dml[0]
            subDM = np.mean(dml)
            if maskfile:
                prepsubband = "prepsubband -sub -subdm %.2f -nsub %d -downsamp %d -mask ../%s -o %s %s" % (subDM, Nsub, subdownsamp, maskfile, rootname, '../'+filename)
            else:
                prepsubband = "prepsubband -sub -subdm %.2f -nsub %d -downsamp %d -o %s %s" % (subDM, Nsub, subdownsamp, rootname, '../'+filename)
            print prepsubband
            output = getoutput(prepsubband)
            logfile.write(output)

            subnames = rootname+"_DM%.2f.sub[0-9]*" % subDM
            #prepsubcmd = "prepsubband -nsub %(Nsub)d -lodm %(lowdm)f -dmstep %(dDM)f -numdms %(NDMs)d -numout %(Nout)d -downsamp %(DownSamp)d -o %(root)s ../%(filfile)s" % {
                    #'Nsub':Nsub, 'lowdm':lodm, 'dDM':dDM, 'NDMs':NDMs, 'Nout':Nout, 'DownSamp':datdownsamp, 'root':rootname, 'filfile':filename}
            prepsubcmd = "prepsubband -nsub %(Nsub)d -lodm %(lowdm)f -dmstep %(dDM)f -numdms %(NDMs)d -numout %(Nout)d -downsamp %(DownSamp)d -o %(root)s %(subfile)s" % {
                    'Nsub':Nsub, 'lowdm':lodm, 'dDM':dDM, 'NDMs':NDMs, 'Nout':Nout, 'DownSamp':datdownsamp, 'root':rootname, 'subfile':subnames}
            print prepsubcmd
            output = getoutput(prepsubcmd)
            logfile.write(output)
    os.system('rm *.sub*')
    logfile.close()
    os.chdir(cwd)

except:
    print 'failed at prepsubband.'
    os.chdir(cwd)
    sys.exit(0)

print '''

================fft-search subbands==================

'''

try:
    os.chdir('subbands')
    datfiles = glob.glob("*.dat")
    logfile = open('fft.log', 'wt')
    for df in datfiles:
        fftcmd = "realfft %s" % df
        print fftcmd
        output = getoutput(fftcmd)
        logfile.write(output)
    logfile.close()
    logfile = open('accelsearch.log', 'wt')
    fftfiles = glob.glob("*.fft")
    for fftf in fftfiles:
        searchcmd = "accelsearch -zmax %d %s"  % (zmax, fftf)
        print searchcmd
        output = getoutput(searchcmd)
        logfile.write(output)
    logfile.close()
    os.chdir(cwd)
except:
    print 'failed at fft search.'
    os.chdir(cwd)
    sys.exit(0)

#"""


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
        cands.sort(sifting.cmp_sigma)
        #for cand in cands[:1]:
            #print cand.filename, cand.candnum, cand.p, cand.DMstr
        #sifting.write_candlist(cands)
    return cands


print '''

================sifting candidates==================

'''

#try:
cwd = os.getcwd()
os.chdir('subbands')
cands = ACCEL_sift(zmax)
os.chdir(cwd)
#except:
    #print 'failed at sifting candidates.'
    #os.chdir(cwd)
    #sys.exit(0)


print '''

================folding candidates==================

'''

try:
    cwd = os.getcwd()
    os.chdir('subbands')
    os.system('ln -s ../%s %s' % (filename, filename))
    logfile = open('folding.log', 'wt')
    for cand in cands:
        #foldcmd = "prepfold -dm %(dm)f -accelcand %(candnum)d -accelfile %(accelfile)s %(datfile)s -noxwin " % {
        #'dm':cand.DM,  'accelfile':cand.filename+'.cand', 'candnum':cand.candnum, 'datfile':('%s_DM%s.dat' % (rootname, cand.DMstr))} #simple plots
        foldcmd = "prepfold -n %(Nint)d -nsub %(Nsub)d -dm %(dm)f -p %(period)f %(filfile)s -o %(outfile)s -noxwin -nodmsearch" % {
                'Nint':Nint, 'Nsub':Nsub, 'dm':cand.DM,  'period':cand.p, 'filfile':filename, 'outfile':rootname+'_DM'+cand.DMstr} #full plots
        print foldcmd
        #os.system(foldcmd)
        output = getoutput(foldcmd)
        logfile.write(output)
    logfile.close()
    os.chdir(cwd)
except:
    print 'failed at folding candidates.'
    os.chdir(cwd)
    sys.exit(0)

