"""
Download PDFs from arxiv server and then convert them to txt 
"""

import os
import sys
import time
import pickle
import shutil
import random
import shutil

from  urllib.request import urlopen
from utils import Config

def download_file(fname, pdf_url):
    """ Download pdf by url. """
    
    # change to different mirror
    if mirror != None:
        pdf_url = pdf_url.replace("arxiv.org",mirror+".arxiv.org")
    
    req = urlopen(pdf_url, None, timeout_secs)
    with open(fname, 'wb') as fp:
        shutil.copyfileobj(req, fp)  

        
def convert_file(pdf_basename, txt_basename):
    
    pdf_path = os.path.join(Config.pdf_dir, pdf_basename)
    txt_path = os.path.join(Config.txt_dir, txt_basename)
    cmd = "pdftotext %s %s" % (pdf_path, txt_path)
    os.system(cmd)
  
    #print('%d/%d %s' % (i, len(files), cmd))
  
    # check output was made
    if not os.path.isfile(txt_path):
        # there was an error with converting the pdf
        print('there was a problem with parsing %s to text, creating an empty text file.' % (pdf_path, ))
        os.system('touch ' + txt_path) # create empty file, but it's a record of having tried to convert
  
    time.sleep(0.01) # silly way for allowing for ctrl+c termination


def setup():
    """ Create required folders. """
    print("-"*60)
    print("Setting up")

    # make sure pdftotext is installed
    if not shutil.which('pdftotext'): # needs Python 3.3+
        print('ERROR: you don\'t have pdftotext installed. Install it first before calling this script')
        sys.exit()
    
    if not os.path.exists(Config.txt_dir):
        print('creating ', Config.txt_dir)
        os.makedirs(Config.txt_dir)

    if not os.path.exists(Config.pdf_dir):
        print('creating ', Config.pdf_dir)
        os.makedirs(Config.pdf_dir)

    global have_pdf
    
    global db
    
    print("Scanning pdfs.")
    have_pdf = set(os.listdir(Config.pdf_dir)) # get list of all pdfs we already have
    
    print("Loading database.")
    db = pickle.load(open(Config.db_path, 'rb'))    
    print("Done.")
    print("-"*60)
    
    
def delete_file(fname):
    os.remove(fname) 

setup()

timeout_secs = 10 # after this many seconds we give up on a paper

numok = 0
numtot = 0

jobs = []
done = []

mirror = sys.argv[1] if len(sys.argv) > 1 else None

if mirror != None:
    print("Using mirror ",mirror)

have_txt = set(os.listdir(Config.txt_dir))

# quick count of work to do
for pid,j in db.items():
    pdfs = [x['href'] for x in j['links'] if x['type'] == 'application/pdf']
    assert len(pdfs) == 1
    pdf_url = pdfs[0] + '.pdf'
    basename = pdf_url.split('/')[-1]
    if basename[:-4]+".txt" not in have_txt:
        jobs.append(basename)
    else:
        done.append(basename)
                
print("Found {0} jobs to do, {1} done, from database of {2} entries".format(len(jobs), len(done), len(db.items())))
        
# Go through each item in the database and if required download and convert it.
for pid,j in db.items():
  
    pdfs = [x['href'] for x in j['links'] if x['type'] == 'application/pdf']
    assert len(pdfs) == 1
    pdf_url = pdfs[0] + '.pdf'
    basename = pdf_url.split('/')[-1]
    fname = os.path.join(Config.pdf_dir, basename)
      
    txt_path = os.path.join(Config.txt_dir, basename)              
    pdf_path = os.path.join(Config.pdf_dir, basename)              
      
    # try retrieve the pdf, then convert it.
    numtot += 1
    try:
        
        tmp_path = pdf_path+".tmp"
        
        # create a blank file right that the start so that another process doesn't
        # try and update this file.  This allows for multiple instances...
        
        if os.path.isfile(tmp_path):
            # someone else is working on this one...
            continue
        
        if os.path.isfile(txt_path[:-4]+".txt"):
            # someone else is working on this one...
            continue
        
        os.system('touch ' + tmp_path) # create empty file, but it's a record of having tried to convert        
        
        if not basename in have_txt:
            time.sleep(0.05 + random.uniform(0,0.1))
            print("Processing {0}".format(pdf_url))
            download_file(fname, pdf_url)
            #print("converting {0}".format(basename))

            _pdf_name = basename[:-4] + ".pdf"
            _txt_name = basename[:-4] + ".txt"
            
            #print("conv ",_pdf_name, _txt_name)
            
            convert_file(_pdf_name, _txt_name)
            delete_file(pdf_path)
            delete_file(tmp_path)
              
            #print(" [ok]")
        else:
            pass
            #print('%s exists, skipping' % (fname, ))
        numok+=1
    except Exception as e:
        print(" [error] error downloading: {0}".format(pdf_url))
        print(e)
        #txt_path = os.path.join(Config.txt_dir, basename)        
        #print(" creating empty file.")
        #os.system('touch ' + txt_path) # create empty file, but it's a record of having tried to convert        
        #print(e)  
    #print('%d/%d of %d downloaded ok.' % (numok, numtot, len(db)))
  
print('final number of papers downloaded okay: %d/%d' % (numok, len(db)))

