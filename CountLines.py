import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import urllib2
import numpy as np
import os,sys
import gzip, json
from dateutil import parser
import os.path
from scipy import misc
from time import strftime,strptime
import re
import math
import pymysql
import MySQLdb

class GnipDataProcessor(object):

    def __init__(self, i_path, collection, chunk_size=50):
        self.path = i_path
        self.chunk = []
        self.chunk_size = chunk_size
        self.collection = collection
        self.total_inserts = 0

    def all_files(self):
        for path, dirs, files in os.walk(self.path):
            for f in files:
                yield os.path.join(path, f)
    def iter_files(self):
        file_generator = self.all_files()

        for f in file_generator:
            try:
                gfile = gzip.open(f)
                for line in gfile:
                    self.process_line(line)
                gfile.close()
            except Exception as e:
                # print e
                pass

    def process_line(self, line):
        try:
            if len(self.chunk) > self.chunk_size:
                self.process_chunk()
                self.chunk = []
            if line.strip() != "":
                data = json.loads(line)
                if 'id' in data:
                    # data['postedTime_mongo'] = parser.parse(data['postedTime'])
                    self.chunk.append(line)

        except Exception as e:
            # print "error storing chunk \n"
            # print line, e.msg()
            raise

    def process_chunk(self):
        #for item in self.chunk:
        try:
            #print self.chunk
            self.total_inserts += len(self.chunk)
            # print "Inserted: %d number of docs" % self.total_inserts
        except:
            # print "issue inserting"
            pass

if __name__ == '__main__':
    insrt = GnipDataProcessor("irma","")
    result = []
    insrt.iter_files()
    print insrt.total_inserts