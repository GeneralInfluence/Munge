#!/usr/bin/env python 2
"""Loading any spreadsheet for downstream Synapsify functions"""

import os
import sys
import bleach
import csv
import _csv
import xlrd
import xlwt
import re
import ujson
import codecs
import numpy as np
import magic
from time import time



#WORD = re.compile("[\w\-]+\'[a-z]+|[A-Za-z]+\'[\w\-]+|[\w\-]+")
WORD = re.compile("(([A-Za-z]\.)+)|([0-9]{1,2}:[0-9][0-9])|(([A-za-z]+\'){0,1}\w+(\-\w+){0,1}(\'[a-z]+){0,1})")
EMAIL_OR_URL = re.compile("(\w|-)+@(\w|-)+\.(\w|-)+|http.+")
ESCAPES = [re.compile(chr(char)) for char in range(1, 32)] # Start at 0??
TWEET_SYMBOLS = ['RT','@','"',"\'","_",":"] # I really need stemming to handle contractions
# EOL = ["\\n","\n"]
# EOL = [re.compile(char) for char in EOL]

thisdir = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(thisdir,"stopwords.txt")) as handle:
    SW = [x[:-1] for x in handle.readlines()]

with open(os.path.join(thisdir,"prepositions.txt")) as handle:
    PREPS = [x[:-1] for x in handle.readlines()]

csv.field_size_limit(sys.maxsize)

class SynapsifyError(Exception):
    """Simple Exception Type"""
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

# Copied from http://stackoverflow.com/questions/9177820/python-utf-16-csv-reader
# Needed by the csv reader if the file is not utf-8 or ascii
class Recoder(object):
    def __init__(self, stream, decoder, encoder, eol='\r\n'):
        self._stream = stream
        self._decoder = decoder if isinstance(decoder, codecs.IncrementalDecoder) else codecs.getincrementaldecoder(decoder)(errors='replace')
        self._encoder = encoder if isinstance(encoder, codecs.IncrementalEncoder) else codecs.getincrementalencoder(encoder)()
        self._buf = ''
        self._eol = eol
        self._reachedEof = False

    def read(self, size=None):
        if size is None:
            r = self._stream.read()
        else:
            r = self._stream.read(size)
        raw = self._decoder.decode(r, size is None)
        return self._encoder.encode(raw)

    def __iter__(self):
        return self

    def __next__(self):
        if self._reachedEof:
            raise StopIteration()
        while True:
            line,eol,rest = self._buf.partition(self._eol)
            if eol == self._eol:
                self._buf = rest
                return self._encoder.encode(line + eol)
            raw = self._stream.read(1024)
            if raw == '':
                self._decoder.decode(b'', True)
                self._reachedEof = True
                return self._encoder.encode(self._buf)
            self._buf += self._decoder.decode(raw)
    next = __next__

    def close(self):
        return self._stream.close()

# helper functions
def sanitize(element):
    """Remove links and email addresses from a string"""
    element = bleach.clean(element)
    if not (isinstance(element, str) or isinstance(element, unicode)):
        element = str(element)
    return re.sub(EMAIL_OR_URL, "", element).strip()

def get_mime_type(inf):
    return magic.from_file(inf, mime=True)

def get_encoding(buf):
    if len(buf) > 0:
        m = magic.Magic(mime_encoding=True)
        encoding = m.from_buffer(buf)
        if encoding == "binary":
            raise SynapsifyError("Unexpected character encoding. Please provide text in a standard encoding.")
        try:
            buf[:100].decode(encoding)
        except LookupError:
            print "Warning, unable to lookup decoder for %s, using default: utf-8" % encoding
            return "utf-8"
        print "Detected %s encoding..." % encoding
        return encoding
    return "utf-8"

def encode_if_necessary(element, force_text=False):
    """Encode unicode as utf-8 if it is a string or unicode object, otherwise, convert strings that are numbers"""
    if not force_text:
        if isinstance(element, int) or isinstance(element, float):
            return element
        try:
            return int(element)
        except ValueError:
            pass
        try:
            value = float(element)
            # don't return nan because we can't json encode it.
            if not np.isnan(value):
                return value
        except ValueError:
            pass
    if isinstance(element,unicode):
        return element.encode("utf-8", errors="ignore")
    return element

def decode_if_necessary(element, encoding):
    if isinstance(element, str):
        return element.decode(encoding, "ignore")
    return element

def get_plain_text_rows(filepath):
    print "Splitting plain text..."
    header = ["Text"]
    text = open(filepath, 'rbU').read(512)
    encoding = get_encoding(text)
    rows = [[x.strip().decode(encoding, "ignore")] for x in text.split("\n\n")]
    return header, rows

def remove_dups_garbage(data, column_number, dedupe, degarbage):
    """Takes in the data and a column number to check for duplicates -- remove duplicates of these fields"""
    # def collect_legit_data(row, num, noDups, comments):
    #     noDups.append(row)
    #     comments.add(row[num])
    #     return noDups,comments
    comments = set()
    noDups = []
    num = int(column_number)
    for row in data:
        try:
            am_empty  = row[num]=='' and row == []
            am_a_dupe = row[num] in comments
            if dedupe and am_a_dupe:
                continue
            if degarbage and am_empty:
                continue

            # noDups.append(row)
            noDups.append([encode_if_necessary(c) for c in row])
            comments.add(row[num])

        except IndexError as e:
            # print row, num
            raise e
    print "Removed %i duplicate comments." % (len(data) - len(noDups))
    return noDups

    # NOTES IN CASE I WANT TO PULL OUT SPECIFIC COLUMNS

        # try:
        #     subrow = [row[int(col)] for c, col in enumerate(column_number)]
        #
        #     am_empty  = subrow=='' and row == []
        #     am_a_dupe = subrow in comments
        #     if dedupe and am_a_dupe:
        #         continue
        #     if degarbage and am_empty:
        #         continue
        #
        #     noDups.append(subrow)
        #     comments.add(subrow)
        #
        # except IndexError as e:
        #     # print row, num
        #     raise e

#___________________________
# CANDIDATE FOR EXTERNAL USE
#   I could see someone with a list of lists where they just want those lists evened out.
def even_rows(header, rows):
    """Make sure all rows have the same number of columns"""
    num_cols = max([len(row) for row in [header]+rows])
    if all(len(x) == num_cols for x in [header]+rows):
        return
    print "Warning: ragged rows"
    if len(header) < num_cols:
        header.extend([""]*(num_cols-len(header)))
    for row in rows:
        if len(row) < num_cols:
            row.extend([""]*(num_cols-len(row)))

DELIMITER_NAMES = {',': "comma", '\t': "tab"}

def get_csv_rows(filepath, dialect=None):
    """Open a csv file and return csv headers and rows as lists"""
    print "Processing csv input..."
    text = open(filepath, 'rbU').read(512)
    encoding = get_encoding(text)
    with open(filepath, 'rbU') as csvfile:
        if encoding not in ["us-ascii", "utf-8"]:
           stream = Recoder(csvfile, encoding, 'utf-8')
        else:
            stream = csvfile
        # csv.Sniffer determines tab/comma delimited
        try:
            dialect = csv.Sniffer().sniff(csvfile.readline(), [',','\t'])
            try:
                print "Detected csv %s delimiter..." % DELIMITER_NAMES[dialect.delimiter]
            except KeyError:
                print "Detected csv delimiter: ", dialect.delimiter
        except _csv.Error: # sometimes a "csv" does not have multiple columns and in that case we cannot detect the delimiter
            dialect = csv.Dialect
            dialect.delimiter = "\t"
        # This goes back to the beginning of the csv so we don't lose any data
        csvfile.seek(0)
        # Only use the delimiter from the sniffer because the quoting parameter is not reliable!
        reader = csv.reader(stream.read().splitlines(), delimiter=dialect.delimiter)
        header = [x.strip() for x in reader.next()]
        rows = [[decode_if_necessary(col, encoding) for col in row] for row in reader if row != []]
    return header, rows

def get_xls_rows(filepath):
    """Open a xls file and return headers and rows as lists"""
    print "Processing xls input..."
    try:
        with xlrd.open_workbook(filepath) as workbook:
            sheet = workbook.sheet_by_index(0)
            header = [x.strip() for x in sheet.row_values(0)]
            rows = [sheet.row_values(i) for i in range(1, sheet.nrows)]
        return header, rows
    except AssertionError: # This is the error created if we can't open the spreadsheet
        # Make it more helpful
        raise SynapsifyError("Input error: The spreadsheet appears to be corrupt.")

def get_spreadsheet_rows(filepath, textcol, dedupe=False, format="text/csv"):
    """
        Determine the type of the spreadsheet file and get the headers and rows as lists

        :param filepath: full filename and path to be loaded.
        :param textcol: column number of text to be loaded (index 0)
        :type textcol: int
        :param dedupe: indication of whether to remove duplicate rows.
        :type dedupe: True/False
        :return: The dictionary object that represents an email with the fields: From, To, Sent, Subject, CC, Attachments, and Body
        :rtype: list of strings.
        """
    if format == "text/plain":
        header, rows = get_plain_text_rows(filepath)
    elif format in ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet','application/vnd.ms-excel'):
        header, rows = get_xls_rows(filepath)
    elif format == 'text/csv':
        header, rows = get_csv_rows(filepath)
    elif format in ('binary', 'application/zip'): # sometimes spreadsheets have this mimetype
        try:
            header, rows = get_xls_rows(filepath)
        except AssertionError: # This is the error we get if we can't open the spreadsheet
            raise SynapsifyError("Unexpected file type: Please provide plain text, an Excel spreadsheet, or a CSV.")
    else:
        raise SynapsifyError("Unexpected format: Please set the format parameter to either 'text/plain' or 'text/csv'. Format submitted is " + format)
    even_rows(header, rows)
    if textcol >= len(header):
        raise SynapsifyError("Bad text column number. This spreadsheet only has %i column(s)." % len(header))

    print "Garbage removal and Deduping with parameter: " + str(dedupe)
    rows = remove_dups_garbage(rows, textcol,dedupe,True)

    return header, rows

def write_csv(filepath, header, rows,dedupe=False):
    """Write the header followed by rows to a csv file in filepath"""
    if dedupe:
        rows = remove_dups_garbage(rows, 0, True, False)
    with open(filepath, 'wb') as csvfile:
        # rewrite each line with commas and quotes
        normalcsv = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_ALL)
        normalcsv.writerow([encode_if_necessary(c) for c in header])
        for row in rows:
            normalcsv.writerow([encode_if_necessary(c) for c in row])

def write_split_csv(filepath, header, rows, max_size=1048576):
    """Write CSVs of a maximum size"""
    size = sys.getsizeof(str(rows))
    if size <= max_size:
        write_csv(filepath, header, rows)
    else:
        num_splits = max(2, round(size/max_size))
        num_rows = int(len(rows) / num_splits)
        splits = [[x, x*num_rows, (x+1)*num_rows] for x in range(num_splits)]
        splits[-1][2] = len(rows) # make sure we reach the end
        for split, begin, end in splits:
            new_filepath = re.sub(".csv","_"+str(split+1)+".csv",filepath)
            new_rows = rows[begin:end]
            write_csv(new_filepath, header, new_rows)


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False

def remove_regex(text,regex_search):
    if regex_search !=None:
        beg = text[0:regex_search.start()]
        end = text[(regex_search.end()+1):-1]
        text = beg +" "+ end
    return text

def remove_escapes(text):
    '''Remove any characters that start with escape-x'''
    for esc in ESCAPES:
        res = esc.search(text)
        text = remove_regex(text,res)
    return text

def remove_url(text):
    '''urls can be useless for a lot of reasons, especially training models.'''
    # urlregex = re.compile("""/^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$/""", flags=re.IGNORECASE)
    res = EMAIL_OR_URL.search(text)
    text = remove_regex(text,res)
    return text

def remove_double_slashes(text):
    '''
    Find the double slash with a u character and remove it and the next 5 characters if applicable
    I would like to include these characters as new characters if possible, perhaps one day I'll map them to unique words.
    '''
    slash_found = True
    while slash_found:
        xx = text.find('\\')
        if xx!=-1:
            beg = text[0:xx]
            end = text[(xx+1):len(text)]
            if text[xx+1]=='u':
                end = text[(xx+6):len(text)]
            text = beg +" "+ end
        else:
            slash_found = False
    return text

def remove_tweet_symbols(text):

    symbol_found = True
    while symbol_found:
        symbol_found = False
        for symb in TWEET_SYMBOLS:
            xx = text.find(symb)
            if xx!=-1:
                symbol_found = True
                beg = text[0:xx]
                end = text[(xx+len(symb)):len(text)]
                text = beg+end
    return text

def clean_tweets(rows,textcol):
    '''Remove \n, urls, RT, '''
    for k,row in enumerate(rows):
        new_row = row[textcol]
        new_row = remove_url(new_row)
        new_row = remove_escapes(new_row)
        new_row = remove_double_slashes(new_row)
        new_row = remove_tweet_symbols(new_row)
        rows[k][textcol] = new_row

    return rows

def base26to10(b26):
    """Convert Excel Alphabet Columns to Numeric"""
    alphabet_cap = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
    try:
        b10 = 0
        for digit in range(0,len(b26)):
            letter = b26[digit]
            num = alphabet_cap.index(letter.upper())+1
            # print b10,digit,num,letter
            b10 = b10*26 + num
        return b10
    except ValueError:
        pass

    return False

#===========================================================================================================

def order_by_var(data,var1d):
    vxx = sorted(range(len(var1d)), key=lambda k: var1d[k])
    oData = [data[xx] for xx in vxx[::-1]] #[0:3000]]
    return oData, vxx

def load_csv(input):
    print "Processing csv input..."
    text = open(input, 'rbU').read(512)
    encoding = get_encoding(text)
    with open(input, 'rbU') as csvfile:
        if encoding not in ["us-ascii", "utf-8"]:
           stream = Recoder(csvfile, encoding, 'utf-8')
        else:
            stream = csvfile
        # csv.Sniffer determines tab/comma delimited
        try:
            dialect = csv.Sniffer().sniff(csvfile.readline(), [',','\t'])
            try:
                print "Detected csv %s delimiter..." % DELIMITER_NAMES[dialect.delimiter]
            except KeyError:
                print "Detected csv delimiter: ", dialect.delimiter
        except _csv.Error: # sometimes a "csv" does not have multiple columns and in that case we cannot detect the delimiter
            dialect = csv.Dialect
            dialect.delimiter = "\t"
        # This goes back to the beginning of the csv so we don't lose any data
        csvfile.seek(0)
        # Only use the delimiter from the sniffer because the quoting parameter is not reliable!
        reader = csv.reader(stream.read().splitlines(), delimiter=dialect.delimiter)
        header = [x.strip() for x in reader.next()]
        rows = [[decode_if_necessary(col, encoding) for col in row] for row in reader if row != []]

    return rows, header


# def main(directory, filename, column_set=[0], header=True, type='csv'):
#     """
#     Designed to cleanly load any spreadsheet.
#
#     :param directory:
#     :param filename:
#     :param column_set: vector of integers giving the columns to extract.
#     :param header: Whether the spreadsheet includes a header or not.
#     :param type: If provided, give the type of file to be loaded and cleaned.
#     :return rows: list of each cleaned row
#     :return header: header associated with those rows
#     """
#
#     header, rows = get_spreadsheet_rows(directory, column_set[0], False) #, format)
#     return header, rows
#
#
# if __name__ == '__main__':
#     directory = sys.argv[0]
#     filename  = sys.argv[1]
#     main(directory, filename)
