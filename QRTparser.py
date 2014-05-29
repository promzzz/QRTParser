'''
Created on May 29, 2014

@author: qyou

@summary:
Problem Analysis:
    The original python source file is in `QRT2csv.py.bak`

    Alignment of memory causes the different QRT format defination

    My code file is `QRTparser.py`.

Usage:
	python QRTparser.py QRT_FILEPATH [OUT_DIRPATH]
	
Thanks to the `QRT2csv.py`. Revised by qyou on May 28, 2014. Enjoy!
'''
from __future__ import with_statement, print_function 
import struct
import time
import os
import stat
import sys

class QRTParser(object):
    ''' QRTParser, parse the QRT file and create csv files
    '''
    def __init__(self, filepath):        
        self.fp = open(filepath, 'rb')
    def __enter__(self):
        return self
    def __exit__(self, type, value, tb):
        self.fp.close()
        
    def _parse_header(self):
        buf = self.fp.read(0x20)
        record_size, = struct.unpack_from('L', buf[12:])
        record_begin_position, = struct.unpack_from('L', buf[20:])
        code_number, = struct.unpack_from('L', buf[28:])
        return (record_size, record_begin_position, code_number)
    
    def _parse_future_map(self, pos, bytes_of_future):
        self.fp.seek(pos)
        buf = self.fp.read(bytes_of_future)
        #future_name, = struct.unpack_from('7s', buf[:7])
        #future_name = "".join(future_name)
        #future_name = future_name.strip('\x00')
        future_name = buf[:7]
        future_name = future_name.strip('\x00')
        records_number, = struct.unpack_from('L', buf[10:14])
        stock_positions = []
        for j in range(33):
            stock_position,= struct.unpack_from("H",buf[(14+j*2):(16+j*2)])
            stock_positions.append(stock_position)
        return {'future_name':future_name, 'records_number':records_number, "stock_positions":stock_positions}
    
    def _parse_one_record(self, pos, bytes_of_record):
        self.fp.seek(pos)
        buf = self.fp.read(bytes_of_record)
        secondsfrom1970, \
        lastprice,          \
        totalvolumetoday, \
        totaltrademoneytoday,     \
        buy1volume, \
        buy2volume, \
        buy3volume, \
        sell1volume, \
        sell2volume, \
        sell3volume, \
        buy1, \
        buy2, \
        buy3, \
        sell1, \
        sell2, \
        sell3, \
        reserved, = struct.unpack_from("LfffHHHHHHBBBBBBH",buf)
        curtime=time.gmtime(secondsfrom1970)
        timestring="%04d-%02d-%02d-%02d-%02d-%02d"%(curtime.tm_year,curtime.tm_mon,curtime.tm_mday,curtime.tm_hour,curtime.tm_min,curtime.tm_sec)
        onerecord=timestring
        onerecord=onerecord+",%8.1f"%(lastprice)
        onerecord=onerecord+",%4.0f"%(totalvolumetoday)
        onerecord=onerecord+",%8.0f"%(totaltrademoneytoday)
        onerecord=onerecord+",%d"%(buy1volume)
        onerecord=onerecord+",%d"%(sell1volume)
        onerecord=onerecord+",%d"%(buy1)
        onerecord=onerecord+",%d"%(sell1)
        return onerecord
    
    def _parse_all(self, record_size, record_begin_position, code_number, out_dir, alignment=False):
        try:
            if alignment is False:
                bytes_of_future = 254
            else:
                bytes_of_future = 252
            future_maps = [self._parse_future_map(0x20+i*bytes_of_future, bytes_of_future) for i in range(code_number)]
            for future in future_maps:
                if future['records_number'] == 0:
                    continue
                csv_filename = os.path.join(out_dir, future['future_name']+".csv")
                with open(csv_filename,'ab') as csv_file:
                    if os.stat(csv_filename)[stat.ST_SIZE]==0:
                        first_line="%s,%s,%s,%s,%s,%s,%s,%s"%("time", 'new_price', 'vol_today', 'ammount','buy_vol','sell_vol','buy_price(relative)','sell_price(relative)')
                        csv_file.write(first_line+'\n')
                
                    for i in range(future['records_number']):
                        base_address = future["stock_positions"][(int)(i/record_size)]
                        if base_address == -1:
                            continue
                        bytes_of_record = 36
                        base_address = base_address*record_size*bytes_of_record+record_begin_position
                        code_pos = base_address+(i%record_size)*bytes_of_record
                        one_record = self._parse_one_record(code_pos, bytes_of_record)
                        csv_file.write(one_record+"\n")
                print(csv_filename)
        except:
            raise Exception('Runtime Error')
        
    def write_all(self, out_dir=None, alignment=False):
        record_size, record_begin_position, code_number = self._parse_header()
        if out_dir is None:
            out_dir = 'csvdir'
        if not os.path.exists(out_dir):
            os.mkdir(out_dir)
        print("---Start writing files...---")
        try: 
            self._parse_all(record_size, record_begin_position, code_number, out_dir, alignment)
        except:
            alignment = not alignment
            self._parse_all(record_size, record_begin_position, code_number, out_dir, alignment)
        print("---End writing files---")

def help():
    print("  Revised by qyou on May 28, 2014")
    print("Usage:\n\tpython QRTparser.py QRT_FILEPATH [OUT_DIRPATH]")
    
def main():
    if len(sys.argv) < 2:
        print('QRT_FILEPATH is NEEDED!\n')
        help()
        return
    if len(sys.argv) >= 2:
        qrt_filepath = sys.argv[1]
    if len(sys.argv) < 3:
        out_dirpath = 'csvdir'
    else:
        out_dirpath = sys.argv[2]
    with QRTParser(qrt_filepath) as parser:
        parser.write_all(out_dirpath)

if __name__ == '__main__':
    main()
