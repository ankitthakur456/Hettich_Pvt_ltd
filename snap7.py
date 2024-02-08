import time
import snap7
import struct
import requests
import logging
import schedule
import datetime
from datetime import date
import os
from dbHelper import DBHelper
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from configparser import ConfigParser
dirname = os.path.dirname(os.path.abspath(__file__))

log_level = logging.INFO

FORMAT = ('%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger()

# checking and creating logs directory here
if not os.path.isdir("./logs"):
    log.info("[-] logs directory doesn't exists")
    try:
        os.mkdir("./logs")
        log.info("[+] Created logs dir successfully")
    except Exception as e:
        log.error(f"[-] Can't create dir logs Error: {e}")

fileHandler = TimedRotatingFileHandler(os.path.join(dirname, f'logs/app_log'),
                                       when='midnight', interval=1)
fileHandler.setFormatter(logFormatter)
fileHandler.suffix = "%Y-%m-%d.log"
log.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
log.addHandler(consoleHandler)
log.setLevel(log_level)

send_data = True
HEADERS = {"Content-Type": "application/json"}
host = 'ithingspro.cloud'  # put ip and port of the HIS application
accessToken = "IAYUAu64GaiHF1eczTUt"  # access token from the device in HIS application
machine_name = "Kinley"
SENDDATA = True


#config parser section
config = ConfigParser()
config.read("config")
data1 = config['data']['data1']
print(data1)
data2 = config['data']['data2']
print(data2)
data3 = config['data']['data3']
print(data3)
data4 = config['data']['data4']
print(data4)
GL_IP = config['PLC_DATA']['ip']
RACK = config['PLC_DATA']['rack']
SLOT = config['PLC_DATA']['slot']
#completed

#snap7 section
client = snap7.client.Client()
sample_rate = 60
c = DBHelper()
parameterName = ['data1',
                 "data2",
                 "data3",
                 "data4",
                 "data5",
                 "data6",
                 ]


def initiate():
    global client
    try:
        client.connect(GL_IP, rack = RACK, slot = SLOT)
        if client.get_connected():
            log.info("Client Connected!")
            return client
        else:
            log.info("No Communication from the client.")
    except Exception as e:
        log.error(f"ERROR initiating {e}")
    return None

def get_bool(data):
    integer_buffer = int.from_bytes(data, "big")
    # print(integer_buffer)
    log.info(integer_buffer)
    binary_data = format(integer_buffer, "08b")
    # print(binary_data)
    log.info(binary_data)
    first_bool = binary_data[-1]
    second_bool = binary_data[-2]
    # print(first_bool, second_bool)
    log.info(first_bool)
    log.info(second_bool)
    return first_bool, second_bool


def read_S7_data():
    global client, parameterName
    while True:
        client = snap7.client.Client()
        #client = initiate()
        if client is not None:
            break
        time.sleep(1)
    try:
        data = list()
        buffer1 = client.read_area(snap7.types.Areas.DB, data1[0], data1[1], data1[2])
        log.info(f'this is buffet  {buffer1}')
        bool1, bool2 = get_bool(buffer1)
        log.info(f'this is bool 1 {bool1}')
        log.info(f'this is bool 2 {bool2}')
        buffer2 = client.read_area(snap7.types.Areas.DB, data2[0], data2[1], data2[2])
        buffer2 = struct.unpack(">f", buffer2)[0]
        log.info(f'this is data2  {buffer2}')
        buffer3 = client.read_area(snap7.types.Areas.DB, data3[0], data3[1], data3[2])
        buffer3 = struct.unpack(">f", buffer3)[0]
        log.info(f'this is data3  {buffer3}')
        buffer4 = client.read_area(snap7.types.Areas.DB, data4[0], data4[1], data4[2])
        buffer4 = struct.unpack(">f", buffer4)[0]
        log.info(f'this is data3  {buffer4}')

        data.append(bool1)
        data.append(bool2)
        data.append(buffer2)
        data.append(buffer3)
        data.append(buffer4)

        print(data)
        client.disconnect()
        return data
    except Exception as e:
        log.error(e)
        client.disconnect()
        return None

def post_error():

    """posting an error in the attributes if the data is None"""

    global HEADERS, machine_name
    url = f'http://{host}/api/v1/{accessToken}/attributes'
    payload = {"error": True}
    # print(url)
    log.info(f"URL: {url}")
    log.info(f"machineId: {machine_name}")
    log.info(f"payload: {str(payload)} ")

    # print(f'"machineId:" {machine_name}', str(payload))
    if send_data:
        try:
            request_response = requests.post(url, json=payload, headers=HEADERS, timeout=5)
            # print(request_response.status_code)
            log.info(request_response.status_code)
        except Exception as e:
            # print("Can't post Error Attribute", e)
            log.info(f"Can't post Error Attribute{e}")


def main():
    data = read_S7_data()
    if data is None:
        post_error()
    else:
        log.info(f'data is {data}')


schedule.every(sample_rate).seconds.do(main)

if __name__ == '__main__':
    # try:
    while True:
        schedule.run_pending()
        time.sleep(0.5)
