import snap7
import time
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

# Gateway ip is 192.168.1.154

client = snap7.client.Client()
sample_rate = 60
c = DBHelper()
parameterName = ['PRODUCTION_ON_OFF',
                 "UV_LIGHT_ON_OFF",
                 "RINSE_RESUDUAL_OZONE_PPM",
                 "PRODUCT_RESUDUAL_OZONE",
                 "PRODUCT_PH",
                 "PRODUCT_TDS",
                 "BOTTLE_RINSE_PRESSURE",
                 "OZONATED_WATER_DRAIN",
                 "RO_TDS_BEFORE_MIXING",
                 "WATER_LEVEL_AT_RINSE",
                 "WATER_LEVEL_AT_PRODUCT",
                 "FLOW_RATE",
                 "1ST_DOSE_DISPLAY",
                 "AFTER_1_ST_DOSE_MIXING_TDS",
                 "DRAIN_VALVE_1",
                 "2ND_DOSE_DISPLAY",
                 "AFTER_2ND_DOSE_MIXING_TDS",
                 "DRAIN_VALVE_2"
                 ]
def initiate():
    global client
    try:
        client.connect("192.168.1.150", 0, 1)  # 2 for s7-300 # first 0 for port number(0 arrg  automatically assign an available port), second 0 for timeout value.
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
        client = initiate()
        if client is not None:
            break
        time.sleep(1)
    try:
        data = list()
        buffer1 = client.read_area(snap7.types.Areas.DB, 50, 0, 1)
        log.info(f'this is buffet  {buffer1}')
        bool1, bool2 = get_bool(buffer1)
        log.info(f'this is bool 1 {bool1}')
        log.info(f'this is bool 2 {bool2}')
        buffer2 = client.read_area(snap7.types.Areas.DB, 50, 4, 4)
        buffer2 = struct.unpack(">f", buffer2)[0]
        buffer3 = client.read_area(snap7.types.Areas.DB, 50, 8, 4)
        buffer3 = struct.unpack(">f", buffer3)[0]
        buffer4 = client.read_area(snap7.types.Areas.DB, 50, 12, 4)
        buffer4 = struct.unpack(">f", buffer4)[0]
        buffer5 = client.read_area(snap7.types.Areas.DB, 50, 16, 4)
        buffer5 = struct.unpack(">f", buffer5)[0]
        buffer6 = client.read_area(snap7.types.Areas.DB, 50, 20, 4)
        buffer6 = struct.unpack(">f", buffer6)[0]
        buffer7 = client.read_area(snap7.types.Areas.DB, 50, 24, 4)
        buffer7 = struct.unpack(">f", buffer7)[0]
        buffer8 = client.read_area(snap7.types.Areas.DB, 50, 28, 4)
        buffer8 = struct.unpack(">f", buffer8)[0]
        buffer9 = client.read_area(snap7.types.Areas.DB, 50, 32, 4)
        buffer9 = struct.unpack(">f", buffer9)[0]
        buffer10 = client.read_area(snap7.types.Areas.DB, 50, 36, 4)
        buffer10 = struct.unpack(">f", buffer10)[0]
        buffer11 = client.read_area(snap7.types.Areas.DB, 50, 40, 4)
        buffer11 = struct.unpack(">f", buffer11)[0]
        buffer12 = client.read_area(snap7.types.Areas.DB, 50, 44, 4)
        buffer12 = struct.unpack(">f", buffer12)[0]
        buffer13 = client.read_area(snap7.types.Areas.DB, 50, 48, 4)
        buffer13 = struct.unpack(">f", buffer13)[0]
        buffer14 = client.read_area(snap7.types.Areas.DB, 50, 52, 4)
        buffer14 = struct.unpack(">f", buffer14)[0]
        buffer15 = client.read_area(snap7.types.Areas.DB, 50, 56, 4)
        buffer15 = struct.unpack(">f", buffer15)[0]
        buffer16 = client.read_area(snap7.types.Areas.DB, 50, 60, 4)
        buffer16 = struct.unpack(">f", buffer16)[0]
        buffer17 = client.read_area(snap7.types.Areas.DB, 50, 64, 4)
        buffer17 = struct.unpack(">f", buffer17)[0]
        data.append(bool1)
        data.append(bool2)
        data.append(buffer2)
        data.append(buffer3)
        data.append(buffer4)
        data.append(buffer5)
        data.append(buffer6)
        data.append(buffer7)
        data.append(buffer8)
        data.append(buffer9)
        data.append(buffer10)
        data.append(buffer11)
        data.append(buffer12)
        data.append(buffer13)
        data.append(buffer14)
        data.append(buffer15)
        data.append(buffer16)
        data.append(buffer17)
        print(data)
        client.disconnect()
        return data
    except Exception as e:
        log.error(e)
        client.disconnect()
        return None


# def post_data(data):
#     global HEADERS
#     # print(data)
#     url = f'http://{host}/api/v1/{accessToken}/telemetry'
#
#     payload = {}
#     for i, pName in enumerate(parameterName):
#         payload[pName] = data[i]
#     print(f'"machineId:" {machine_name}', str(payload))
#     print(url)
#     if send_data:
#         print(payload)
#         try:
#             request_response = requests.post(url, json=payload, headers=HEADERS,timeout=5)
#             print(request_response.status_code)
#         except Exception as e:
#             print("Can't send data", e)

# def post_data(data):
#     """posting OEE DATA to the SERVER"""
#     url = f'http://{host}:8080/api/v1/{accessToken}/telemetry'
#     log.info("[+] Sending OEE data to server")
#
#     if SENDDATA:
#         print(payload)
#         try:
#             send_req = requests.post(url, json=payload, headers=HEADERS, timeout=2)
#             log.info(send_req.status_code)
#             send_req.raise_for_status()
#             for i in c.get_sync_data():
#                 max_ts = max([j['ts'] for j in i])
#                 try:
#                     sync_req = requests.post(url, json=i, headers=HEADERS, timeout=2)
#                     sync_req.raise_for_status()
#                     log.info(f"[+] Sync_successful for timestamp less or equal to {max_ts}")
#                     # clearing sync_payloads only when data is synced successfully
#                     c.clear_sync_data(max_ts)
#                     with open(os.path.join(dirname, f'logs/sync_log.{date.today()}.txt'), 'a') as f:
#                         pname = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ---- SYNC DONE\n'
#                         f.write(pname)
#                     time.sleep(0.1)
#                 except Exception as e:
#                     log.error(f"[-] Error in sending SYNC Cycle time data {e}")
#
#         except Exception as e:
#             log.error(f"[-] Error in sending Cycle time data {e}")
#             c.add_sync_data(payload)
def post_data(data, machine_id):
    """posting OEE DATA to the SERVER"""
    url = f'http://{host}:8080/api/v1/{accessToken}/telemetry'
    # print("[+] Sending CNC data to server")
    log.info("[+] Sending CNC data to server")
    payload = {}
    for i, pName in enumerate(parameterName):
        payload[pName] = data[i]
    print(payload)
    if send_data:
        try:
            send_req = requests.post(url, json=payload, headers=HEADERS, timeout=2)
            # print(send_req.status_code)
            log.info(send_req.status_code)

            send_req.raise_for_status()
            sync_data = c.get_sync_data()
            if sync_data:
                for i in sync_data:
                    if i:
                        machine_id_sync = i.get('machine_id')
                        payload = i.get('payload')
                        # print(f"[+] ----Machine_ID-----{machine_id_sync}")
                        log.info(f"[+] ----Machine_ID-----{machine_id_sync}")
                        for sync_payload in payload:
                            max_ts = max([j['ts'] for j in sync_payload])
                            try:
                                url = f'http://{host}:8080/api/v1/{accessToken}/telemetry'
                                # print(url)
                                log.info(url)
                                sync_req = requests.post(url, json=sync_payload, headers=HEADERS, timeout=2)
                                sync_req.raise_for_status()
                                # print(f"[+] Sync_successful for timestamp less or equal to {max_ts}")
                                log.info(f"[+] Sync_successful for timestamp less or equal to {max_ts}")
                                # clearing sync_payloads only when data is synced successfully
                                c.clear_sync_data(max_ts, machine_id_sync)
                                with open(os.path.join(dirname, f'logs/sync_log{datetime.date.today()}.txt'), 'a') as f:
                                    pname = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ---- SYNC DONE\n'
                                    f.write(pname)
                                time.sleep(0.1)
                            except Exception as e:
                                # print(f"[-] Error in sending SYNC Cycle time data {e}")
                                log.info(f"[-] Error in sending SYNC Cycle time data {e}")
                    else:
                        # print("(^-^) No data to sync")
                        log.info("(^-^) No data to sync")
                        break

        except Exception as e:
            # print(f"[-] Error in sending Cycle time data {e}")
            log.info(f"[-] Error in sending Cycle time data {e}")
            c.add_sync_data(payload, machine_id)


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
        post_data(data, machine_name)


schedule.every(sample_rate).seconds.do(main)

if __name__ == '__main__':
    # try:
    while True:
        schedule.run_pending()
        time.sleep(0.5)
