from configparser import ConfigParser
config = ConfigParser()
config["data"] = {
    'data1': (50, 0, 4),
    'data2': (50, 4, 4),
    'data3': (50, 8, 4),
    'data4': (50, 12, 4)
                  }
config['PLC_DATA'] = {
    'ip': "'192.1.3.2'",
    'rack': "0",
    'slot': "2"
}

with open("config", "w") as f:
    config.write(f)
