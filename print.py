from configparser import ConfigParser
config = ConfigParser()
config.read("config")
config_data = config['data']['data1']
config_data2 = config['data']['data2']
config3 = config['data']['data3']
config4 = config['data']['data4']
config5 = config['PLC_DATA']['ip']
config6 = config['PLC_DATA']['rack']
config7 = config['PLC_DATA']['slot']
print(config_data)
print(config_data2)
print(config3)
print(config4)
print(config5)
print(config6)
print(config7)
