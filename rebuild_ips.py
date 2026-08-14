import json, os, configparser

setting_path = os.path.expanduser("~/.quantaxis/setting")
stock_ip_list = [
    {"ip": "124.71.187.122", "port": 7709, "name": "上海双线主站14"},
    {"ip": "119.97.185.59", "port": 7709, "name": "武汉电信主站1"},
    {"ip": "47.107.64.168", "port": 7709, "name": "深圳双线主站7"},
    {"ip": "124.70.75.113", "port": 7709, "name": "北京双线主站4"},
    {"ip": "124.71.9.153", "port": 7709, "name": "广州双线主站4"},
    {"ip": "123.60.84.66", "port": 7709, "name": "上海双线主站15"},
    {"ip": "47.107.228.47", "port": 7719, "name": "深圳双线主站8"},
    {"ip": "120.46.186.223", "port": 7709, "name": "北京双线主站5"},
    {"ip": "124.70.22.210", "port": 7709, "name": "北京双线主站6"},
    {"ip": "139.9.133.247", "port": 7709, "name": "北京双线主站7"},
    {"ip": "116.205.163.254", "port": 7709, "name": "广州双线主站5"},
    {"ip": "116.205.171.132", "port": 7709, "name": "广州双线主站6"},
    {"ip": "116.205.183.150", "port": 7709, "name": "广州双线主站7"},
    {"ip": "106.120.74.86", "port": 7711, "name": "北京行情主站1"},
    {"ip": "113.105.73.88", "port": 7709, "name": "深圳行情主站"},
    {"ip": "114.80.80.222", "port": 7711, "name": "上海行情主站"},
    {"ip": "117.184.140.156", "port": 7711, "name": "移动行情主站"},
    {"ip": "119.147.171.206", "port": 443, "name": "广州行情主站"},
    {"ip": "119.147.171.206", "port": 80, "name": "广州行情主站"},
    {"ip": "218.108.50.178", "port": 7711, "name": "杭州行情主站"},
    {"ip": "221.194.181.176", "port": 7711, "name": "北京行情主站2"},
    {"ip": "106.120.74.86", "port": 7709},
    {"ip": "112.95.140.74", "port": 7709},
    {"ip": "112.95.140.92", "port": 7709},
    {"ip": "112.95.140.93", "port": 7709},
    {"ip": "113.05.73.88", "port": 7709},
    {"ip": "114.67.61.70", "port": 7709},
    {"ip": "114.80.149.19", "port": 7709},
    {"ip": "114.80.149.22", "port": 7709},
    {"ip": "114.80.149.84", "port": 7709},
    {"ip": "114.80.80.222", "port": 7709},
    {"ip": "115.238.56.198", "port": 7709},
    {"ip": "115.238.90.165", "port": 7709},
    {"ip": "117.184.140.156", "port": 7709},
    {"ip": "119.147.164.60", "port": 7709},
    {"ip": "119.147.171.206", "port": 7709},
    {"ip": "119.29.51.30", "port": 7709},
    {"ip": "121.14.104.70", "port": 7709},
    {"ip": "121.14.104.72", "port": 7709},
    {"ip": "121.14.110.194", "port": 7709},
    {"ip": "121.14.2.7", "port": 7709},
    {"ip": "123.125.108.23", "port": 7709},
    {"ip": "123.125.108.24", "port": 7709},
    {"ip": "124.160.88.183", "port": 7709},
    {"ip": "180.153.18.17", "port": 7709},
    {"ip": "180.153.18.170", "port": 7709},
    {"ip": "180.153.18.171", "port": 7709},
    {"ip": "180.153.39.51", "port": 7709},
    {"ip": "218.108.47.69", "port": 7709},
    {"ip": "218.108.50.178", "port": 7709},
    {"ip": "218.108.98.244", "port": 7709},
    {"ip": "218.75.126.9", "port": 7709},
    {"ip": "218.9.148.108", "port": 7709},
    {"ip": "221.194.181.176", "port": 7709},
    {"ip": "59.173.18.69", "port": 7709},
    {"ip": "60.12.136.250", "port": 7709},
    {"ip": "60.191.117.167", "port": 7709},
    {"ip": "60.28.29.69", "port": 7709},
    {"ip": "61.135.142.73", "port": 7709},
    {"ip": "61.135.142.88", "port": 7709},
    {"ip": "61.152.107.168", "port": 7721},
    {"ip": "61.152.249.56", "port": 7709},
    {"ip": "61.153.144.179", "port": 7709},
    {"ip": "61.153.209.138", "port": 7709},
    {"ip": "61.153.209.139", "port": 7709},
    {"ip": "hq.cjis.cn", "port": 7709},
    {"ip": "hq1.daton.com.cn", "port": 7709},
    {"ip": "jstdx.gtjas.com", "port": 7709},
    {"ip": "shtdx.gtjas.com", "port": 7709},
    {"ip": "sztdx.gtjas.com", "port": 7709},
    {"ip": "113.105.142.162", "port": 7721},
    {"ip": "23.129.245.199", "port": 7721},
    {"ip": "110.41.174.169", "port": 7709, "name": "深圳双线主站9"},
    {"ip": "121.37.183.82", "port": 7709, "name": "上海双线主站16"},
]
with open(os.path.join(setting_path, "stock_ip.json"), "w") as f:
    json.dump(stock_ip_list, f, ensure_ascii=False)
print("Rebuilt stock_ip.json with", len(stock_ip_list), "IPs")

# Clear cached IP defaults in config.ini
config_path = os.path.join(setting_path, "config.ini")
config = configparser.ConfigParser()
if os.path.exists(config_path):
    config.read(config_path)
if config.has_section("IPLIST"):
    config.remove_section("IPLIST")
if not config.has_section("MONGODB"):
    config.add_section("MONGODB")
    config.set("MONGODB", "uri", "mongodb://localhost:27017")
with open(config_path, "w") as f:
    config.write(f)
print("Cleared IPLIST cache in config.ini")
