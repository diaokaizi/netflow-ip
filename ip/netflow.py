import requests
import re
import json
import datetime
from urllib import parse

# @retry(stop_max_attempt_number=1)
# def download(*args, **kwargs):
#     method = kwargs.pop("method", "")
#     if method.upper() == "POST":
#         resp = requests.post(*args, **kwargs)
#     else:
#         resp = requests.get(*args, **kwargs)
#     if resp.status_code != 200:
#         raise Exception(f"StatusCode:{resp.status_code} != 200\n{args}\n{kwargs}")
#     return resp

# def _query_url(ts, job):
#     job = "{" + f'job="{job}"' + "}"
#     return f'http://172.16.152.28:33000/loki/api/v1/query_range?query=count_over_time({job} [1m])&start={ts}&end={ts}'

class netflowObj:
    def __init__(self, src_ip, src_port, des_ip, des_port, bytes):
        self.src_ip = src_ip
        self.src_port = src_port
        self.des_ip = des_ip
        self.des_port = des_port
        self.bytes = bytes

    def __str__(self) -> str:
        return f"{self.src_ip}:{self.src_port} -> {self.des_ip}:{self.des_port} ======{self.bytes}"

    def get_key(self) -> str:
        return f"{self.src_ip}:{self.src_port}_{self.des_ip}:{self.des_port}"

def lokiapi(host: str, start: str, end: str, limit: int = None) -> list[netflowObj]:
    startdt = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    enddt = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    query_data = {
        'start': startdt.timestamp(),
        'end': enddt.timestamp()
    }
    if limit is not None:
        query_data['limit'] = limit
    query = parse.urlencode(query=query_data)
    base_url = f"http://{host}/loki/api/v1/query_range?query={{job=%22netflow%22}}"
    resp = requests.get(f'{base_url}&{query}')
    # url = f"http://{host}/loki/api/v1/query_range?query={{job=%22netflow%22}}&limit={limit}&start={startdt.timestamp()}&end={enddt.timestamp()}"
    print(f'{base_url}?{query}')
    if resp.status_code != 200:
        raise Exception(f"{resp.text}")
    return parse_lokiapi_data(resp.text)

def parse_lokiapi_data(data):
    source = json.loads(data)['data']['result'][0]['values']
    newflows = [json.loads(record[1]) for record in source]
    data = []
    for newflow in newflows:
        data.append(netflowObj(src_ip=newflow['source']['ip'], src_port=newflow['source']['port'],
                   des_ip=newflow['destination']['ip'], des_port=newflow['destination']['port'],
                   bytes=newflow['network']['bytes']))
    return data

def get_data_by_5min(host, start):
    end = (datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    return lokiapi(host=host, start=start, end=end, limit=1000000)

if __name__ == '__main__':
    host = "223.193.36.79:7140"
    start = "2023-11-23 10:15:00"
    data = get_data_by_5min(host=host, start=start)
    dic = {}
    print(len(data))
    for netflow in data:
        dic[netflow.get_key()] = dic.get(netflow.get_key(), 0) + netflow.bytes
    print(len(dic.keys()))
    data = list(dic.values())

# http://223.193.36.79:7140/loki/api/v1/query_range?query={job=%22netflow%22}?start=1700619300.0&end=1700619600.0&limit=1000000
# 103012
# 75806
