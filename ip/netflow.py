import requests
import re
import json
import datetime
from urllib import parse
import time
# import draw
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
    def __init__(self, src_ip, src_port, des_ip, des_port, bytes, proto):
        self.src_ip = src_ip
        self.src_port = src_port
        self.des_ip = des_ip
        self.des_port = des_port
        self.bytes = bytes
        self.proto = proto

    def __str__(self) -> str:
        return f"{self.src_ip}:{self.src_port} -> {self.des_ip}:{self.des_port} ======{self.bytes}"

    def get_key(self) -> str:
        return f"{self.src_ip}:{self.des_ip}"
        # return f"{self.src_ip}_{self.des_ip}"

    
    

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
    if resp.status_code != 200:
        raise Exception(f"{resp.text}")
    return parse_lokiapi_data(resp.text)

def parse_lokiapi_data(data):
    try:
        source = json.loads(data)['data']['result'][0]['values']
        newflows = [json.loads(record[1]) for record in source]
        data = []
        for newflow in newflows:
            data.append(netflowObj(src_ip=newflow['source']['ip'], src_port=newflow['source']['port'],
                    des_ip=newflow['destination']['ip'], des_port=newflow['destination']['port'],
                    bytes=newflow['network']['bytes'], proto=newflow['network']['transport']))
        return data
    except:
        return []

def get_data_by_5m(host, start) -> list[netflowObj]:
    end = (datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    return lokiapi(host=host, start=start, end=end, limit=1000000)

def get_data_by_1h(host, start) -> list[netflowObj]:
    data = []
    for i in range(12):
        data.append(get_data_by_5m(host=host, start=start))
        start = (datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    return data

def test_raw_5m():
    host = "223.193.36.79:7140"
    start = "2023-11-23 10:15:00"
    data = get_data_by_5m(host=host, start=start)
    data = [netflow.bytes for netflow in data]

def test_raw_1h():
    host = "223.193.36.79:7140"
    start = "2023-11-23 10:15:00"
    data =[]
    for i in range(12):
        data_5m = get_data_by_5m(host=host, start=start)
        data.extend(data_5m)
        start = (datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    data = [_.bytes for _ in data]
    print(len(data))
    # draw.count_pic(data=data, xlabel="bytes(power of 10)", ylabel="count", title="1h", filename="1h.png")
    # draw.draw_rank_ratio(data=data)

def test_5m():
    host = "223.193.36.79:7140"
    start = "2023-11-24 10:15:00"
    data = get_data_by_5m(host=host, start=start)
    dic = {}
    for netflow in data:
        dic[netflow.get_key()] = dic.get(netflow.get_key(), 0) + netflow.bytes
    print(len(data), len(dic.values()), len(dic.values()) / len(data))
    data = list(dic.values())
    # draw.count_pic(data=data, xlabel="bytes(power of 10)", ylabel="count", title="5min", filename="5min.png")

def test_1h():
    host = "223.193.36.79:7140"
    start = "2023-11-23 10:15:00"
    total_len = 0
    dic = {}
    for i in range(12):
        data_5m = get_data_by_5m(host=host, start=start)
        total_len = total_len + len(data_5m)
        for netflow in data_5m:
            dic[netflow.get_key()] = dic.get(netflow.get_key(), 0) + netflow.bytes
        start = (datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    data = list(dic.values())
    print(total_len, len(dic.values()), len(dic.values()) / total_len)
    # draw.count_pic(data=data, xlabel="bytes(power of 10)", ylabel="count", title="1h", filename="1h.png")
    # draw.draw_rank_ratio(data=data)

def test_1d():
    host = "223.193.36.79:7140"
    start = "2023-11-23 00:00:00"
    total_len = 0
    dic = {}
    for j in range(24):
        print(f"hour:{start}")
        for i in range(12):
            data_5m = get_data_by_5m(host=host, start=start)
            total_len = total_len + len(data_5m)
            for netflow in data_5m:
                dic[netflow.get_key()] = dic.get(netflow.get_key(), 0) + netflow.bytes
            start = (datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
        print(total_len)
    data = list(dic.values())
    print(total_len, len(dic.values()), len(dic.values()) / total_len)
    with open('1d.txt', 'w') as f:
        f.write(f'{total_len}, {len(dic.values())}, {len(dic.values()) / total_len}')
    # draw.count_pic(data=data, xlabel="bytes(power of 10)", ylabel="count", title="1day", filename="1day.png")


if __name__ == '__main__':
    time_start = time.time()
    # test_5m()
    time_end = time.time()
    time_sum = time_end - time_start
    print(f"time(s):{time_sum}")
    # draw.draw_aggregation_compare()
    # a = '{"log":"level=info ts=2019-04-30T02:12:41.844179Z caller=filetargetmanager.go:180 msg=\"Adding target\"\n","stream":"stderr","time":"2019-04-30T02:12:41.8443515Z"}'
    a = {"log":"level=info ts=2019-04-30T02:12:41.844179Z caller=filetargetmanager.go:180 msg=\"Adding target\"\n","stream":"stderr","time":"2019-04-30T02:12:41.8443515Z"}
    print(a)
# http://223.193.36.79:7140/loki/api/v1/query_range?query={job=%22netflow%22}?start=1700619300.0&end=1700619600.0&limit=1000000
# 103012
# 75806
