import requests
import re
import datetime
import pandas as pd
import numpy as np
import os

from tqdm import tqdm
from time import sleep
import json


url_szse="http://bond.szse.cn/marketdata/statistics/report/trade/index.html"

headers_szse={
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Content-Type": "application/json",
    "Host": "bond.szse.cn",
    "Proxy-Connection": "keep-alive",
    "Referer": "http://bond.szse.cn/marketdata/statistics/report/trade/index.html",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "X-Request-Type": "ajax",
    "X-Requested-With": "XMLHttpRequest",
}

params_szse={
    "SHOWTYPE": "JSON",
    "CATALOGID": "zqxqjycyyb",
    "TABKEY": "tab1",
    "jyrqStart": "2023-01",
    "jyrqEnd": "2023-01",
    "random": 0.012453676957804527,
}

mapping_szse={
    "tzzlb": "投资者类别",
    "hjje": "合计金额",
    "hjzb": "合计占比",
    "gx": "国债",
    "dz": "地方政府债",
    "zj": "政策性金融债",
    "bj": "政府支持债券",
    "qx": "企业债券",
    "sz": "公司债券",
    "kz": "可转换债券",
    "hz": "可交换公司债券",
    "sm": "非公开发行公司债券",
    "js": "非公开发行可交换公司债券",
    "bc": "创新创业可转换债券",
    "zc": "证券公司次级债券",
    "zd": "证券公司短期债券",
    "zr": "企业资产支持证券",
    "rt": "不动产投资信托"
}

url_sse="http://query.sse.com.cn/commonQuery.do"

headers_sse={
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Cookie": "gdp_user_id=gioenc-402ee68d%2Ce4ac%2C51g2%2Ca1dd%2Ceb984dggg372; ba17301551dcbaf9_gdp_session_id=7a5e4515-18ef-491f-a108-b6b0f3100370; ba17301551dcbaf9_gdp_session_id_7a5e4515-18ef-491f-a108-b6b0f3100370=true",
    "Host": "query.sse.com.cn",
    "Referer": "http://bond.sse.com.cn/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
}

params_sse={
    "jsonCallBack": "jsonpCallback89433",
    "isPagination": "false",
    "sqlId": "COMMON_BOND_SCSJ_SCTJ_TJYB_XQJYYB_L",
    "TRADEDATE": "2023-1",
    "_": "1676809107894",
}

mapping_sse={
    "TYPE": "投资者类别",
    "TRADE_DATE": "报告期",
    "AMT": "合计金额",
    "AMT_PERCENT": "合计占比",
    "JZGZ_AMT": "记账式国债",
    "DFZ_AMT": "地方政府债券",
    "JRZ_AMT": "普通金融债",
    "QYZ_AMT": "企业债券",
    "SMZ_AMT": "中小企业私募债券",
    "GKGSZ_AMT": "公司债券",
    "FGKGSZ_AMT": "非公开发行公司债券",
    "KZZ_AMT": "可转换公司债券",
    "FLZ_AMT": "分离交易的可转换公司债券",
    "QYZC_AMT": "企业资产支持证券",
    "XDZC_AMT": "信贷资产支持证券",
    "BXCJZ_AMT": "可交换公司债券",
    "OTHER_AMT": "其他债券",
}


path_base=os.path.abspath("./")
path_data=os.path.join(path_base,"data")

if not os.path.exists(path_data):
    os.mkdir(path_data)

report_date=datetime.date(2023,1,31)

#更新月报数据的请求参数
params_szse.update({
    "jyrqStart":report_date.strftime("%Y-%m"),
    "jyrqEnd":report_date.strftime("%Y-%m"),
    "random": np.random.random(),
})

response_szse=requests.get(
    url=url_szse,
    headers=headers_szse,
    params=params_szse,
)

if response_szse.status_code==200:
    data_json_szse=json.loads(response_szse.text)
    data_szse=pd.DataFrame.from_dict(data_json_szse[0]["data"])
    data_szse=data_szse.reindex(columns=mapping_szse.keys())
    data_szse.rename(columns=mapping_szse,inplace=True)
    data_szse["报告期"]=report_date+pd.offsets.MonthEnd(0)
    data_szse.set_index(["投资者类别","报告期"], inplace=True)
    data_szse=data_szse.applymap(lambda x: x.replace(',','')).replace("",np.nan).astype(float)
    data_szse.to_csv(f"{path_data}/bond_trade_szse_{report_date.strftime('%Y%m%d')}.csv",encoding="GB18030")

params_sse.update({
    "jsonCallBack": f"jsonCallback{np.random.randint(10000,99999)}",
    "TRADEDATE": f"{report_date.year}-{report_date.month}",
    "_": int(datetime.datetime.now().timestamp()*1000),
})

response_sse=requests.get(
    url=url_sse,
    headers=headers_sse,
    params=params_sse,
)

if response_sse.status_code==200:
    data_raw_sse=re.findall("^.+\((\{.+\})\)$",response_sse.text)[0]
    data_json_sse=json.loads(data_raw_sse)
    data_sse=pd.DataFrame.from_dict(data_json_sse["result"])
    data_sse=data_sse.reindex(columns=mapping_sse.keys())
    data_sse.rename(columns=mapping_sse,inplace=True)
    data_sse["报告期"]=report_date+pd.offsets.MonthEnd(0)
    data_sse.set_index(["投资者类别","报告期"],inplace=True)
    data_sse=data_sse.astype(float)
    data_sse.to_csv(f"{path_data}/bond_trade_sse_{report_date.strftime('%Y%m%d')}.csv",encoding="GB18030")

report_dates=pd.date_range(datetime.date(2022,1,1),datetime.date(2023,1,31),freq="M")

pbar=tqdm(report_dates)
for loop_date in pbar:
    pbar.set_description(loop_date.strftime("%Y-%m-%d"))
    if not os.path.exists(f"{path_data}/bond_trade_szse_{loop_date.strftime('%Y%m%d')}.csv"):
        pass
    if not os.path.exists(f"{path_data}/bond_trade_sse_{loop_date.strftime('%Y%m%d')}.csv"):
        pass

    sleep(1)

