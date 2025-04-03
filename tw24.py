from flask import Flask, jsonify
import requests
import json
from bs4 import BeautifulSoup
from datetime import datetime

def difference_time(timestamp):
    now = datetime.now().timestamp()
    difference = (now - int(float(timestamp))) / 3600
    if int(float(difference)) <= 0:
        return "0"
    else:
        return str(int(float(difference)))

def get_info():
    response = {}
    web_response = requests.get("https://trends24.in/france",headers={"user-agent":"Mozilla/5.0"})
    content_bs = BeautifulSoup(web_response.content,'html.parser')
    for div in content_bs.find_all('div',attrs={"class":"list-container"}):
        timestamp = div.find('h3',attrs={'class','title'})
        if timestamp != None:
            timestamp = timestamp['data-timestamp']
            dif = difference_time(timestamp)
            ol = div.find('ol',attrs={"class":"trend-card__list"})
            temp = {'trend':[]}
            for li in ol.find_all('li'):
                tn = li.find('span',attrs={"class":"trend-name"})
                trend_name = tn.find('a',attrs={'class':'trend-link'}).text
                trend_count = tn.find('span',attrs={'class':'tweet-count'})['data-count']
                temp['trend'].append({'name':trend_name,'count':trend_count})
            response[dif] = temp
    with open('./x_trends.json', 'w',encoding='utf-8') as f:
        json.dump(response,f, indent=2, ensure_ascii=False)
    return '/tmp/x_trends.json'

def json_to_csv():
    with open('./x_trends.json', 'r',encoding='utf-8') as f:
        data = json.load(f)
    with open('./x_trends.csv', 'w',encoding='utf-8') as f:
        f.write('timestamp;trend_name;trend_count\n')
        for timestamp in data:
            for trend in data[timestamp]['trend']:
                f.write(f'{timestamp};{trend["name"]};{trend["count"]}\n')

if __name__ == '__main__':
    get_info()
