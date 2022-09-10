import requests
import json

# headers = {
#     'X-Nickname': 'a-v.shapowal',
#     'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',
#     'X-Cohort': '3'
# }

# r = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants', headers=headers)
# restaurants = json.loads(r.text)
# for rest in restaurants:
#     print(rest['_id'], rest['name'])

headers = {
    'X-Nickname': 'a-v.shapowal',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',
    'X-Cohort': '3'
}

url = f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?offset=0"
r = requests.get(url, headers=headers)
print(r.text)
