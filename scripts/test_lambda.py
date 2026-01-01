import requests

url = "https://2qv9hwa10j.execute-api.eu-west-2.amazonaws.com/prod?partition=1of31"
res = requests.get(url)
print(res.text)