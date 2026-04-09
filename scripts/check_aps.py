"""Quick check: what DocumentLines does APS 1376 have in SAP?"""
import json
import urllib.request
import ssl

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def sap_req(url, method="GET", data=None, cookie=None):
    headers = {"Content-Type": "application/json"}
    if cookie:
        headers["Cookie"] = cookie
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    resp = urllib.request.urlopen(req, context=ctx, timeout=30)
    return json.loads(resp.read().decode())

# Login to SBOCIMU
login = sap_req("https://10.1.0.9:50000/b1s/v1/Login", "POST", {
    "CompanyDB": "SBOCIMU", "UserName": "manager", "Password": "Grup0$1Mu$"
})
cookie = f"B1SESSION={login['SessionId']}"
print("Logged in to SBOCIMU\n")

# Get APS 1376
url = (
    "https://10.1.0.9:50000/b1s/v1/Orders"
    "?$filter=DocNum%20ge%201376%20and%20DocNum%20le%201376"
    "&$select=DocEntry,DocNum,CardCode,CardName,DocumentLines"
)
order = sap_req(url, cookie=cookie)
o = order["value"][0]
print(f"APS {o['DocNum']} - {o['CardName']} (DocEntry: {o['DocEntry']})")
print(f"Total DocumentLines: {len(o['DocumentLines'])}\n")

for i, line in enumerate(o["DocumentLines"]):
    ic = line.get("ItemCode", "")
    desc = line.get("ItemDescription", "")
    price = line.get("Price", 0)
    qty = line.get("Quantity", 0)
    print(f"  Line {i:2d}: {ic:15s} | {desc:50s} | Price={price} | Qty={qty}")
