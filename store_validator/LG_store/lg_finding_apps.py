import httpx
import pandas as pd
from time import sleep

# Base API URL
url = "https://us.lgappstv.com/api/tvapp/retrieveMoreAppList.ajax"

# Headers to mimic a browser
headers = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://us.lgappstv.com",
    "Referer": "https://us.lgappstv.com/main/tvapp",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
}

# Session to handle cookies automatically
with httpx.Client(http2=True, follow_redirects=True) as client:
    
    all_apps = []
    page = 1
    row_count = 50  # number of apps per page
    total_count = 1  # placeholder to enter the loop

    while len(all_apps) < total_count:
        data = {
            "catCode1": "",
            "moreYn": "Y",
            "orderType": "0",
            "appRankCode": "",
            "prodCode": "P000000030",
            "plfmCode": "W23P",
            "curPage": str(page),
            "rowCount": str(row_count),
            "pageCount": "10",
            "totalCount": str(total_count)
        }

        response = client.post(url, headers=headers, data=data)
        if response.status_code != 200:
            print(f"Failed at page {page}, status: {response.status_code}")
            break

        json_data = response.json()
        if page == 1:
            total_count = int(json_data.get("totalCount", 0))
            print(f"Total apps to retrieve: {total_count}")

        app_list = json_data.get("appList", [])
        if not app_list:
            break

        all_apps.extend(app_list)
        print(f"Retrieved page {page}, total apps so far: {len(all_apps)}")
        page += 1

        sleep(0.5)  # polite delay

# Convert to DataFrame
df = pd.DataFrame(all_apps)

# Save to CSV
df.to_csv("lg_all_apps.csv", index=False)
print(f"Saved {len(df)} apps to lg_all_apps.csv")
