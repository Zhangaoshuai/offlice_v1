import logging
from datetime import datetime
from typing import Optional,List,Dict,Any

from lxml import html
from lxml import etree
from stream_py.utils import ChromeDebugControllerUtils



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spider_amap_weather.log'),
        logging.StreamHandler()
    ]
)
logging = logging.getLogger(__name__)

local_debug_dir = 'D:\dev_env\Chrome_debug_dir'
local_chrome_debug_port = 9222

chrome_debug_controller = ChromeDebugControllerUtils.ChromeDebugController(
    port=local_chrome_debug_port,
    user_data_dir=local_debug_dir)

def start_chrome_driver():
    chrome_debug_controller.start()
    return chrome_debug_controller.connect_drissionpage()

drissionpage = start_chrome_driver()

drissionpage.get('https://www.chinazy.org/zcfg.htm')
drissionpage.wait(1,2)

print(drissionpage.html)



# headers = {
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#     'Accept-Language': 'zh,zh-CN;q=0.9',
#     'Cache-Control': 'max-age=0',
#     'Connection': 'keep-alive',
#     'Referer':'https://www.chinazy.org/zcfg/70.htm',
#     'Sec-Fetch-Dest': 'document',
#     'Sec-Fetch-Mode': 'navigate',
#     'Sec-Fetch-Site': 'none',
#     'Sec-Fetch-User': '?1',
#     'Upgrade-Insecure-Requests': '1',
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
#     'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
#     'sec-ch-ua-mobile': '?0',
#     'sec-ch-ua-platform': '"Windows"'
# }
#
# response = requests.get('https://www.chinazy.org/zcfg.htm',  headers=headers)
# if response.status_code==200:
#     print(response.text)

