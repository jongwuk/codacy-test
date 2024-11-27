import json
import boto3
import requests
import logging
import threading
from bs4 import BeautifulSoup
import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(levelname)s] %(asctime)s %(message)s')

WORKSHEET_ID = '10VFUxbA9X43Min123S1IDdbZ6p2Ln_5E5IDhnVV6xp4'
credentials_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])

#lambda 클라이언트 초기화
lambda_client = boto3.client('lambda')

# log를 console에 출력
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def make_work_sheet(purchase_request_url,product_amount,response_url):
    scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
    ]
    
    #슬랙 봇으로 받아와야하는 데이터
    product_category = "기타"
    url = purchase_request_url
    product_quantity = product_amount
    product_title = ''
    total_price = ''
    
    # 상품 제목,가격 크롤링
    crawled_product_title, crawled_total_price = get_product_info(url,response_url)
    response = requests.post(response_url, json={'response_type':'in_channel','text':'간식구매 요청 작업 진행중 입니다...'}, headers={'Content-Type':'application/json'})
    logger.info(f'response status {response.status_code}')
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_json)
    client = gspread.authorize(creds)
    
    
    
    
    
    logger.info("Google Sheets API has been authorized.")
    # 상품 구매 요청 링크
    
    spreadsheet = client.open_by_key(WORKSHEET_ID)
    worksheet = spreadsheet.worksheet('간식')
    logger.info(f"Found Worksheet {worksheet}")
    requests.post(response_url, json={'response_type':'in_channel','text':'간식구매 요청 작업 진행중 입니다...'}, headers={'Content-Type':'application/json'})
    
    
    
    requests.post(response_url, json={'response_type':'in_channel','text':'간식구매 요청 작업 진행중 입니다...'}, headers={'Content-Type':'application/json'})
    
    product_title = crawled_product_title
    total_price = crawled_total_price
    data = [
        [
            product_category,
            product_title,
            url,
            total_price,
            product_quantity,
        ]
    ]
    
    all_data = worksheet.get_all_values()
    for i in range(len(all_data) - 1, -1, -1):
        ROW_C = all_data[i][2]
        if any(ROW_C):
            last_row_with_data = i + 1
            break
    else:
        last_row_with_data = 0

    next_row_number = last_row_with_data + 1
    range_to_update = f"B{next_row_number}:F{next_row_number}"
    
    worksheet.update(range_name=range_to_update, values=data)
    requests.post(response_url, json={'response_type':'in_channel','text':'간식구매 요청 성공!!'}, headers={'Content-Type':'application/json'})
    logger.info('====== worksheet update success!========')

def get_product_info(url,response_url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    product_title = ''
    total_price = ''
    soup = BeautifulSoup(response.text, "html.parser")
    requests.post(response_url, json={'response_type':'in_channel','text':'간식구매 요청 작업 진행중 입니다...'}, headers={'Content-Type':'application/json'})
    product_title_element = soup.find(class_="prod-buy-header__title")
    total_price_element = soup.find(class_="total-price")
    if soup:
        product_title = product_title_element.get_text() if product_title_element else "제목을 찾을 수 없음"
        total_price = total_price_element.get_text() if total_price_element else "총 가격을 찾을 수 없음"
    if total_price:
        total_price = total_price.replace(",", "").replace("원", "").strip()
    logger.info("Web crawling completed. Product: %s, Price: %s", product_title, total_price)
    return product_title, total_price
    
def find_nested_key(d, key):
    if key in d:
        return d[key]
    for k, v in d.items():
        if isinstance(v, dict):
            found = find_nested_key(v, key)
            if found is not None:
                return found
    return None
    

def lambda_handler(event,context):
    #TODO implement
    
    logger.info("Event: %s", event)
    # 기본값을 설정하거나 존재 여부를 확인하여 KeyError 방지
    try:
        if event.get('text') is None:
            option = find_nested_key(event, 'text')
            command = find_nested_key(event, 'command')
            response_url = find_nested_key(event, 'response_url')
        else:
            option = event.get('text')
            command = event.get('command')
            response_url = event.get('response_url')
        if not option or not command or not response_url:
            raise KeyError('One or more required keys are missing in the event data')
    except KeyError as e:
        logger.error(f'Missing key in event data: {str(e)}')
        return {
            'response_type': 'ephemeral',
            'text': f'Missing key in event data: {str(e)}',
        }
    except Error as e:
        logger.error(f'예외 발생 원인 : {str(e)}')

    try:
        purchase_request_url, purchase_amount = option.split(' ', 1)
    except ValueError as e:
        logger.error(f'Invalid option format: {str(e)}')
        return {
            'response_type': 'ephemeral',
            'text': 'Invalid option format. Please provide a URL and amount separated by a space.',
        }

    message = '간식구매 요청을 시작합니다.'
    function_name = "update-spread-sheet"
    payload = json.dumps(event)
    
    logger.info(f'purchase_request_url: {purchase_request_url}, purchase_amount: {purchase_amount}, response_url: {response_url}')
    
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=payload
        )
        
        response_payload = response['Payload'].read()
        logger.info(f"Response from second function: {response_payload}")
        
    except Exception as e:
        logger.error(f'에러가 발생했습니다. 원인: {str(e)}')
        message = f'에러가 발생했습니다. 원인: {str(e)}'
    
    return {
        'response_type': 'in_channel',
        'text': message,
    }
    
    