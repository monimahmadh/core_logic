import csv
import copy
import os
0
# from selenium.webdriver.chrome.service import Service
# from selenium import webdriver
import scrapy
import requests,json,time,threading,queue,os
# from webdriver_manager.chrome import ChromeDriverManager
# from webdriver_manager.firefox import GeckoDriverManager
from datetime import datetime
# from selenium.webdriver.common.by import By

# import undetected_chromedriver as uc
class LogicDataSpider(scrapy.Spider):
    name = "logic_data"
    # cookies={}
    cookies = {
    'visid_incap_2613123': 'TE/hxFgESzWc3Pn0OBjGJghq9GYAAAAAQUIPAAAAAACe/ZupH1LAGBtGLO3eiN3L',
    'ajs_user_id': 'gebethner',
    'ajs_anonymous_id': 'f9df1443-88af-42e6-8343-f19549261025',
    'visid_incap_2595776': '8XoaLyUkSF2Rt0iwTOM3miNt9GYAAAAAQUIPAAAAAABjMFh8ww3R1j3BMVxJwHdm',
    '_fbp': 'fb.2.1727294758818.811572330933344473',
    '__zlcmid': '1Nvn2zxJAL2RtAp',
    '_conv_r': 's%3Awww.google.com*m%3Aorganic*t%3A*c%3A',
    '_gcl_au': '1.1.1738887025.1727553555',
    '__utmzz': 'utmcsr=google|utmcmd=organic|utmccn=(not set)|sfID=7018w0000009UnCAAU|crmID=|utmctr=(not provided)',
    '_hjSessionUser_2879853': 'eyJpZCI6IjlhN2MxZThlLTUwYzktNWY0OC1iOTNkLWNiZjU2YzcxZGJmNyIsImNyZWF0ZWQiOjE3Mjc1NTM1NjM3NjQsImV4aXN0aW5nIjp0cnVlfQ==',
    '_conv_v': 'vi%3A1*sc%3A2*cs%3A1727598886*fs%3A1727553551*pv%3A3*exp%3A%7B100455401.%7Bv.1004138237-g.%7B100417280.1-100417281.1-100439803.1-100444439.1%7D%7D-100456010.%7Bv.1004139642-g.%7B100417280.1-100417281.1-100439803.1-100444439.1%7D%7D-100462971.%7Bv.1004154977-g.%7B100417280.1-100417281.1-100439803.1-100444439.1%7D%7D%7D*ps%3A1727553551',
    '_ga_610QKV2T7G': 'GS1.3.1727598906.2.1.1727599070.60.0.0',
    '_ga': 'GA1.1.2005574307.1727553556',
    '_ga_9Y2TJ80HY8': 'GS1.1.1727598898.2.1.1727599124.60.0.0',
    '_ga_EDZWFYVBP8': 'GS1.1.1727598899.2.1.1727599124.60.0.0',
    '_ga_SDRJGMEL01': 'GS1.1.1727598900.2.1.1727599124.60.0.0',
    '_ga_W3HVTE2YXF': 'GS1.1.1727598910.2.1.1727599124.0.0.0',
    'nlbi_2613123': '/CC/ft36wSv4H6p5X7nQvQAAAAB99wdTLm1oiy+STB1ftEtQ',
    'incap_ses_974_2613123': 'v1LWRYwXll6PFIXUB1iEDTbm/GYAAAAADPEtnKOMlEYq30NSJzXABA==',
    'APP2SESSION-XSRF': 'b64fbe64-5671-4bfc-aa81-42b238d7dae9',
    'APP2SESSION': 'ZGM0ZWIxMzctNTYwZS00ZDg2LWFhZjAtODdjY2RlNTY3NmEy',
    '_clck': '1ycynb5%7C2%7Cfpo%7C0%7C1729',
    '_clsk': 'efowmq%7C1727850060984%7C1%7C1%7Cu.clarity.ms%2Fcollect',
}

    headers = {
    'ADRUM': 'isAjax:true',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json',
    'Origin': 'https://rpp.corelogic.com.au',
    'Referer': 'https://rpp.corelogic.com.au/search/multi',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'X-XSRF-TOKEN': 'b64fbe64-5671-4bfc-aa81-42b238d7dae9',
    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}
    #

    #payloads for dynamic content

    json_data = {
        'requests': [
            {
                'resultsFormat': {
                    'offset': 1,
                    'limit': 100,
                    'metadata': False,
                    'sort': '+address',
                },
                'filters': [
                    {
                        'operation': 'and',
                        'filters': [
                            {
                                'field': 'addressSuburbStatePostcode',
                                'operation': 'equals',
                                'value': 'Oatley NSW 2223',
                            },
                        ],
                    },
                ],
            },
        ],
    }
    def read_input_file(self):
        file_path = os.path.join('input', 'suburb_output.csv')
        with open(file_path, 'r') as file:
            file_data=list(csv.DictReader(file))
            return file_data


    detail_page_list=[]
    custom_settings = {
        'FEED_URI': f'outputs/Core_logic_Data{datetime.now().strftime("%d_%b_%Y_%H_%M_%S")}.json',
        'FEED_FORMAT': 'json',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'RETRY_TIMES': 5,
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 0.1,
        'REDIRECT_ENABLED': False,
        'HTTPERROR_ALLOW_ALL':True

    }

    def start_requests(self):
        yield scrapy.Request(url="https://quotes.toscrape.com/", callback=self.parse)
    def parse(self, response):
        file_data = self.read_input_file()
        for data in file_data:
            full_suburb= data.get('full_suburb','').replace(',','').strip()
            url='https://rpp.corelogic.com.au/api/batch/search?clAppAccountUserGuid=f606eaec-bb49-43e7-83de-61c23c7e8642'
            offset= 0
            payloads= copy.deepcopy(self.json_data)
            payloads['requests'][0]['resultsFormat']['offset']= offset
            payloads['requests'][0]['filters'][0]['filters'][0]['value'] = str(full_suburb)
            yield scrapy.Request(url=url, headers=self.headers,callback=self.detail_page, method='POST',
                                 body=json.dumps(payloads),cookies=self.cookies,
                                meta={'offset':offset, 'payloads':payloads,'full_suburb':full_suburb})


    def detail_page(self, response):
        print("in detail_page")
        print(response.status)
        status = response.meta.get('status', '')
        offset = response.meta.get('offset', '')
        payloads = response.meta.get('payloads', '')
        full_suburb = response.meta.get('full_suburb', '')
        api_data = json.loads(response.text)
        totalPages = ''
        meta_Data = api_data.get('metadata', [])
        for meta in meta_Data:
            totalPages = meta.get('totalPages', '')
        data=api_data.get('data',[])
        for each_data in data:
            item = dict()
            item['complete_address']=each_data.get('addressComplete','')
            address = {
                'addressUnitNumber': each_data.get('addressUnitNumber', ''),
                'addressStreetNumber': each_data.get('addressStreetNumber', ''),
                'addressStreetName': each_data.get('addressStreetName', ''),
                'addressStreetExtension': each_data.get('addressStreetExtension', ''),
                'addressSuburb': each_data.get('addressSuburb', ''),
                'addressState': each_data.get('addressState', ''),
                'addressPostcode': each_data.get('addressPostcode', ''),
                'addressStreetStartAlphaReferenceNumber': each_data.get('addressStreetStartAlphaReferenceNumber', '')
            }
            item['address'] = address
            addressLocation= each_data.get('addressLocation',{})
            item['latitude']= addressLocation.get('lat','')
            item['longitude']= addressLocation.get('lon','')
            item['listed_Price']= each_data.get('salesLastCampaignLastListedPriceDescription','')
            item['listing Date']= each_data.get('salesLastCampaignEndDate','')
            item['listing Type'] = each_data.get('salesLastCampaignListedType', '')
            item['rental_price']= each_data.get('rentalLastCampaignFirstListedPriceDescription','')
            item['rental_date']= each_data.get('rentalLastCampaignEndDate','')
            item['yearBuilt'] = each_data.get('yearBuilt', '')
            address=each_data.get('addressComplete','')
            item['status']= status

            new_address= address.lower().replace(' ','-').replace('/','-')
            item['id']=each_data.get('id','')
            id=each_data.get('id','')
            item['url']= f'https://rpp.corelogic.com.au/property/{new_address}/{id}'
            item['beds']=each_data.get('beds','')
            item['baths']=each_data.get('baths','')
            item['carSpaces']=each_data.get('carSpaces','')
            item['landArea']=each_data.get('landArea','')
            item['floorArea']=each_data.get('floorArea','')
            item['Agent']=each_data.get('salesLastCampaignAgent','')
            item['Agency']=each_data.get('salesLastCampaignAgency','')
            # description api
            url=f'https://rpp.corelogic.com.au/api/properties/au/v1/property/{id}/advertisements.json?includeAdvertisementDescription=true'
            response = requests.request("GET", url, headers=self.headers, cookies=self.cookies)
            description =''
            try:
                description_api=json.loads(response.text)
                advertiserList= description_api.get('advertisementList',[])
                for row in advertiserList:
                    description=row.get('advertisementDescription','').strip()
                item['Description']= description
            except Exception as e:
                print(e)
            # images api
            url_photos=f'https://rpp.corelogic.com.au/api/properties/{id}/photos'
            response_photos = requests.request("GET", url_photos, headers=self.headers, cookies=self.cookies)
            if response_photos:
                images= json.loads(response_photos.text)
                complete_images=[]
                for image in images:
                    basePhotoUrl=image.get('basePhotoUrl','')
                    complete_images.append(basePhotoUrl)
                item['images']= complete_images

            # target_property_deails
            sold_sale_deatils= f'https://rpp.corelogic.com.au/api/properties/{id}/targetPropertyDetail'
            sold_sale_response= requests.request("GET", sold_sale_deatils, headers=self.headers, cookies=self.cookies)
            try:
                sold_sale_response_data = json.loads(sold_sale_response.text)
                lastSale= sold_sale_response_data.get('lastSale')
                if lastSale:
                    item['sold_price']= lastSale.get('price','')
                    item['sold_date']= lastSale.get('contractDate','')
                    saleCampaign= sold_sale_response_data.get('saleCampaign',{})
                    item['salePrice']= saleCampaign.get('firstPublishedPrice','')
                    item['saleDate']= saleCampaign.get('saleDate','')
                    item['daysOnMarket']= saleCampaign.get('daysOnMarket','')
            except Exception as e:
                print(e)

            # householdInformation
            household = f'https://rpp.corelogic.com.au/api/properties/{id}/householdInformation?includeCommons=false'
            response_household= requests.request("GET", household, headers=self.headers, cookies=self.cookies)
            try:
                api_data_household = json.loads(response_household.text)
                owner_complete_name = []
                owner_complete_address = []
                sales = api_data_household.get('sales', {})
                currentOwnershipList = sales.get('currentOwnershipList', [])
                for row in currentOwnershipList:
                    person = row.get('person', {})
                    firstName = person.get('firstName', '').title()
                    lastName = person.get('lastName', '').title()
                    full_name = f'{firstName} {lastName}'
                    owner_complete_name.append(full_name)
                    mailingAddress = row.get('mailingAddress', {})
                    line1 = mailingAddress.get('line1', '').title()
                    line2 = mailingAddress.get('line2', '').title()
                    postcode = mailingAddress.get('postcode', '').title()
                    full_address = f'{line1} {line2} {postcode}'
                    owner_complete_address.append(full_address)
                item['owner_name'] = owner_complete_name
                item['owner_address'] = owner_complete_address
                item['occupancy_type'] = api_data_household.get('occupancyDetails', {}).get('occupancyType', '')
            except Exception as e:
                print(e)
            # yield item
            if item['url'] not in self.detail_page_list:
                self.detail_page_list.append(item['url'])
                yield item
            else:
                print("already exist", item['url'])

        # pagination
        if offset<=totalPages:
            offset = offset+1
            payloads['requests'][0]['resultsFormat']['offset'] = offset
            url = 'https://rpp.corelogic.com.au/api/batch/search?clAppAccountUserGuid=f606eaec-bb49-43e7-83de-61c23c7e8642'
            yield scrapy.Request(url=url, headers=self.headers, callback=self.detail_page, method='POST',
                                 body=json.dumps(payloads), cookies=self.cookies,
                                 meta={'offset': offset, 'payloads': payloads,'status':status})























