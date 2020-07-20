import gc
import json
import time
from copy import deepcopy
from random import random

import scrapy

from ..items import (
    AtokaContactsItem,
)
from ..settings import DEFAULT_REQUEST_HEADERS


class AtokaSpider(scrapy.Spider):
    name = 'atoka'

    domain = 'https://atoka.io/it/'

    init_url = 'https://atoka.io/api/companysearch/init/'
    init_body = {'quid': 'f34202d7a70392b5ae6d76b5ece852'}

    facet_url = 'https://atoka.io/api/companysearch/facet/'
    facet_body = {
        'version': '3.1.0',
        'facetsConfig': {
            'email': {
                'isCollapsed': False,
                'isActive': True
            },
            'hasPhone': {
                'mode': 'include'
            },
            'hasWebsite': {
                'mode': 'include'
            }
        },
        'includeFacets': []
    }

    search_url = 'https://atoka.io/api/companysearch/search/'
    search_body = {
        'version': '3.1.0',
        'meta': {},
        'facetsConfig': {
            'email': {
                'isCollapsed': False,
                'isActive': True
            },
            'hasPhone': {
                'mode': 'include'
            },
            'hasWebsite': {
                'mode': 'include'
            }
        }
    }

    contacts_url = 'https://atoka.io/api/company-details/companies/{uid}/tab-contents/'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.step = 10
        self.companies_amount = 5
        self.multiply = 0
        self.buffer = {}
        self.code_main_company = {}
        self.code_elements = {}

    def start_requests(self):
        yield scrapy.Request(
            url=self.init_url,
            method='POST',
            body=json.dumps(self.init_body),
            headers=DEFAULT_REQUEST_HEADERS,
            encoding='utf-8',
            callback=self.parse_init_response,
        )

    def parse_init_response(self, response):
        if response.url == self.init_url:
            self.logger.info(f'SUCCESSFULLY PARSED: {response.url}')
            yield scrapy.Request(
                url=self.facet_url,
                method='POST',
                body=json.dumps(self.facet_body),
                headers=DEFAULT_REQUEST_HEADERS,
                encoding='utf-8',
                callback=self.parse_facet_response,
                dont_filter=True,
            )

    def parse_facet_response(self, response):
        if response.url == self.facet_url:
            self.logger.info(f'SUCCESSFULLY PARSED: {response.url}')
            body = deepcopy(self.search_body)
            yield scrapy.Request(
                url=self.search_url,
                method='POST',
                body=json.dumps(body),
                headers=DEFAULT_REQUEST_HEADERS,
                encoding='utf-8',
                callback=self.parse,
                dont_filter=True,
            )

    def parse(self, response):
        response_json_obj = json.loads(str(response.text))
        response_company_data = response_json_obj.get('data')

        contacts_urls = []
        for item in response_company_data:
            company_uid = item.get('id')
            contacts_urls.append(self.contacts_url.format(uid=company_uid))

        yield from response.follow_all(
            urls=contacts_urls,
            method='GET',
            headers=DEFAULT_REQUEST_HEADERS,
            encoding='utf-8',
            callback=self.parse_contacts
        )
        self._controller_sleep(15)

        gc.collect()

        self.multiply += 1
        start_value_to_search_from = self.multiply*self.step
        if start_value_to_search_from < self.companies_amount:
            search_body = deepcopy(self.search_body)
            search_body['meta'] = {'start': start_value_to_search_from}
            yield scrapy.Request(
                url=self.search_url,
                method='POST',
                body=json.dumps(search_body),
                headers=DEFAULT_REQUEST_HEADERS,
                encoding='utf-8',
                callback=self.parse,
                dont_filter=True,
            )
            self._controller_sleep(5)

    def parse_contacts(self, response):
        if 'tab-contents' in response.url:
            response_json_obj = json.loads(response.text)
            overview = response_json_obj.get('overview')
            contacts = response_json_obj.get('contacts')

            cod_fiscale = overview.get('taxId') or ''
            vat_id = overview.get('vatId') or ''
            company_name = overview.get('legalName') or ''

            emails = contacts.get('emails')
            phones = contacts.get('phones')
            websites = contacts.get('websites')

            instance = AtokaContactsItem(
                code=cod_fiscale,
                company_name=company_name,
                vat_id=vat_id,
                emails=emails,
                phones=phones,
                websites=websites,
            )
            yield instance

    def _controller_sleep(self, seconds=2):
        self.crawler.engine.pause()
        time.sleep(random() * seconds)
        self.crawler.engine.unpause()
