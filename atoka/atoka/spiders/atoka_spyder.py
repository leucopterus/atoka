import gc
import json
import os
import time
from copy import deepcopy
from random import random

import openpyxl
import scrapy

from ..items import (
    AtokaContactsItem,
    AtokaErrorContactsItem,
)
from ..settings import DEFAULT_REQUEST_HEADERS, BASE_DIR


class AtokaSpider(scrapy.Spider):
    name = 'atoka'

    domain = 'https://atoka.io/it/'

    query_url = 'https://atoka.io/api/finder/get_quid_for_companies/?name={0}'
    init_url = 'https://atoka.io/api/companysearch/init/'
    facet_url = 'https://atoka.io/api/companysearch/facet/'
    facet_body = {
        'version': '3.1.0',
        'facetsConfig': {
            'name': {
                'value': '{code}'
            }
        },
        'includeFacets': []
    }

    search_url = 'https://atoka.io/api/companysearch/search/'
    search_body = {
        'version': '3.1.0',
        'meta': {},
        'facetsConfig': {
            'name': {
                'value': '{code}'
            }
        }
    }

    contacts_url = 'https://atoka.io/api/company-details/companies/{uid}/tab-contents/'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = os.path.join(BASE_DIR, 'atoka/spiders/input/input.xlsx')
        self.max_objects_in_search = 3

        self.buffer = {}
        self.code_main_company = {}
        self.code_elements = {}

        self.input_row_number = 2
        self.input_code_fiscale = self._get_companies_cod_fiscale_from_excel(self.input_row_number)
        self.input_row_number += 1

    def start_requests(self):
        query_url = self.query_url.format(self.input_code_fiscale)
        yield scrapy.Request(
            url=query_url,
            method='GET',
            headers=DEFAULT_REQUEST_HEADERS,
            encoding='utf-8',
            callback=self.parse_query_response,
            cb_kwargs={'code': self.input_code_fiscale, 'query_url': query_url},
            dont_filter=True,
        )

    def parse_query_response(self, response, code, query_url):
        if response.url == query_url:
            self.logger.info(f'SUCCESSFULLY PARSED: {response.url}')
            query_init_body = json.loads(response.text)
            yield scrapy.Request(
                url=self.init_url,
                method='POST',
                body=json.dumps(query_init_body),
                headers=DEFAULT_REQUEST_HEADERS,
                encoding='utf-8',
                callback=self.parse_init_response,
                cb_kwargs={'code': code},
                dont_filter=True,
            )

    def parse_init_response(self, response, code):
        if response.url == self.init_url:
            self.logger.info(f'SUCCESSFULLY PARSED: {response.url}')
            facet_body = deepcopy(self.facet_body)
            facet_body['facetsConfig']['name']['value'] = facet_body['facetsConfig']['name']['value'].format(code=code)
            yield scrapy.Request(
                url=self.facet_url,
                method='POST',
                body=json.dumps(facet_body),
                headers=DEFAULT_REQUEST_HEADERS,
                encoding='utf-8',
                callback=self.parse_facet_response,
                cb_kwargs={'code': code},
                dont_filter=True,
            )

    def parse_facet_response(self, response, code):
        if response.url == self.facet_url:
            self.logger.info(f'SUCCESSFULLY PARSED: {response.url}')
            body = deepcopy(self.search_body)
            body['facetsConfig']['name']['value'] = body['facetsConfig']['name']['value'].format(code=code)
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
        number_of_search_data = response_json_obj.get('meta').get('total')

        if number_of_search_data == 0:
            yield AtokaErrorContactsItem(
                code=self.input_code_fiscale,
                reason='not found',
            )
        elif number_of_search_data <= self.max_objects_in_search:
            if number_of_search_data != 1:
                yield AtokaErrorContactsItem(
                    code=self.input_code_fiscale,
                    reason=f'results were handled: {number_of_search_data}',
                )
            if response_company_data:
                self.code_main_company[self.input_code_fiscale] = response_company_data[0].get('id')
                self.code_elements[self.input_code_fiscale] = [item.get('id') for item in response_company_data]
                for item in response_company_data:
                    company_uid = item.get('id')
                    contacts_url = self.contacts_url.format(uid=company_uid)

                    yield scrapy.Request(
                        url=contacts_url,
                        method='GET',
                        headers=DEFAULT_REQUEST_HEADERS,
                        encoding='utf-8',
                        callback=self.parse_contacts,
                        cb_kwargs={
                            'code': self.input_code_fiscale,
                            'company_uid': company_uid,
                        },
                        dont_filter=True,
                    )
                    self._controller_sleep()
        else:
            yield AtokaErrorContactsItem(
                code=self.input_code_fiscale,
                reason=f'too many results: {number_of_search_data}',
            )
        gc.collect()

        self.input_code_fiscale = self._get_companies_cod_fiscale_from_excel(self.input_row_number)
        if self.input_code_fiscale:
            self.input_row_number += 1
            query_url = self.query_url.format(self.input_code_fiscale)
            yield scrapy.Request(
                url=query_url,
                method='GET',
                headers=DEFAULT_REQUEST_HEADERS,
                encoding='utf-8',
                callback=self.parse_query_response,
                cb_kwargs={'code': self.input_code_fiscale, 'query_url': query_url},
                dont_filter=True,
            )

    def parse_contacts(self, response, code, company_uid):
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

            if self.code_main_company[code] == company_uid and len(self.code_elements[code]) == 1 \
                    and self.buffer.get(code) is None:
                del self.code_main_company[code]
                del self.code_elements[code]
                yield instance

            else:
                if self.buffer.get(code) is None:
                    self.buffer[code] = []
                del self.code_elements[code][self.code_elements[code].index(company_uid)]
                if company_uid == self.code_main_company[code]:
                    self.buffer[code].insert(0, instance)
                else:
                    self.buffer[code].append(instance)
                if not self.code_elements[code]:
                    output = self.buffer[code][0]
                    for item in self.buffer[code][1:]:
                        output = output + item
                    del self.buffer[code]
                    del self.code_elements[code]
                    del self.code_main_company[code]
                    yield output

        gc.collect()

    def _get_companies_cod_fiscale_from_excel(self, row_number=None):
        if os.path.isfile(self.file_path):
            if row_number is not None:
                wb = openpyxl.load_workbook(self.file_path, read_only=True)
                ws = wb.active
                cod_fiscale = ws.cell(row=row_number, column=1).value
                wb.close()
                return cod_fiscale

    def _controller_sleep(self, seconds=10):
        self.crawler.engine.pause()
        time.sleep(15 + random() * seconds)
        self.crawler.engine.unpause()
