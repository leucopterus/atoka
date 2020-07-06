# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import os
from datetime import date

import openpyxl
from scrapy.exceptions import DropItem

from .items import (
    AtokaContactsItem,
    # AtokaPersonsInfoItem,
    AtokaErrorContactsItem,
)
from .settings import BASE_DIR


class ExcelOutputPipeline:
    def __init__(self):
        self.wb_path = os.path.join(BASE_DIR, 'atoka/spiders/output/output.xlsx')
        self.error_wb_path = os.path.join(BASE_DIR, 'atoka/spiders/output/error.xlsx')
        self.cod_fiscale_row_mapping = {}
        self.last_row_output = 1
        output = [
            'Cod. Fiscale',
            'company',
            # 'url',
            'Partita IVA',
            'Numero REA',
            'emails',
            'phones',
            'faxes',
            'websites',
            'wikipedia',
            'social',
            # 'people'
        ]
        self.number_of_elements = len(output)

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(output)
        wb.save(self.wb_path)
        wb.close()

    def process_item(self, item, spider):
        if isinstance(item, AtokaContactsItem):
            self._fill_excel_with_company_data(item)
            return item
        # elif isinstance(item, AtokaPersonsInfoItem):
        #     code = item.get('code')
        #     persons = self._sort_persons_info(item.get('people'))
        #     self._fill_excel_with_persons_data(code, persons)
        elif isinstance(item, AtokaErrorContactsItem):
            self._fill_error_excel_with_code_data(item)
        raise DropItem

    def _collect_items_from_list(self, objects, field, obj_type=None):
        if field == 'address':
            verified = [''.join(['(A) ', obj.get(f'{field}'), ' [', obj.get(f'{obj_type}'), ']']) for obj in objects if
                        obj.get('isVerified')]
            not_verified = [''.join([obj.get(f'{field}'), ' [', obj.get(f'{obj_type}'), ']']) for obj in objects if
                            not obj.get('isVerified')]
        else:
            verified = [' '.join(['(A)', obj.get(f'{field}')]) for obj in objects if
                        obj.get('isVerified')]
            not_verified = [obj.get(f'{field}')for obj in objects if not obj.get('isVerified')]
        return verified + not_verified

    def _collect_social_accounts(self, social):
        social_keys = ['facebookAccounts', 'twitterAccounts',
                       'otherAccounts', 'blogs']
        url_key = 'url'
        social_res = []
        for key, value in social.items():
            if key in social_keys:
                for item in value:
                    if item.get(url_key):
                        social_res.append(item.get(url_key))
        return social_res

    def _sort_persons_info(self, list_of_people=None):
        if list_of_people is None:
            return
        output = []
        for item in list_of_people:
            name_string = ''.join(['Name: ', item.get('fullName')])
            birthday = item.get('birthDate', '') or ''
            linkedin = item.get('linkedin', '')
            official = item.get('officialRoles', '')
            non_official = item.get('nonOfficialRoles', '')

            age = str(self._calculate_age(birthday))
            day_of_birth_string = ''.join(['Age: ', age, ', ', birthday]) if birthday else ''

            linkedin_string = linkedin

            official_roles = self._get_jobs_list(official, official=True)
            non_official_roles = self._get_jobs_list(non_official)

            official_roles.extend(non_official_roles)

            previous_work_string = ' | '.join(official_roles)
            sep = '---'
            data = [obj for obj in [name_string, day_of_birth_string, previous_work_string, linkedin_string, sep] if obj]
            person = '\n'.join(data)
            output.append(person)

        return '\n'.join(output)

    def _calculate_age(self, birthday=None):
        if isinstance(birthday, str) and birthday:
            today = date.today()
            day_of_birth = date.fromisoformat(birthday)
            age = today.year - day_of_birth.year
            return age if today.month > day_of_birth.month or (
                        today.month == day_of_birth.month and today.day >= day_of_birth.day) else age - 1
        return ''

    def _get_jobs_list(self, jobs, official=False):
        output = []
        for job in jobs:
            name = job.get('name', '') or ''
            since = job.get('since', '') or ''
            if name:
                if official:
                    output_data = ''.join(['(A) ', name, ' (', since, ')']) if since else ''.join(['(A) ', name])
                    output.append(output_data)
                else:
                    output_data = ''.join([name, ' (', since, ')']) if since else name
                    output.append(output_data)
        return output

    def _fill_excel_with_company_data(self, data=None):
        if data is None:
            return

        code = data.get('code')
        company_name = data.get('company_name')
        # url = data.get('url')
        vat_id = data.get('vat_id')
        numero_rea = data.get('numero_rea')
        all_emails = self._collect_items_from_list(data.get('emails'), 'address', 'type')
        all_phones = self._collect_items_from_list(data.get('phones'), 'number')
        all_faxes = data.get('faxes')
        all_websites = self._collect_items_from_list(data.get('websites'), 'url')
        all_social = self._collect_social_accounts(data.get('social'))
        wikipedia = data.get('wikipedia')
        emails = '\n'.join(all_emails)
        phones = '\n'.join(all_phones)
        faxes = '\n'.join(all_faxes)
        websites = '\n'.join(all_websites)
        social = '\n'.join(all_social)
        output = [code, company_name, vat_id, numero_rea,
                  emails, phones, faxes, websites, wikipedia,
                  social]

        wb = openpyxl.load_workbook(self.wb_path)
        ws = wb.active
        if code not in self.cod_fiscale_row_mapping:
            self.last_row_output += 1
            # self.cod_fiscale_row_mapping[code] = self.last_row_output
            ws.append(output)
        else:
            for col_number, item in enumerate(output, start=1):
                ws.cell(row=self.cod_fiscale_row_mapping[code], column=col_number).value = item
            del self.cod_fiscale_row_mapping[code]
        wb.save(self.wb_path)
        wb.close()

    def _fill_excel_with_persons_data(self, code, persons):
        person_column_number = self.number_of_elements
        wb = openpyxl.load_workbook(self.wb_path)
        ws = wb.active
        if code not in self.cod_fiscale_row_mapping:
            self.last_row_output += 1
            self.cod_fiscale_row_mapping[code] = self.last_row_output
            ws.cell(row=self.cod_fiscale_row_mapping[code], column=person_column_number).value = persons
        else:
            ws.cell(row=self.cod_fiscale_row_mapping[code], column=person_column_number).value = persons
            del self.cod_fiscale_row_mapping[code]
        wb.save(self.wb_path)
        wb.close()

    def _fill_error_excel_with_code_data(self, item):
        if not os.path.isfile(self.error_wb_path):
            wb = openpyxl.Workbook()
        else:
            wb = openpyxl.load_workbook(self.error_wb_path)
        ws = wb.active
        ws.append([item.get('code'), item.get('reason')])
        wb.save(self.error_wb_path)
        wb.close()
