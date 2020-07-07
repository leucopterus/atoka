# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AtokaContactsItem(scrapy.Item):
    company_name = scrapy.item.Field(serializer=str)
    code = scrapy.item.Field(serializer=str)
    vat_id = scrapy.item.Field(serializer=str)
    emails = scrapy.item.Field()
    phones = scrapy.item.Field()
    websites = scrapy.item.Field()

    def __add__(self, other):
        data_field_mapping = {
            'emails': 'address',
            'phones': 'number',
            'websites': 'url',
        }
        if isinstance(other, self.__class__):
            for field in self.fields:
                if other[field]:
                    if isinstance(self[field], str):
                        if other[field] not in self[field]:
                            self[field] = self[field] + ' / ' + other[field]
                    elif isinstance(self[field], list):
                        self._add_list_items(other[field], field, data_field_mapping[field])
                    elif isinstance(self[field], dict):
                        self._add_dict_items(other[field], field, data_field_mapping[field])
        return self

    def _add_list_items(self, objects, main_field, field=None):
        for obj in objects:
            if field is not None:
                obj[field] = obj[field] + ' (O)' if obj.get(field) else ''
                if obj[field]:
                    self[main_field].append(obj)
            else:
                self[main_field].append(''.join([obj, ' (O)']))

    def _add_dict_items(self, objects, main_field, field):
        for key, value in objects.items():
            if value and isinstance(value, list):
                for obj in value:
                    obj[field] = obj[field] + ' (O)' if obj.get(field) else ''
                    if obj[field]:
                        if self[main_field].get(key) is None:
                            self[main_field][key] = []
                        self[main_field][key].append(obj)


class AtokaErrorContactsItem(scrapy.Item):
    code = scrapy.item.Field(serializer=str)
    reason = scrapy.item.Field(serializer=str)
