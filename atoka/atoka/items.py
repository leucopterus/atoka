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


class AtokaErrorContactsItem(scrapy.Item):
    code = scrapy.item.Field(serializer=str)
    reason = scrapy.item.Field(serializer=str)
