# -*- coding: utf-8 -*-
import time
import functools

import requests

from datetime import datetime

from bs4 import BeautifulSoup
from torrequest import TorRequest

from libs.pymongodb import pymongodb
from libs import decorators
from libs import utils


class Parser(object):
    def __init__(self):
        self.url = 'https://bits.media/news/nav_feed/page-{}/'
        self.mongo = pymongodb.MongoDB('bitsmedia')
        self.last_new = None
        self.next = False

    @staticmethod
    def get_html(url):
        """
        Method which send GET request to specific url and return html.
        :param url:
        :return:
        """
        time.sleep(2)

        try:
            html = requests.get(url, timeout=(10, 27), stream=True).content
        except Exception as e:
            print(e)

            with TorRequest(proxy_port=9050, ctrl_port=9051, password=None) as tr:
                tr.reset_identity()
                html = tr.get(url, timeout=(10, 27), stream=True).content
        return html

    def write_data(self, **kwargs):
        """
        Record processed data.
        :param kwargs:
        :return:
        """
        self.mongo.insert_one(kwargs, 'news')
        self.mongo.finish()

    def get_last_new(self):
        """
        Get last new from db by current date. If there are no news on current date - trying to get yesterday news.
        Else - write clear list into last_new.
        :return:
        """
        day = datetime.today().day
        date = datetime.today().strftime('{}.%m.%Y')

        self.last_new = self.mongo.find({'date': date.format(day)}, 'news')

        if not self.last_new:
            self.last_new = self.mongo.find({'date': date.format(day - 1)}, 'news')

    def parse(self, page_num):
        """
        Parse data by page number. Processing and recording data into db.
        :param page_num:
        :return:
        """
        bs_obj = BeautifulSoup(self.get_html(self.url.format(page_num)), 'lxml')

        # Get news images.
        imgs_src = bs_obj.findAll('a', {'class': 'img-box'})
        imgs_src = [img_url.find('img')['src'] for img_url in imgs_src]
        imgs_src = list(map(lambda x: 'https://bits.media' + x, imgs_src))

        # Get news title and full news description link.
        titles = bs_obj.findAll('a', {'class': 'news-name'})
        news_names = [title.text for title in titles]

        full_news_links = [title['href'] for title in titles]
        full_news_links = list(map(lambda x: 'https://bits.media' + x, full_news_links))

        # Get news date.
        dates = bs_obj.findAll('span', {'class': 'news-date'})
        dates = [date.text for date in dates]

        # Processing and recording data.
        data_lst = [imgs_src, full_news_links, news_names, dates]

        if self.last_new:   # When database is not clear.
            try:
                index = news_names.index(self.last_new[0]['name'])
                self.next = False

                # Remove unnecessary items.
                data_lst = list(map(functools.partial(utils.del_items_by_index, index=index), data_lst))

            except ValueError:      # Last new from db not in current parsed list.
                self.next = True

            if data_lst[0]:     # Not clear lst.
                # Write data into db.
                for i in range(len(data_lst[0])):
                    self.write_data(img=data_lst[0][i], full_description=data_lst[1][i],
                                    name=data_lst[2][i], date=data_lst[3][i])

        else:   # When database is clear or no records for current and yesterday date.
            for i in range(len(data_lst[0])):
                self.write_data(img=data_lst[0][i], full_description=data_lst[1][i],
                                name=data_lst[2][i], date=data_lst[3][i])

    @decorators.log
    def run(self):
        """
        Try to get last new from db and parse data.
        :return:
        """
        self.get_last_new()

        # Parse data.
        page_count = 1

        while True:
            self.parse(page_count)

            if self.next is False:
                break

            page_count += 1


if __name__ == '__main__':
    try:
        Parser().run()
    except:
        utils.logger('Success status: %s' % 'ERROR', 'ailachi.log')