import requests
import re
import logging
from urllib.parse import urljoin, urlsplit, parse_qs
from bs4 import BeautifulSoup

class JKRTender:
    def __init__(self, url, details_fields, start_offset=0, page_items=20):
        self.url = url
        self.page_offset = start_offset
        self.page_items = page_items
        self.details_fields = details_fields

    def get_rows(self):
        while True:
            logging.info("Processing offset {}...".format(self.page_offset))
            page = JKRTenderPage(self.url, self.page_offset, self.details_fields)
            for row in page.get_rows():
                yield row
            if page.is_last_page:
                break
            self.page_offset += self.page_items

class JKRTenderPage:
    def __init__(self, base_url, offset, details_fields):
        self.base_url = base_url
        self.offset = offset
        self.details_fields = details_fields
        self._get_page()
        self._scrape_page()

    def _get_page(self):
        query = {'offset': self.offset}
        response = requests.get(self.base_url, query, verify="cabundle.pem")
        self.url = response.url
        self.html = response.text

    def _scrape_page(self):
        soup = BeautifulSoup(self.html, "html.parser")

        # Find table with data
        main_table = soup.find(class_="mt1")
        self._rows = main_table.find_all("tr", recursive=False)[1:]

        # Find "Last" link to determine if last page
        last_page_text = main_table.tfoot.find(string=re.compile(r"Last"))
        self.is_last_page = last_page_text is None

    def get_rows(self):
        for row in self._rows:
            href = row.find("a")['href']
            details_url = urljoin(self.url, href)
            details = JKRTenderDetails(details_url, self.details_fields)
            yield details.data

class JKRTenderDetails:
    def __init__(self, url, fields):
        self.url = url
        self.fields = fields

        url_parts = urlsplit(self.url)
        query = parse_qs(url_parts.query)
        self.project_num = query['No_Proj'][0]

        self._get_page()
        self._scrape_page()

    def _get_page(self):
        logging.info("Processing project {}...".format(self.project_num))
        response = requests.get(self.url, verify="cabundle.pem")
        self.html = response.text

    def _scrape_page(self):
        soup = BeautifulSoup(self.html, "html.parser")
        tables = soup.find_all(class_="mt2")

        # Compile all strings in tables into a 2D list of table_index, string_index
        tables_strings = []
        for table in tables:
            tds = table.find_all("td")
            table_strings = [td.get_text(strip=True) for td in tds]
            tables_strings.append(table_strings)

        data = {}
        data['id'] = self.project_num

        for key in self.fields:
            position = self.fields[key]
            data[key] = tables_strings[position[0]][position[1]]
        data['source_url'] = self.url
        
        self.data = data
