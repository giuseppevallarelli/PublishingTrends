import asyncio
from dataclasses import dataclass
import json
import math
from pathlib import Path
from time import time
from urllib import parse

import aiofiles
import aiohttp
from tqdm.asyncio import tqdm

from ingestion.utils import join_data_files


@dataclass
class ScraperParams:
    start_date: str
    end_date: str
    entries_per_page: int
    json_dir: Path
    cover_img_dir: Path
    img_size = '500w'
    url = 'https://www.oreilly.com/search/api/search/'
    headers = {"accept": "*/*", "authority": "www.oreilly.com", "referer": "https://www.oreilly.com/",
               "user-agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/114.0.0.0 Safari/537.36")}

    def build_book_page_url(self, page_num=1):
        params = parse.urlencode([('q', '*'), ('type', 'book'), ('rows', self.entries_per_page), ('page', page_num),
                ('issued_after', self.start_date), ('issued_before', self.end_date), ('publishers', 'Apress'),
                ('publishers', "O'Reilly Media, Inc."), ('publishers', 'Manning Publications'),
                ('publishers', 'Packt Publishing'), ('publishers', 'Pearson'),
                ('publishers', 'Addison-Wesley Professional'), ('publishers', 'Microsoft Press'),
                ('publishers', 'Pragmatic Bookshelf'), ('publishers', 'No Starch Press'),
                ('order_by', 'created_at')])
        return f'{self.url}?{params}'

    def build_cover_img_url(self, base_url):
        return f'{base_url}{self.img_size}'

    def page_num_from_url(self, url):
        query = parse.urlsplit(url).query
        params = parse.parse_qs(query)
        page = params['page'][0]
        return page


async def retrieve_cover(url: str, params: ScraperParams, sem: asyncio.Semaphore):
    """
    Retrieve book cover at base_url and serialize to disk.
    """
    is_ok = False
    async with sem:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        isbn = url.split('/')[-2]
                        cover_path = Path(params.cover_img_dir, f'{isbn}.jpeg')
                        async with aiofiles.open(cover_path, 'wb') as f:
                            await f.write(await response.read())
                            is_ok = True
            except aiohttp.ClientConnectorError as e:
                print('Connection Error', str(e))
                print(f'Failed to retrieve cover with url {url}')
            return is_ok


async def retrieve_book_page(url: str, params: ScraperParams, sem: asyncio.Semaphore):
    """
    Retrieve books metadata from O'Reilly API (results are paginated), for each book extract some fields of interest
    and retrieve the book cover.
    """
    async with sem:
        async with aiohttp.ClientSession() as session:
            is_ok = False
            elems = []
            try:
                async with session.get(url, headers=params.headers) as response:
                    content = await response.json()
                    products = content['data']['products']
                    for prod in products:
                        # Extract relevant data
                        # top level fields
                        base_fields = ['product_id', 'title', 'authors', 'description', 'language', 'categories',
                                       'url', 'cover_image']
                        data = {field: prod.get(field) for field in base_fields}
                        # custom attributes fields
                        custom_attr_fields = ['publication_date', 'publishers', 'page_count', 'average_rating']
                        for field in custom_attr_fields:
                            data[field] = prod['custom_attributes'][field]

                        cover_url = params.build_cover_img_url(data.get('cover_image'))
                        cover_status = await retrieve_cover(cover_url, params, sem)
                        elems.append((data, int(cover_status)))  # cover_statuses will be later aggregated
                    is_ok = True
            except aiohttp.ClientConnectorError as e:
                print('Connection Error', str(e))
                print(f'Failed to retrieve page {url}')

            return elems, url, is_ok


def serialize_results(results, params: ScraperParams, book_count=0, cover_count=0):
    content_i, cover_status_i = 0, 1
    for res, url, _ in results:
        page_num = params.page_num_from_url(url)
        page_path = Path(params.json_dir, f'{page_num}.json')
        serialize_data = [r[content_i] for r in res]
        with open(page_path, 'w') as fobj:
            json.dump(serialize_data, fobj, indent=3)
            print(f'Serialized page {page_num}')
        book_count += len(serialize_data)
        cover_count += sum([book_cover[cover_status_i] for book_cover in res])
    return book_count, cover_count


async def plan(params: ScraperParams):
    async with aiohttp.ClientSession() as session:
        try:
            url = params.build_book_page_url()
            async with session.get(url, headers=params.headers) as response:
                content = await response.json()
                num_books = content['data']['total']
                pages = math.ceil(num_books / entries_per_page)
        except aiohttp.ClientConnectorError as e:
            print('Connection Error', str(e))
        else:
            return num_books, pages


async def main(urls, params: ScraperParams, semaphore: asyncio.Semaphore):
    coros = [retrieve_book_page(url, params, semaphore) for url in urls]
    return await tqdm.gather(*coros)


if __name__ == '__main__':
    start_time = time()

    # Organize storage dirs
    root_folder = Path(__file__).parent.parent
    data_dir = Path(root_folder, 'data')
    cover_dir = Path(root_folder, 'cover_images')
    data_dir.mkdir(exist_ok=True)
    cover_dir.mkdir(exist_ok=True)
    dest_fname = Path(data_dir, 'dataset.json')

    # Default scraper params
    start_date, end_date = '2018-01-01', '2023-12-31'
    entries_per_page = 50
    sp = ScraperParams(start_date, end_date, entries_per_page, data_dir, cover_dir)
    num_books, planned_num_pages = asyncio.run(plan(sp))
    # Total number of books: 5522
    # Planned pages: 111
    print(f'Total number of books: {num_books}')
    print(f'Planned pages: {planned_num_pages}')
    semaphore = asyncio.Semaphore(250)

    # First run
    urls = [sp.build_book_page_url(page_num) for page_num in range(1, planned_num_pages + 1)]
    results = asyncio.run(main(urls, sp, semaphore))

    url_idx = 1
    is_ok_idx = 2
    retrieved_pages = [res[url_idx] for res in results if res[is_ok_idx]]
    missing_pages = [res[url_idx] for res in results if not res[is_ok_idx]]
    num_retrieved, num_missing = len(retrieved_pages), len(missing_pages)
    print(f'Retrived pages: {num_retrieved}')
    print(f'Missing pages: {num_missing}')
    b_count, c_count = serialize_results(results, sp)
    if missing_pages:
        # Second run
        results = asyncio.run(main(missing_pages, sp, semaphore))
        retrieved_pages = [res for res in results if res[is_ok_idx]]
        missing_pages = [res[url_idx] for res in results if not res[is_ok_idx]]
        b_count, c_count = serialize_results(retrieved_pages, sp, b_count, c_count)
        print(f'Failed 2nd attempt those pages: {missing_pages}')

    r = join_data_files(data_dir, dest_fname)
    print(f'Am I completed ? {r}')
    end_time = time()
    completed_time = end_time - start_time
    # 241 secs
    print(f'Finished in {completed_time:.2f} secs')
    print(f'Book count: {b_count}')
    print(f'Cover count: {c_count}')
