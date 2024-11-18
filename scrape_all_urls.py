import boto3
from bs4 import BeautifulSoup
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from chrome_driver import WebDriverPool
import os

logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


class StockData:
    def __init__(self, driver_pool):
        self.s3_bucket = "stock-prediction-forum-data"
        self.processed_files = self.list_all_s3_objects()
        self.driver_pool = driver_pool
        self.s3_pool = ThreadPoolExecutor(max_workers=5)


    def _upload_to_s3(self, file_name, data):
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        )
        try:
            s3.put_object(Bucket=self.s3_bucket, Key=file_name, Body=data)
            logging.info(f"File '{file_name}' uploaded successfully.")
            return True
        except Exception as e:
            logging.info(f"An error occurred: {e}")
            return False

    async def _infinte_scroll_scrape(self, url, driver):
        logging.info(f"Scraping {url}")
        driver.get(url)
        scroll_pause_time = 2
        screen_height = driver.execute_script("return window.screen.height;")
        i = 1
        while True:
            driver.execute_script(f"window.scrollTo(0, {screen_height * i});")
            i += 1
            await asyncio.sleep(scroll_pause_time)
            scroll_height = driver.execute_script("return document.body.scrollHeight;")
            if screen_height * i > scroll_height:
                break
            logging.info(f"Scrolled page content {i} times")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        text = soup.get_text()
        return text


    def list_all_s3_objects(self):
        s3_client = boto3.client('s3')
        file_names = []
        paginator = s3_client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.s3_bucket}

        for page in paginator.paginate(**operation_parameters):
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_name = obj['Key']
                    if file_name.endswith('.txt'):
                        file_names.append(file_name[:-4])  # Remove the '.txt' extension
                    else:
                        file_names.append(file_name)

        return file_names


    def _get_posts_with_url_dict(self):
        import json
        file_path = 'links_to_process.json'
        with open(file_path, 'r') as json_file:
            data_dict = json.load(json_file)
        return data_dict

    async def _process(self, data_dict):
        tasks = []
        for title, link in data_dict.items():
            if title not in self.processed_files:
                tasks.append(self._process_single(title, link))
        print(f"Found {len(tasks)} to execute")
        await asyncio.gather(*tasks)

    async def _process_single(self, title, link):
        driver = await self.driver_pool.get_driver()
        try:
            text = await self._infinte_scroll_scrape(link, driver)
            # Upload to S3 in a separate thread
            await asyncio.get_event_loop().run_in_executor(
                self.s3_pool, self._upload_to_s3, f'{title}.txt', text
            )
            logging.info(f'Scraped and uploaded link: {link}')

        except Exception as e:
            logging.info(f"Failed to scrape {link}")

        finally:
            self.driver_pool.release_driver(driver)

    async def close(self):
        await self.session.close()
        self.s3_pool.shutdown()


async def main():
    driver_pool = WebDriverPool(num_drivers=5)  # Create 5 WebDriver instances
    stock_data = StockData(driver_pool)

    data_dict = stock_data._get_posts_with_url_dict()
    await stock_data._process(data_dict)

    driver_pool.close_all()
    stock_data.s3_pool.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
