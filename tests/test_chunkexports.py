import os
import json
import shutil
import unittest

from scrapy.spider import Spider
from scrapy import Item, Field
from scrapy.utils.test import get_crawler
from scrapy.exceptions import NotConfigured

from scrapylib.chunkexports import ChunkedFeedExporter

EXPORT_TEMP_DIR = '.exports'
EXPORT_FILE_PATTERN = EXPORT_TEMP_DIR + '/export_test_%(chunk_number)02d.json'
JSON_FEED_EXPORTERS = {'json': 'scrapy.contrib.exporter.JsonItemExporter'}


class FakeItem(Item):
    id = Field()


class ItemGenerator(object):
    item_id = 0

    @classmethod
    def generate(cls):
        cls.item_id += 1
        return FakeItem(id=cls.item_id)

    @classmethod
    def reset(cls):
        cls.item_id = 0


class ChunkExtensionTest(object):
    settings = {}

    def tearDown(self):
        self.remove_temp_dir()

    def start(self, n_items_per_chunk=None, n_items=None, settings=None):

        # Reset item generator and remove temporary dir
        ItemGenerator.reset()
        self.remove_temp_dir()

        # Setup settings
        settings = settings or self.settings.copy()
        if n_items_per_chunk is not None:
            settings['CHUNKED_FEED_ITEMS_PER_CHUNK'] = n_items_per_chunk

        # Init Scrapy
        self.crawler = get_crawler(settings)
        self.spider = Spider('chunk_test')
        self.spider.set_crawler(self.crawler)
        self.extension = ChunkedFeedExporter.from_crawler(self.crawler)
        self.extension.open_spider(self.spider)

        # Add items if we have to
        if n_items:
            self.add_items(n_items)

    def stop(self):
        return self.extension.close_spider(self.spider)

    def remove_temp_dir(self):
        shutil.rmtree(EXPORT_TEMP_DIR, ignore_errors=True)

    def add_items(self, n_items):
        for i in range(1, n_items+1):
            item = ItemGenerator.generate()
            self.extension.item_scraped(item, self.spider)

    def get_chunk_filename(self, chunk):
        return EXPORT_FILE_PATTERN % {'chunk_number':chunk}

    def get_chunk_filenames(self):
        return [f for f in os.listdir(EXPORT_TEMP_DIR) if f.endswith(".json")]

    def get_number_of_chunks(self):
        return len(self.get_chunk_filenames())

    def get_chunk_content(self, chunk):
        with open(self.get_chunk_filename(chunk)) as f:
            return json.load(f)

    def ensure_number_of_chunks(self, n_chunks):
        n = self.get_number_of_chunks()
        assert n_chunks == n, "Wrong number of chunks. found %d, expecting %d" % (n, n_chunks)

    def ensure_number_of_exported_items_per_chunk(self, chunk, n_items):
        n_exported_items = len(self.get_chunk_content(chunk))
        assert n_items == n_exported_items, "Wrong number of exported items. found %d, expecting %d" % \
                                            (n_exported_items, n_items)


class ConfigFailures(ChunkExtensionTest, unittest.TestCase):

    def test_no_settings(self):
        self.assertRaises(NotConfigured, self.start, settings={})

    def test_no_feed_uri(self):
        self.assertRaises(NotConfigured, self.start, settings={
            'CHUNKED_FEED_FORMAT': 'json',
            'CHUNKED_FEED_ITEMS_PER_CHUNK': 1,
            'FEED_EXPORTERS': JSON_FEED_EXPORTERS
        })

    def test_no_feed_format(self):
        self.assertRaises(NotConfigured, self.start, settings={
            'CHUNKED_FEED_URI': EXPORT_FILE_PATTERN,
            'CHUNKED_FEED_ITEMS_PER_CHUNK': 1,
            'FEED_EXPORTERS': JSON_FEED_EXPORTERS
        })

    def test_no_feed_items_per_chunk(self):
        self.assertRaises(NotConfigured, self.start, settings={
            'CHUNKED_FEED_URI': EXPORT_FILE_PATTERN,
            'CHUNKED_FEED_FORMAT': 'json',
            'FEED_EXPORTERS': JSON_FEED_EXPORTERS
        })

    def test_zero_feed_items_per_chunk(self):
        self.assertRaises(NotConfigured, self.start, settings={
            'CHUNKED_FEED_URI': EXPORT_FILE_PATTERN,
            'CHUNKED_FEED_FORMAT': 'json',
            'CHUNKED_FEED_ITEMS_PER_CHUNK': 0,
            'FEED_EXPORTERS': JSON_FEED_EXPORTERS
        })


class ItemsAndChunks(ChunkExtensionTest, unittest.TestCase):
    settings = {
        'CHUNKED_FEED_URI': EXPORT_FILE_PATTERN,
        'CHUNKED_FEED_FORMAT': 'json',
        'CHUNKED_FEED_ITEMS_PER_CHUNK': 1,
        'FEED_EXPORTERS': JSON_FEED_EXPORTERS
    }

    def test_items_0(self):
        # FIXME: Scrapy exporter creates always one file
        self.start(n_items=0, n_items_per_chunk=1)
        self.stop()
        #self.ensure_number_of_chunks(n_chunks=0)

    def test_items_1_chunksize_1(self):
        self.start(n_items=1, n_items_per_chunk=1)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=1)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=1)

    def test_items_1_chunksize_2(self):
        self.start(n_items=1, n_items_per_chunk=2)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=1)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=1)

    def test_items_2_chunksize_1(self):
        self.start(n_items=2, n_items_per_chunk=1)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=2)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=1)
        self.ensure_number_of_exported_items_per_chunk(chunk=2, n_items=1)

    def test_items_2_chunksize_2(self):
        self.start(n_items=2, n_items_per_chunk=2)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=1)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=2)

    def test_items_2_chunksize_3(self):
        self.start(n_items=2, n_items_per_chunk=3)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=1)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=2)

    def test_items_5_chunksize_1(self):
        self.start(n_items=5, n_items_per_chunk=1)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=5)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=1)
        self.ensure_number_of_exported_items_per_chunk(chunk=2, n_items=1)
        self.ensure_number_of_exported_items_per_chunk(chunk=3, n_items=1)
        self.ensure_number_of_exported_items_per_chunk(chunk=4, n_items=1)
        self.ensure_number_of_exported_items_per_chunk(chunk=5, n_items=1)

    def test_items_5_chunksize_2(self):
        self.start(n_items=5, n_items_per_chunk=2)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=3)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=2)
        self.ensure_number_of_exported_items_per_chunk(chunk=2, n_items=2)
        self.ensure_number_of_exported_items_per_chunk(chunk=3, n_items=1)

    def test_items_5_chunksize_3(self):
        self.start(n_items=5, n_items_per_chunk=3)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=2)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=3)
        self.ensure_number_of_exported_items_per_chunk(chunk=2, n_items=2)

    def test_items_5_chunksize_4(self):
        self.start(n_items=5, n_items_per_chunk=4)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=2)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=4)
        self.ensure_number_of_exported_items_per_chunk(chunk=2, n_items=1)

    def test_items_5_chunksize_5(self):
        self.start(n_items=5, n_items_per_chunk=5)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=1)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=5)

    def test_items_5_chunksize_6(self):
        self.start(n_items=5, n_items_per_chunk=6)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=1)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=5)

    def test_items_100_chunksize_25(self):
        self.start(n_items=100, n_items_per_chunk=25)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=4)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=25)
        self.ensure_number_of_exported_items_per_chunk(chunk=2, n_items=25)
        self.ensure_number_of_exported_items_per_chunk(chunk=3, n_items=25)
        self.ensure_number_of_exported_items_per_chunk(chunk=4, n_items=25)

    def test_items_100_chunksize_24(self):
        self.start(n_items=100, n_items_per_chunk=24)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=5)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=24)
        self.ensure_number_of_exported_items_per_chunk(chunk=2, n_items=24)
        self.ensure_number_of_exported_items_per_chunk(chunk=3, n_items=24)
        self.ensure_number_of_exported_items_per_chunk(chunk=4, n_items=24)
        self.ensure_number_of_exported_items_per_chunk(chunk=5, n_items=4)

    def test_items_100_chunksize_26(self):
        self.start(n_items=100, n_items_per_chunk=26)
        self.stop()
        self.ensure_number_of_chunks(n_chunks=4)
        self.ensure_number_of_exported_items_per_chunk(chunk=1, n_items=26)
        self.ensure_number_of_exported_items_per_chunk(chunk=2, n_items=26)
        self.ensure_number_of_exported_items_per_chunk(chunk=3, n_items=26)
        self.ensure_number_of_exported_items_per_chunk(chunk=4, n_items=22)
