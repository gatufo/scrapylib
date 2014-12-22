import os
import copy
from datetime import datetime

from scrapy.contrib.feedexport import FeedExporter
from scrapy.exceptions import NotConfigured


DEFAULT_TIMESTAMP_FORMAT = '%Y-%m-%d-%H'


class ChunkedFeedExporter(FeedExporter):
    """Extension for breaking item exports into chunks.

    Settings:
        * CHUNKED_FEED_URI: The feed uri to use for exporting (Overrides FEED_URI setting).
        * CHUNKED_FEED_FORMAT: The feed format to use for exporting (Overrides FEED_FORMAT setting).
        * CHUNKED_FEED_ITEMS_PER_CHUNK: Number of items included in each chunk
        * CHUNKED_FEED_TIMESTAMP_FORMAT: A string representing the format to be used for representing \
                                         the ``timestamp`` uri parameter.

    Example:
        CHUNKED_FEED_URI = 'export_%(chunk_number)02d.json'
        CHUNKED_FEED_FORMAT = 'json'
        CHUNKED_FEED_ITEMS_PER_CHUNK = 100

        For 250 items will generate the following files:
            * export_01.json (100 items)
            * export_02.json (100 items)
            * export_03.json (50 items)

    Available uri format values:
        * chunk_number: The active chunk counter. (Starts in 1).
        * scrapy_job: The Scrapy job (if available).
        * scrapy_project_id: The Scrapy job id (if available).
        * timestamp: Current timestamp in UTC (formatted with CHUNKED_FEED_TIMESTAMP_FORMAT setting).

    """

    def __init__(self, settings):

        # Override settings object to reuse feed exporter settings
        settings = copy.deepcopy(settings)
        self.settings = settings

        # Get chunked settings
        chunked_feed_uri = self._get_from_settings_or_not_configured('CHUNKED_FEED_URI', None)
        chunked_feed_format = self._get_from_settings_or_not_configured('CHUNKED_FEED_FORMAT')
        self._items_per_chunk = self._get_from_settings_or_not_configured('CHUNKED_FEED_ITEMS_PER_CHUNK')

        # Settings override
        settings.set('FEED_URI', chunked_feed_uri, 100)
        settings.set('FEED_FORMAT', chunked_feed_format, 100)

        # Parent call with overridden settings
        super(ChunkedFeedExporter, self).__init__(settings)

        # Internal stuff
        self._chunk_number = 1
        self._uripar = self.get_uri_parameters
        self._timestamp_format = settings.get('CHUNKED_FEED_TIMESTAMP_FORMAT', DEFAULT_TIMESTAMP_FORMAT)

        # Get uri parameters from settings or environment
        self.settings = settings
        self._scrapy_job = self._get_from_settings_or_environ('SCRAPY_JOB', 'nojob')
        self._scrapy_project = self._get_from_settings_or_environ('SCRAPY_PROJECT', 'noproject')
        self._scrapy_project_id = self._get_from_settings_or_environ('SCRAPY_PROJECT_ID', 'noprojectid')

    def get_uri_parameters(self, params, spider):
        """Update feed uri available parameters. Override if you want to add more parameters"""
        params.update({
            'chunk_number': self._chunk_number,
            'scrapy_job': self._scrapy_job,
            'scrapy_project_id': self._scrapy_project_id,
            'timestamp': datetime.utcnow().strftime("%Y-%m-%d-%H"),
        })

    def item_scraped(self, item, spider):
        if self._items_per_chunk and self.slot.itemcount >= self._items_per_chunk:
            self._reset_exporter(spider)
        item = super(ChunkedFeedExporter, self).item_scraped(item, spider)
        return item


    def _reset_exporter(self, spider):
        self.close_spider(spider)
        self._chunk_number += 1
        self.open_spider(spider)

    def _get_from_settings_or_environ(self, name, default):
        return self.settings.get(name=name, default=os.environ.get(name, default))

    def _get_from_settings_or_not_configured(self, name, default=None):
        value = self.settings.get(name, default)
        if not value:
            raise NotConfigured
        return value
