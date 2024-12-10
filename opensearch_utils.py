from opensearchpy import OpenSearch
from configuration import OPEN_SEARCH_HOST, OPEN_SEARCH_PORT, OPEN_SEARCH_USE_SSL, OPEN_SEARCH_USE_CERT



class OpenSearchUtils:
    def __init__(self):
        self.client = OpenSearch(hosts = [{'host': OPEN_SEARCH_HOST, 'port': OPEN_SEARCH_PORT}],
                                 use_ssl = OPEN_SEARCH_USE_SSL,
                                 verify_certs = OPEN_SEARCH_USE_CERT,
                                 ssl_assert_hostname = False,
                                 ssl_show_warn = False
                                 )
