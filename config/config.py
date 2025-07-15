# Configuration
import os


BASE_URL = 'https://books.toscrape.com'
MAX_PAGES = 5
DELAY = 1
MIN_IO_URL = os.environ.get('MIN_IO_URL','host.docker.internal:9000')
MIN_IO_ACCESS_KEY = os.environ.get("MIN_IO_ACCESS_KEY")
MIN_IO_SECRET_KEY = os.environ.get("MIN_IO_SECRET_KEY")
BUCKET = os.environ.get("BUCKET", 'books')
DB_COLLECTION_NAME = 'books_data'
CHROMA_HOST = os.environ.get("CHROMA_HOST")
CHROMA_PORT = os.environ.get("CHROMA_PORT")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL")
LLM_MODEL = os.environ.get("LLM_MODEL", "mistral:7b")
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "ollama")
OLLAMA_PORT = os.environ.get("OLLAMA_PORT", "11434")


USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0',
        'Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
    ]

BOOK_DETAILS_XPATH = {
    "title_xpath":[
        "//*[@id='content_inner']/article/div[1]/div[2]/h1/text()"
    ],
    "price_xpath": [
                "//div[@class='col-sm-6 product_main']//p[@class='price_color']/text()",
                "//p[@class='price_color']/text()",
                "//div[@class='product_main']//p[@class='price_color']/text()"
            ],
    "availability_xpaths":[
                "//div[@class='col-sm-6 product_main']//p[@class='instock availability']/text()[normalize-space()]",
                "//p[@class='instock availability']/text()[normalize-space()]",
                "//div[@class='product_main']//p[@class='instock availability']/text()[normalize-space()]"
            ],
    "description_xpaths": [
                "//div[@id='product_description']/following-sibling::p/text()",
                "//div[@id='product_description']/following-sibling::p[1]/text()",
                "//article[@class='product_page']//p[not(@class)]/text()"
            ],
    "category_xpath": [
            "//*[@id='default']/div/div/ul/li[3]/a/text()"
            ],
    "review_count_xpath":[
        "//table[@class='table table-striped']//th[text()='Number of reviews']/following-sibling::td/text()"
    ]
}