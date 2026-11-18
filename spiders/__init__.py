from spiders.base import BaseEbookSpider, BookData
from spiders.chapter_sites import IsachSpider, SachHayOnlineSpider
from spiders.dtv_ebook import DtvEbookSpider
from spiders.nhasachmienphi import NhaSachMienPhiSpider
from spiders.sach_gallery import SachGallerySpider
from spiders.sachvui import SachVuiSpider
from spiders.thuviensach import MetaisachSpider, ThuVienSachSpider

SPIDER_REGISTRY = {
    "sach_gallery": SachGallerySpider,
    "nhasachmienphi": NhaSachMienPhiSpider,
    "sachvui": SachVuiSpider,
    "dtv_ebook": DtvEbookSpider,
    "metaisach": MetaisachSpider,
    "thuviensach": ThuVienSachSpider,
    "isach": IsachSpider,
    "sachhayonline": SachHayOnlineSpider,
}

__all__ = [
    "BaseEbookSpider",
    "BookData",
    "SPIDER_REGISTRY",
    "SachGallerySpider",
    "NhaSachMienPhiSpider",
    "SachVuiSpider",
    "DtvEbookSpider",
    "MetaisachSpider",
    "ThuVienSachSpider",
    "IsachSpider",
    "SachHayOnlineSpider",
]
