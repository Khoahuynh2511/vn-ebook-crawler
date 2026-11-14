# VN Ebook Crawler

Công cụ crawl sách điện tử tiếng Việt đa luồng song song (`asyncio` + `httpx` + **ProjectDiscovery Katana**), tự động quét toàn bộ kho sách qua Katana (`-kf all -jc`), Sitemap XML và danh mục phân trang, **ưu tiên tải bản EPUB** (hoặc tự biên dịch **EPUB3** từ các trang đọc chương online; chỉ tải PDF dự phòng nếu sách không có EPUB), có lưu trạng thái vào SQLite (`catalog.db`) + `crawl_progress.json` để chống trùng lặp và dừng/tiếp tục an toàn.

## Cài đặt
```bash
pip install -r requirements.txt
```
*(Tùy chọn)* Đặt file thực thi `katana.exe` của [ProjectDiscovery Katana](https://github.com/projectdiscovery/katana) vào thư mục `bin/katana.exe` hoặc trong `PATH` hệ thống để tăng tốc pha khám phá URL (nếu không có, crawler tự chạy bộ quét Python thuần).

## Tích hợp ProjectDiscovery Katana (`bin/katana.exe`)
- Mặc định hệ thống dùng **Katana** (viết bằng Go) ở pha khám phá URL để đọc `sitemap.xml`, `robots.txt` và phân tích JS bundle (`-jc`) tìm link sách với tốc độ cao.
- Dùng `--no-katana` để chuyển về chế độ quét Python thuần, `--katana-depth N` để chỉnh độ sâu crawl, hoặc `--katana-headless` để bật chế độ headless (`-hl`) cho trang render bằng JS.

## Nguồn sách hỗ trợ
Khi chạy `--sources all` (mặc định), crawler tập trung vào **4 nguồn hiệu quả** (`ACTIVE_SOURCES`):
- `sach_gallery` (`www.sach.gallery`) — Kho EPUB chọn lọc chất lượng cao, tải trực tiếp `/tai/<id>` (lưu ý giới hạn 20 lượt tải `/tai/` mỗi ngày/IP) và đọc chương `/doc/`.
- `metaisach` (`metaisach.com`) — Kho PDF/EPUB đồ sộ (hơn 8.200 đầu sách tải thẳng từ `media.metaisach.com`).
- `thuviensach` (`thuviensach.vn`) — Thư viện ebook tổng hợp (ưu tiên các chuyên mục có link EPUB/PDF trực tiếp).
- `sachhayonline` (`www.sachhayonline.com`) — Sách văn học, lịch sử, kỹ năng theo chương; tự động gom chương đóng gói thành file `.epub`.

*(Các adapter cũ không còn khả dụng hoặc bị chặn Cloudflare/CAPTCHA như `isach`, `sachvui`, `nhasachmienphi`, `dtv_ebook` vẫn được giữ trong mã nguồn nếu muốn gọi đích danh qua `--sources`, nhưng mặc định được bỏ qua).*

## Cách sử dụng

Mở terminal tại `D:\vn-ebook-crawler`:

```bash
# 1. Thử nghiệm nhanh: quét và tải tối đa 2 sách/nguồn trên tất cả các nguồn song song vào D:\Books
python main.py --sources all --limit 2 --workers 3 --output-dir D:\Books

# 2. Quét và tải TẤT CẢ sách từ tất cả các nguồn mở (không giới hạn) vào D:\Books
python main.py --sources all --workers 5

# 3. Chỉ chạy các nguồn cụ thể và chỉ tải EPUB
python main.py --sources sach_gallery,metaisach,sachhayonline --formats epub

# 4. Xem thống kê sách đã thu thập trong catalog.db
python main.py --stats

# 5. Dừng & Tiếp tục sau (Stop / Resume)
# - Có thể bấm Ctrl+C bất kỳ lúc nào; các sách đang tải dở sẽ được trả về trạng thái chờ (discovered),
#   và toàn bộ tiến trình được lưu tự động vào D:\vn-ebook-crawler\catalog.db + D:\Books\crawl_progress.json.
# - Chạy lại cùng lệnh (hoặc tăng --limit / bỏ --limit): chương trình tự động bỏ qua sách đã tải (cột Đã có (Skip))
#   và tự tải lại nếu file đó trên ổ đĩa D:\Books bị xóa thủ công.
python main.py --sources all --workers 5

# 6. Thử tải lại các sách từng lỗi mạng hoặc quét lại danh mục từ đầu (vẫn bỏ qua sách đã tải xong)
python main.py --sources all --retry-failed
python main.py --sources all --reset-discovery
```

## Cơ chế Lưu tiến trình (Stop/Resume Checkpoint)
- **`D:\vn-ebook-crawler\catalog.db`**: Lưu từng đầu sách (`discovered`, `processing`, `completed`, `partial`, `no_files`, `failed`, `blocked_by_captcha`) cùng các trang danh mục đã duyệt (`visited_pages`).
- **`D:\Books\crawl_progress.json`**: File JSON tóm tắt tiến trình dễ đọc (thống kê theo từng nguồn + danh sách sách đã tải và đường dẫn file).
- Nếu bạn nhấn `Ctrl+C` để dừng giữa chừng, hệ thống tự động đặt lại các cuốn `processing` về `discovered` và cập nhật checkpoint trước khi thoát.

## Cấu trúc thư mục xuất ra
```text
D:\Books\
├── crawl_progress.json
└── <nguon>\
    └── <the-loai>\
        ├── <ten-sach>.epub
        ├── <ten-sach>.pdf
        └── <ten-sach>.meta.json
```
