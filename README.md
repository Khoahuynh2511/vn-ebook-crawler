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
# 1. Thử nghiệm nhanh: quét và tải tối đa 2 sách/nguồn trên 4 nguồn hiệu quả thẳng lên Google Drive ("Books")
python main.py --sources all --limit 2 --workers 3

# 2. Quét và tải TẤT CẢ sách lên Google Drive (dùng thư mục đệm %TEMP% và tự xóa file tạm sau khi upload)
python main.py --sources all --workers 5

# 3. Chỉ chạy các nguồn cụ thể và giữ thêm bản sao cục bộ trên máy (--keep-local)
python main.py --sources metaisach,thuviensach,sachhayonline --limit 10 --keep-local --output-dir D:\Books

# 4. Xem thống kê sách đã thu thập trong catalog.db (bao gồm số lượng đã lên Google Drive)
python main.py --stats

# 5. Dừng & Tiếp tục sau (Stop / Resume)
# - Có thể bấm Ctrl+C bất kỳ lúc nào; các sách đang tải dở sẽ được trả về trạng thái chờ (discovered),
#   và toàn bộ tiến trình được lưu tự động vào catalog.db + crawl_progress.json (trên Google Drive Books/).
# - Chạy lại cùng lệnh: chương trình tự động bỏ qua sách đã lưu trên Google Drive (cột Đã có (Skip)).
python main.py --sources all --workers 5

# 6. Đồng bộ lên Google Drive / Chế độ cục bộ
# Đẩy toàn bộ sách cục bộ chưa upload lên Google Drive rồi xóa file tạm:
python main.py --sync-gdrive

# Tắt Google Drive, chỉ lưu vào thư mục cục bộ được chỉ định:
python main.py --sources all --limit 10 --no-gdrive --output-dir D:\Books
```

## Cơ chế Lưu tiến trình (Stop/Resume Checkpoint)
- **`D:\vn-ebook-crawler\catalog.db`**: Lưu từng đầu sách (`discovered`, `processing`, `completed`, `no_files`, `failed`, `blocked_by_captcha`), định dạng (`file_format`), `gdrive_id`/`gdrive_url` cùng các trang danh mục đã duyệt (`visited_pages`).
- **`crawl_progress.json`**: File JSON tóm tắt tiến trình dễ đọc được tự động đồng bộ lên thư mục gốc `Books/` trên Google Drive.
- Nếu bạn nhấn `Ctrl+C` để dừng giữa chừng, hệ thống tự động đặt lại các cuốn `processing` về `discovered` và đồng bộ checkpoint lên Google Drive trước khi thoát.

## Cấu trúc thư mục trên Google Drive (`Books/`)
```text
Books/
├── crawl_progress.json
└── <nguon>/
    └── <the-loai>/
        └── <ten-sach>.epub (hoặc .pdf nếu không có EPUB)
```
