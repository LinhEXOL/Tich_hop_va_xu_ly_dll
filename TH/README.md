# 🚀 Job Market Analytics (Simplified - JSON-based)

## 📋 Tổng quan Dự án

Dự án phân tích dữ liệu thị trường việc làm từ 3 nguồn khác nhau với kiến trúc đơn giản dựa trên file JSON.

### 🎯 Mục tiêu
- ✅ Tích hợp và làm sạch dữ liệu từ 3 nguồn: CareerLink, Joboko, TopCV
- ✅ Phân tích toàn diện thị trường việc làm tại Việt Nam
- ✅ Xuất dữ liệu đã xử lý ra file JSON (không cần database)
- ✅ Cung cấp API và Dashboard để truy vấn và visualize dữ liệu

### ✨ Điểm khác biệt phiên bản mới
- **Không cần PostgreSQL hay MongoDB** - Tất cả dữ liệu được lưu trong file JSON
- **Đơn giản hơn** - Dễ setup và chạy
- **Linh hoạt** - Dữ liệu JSON dễ chia sẻ và xử lý
- **Nhanh hơn** - Không cần quản lý database

## 📊 Dataset

| Dataset | Nguồn | Kích thước | Mô tả |
|---------|-------|------------|-------|
| `data_careerlink.json` | CareerLink | ~15,772 records | Việc làm IT tại Việt Nam |
| `data_joboko.json` | Joboko | ~7,522 records | Việc làm đa dạng tại Việt Nam |
| `data_topcv.json` | TopCV | ~2,420 records | Việc làm IT/CNTT tại Việt Nam |

### Cấu trúc Dữ liệu
- **CareerLink**: Tên công việc, Tên công ty, Địa điểm, Mức lương, Kinh nghiệm, Mô tả, Kỹ năng yêu cầu
- **Joboko**: Tên công việc, Tên công ty, Địa điểm, Mức lương, Kinh nghiệm, Mô tả, Kỹ năng yêu cầu, Ngành nghề
- **TopCV**: Tên công việc, Tên công ty, Địa điểm, Mức lương, Kinh nghiệm, Mô tả, Kỹ năng yêu cầu, Quyền lợi

## 🏗️ Kiến trúc Hệ thống (Simplified)

```
📥 DATA SOURCES (JSON Files)
├── data/data_careerlink.json
├── data/data_joboko.json
└── data/data_topcv.json
         ⬇️
🔄 ETL PIPELINE (json_export_pipeline.py)
├── Load: DataLoader
├── Clean: DataCleaner
├── Transform: Schema Standardization
└── Analytics: ComprehensiveAnalyzer
         ⬇️
💾 OUTPUT (JSON Files)
├── output/processed_jobs_TIMESTAMP.json       (Dữ liệu đã xử lý)
├── output/analytics_report_TIMESTAMP.json     (Báo cáo phân tích)
└── output/pipeline_summary.json               (Tóm tắt pipeline)



## 🛠️ Công nghệ Sử dụng

### Core Technologies
- **Python 3.8+**: Ngôn ngữ chính
- **Pandas**: Data manipulation và analysis
- **NumPy**: Numerical computing
- **Scikit-learn**: Machine learning

### Storage
- **JSON Files**: Lưu trữ dữ liệu đơn giản, dễ chia sẻ

### Analytics & Visualization
- **Plotly**: Interactive charts
- **Streamlit**: Web dashboard
- **FastAPI**: REST API
- **Jupyter**: Interactive analysis

### Infrastructure
- **Docker** (optional): Containerization
- **Git**: Version control

## 🚀 Cài đặt và Chạy Dự án

### Yêu cầu Hệ thống
- Python 3.8+ 
- pip (Python package manager)
- (Optional) Docker & Docker Compose

### Cách 1: Chạy trực tiếp (Khuyên dùng - Đơn giản nhất)

**Bước 1: Cài đặt dependencies**

```bash
# Tạo virtual environment (khuyên dùng)
python -m venv venv

# Kích hoạt virtual environment
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# Cài đặt thư viện
pip install --upgrade pip
pip install -r requirements.txt
```

**Bước 2: Chuẩn bị dữ liệu**

Đảm bảo các file dữ liệu có trong thư mục `data/`:
- `data/data_careerlink.json`
- `data/data_joboko.json`
- `data/data_topcv.json`

**Bước 3: Chạy ETL Pipeline**

```bash
python json_export_pipeline.py
```

Pipeline sẽ:
- ✅ Load dữ liệu từ 3 nguồn JSON
- ✅ Làm sạch và chuẩn hóa dữ liệu
- ✅ Chạy các phân tích toàn diện
- ✅ Xuất kết quả ra thư mục `output/`:
  - `processed_jobs_TIMESTAMP.json` - Dữ liệu đã xử lý
  - `analytics_report_TIMESTAMP.json` - Báo cáo phân tích
  - `pipeline_summary.json` - Tóm tắt pipeline

### Cách 2: Chạy với Docker

**Bước 1: Build và chạy containers**

```bash
# Chạy pipeline bên ngoài container
python json_export_pipeline.py

# Hoặc chạy trong container
docker-compose exec api python json_export_pipeline.py
```

## 📁 Cấu trúc Thư mục

```
tichhop-git/
├── data/                           # 📥 Dữ liệu nguồn (JSON)
│   ├── data_careerlink.json
│   ├── data_joboko.json
│   └── data_topcv.json
│
├── output/                         # 💾 Dữ liệu đã xử lý (JSON)
│   ├── processed_jobs_*.json       # Dữ liệu đã làm sạch
│   ├── analytics_report_*.json    # Báo cáo phân tích
│   └── pipeline_summary.json      # Tóm tắt pipeline
│
├── src/                           # 📦 Source code
│   ├── etl/                      # ETL modules
│   │   ├── data_loader.py        # Load dữ liệu
│   │   ├── data_cleaner.py       # Làm sạch dữ liệu
│   │   └── schema_matcher.py     # Schema matching
│   └── analytics/                # Analytics modules
│       ├── comprehensive_analyzer.py
│       ├── trend_analyzer.py
│       ├── salary_predictor.py
│       └── ...
│
├── api/                           # 🌐 FastAPI endpoints
│   ├── simple_main.py            # API đọc từ JSON
│   └── ...
│
├── notebooks/                     # 📓 Jupyter notebooks
│   └── 01_data_exploration.ipynb
│
├── config/                        # ⚙️ Configuration files
│   └── config.yaml
│
├── docker/                        # 🐳 Docker configurations
│   ├── Dockerfile.api
│   ├── Dockerfile.dashboard
│   └── Dockerfile.jupyter
│
├── json_export_pipeline.py        # 🚀 Main ETL pipeline
├── requirements.txt               # 📋 Python dependencies
├── docker-compose.yml             # 🐳 Docker compose
└── README.md                      # 📖 This file
```

## 📊 Kết quả và Tính năng

### Pipeline Output
Sau khi chạy `json_export_pipeline.py`, bạn sẽ có:

1. **processed_jobs_TIMESTAMP.json**: Dữ liệu đã làm sạch với các trường chuẩn hóa:
   - job_title_clean, company_name, location_clean
   - city, country, salary_min, salary_max
   - skills, experience, industry, job_description
   - Và nhiều trường khác...

2. **analytics_report_TIMESTAMP.json**: Báo cáo phân tích toàn diện:
   - Phân tích xu hướng (Trend Analysis)
   - Dự đoán lương (Salary Prediction)
   - Phân tích cảm xúc (Sentiment Analysis)
   - Phát hiện gian lận (Fraud Detection)
   - Và nhiều phân tích khác...

3. **pipeline_summary.json**: Tóm tắt kết quả chạy pipeline

### Dashboard Features
- 📋 **Phân bố công việc**: Top job titles, industries
- 🌍 **Phân tích địa lý**: Jobs by country/city
- 💰 **Phân tích lương**: Salary distribution, top paying jobs
- 📈 **Xu hướng**: Source distribution, experience requirements
- 🔧 **Phân tích kỹ năng**: Top skills demanded
- 💾 **Export**: Download filtered data as CSV/JSON

### API Endpoints

**Cơ bản:**
- `GET /` - Thông tin API
- `GET /health` - Health check
- `POST /api/reload` - Reload dữ liệu từ JSON

**Dữ liệu:**
- `GET /api/jobs` - Lấy danh sách việc làm (có filter)

**Phân tích:**
- `GET /api/analytics/summary` - Tổng quan phân tích
- `GET /api/analytics/trends` - Xu hướng thị trường
- `GET /api/analytics/salary-prediction` - Dự đoán mức lương
- `GET /api/analytics/skills` - Phân tích kỹ năng
- `GET /api/analytics/geographic` - Phân tích địa lý

API docs chi tiết: `http://localhost:8000/docs`

## 🎯 Use Cases

### 1. Phân tích thị trường việc làm
- Xem xu hướng tuyển dụng theo ngành, địa điểm
- So sánh mức lương giữa các vị trí
- Phân tích kỹ năng được yêu cầu nhiều nhất

### 2. Dự đoán lương
- API `/api/analytics/salary-prediction` cung cấp dự đoán lương dựa trên:
  - Tên công việc
  - Địa điểm
  - Kinh nghiệm
  - Ngành nghề

### 3. Tìm kiếm và lọc công việc
- API `/api/jobs` cho phép filter theo nhiều tiêu chí
- Dashboard cung cấp UI trực quan để explore data

### 4. Export và share data
- Dữ liệu JSON dễ dàng chia sẻ
- Dashboard có chức năng download CSV/JSON

## 🔧 Customization

### Thêm nguồn dữ liệu mới
1. Thêm file JSON vào thư mục `data/`
2. Cập nhật `config/config.yaml`
3. (Nếu cần) Thêm cleaning method trong `src/etl/data_cleaner.py`

### Thêm phân tích mới
1. Tạo module mới trong `src/analytics/`
2. Import và sử dụng trong `json_export_pipeline.py`

### Tùy chỉnh Dashboard/API
- Sửa `dashboard/simple_app.py` để thêm charts mới
- Sửa `api/simple_main.py` để thêm endpoints mới

## 📈 Performance

- **Processing Speed**: ~10-30 giây cho 25,000+ records
- **File Size**: 
  - Processed data: ~10-20 MB
  - Analytics report: ~1-5 MB
- **Dashboard Load Time**: <2 giây
- **API Response Time**: <500ms

## ❓ FAQ

**Q: Tại sao không dùng database?**
A: Để đơn giản hóa dự án. File JSON đủ cho dataset cỡ nhỏ-trung bình (<100K records), dễ chia sẻ và không cần setup database.

**Q: Làm sao để update dữ liệu?**
A: Chạy lại `python json_export_pipeline.py`. Dashboard và API sẽ tự động load file JSON mới nhất.

**Q: Có thể dùng với dữ liệu lớn hơn không?**
A: Với >100K records, nên cân nhắc dùng database (PostgreSQL) hoặc Parquet files để tối ưu performance.

**Q: Làm sao để deploy lên production?**
A: 
1. Sử dụng Docker: `docker-compose up -d`
2. Hoặc deploy trên cloud (Heroku, AWS, GCP, Azure)
3. Setup cron job để chạy pipeline định kỳ

**Q: File cũ trong output/ có bị ghi đè không?**
A: Không! Mỗi lần chạy pipeline sẽ tạo file mới với timestamp khác nhau.

## 🐛 Troubleshooting

**Lỗi: "No module named 'src'"**
```bash
# Đảm bảo đang ở thư mục gốc của project
cd /path/to/tichhop-git
python json_export_pipeline.py
```

**Lỗi: "File not found: data/data_careerlink.json"**
```bash
# Kiểm tra file tồn tại
ls data/
# Đảm bảo các file JSON có trong thư mục data/
```

**Dashboard không hiển thị dữ liệu**
```bash
# Chạy pipeline trước
python json_export_pipeline.py
# Sau đó chạy dashboard
streamlit run dashboard/simple_app.py
```

## 📄 License

MIT License

## 📞 Contact

- **Author**: Nguyễn Thuỳ Dương
- **Email**: Duong.NT252022M@sis.hust.edu.vn

---

## 📚 Tech Stack References

- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Plotly Documentation](https://plotly.com/python/)

---

**✨ Dự án phân tích thị trường việc làm - Phiên bản đơn giản với JSON**

*Last updated: 2025*
