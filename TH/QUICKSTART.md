# 🚀 Quick Start Guide - Job Market Analytics

## Bắt đầu nhanh trong 3 bước

### Bước 1: Cài đặt (1 phút)

```bash
# Clone hoặc cd vào thư mục project
cd tichhop-git

# Tạo và kích hoạt virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# hoặc
venv\Scripts\activate     # Windows

# Cài đặt dependencies
pip install -r requirements.txt
```

### Bước 2: Chạy Pipeline (30 giây)

```bash
python json_export_pipeline.py
```

**Kết quả:** 
- ✅ Dữ liệu đã xử lý: `output/processed_jobs_*.json`
- ✅ Báo cáo phân tích: `output/analytics_report_*.json`
- ✅ Tóm tắt: `output/pipeline_summary.json`

### Bước 3: Xem kết quả

**Option A: Dashboard (Recommended)**
```bash
streamlit run dashboard/simple_app.py
```
Mở trình duyệt: http://localhost:8501

**Option B: API**
```bash
uvicorn api.simple_main:app --reload
```
Xem docs: http://localhost:8000/docs

**Option C: Xem file JSON trực tiếp**
```bash
# Xem file processed data
cat output/processed_jobs_*.json | jq '.[0:3]'

# Xem analytics report
cat output/analytics_report_*.json | jq '.analytics_summary'
```

---

## 📊 Các tính năng chính

### Dashboard
- 📋 Phân bố công việc theo vị trí, ngành nghề
- 🌍 Phân tích địa lý theo quốc gia, thành phố
- 💰 Phân tích và so sánh mức lương
- 📈 Xu hướng thị trường việc làm
- 🔧 Top skills được yêu cầu
- 💾 Export dữ liệu ra CSV/JSON

### API Endpoints
```bash
# Lấy tất cả jobs
curl http://localhost:8000/api/jobs?limit=10

# Dự đoán lương cho Data Scientist ở Hà Nội
curl "http://localhost:8000/api/analytics/salary-prediction?job_title=Data%20Scientist&location=Hà%20Nội"

# Phân tích top skills
curl http://localhost:8000/api/analytics/skills

# Tổng quan analytics
curl http://localhost:8000/api/analytics/summary
```

---

## 🔄 Workflow thông thường

```
1. Có dữ liệu mới
   ⬇️
2. Chạy: python json_export_pipeline.py
   ⬇️
3. Dashboard/API tự động load file mới nhất
   ⬇️
4. Phân tích và visualize
```

---

## 💡 Tips

**Reload dữ liệu trong Dashboard:**
- Nhấn nút "🔄 Tải lại dữ liệu" trong sidebar

**Reload dữ liệu trong API:**
```bash
curl -X POST http://localhost:8000/api/reload
```

**Filter dữ liệu trong Dashboard:**
- Dùng sidebar để chọn nguồn, vị trí, thành phố
- Dashboard sẽ tự động update charts

**Xem log pipeline:**
Pipeline sẽ in ra console các thông tin chi tiết về:
- Số records đã load
- Data quality metrics
- Analytics results
- File paths

---

## ❓ Troubleshooting

**Pipeline chạy lâu?**
- Bình thường: 10-30 giây cho 25K records
- Nếu >1 phút, kiểm tra dung lượng file data

**Dashboard không hiển thị dữ liệu?**
- Chạy pipeline trước: `python json_export_pipeline.py`
- Đảm bảo có file trong `output/`

**API trả về empty data?**
- Reload data: `curl -X POST http://localhost:8000/api/reload`
- Hoặc restart API

---

## 🎯 Next Steps

1. **Khám phá Dashboard**: Thử các filter khác nhau
2. **Test API**: Xem API docs tại `/docs`
3. **Customize**: 
   - Thêm charts mới trong `dashboard/simple_app.py`
   - Thêm endpoints mới trong `api/simple_main.py`
4. **Jupyter Notebooks**: Phân tích sâu hơn trong `notebooks/`

---

**Happy Analyzing! 📊✨**

Xem thêm: [README.md](README.md) để biết chi tiết

