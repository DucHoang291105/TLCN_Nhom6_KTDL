const { P, H1, H2, H3, bullets, T, setChapter } = require("./lib");
const { unnumberedTitle } = require("./content_front");

function summary() {
  return [
    unnumberedTitle("TÓM TẮT"),
    P("Thị trường bất động sản Việt Nam có hàng trăm nghìn tin rao bán được đăng trên nhiều trang khác nhau. Mỗi trang dùng một cấu trúc dữ liệu riêng, nhiều tin bị đăng lại ở nhiều nơi, giá và diện tích thường bị nhập sai đơn vị, còn đặc điểm quan trọng như pháp lý hay mặt tiền chỉ nằm trong tiêu đề. Người mua vì vậy khó trả lời những câu hỏi rất thực tế: với số tiền đang có thì nên mua ở đâu, một tin rao có đắt so với các căn tương tự hay không, và có khu vực nào rẻ hơn mà vẫn tiện đi làm."),
    P("Tiểu luận xây dựng một **Data Lakehouse** cho dữ liệu tin rao bất động sản theo kiến trúc ba tầng Bronze – Silver – Gold. Dữ liệu được thu thập từ 9 nguồn (3 nguồn thu thập định kỳ hai lần mỗi tuần và 6 nguồn dữ liệu lịch sử), lưu trên MinIO, xử lý bằng Apache Spark, lưu thành bảng Apache Iceberg, truy vấn bằng Trino và trình bày trên 5 dashboard Apache Superset. Toàn bộ quy trình được điều phối bằng một DAG Apache Airflow gồm 26 task; mỗi tầng đều có bước kiểm định tự động, task lỗi sẽ chặn các bước phía sau."),
    P("Tầng Silver chuẩn hóa dữ liệu về một schema chung 27 cột, phân loại bản ghi PASS/WARN/REJECT theo 16 quy tắc chất lượng, lưu lịch sử thay đổi của từng tin theo kiểu SCD loại 2 bằng lệnh `MERGE INTO` của Iceberg, chuẩn hóa vị trí theo 34 tỉnh/thành sau sắp xếp năm 2025 và trích xuất loại hình cùng 5 đặc điểm từ tiêu đề. Tầng Gold tổ chức theo lược đồ hình sao (9 dimension và bảng fact_listing) và các bảng phục vụ ba câu hỏi nghiệp vụ: BQ1 – đánh đổi trong cùng ngân sách (tập tin không bị trội Pareto); BQ2 – vị trí giá so với nhóm tương đồng (P25–P75) và mô hình giá hedonic kết hợp baseline; BQ3 – khu vực thay thế rẻ hơn với quãng đường tới nơi làm/học tương đương."),
    P("Trên dữ liệu tới batch 17/11/2026 (346.558 dòng Bronze, 134.660 tin bán trong Gold), hệ thống vượt qua bộ nghiệm thu chính thức 77/77 điều kiện: 26/26 task Airflow thành công, Silver 30/30 và Gold 138/138 phép kiểm định đạt, hai lần dựng lại cho kết quả giống hệt nhau trên 38 bảng, 543 unit test đạt. Mô hình giá giảm sai số tuyệt đối trung bình 7,4% so với baseline trung vị nhóm (41,8 so với 45,1 triệu đồng/m²). Báo cáo trình bày rõ các giới hạn: giá là giá chào bán, kết quả là tương quan, giả thuyết \"tin rao cao phải hạ giá\" chỉ rõ ở TP.HCM, và nhãn kiểm tra độ chính xác mới có một lượt rà soát."),
    P("**Từ khóa:** Data Lakehouse, Apache Iceberg, Apache Spark, Apache Airflow, bất động sản, chất lượng dữ liệu, mô hình hedonic.", { noIndent: true }),
  ];
}

function moDau() {
  setChapter("0");
  return [
    H1("MỞ ĐẦU"),
    H2("1. Tính cấp thiết của đề tài"),
    P("Bất động sản là tài sản có giá trị lớn nhất mà phần đông hộ gia đình từng mua. Trước khi quyết định, người mua thường phải đọc hàng trăm tin rao trên nhiều trang như Batdongsan, Chợ Tốt, Mogi, Guland hay Nhadatvui. Mỗi trang trình bày khác nhau, cùng một căn có thể được đăng lại nhiều lần với giá khác nhau, giá chào bán thay đổi theo thời gian, và nhiều thông tin quan trọng (sổ hồng, mặt tiền, hẻm xe hơi) chỉ xuất hiện trong tiêu đề. Việc so sánh thủ công vừa tốn thời gian vừa dễ sai."),
    P("Dữ liệu tin rao nếu được thu thập đều đặn, làm sạch và lưu lịch sử thì có thể trả lời những câu hỏi định lượng mà người mua và môi giới thực sự quan tâm: cùng số tiền thì ở quận nào được diện tích lớn hơn, tin đang xem đắt hay rẻ so với các căn tương tự, và nếu không đủ tiền ở khu vực mong muốn thì có lựa chọn nào rẻ hơn mà quãng đường đi làm không xa hơn. Để làm được việc đó cần một nền tảng dữ liệu xử lý được dữ liệu bán cấu trúc từ nhiều nguồn, giữ được lịch sử thay đổi, kiểm soát được chất lượng và cho phép truy vấn phân tích."),
    P("Kiến trúc Data Lakehouse kết hợp ưu điểm của hồ dữ liệu (lưu trữ rẻ, giữ nguyên dữ liệu gốc) và kho dữ liệu (bảng có schema, giao dịch, truy vấn SQL) trên cùng một lớp lưu trữ đối tượng. Đây là hướng được nhiều tổ chức áp dụng trong những năm gần đây và phù hợp với một bài toán có dữ liệu tăng dần theo từng đợt thu thập như tin rao bất động sản. Vì vậy, nhóm chọn đề tài **\"Xây dựng Data Lakehouse phân tích thị trường bất động sản Việt Nam\"**."),
    H2("2. Mục đích của đề tài"),
    bullets([
      "Xây dựng một pipeline dữ liệu hoàn chỉnh từ thu thập đến trực quan hóa cho tin rao bất động sản, chạy tự động theo lịch và tái lập được trên máy khác bằng Docker Compose.",
      "Thiết kế các tầng dữ liệu Bronze – Silver – Gold có schema rõ ràng, kiểm soát chất lượng ở từng bước và lưu được lịch sử thay đổi của mỗi tin.",
      "Trả lời ba câu hỏi nghiệp vụ (BQ1–BQ3) phục vụ người mua và môi giới bằng số liệu có thể kiểm chứng, kèm giới hạn diễn giải rõ ràng.",
      "Xây dựng bộ kiểm định và nghiệm thu để chứng minh kết quả đúng với dữ liệu đầu vào và tái lập được.",
    ]).flat(),
    H2("3. Đối tượng và phạm vi nghiên cứu"),
    P("**Đối tượng nghiên cứu** gồm: kiến trúc Data Lakehouse và các công nghệ mã nguồn mở hiện thực nó (MinIO, Apache Spark, Apache Iceberg, Trino, Apache Airflow, Apache Superset); dữ liệu tin rao bán bất động sản tại Việt Nam; các phương pháp thống kê dùng để so sánh giá (phân vị theo nhóm, hồi quy hedonic, kiểm định tỷ lệ)."),
    P("**Phạm vi dữ liệu:** tin rao **bán** (tin cho thuê bị loại khỏi Gold) từ 3 nguồn thu thập định kỳ (Batdongsan, Guland, Nhadatvui; 48 batch từ 22/09 đến 17/11/2026) và 6 nguồn dữ liệu lịch sử (Chợ Tốt, Mogi, Alonhadat, Luachonnhadat, Muaban, Homedy; 30.317 dòng). Giá trong dữ liệu là **giá chào bán**, không phải giá giao dịch thực tế."),
    P("**Phạm vi hệ thống:** triển khai trên một máy cá nhân bằng Docker Compose; catalog Iceberg dùng SQLite nên Spark (ghi) và Trino (đọc) hoạt động trong hai cửa sổ thời gian tách biệt. Đề tài không xây dựng ứng dụng web cho người dùng cuối ngoài dashboard Superset, không dự báo giá tương lai và không khẳng định quan hệ nhân quả."),
    H2("4. Các công trình và hướng nghiên cứu liên quan"),
    P("Về kiến trúc, Inmon [11] và Kimball [12] đặt nền móng cho kho dữ liệu với mô hình chuẩn hóa và mô hình chiều. Khi dữ liệu bán cấu trúc tăng nhanh, hồ dữ liệu trên Hadoop rồi trên lưu trữ đối tượng trở nên phổ biến nhưng thường thiếu giao dịch và quản lý schema. Armbrust và cộng sự [4] đề xuất khái niệm Lakehouse: dùng định dạng bảng mở có metadata giao dịch đặt trên lưu trữ đối tượng để vừa giữ chi phí thấp vừa hỗ trợ truy vấn SQL và học máy. Apache Iceberg [2] là một định dạng bảng mở như vậy, được dùng trong đề tài."),
    P("Về phân tích giá nhà, mô hình giá hedonic của Rosen [14] coi giá một tài sản là tổng giá trị ngầm của các đặc điểm cấu thành; đây là cơ sở của nhiều chỉ số giá nhà. Hồi quy ridge [10] giúp ổn định ước lượng khi có nhiều biến giả (quận, phường). Việc so sánh một tin với nhóm tương đồng bằng phân vị là cách làm phổ biến của các trang định giá trực tuyến. Truy vấn \"skyline\" của Börzsönyi và cộng sự [5] chính thức hóa khái niệm tin không bị tin khác trội hơn ở mọi tiêu chí, được dùng cho BQ1."),
    P("Các nghiên cứu và sản phẩm trong nước chủ yếu dừng ở mức thu thập tin rao và thống kê giá trung bình theo quận. Đề tài này hướng tới một nền tảng có lịch sử, có kiểm định và trả lời câu hỏi theo góc nhìn ra quyết định của người mua, đồng thời công bố rõ phương pháp và giới hạn của từng kết luận."),
    H2("5. Phương pháp thực hiện"),
    bullets([
      "Khảo sát nguồn dữ liệu, định nghĩa hợp đồng dữ liệu (schema chuẩn 27 cột) và 16 quy tắc chất lượng trước khi viết mã.",
      "Phát triển tăng dần theo tầng: Bronze → Silver → Gold → dashboard; mỗi tầng có script kiểm định riêng và chỉ chuyển sang tầng sau khi kiểm định đạt.",
      "Đặt câu hỏi nghiệp vụ kèm tiêu chí chấp nhận đo được (tài liệu `bq_acceptance_criteria.md`) trước khi xây bảng Gold tương ứng.",
      "Đánh giá mô hình trên tập test tách cố định, chọn tham số bằng kiểm định chéo chỉ trên tập train; đánh giá trích xuất đặc điểm trên mẫu 198 tin gán nhãn thủ công.",
      "Làm việc nhóm qua GitHub (nhánh, pull request, CI chạy unit test trên 3 cấu hình), gắn tag bất biến cho mỗi bản đã nghiệm thu.",
    ]).flat(),
    H2("6. Kết quả đạt được"),
    bullets([
      "Một Data Lakehouse chạy được bằng Docker Compose gồm MinIO, Spark, Iceberg REST Catalog, Trino, Airflow, Superset và PostgreSQL.",
      "38 bảng Iceberg (9 Silver, 29 Gold) cùng 26 task Airflow tự động từ thu thập tới kiểm định.",
      "5 dashboard với 49 biểu đồ, có bộ lọc theo tỉnh, loại hình, ngân sách và nơi làm/học; thẻ kết luận được tính bằng SQL theo lựa chọn đang lọc.",
      "Bộ nghiệm thu chính thức 77/77 điều kiện đạt, tag `v1.1.1-bao-ve`.",
    ]).flat(),
    H2("7. Bố cục báo cáo"),
    P("Ngoài phần Mở đầu và Kết luận, báo cáo gồm bốn chương. **Chương 1** trình bày cơ sở lý thuyết về Data Lakehouse, các công nghệ và phương pháp phân tích được dùng. **Chương 2** phân tích yêu cầu và thiết kế hệ thống: người dùng, câu hỏi nghiệp vụ, kiến trúc, mô hình dữ liệu và điều phối. **Chương 3** mô tả chi tiết cách hiện thực từng tầng, từ thu thập dữ liệu tới dashboard. **Chương 4** trình bày kết quả, đánh giá và các hạn chế. Phần Phụ lục cung cấp hướng dẫn cài đặt, danh sách bảng và phân công công việc."),
  ];
}

function chapter1() {
  setChapter("1");
  return [
    H1("CHƯƠNG 1. CƠ SỞ LÝ THUYẾT"),
    H2("1.1. Từ kho dữ liệu đến Data Lakehouse"),
    H3("1.1.1. Kho dữ liệu (Data Warehouse)"),
    P("Kho dữ liệu là cơ sở dữ liệu tổng hợp, hướng chủ đề, tích hợp từ nhiều nguồn và lưu theo thời gian để phục vụ phân tích [11]. Dữ liệu được làm sạch và biến đổi trước khi nạp (ETL), lưu theo schema cố định, hỗ trợ truy vấn SQL nhanh và nhất quán. Nhược điểm là chi phí lưu trữ cao, khó chứa dữ liệu bán cấu trúc và khó giữ lại dữ liệu gốc chưa xử lý để làm lại khi quy tắc nghiệp vụ thay đổi."),
    H3("1.1.2. Hồ dữ liệu (Data Lake)"),
    P("Hồ dữ liệu lưu dữ liệu ở dạng gốc (CSV, JSON, Parquet…) trên hệ thống tệp phân tán hoặc lưu trữ đối tượng với chi phí thấp, schema chỉ được áp khi đọc. Cách này linh hoạt nhưng thiếu giao dịch ACID, khó cập nhật/xóa từng dòng, khó quản lý phiên bản schema và dễ trở thành \"đầm lầy dữ liệu\" nếu không có quy tắc chất lượng."),
    H3("1.1.3. Data Lakehouse"),
    P("Data Lakehouse [4] đặt một lớp metadata giao dịch lên trên tệp dữ liệu mở trong lưu trữ đối tượng. Lớp này ghi lại bảng gồm những tệp nào ở từng phiên bản (snapshot), nhờ đó hỗ trợ: giao dịch nguyên tử khi ghi; cập nhật, xóa, `MERGE` từng dòng; tiến hóa schema; truy vấn phiên bản cũ (time travel); và cho phép nhiều engine (Spark, Trino) cùng đọc một bảng. Dữ liệu chỉ lưu một bản trên lưu trữ rẻ nhưng vẫn có tính chất của một kho dữ liệu."),
    H3("1.1.4. Kiến trúc nhiều tầng Bronze – Silver – Gold"),
    P("Kiến trúc nhiều tầng (còn gọi là kiến trúc medallion) chia dữ liệu theo mức độ tinh chỉnh. **Bronze** giữ dữ liệu gốc gần như nguyên trạng kèm metadata nguồn và batch, để luôn có thể xử lý lại. **Silver** chứa dữ liệu đã chuẩn hóa, làm sạch, loại trùng, có lịch sử và được làm giàu. **Gold** chứa dữ liệu tổ chức theo nhu cầu phân tích: mô hình chiều, bảng tổng hợp và bảng trả lời trực tiếp câu hỏi nghiệp vụ. Mỗi tầng có một mục đích riêng nên dễ kiểm định và dễ truy vết lỗi về tầng trước."),
    H2("1.2. Các công nghệ sử dụng"),
    H3("1.2.1. Apache Iceberg"),
    P("Apache Iceberg [2] là định dạng bảng mở cho dữ liệu phân tích lớn. Mỗi bảng có một tệp metadata trỏ tới danh sách manifest, mỗi manifest liệt kê các tệp dữ liệu (Parquet) cùng thống kê cột. Mỗi lần ghi tạo một snapshot mới và cập nhật con trỏ metadata một cách nguyên tử thông qua catalog. Iceberg hỗ trợ `MERGE INTO`, tiến hóa schema và partition, time travel theo snapshot và truy vấn từ nhiều engine. Trong đề tài, Iceberg dùng REST Catalog; Spark và Trino cùng trỏ vào catalog này."),
    H3("1.2.2. Apache Spark"),
    P("Apache Spark [18] là engine xử lý dữ liệu phân tán trong bộ nhớ, cung cấp API DataFrame và Spark SQL. Spark đọc Parquet trên MinIO qua giao thức S3, thực hiện các phép biến đổi (chuẩn hóa, join, gom nhóm, cửa sổ) và ghi kết quả ra bảng Iceberg. Đề tài dùng Spark 3.5.9 ở chế độ standalone với một master và một worker trong Docker."),
    H3("1.2.3. MinIO và lưu trữ đối tượng"),
    P("MinIO là hệ thống lưu trữ đối tượng tương thích API Amazon S3, chạy được cục bộ. Dữ liệu được tổ chức theo bucket: `lakehouse-bronze` (Parquet theo nguồn và batch), `lakehouse-silver`, `lakehouse-gold` (bảng Iceberg) và `warehouse` (warehouse mặc định của catalog). Toàn bộ dữ liệu nghiệp vụ của hệ thống nằm trên MinIO."),
    H3("1.2.4. Trino"),
    P("Trino [17] (trước đây là PrestoSQL [15]) là engine truy vấn SQL phân tán, đọc trực tiếp bảng Iceberg mà không cần sao chép dữ liệu. Superset gửi truy vấn tới Trino; Trino đọc metadata từ REST Catalog và đọc tệp Parquet trên MinIO."),
    H3("1.2.5. Apache Airflow"),
    P("Apache Airflow [1] điều phối quy trình dưới dạng đồ thị có hướng không chu trình (DAG). Mỗi nút là một task; Airflow lập lịch, chạy theo thứ tự phụ thuộc, ghi trạng thái và nhật ký từng task. Khi một task thất bại, các task phụ thuộc phía sau chuyển sang trạng thái `upstream_failed` và không chạy. Đề tài dùng Airflow 3.3.1 với cảm biến (sensor) để chờ tệp CSV xuất thủ công."),
    H3("1.2.6. Apache Superset"),
    P("Apache Superset [3] là công cụ trực quan hóa mã nguồn mở. Dataset có thể là bảng hoặc câu SQL ảo; biểu đồ được gắn vào dashboard và có bộ lọc chung. Đề tài cấu hình toàn bộ dataset, biểu đồ, bộ lọc và dashboard bằng mã thông qua REST API, nên dashboard tái tạo được hoàn toàn từ mã nguồn."),
    H3("1.2.7. Docker Compose và Git LFS"),
    P("Docker Compose khai báo các dịch vụ, mạng, volume và biến môi trường trong một tệp duy nhất, giúp dựng lại toàn bộ môi trường trên máy khác. Git LFS (Large File Storage) lưu các tệp CSV đầu vào lớn ngoài lịch sử Git thông thường; repository chỉ giữ con trỏ tới tệp."),
    H2("1.3. Mô hình hóa dữ liệu"),
    H3("1.3.1. Mô hình chiều và lược đồ hình sao"),
    P("Mô hình chiều [12] tổ chức dữ liệu thành bảng **fact** chứa các phép đo ở một mức chi tiết (grain) xác định và các bảng **dimension** chứa ngữ cảnh mô tả (thời gian, vị trí, loại hình…). Lược đồ hình sao đặt bảng fact ở trung tâm, mỗi dimension nối trực tiếp với fact bằng khóa thay thế. Mô hình này dễ hiểu với người phân tích và tối ưu cho truy vấn gom nhóm."),
    H3("1.3.2. Lưu lịch sử thay đổi theo SCD loại 2"),
    P("Slowly Changing Dimension loại 2 (SCD2) giữ toàn bộ lịch sử thay đổi bằng cách tạo một phiên bản mới mỗi khi nội dung nghiệp vụ thay đổi; mỗi phiên bản có thời điểm hiệu lực và cờ phiên bản hiện hành. Với tin rao, mỗi lần giá, diện tích hay tiêu đề thay đổi giữa hai batch sẽ sinh một phiên bản; nhờ đó có thể tính các sự kiện đổi giá và vòng đời quan sát của tin."),
    H3("1.3.3. Chất lượng dữ liệu"),
    P("Chất lượng dữ liệu được đo theo các chiều: đầy đủ (không thiếu trường bắt buộc), hợp lệ (giá trị trong miền cho phép), nhất quán (giá/m² khớp giá chia diện tích), duy nhất (không trùng) và kịp thời. Đề tài gắn mỗi bản ghi một trạng thái PASS, WARN hoặc REJECT kèm mã lý do, giữ bản ghi lỗi trong bảng cách ly (quarantine) thay vì xóa, để có thể truy vết ngược về tệp Bronze."),
    H2("1.4. Các phương pháp phân tích"),
    H3("1.4.1. So sánh với nhóm tương đồng bằng phân vị"),
    P("Một tin được so sánh với các tin cùng vị trí, cùng loại hình, cùng nhóm diện tích và nhóm số phòng. Phân vị thứ 25 (P25) và thứ 75 (P75) của giá/m² trong nhóm chia các tin thành ba vị trí: dưới P25, trong P25–P75 và trên P75. Khi nhóm có ít hơn 5 tin, hệ thống lùi dần về nhóm rộng hơn (bỏ nhóm số phòng, rồi bỏ nhóm diện tích). Cách so sánh này dễ giải thích nhưng chỉ là mô tả: nằm trong P25–P75 không chứng minh giá hợp lý."),
    H3("1.4.2. Mô hình giá hedonic và hồi quy ridge"),
    P("Mô hình hedonic [14] ước lượng giá như hàm của các đặc điểm. Đề tài dùng dạng log-tuyến tính: `log(giá/m²) = β0 + Σ βk·xk + ε`, trong đó xk là biến giả của vị trí, nhóm diện tích, nhóm số phòng, 5 cờ đặc điểm và các biến mở rộng. Khi đó `exp(βk) − 1` là phần trăm chênh giá/m² gắn với đặc điểm k khi các yếu tố khác giữ nguyên. Hồi quy ridge [10] cộng thêm phạt λ·||β||² để ổn định nghiệm khi có nhiều biến giả thưa; nghiệm thu được bằng phương trình chuẩn `(XᵀX + λI)β = Xᵀy` giải bằng phân rã Cholesky."),
    P("Dự đoán cuối cùng trộn mô hình hedonic với baseline trung vị nhóm trong không gian log: `log(dự đoán) = w·log(mô hình) + (1 − w)·log(baseline)`, với trọng số w riêng cho từng loại hình. Kết hợp như vậy giữ được ưu điểm của baseline ở nhóm đông tin và của mô hình ở nhóm thưa."),
    H3("1.4.3. Đánh giá mô hình"),
    P("Dữ liệu được chia cố định 80/20 thành tập train và test theo giá trị băm của mã tin, nên mỗi tin luôn rơi vào cùng một tập qua các lần chạy. Các tham số (w, λ) được chọn bằng kiểm định chéo 5 phần (5-fold cross-validation) chỉ trên tập train, sau đó tập test chỉ dùng một lần để báo cáo [9]. Các chỉ số gồm MAE (sai số tuyệt đối trung bình), MdAPE (trung vị sai số tương đối |thật − dự đoán| / thật) và R² trên thang log. Độ bất định của chênh lệch sai số giữa mô hình và baseline được ước lượng bằng bootstrap ghép cặp [8]: lấy mẫu lặp lại các tin test, tính chênh lệch trung bình, lấy khoảng 2,5%–97,5%."),
    H3("1.4.4. Kiểm định tỷ lệ"),
    P("Để kiểm tra giả thuyết \"tin rao cao hơn nhóm lúc mới đăng thì hay phải hạ giá hơn\", hệ thống so sánh tỷ lệ hạ giá giữa hai nhóm bằng kiểm định z hai tỷ lệ. Vì các nguồn có nhịp cập nhật giá khác nhau, kiểm định Cochran–Mantel–Haenszel (CMH) [13] được dùng để so sánh trong từng tầng (nguồn, hoặc nguồn × tỉnh) rồi gộp lại thành tỷ số odds chung. Khi kiểm định nhiều giả thuyết chính, ngưỡng ý nghĩa được hiệu chỉnh Bonferroni [7], ví dụ 0,05/3 cho ba giả thuyết."),
    H3("1.4.5. Tối ưu Pareto và khoảng cách haversine"),
    P("Trong cùng một nhóm ngân sách và quận, tin A **trội hơn** tin B nếu A không tệ hơn B ở mọi tiêu chí (giá thấp hơn hoặc bằng, diện tích và số phòng lớn hơn hoặc bằng, đặc điểm không ít hơn) và tốt hơn ở ít nhất một tiêu chí. Tập các tin không bị tin nào trội hơn là tập Pareto (skyline [5]); đây là danh sách nên xem trước. Khoảng cách giữa hai tọa độ được tính bằng công thức haversine [16] (khoảng cách đường chim bay trên mặt cầu), không phải quãng đường giao thông thực tế."),
    H3("1.4.6. Đo độ chính xác trích xuất đặc điểm"),
    P("Độ chính xác của luật trích xuất đặc điểm từ tiêu đề được đo trên mẫu có nhãn thủ công. Với mỗi cờ: precision = TP / (TP + FP) cho biết trong các tin hệ thống bật cờ, bao nhiêu phần trăm đúng; recall = TP / (TP + FN) cho biết trong các tin thật sự có đặc điểm, hệ thống bắt được bao nhiêu. Độ nhất quán giữa các lần gán nhãn có thể đo bằng hệ số kappa của Cohen [6]."),
    H2("1.5. Tiểu kết chương 1"),
    P("Chương 1 đã trình bày nền tảng của kiến trúc Lakehouse, vai trò của từng công nghệ trong hệ thống và các phương pháp phân tích được dùng để trả lời câu hỏi nghiệp vụ. Chương 2 sẽ áp dụng các nền tảng này vào phân tích yêu cầu và thiết kế hệ thống cụ thể."),
  ];
}

module.exports = { summary, moDau, chapter1 };
