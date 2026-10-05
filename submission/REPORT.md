# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Đinh Đức Thái / 2A202602648
**Repo:** https://github.com/ducthais/K4-Track02-Day17-DinhDucThai-2A202602648-Data-Pipeline-Engineering
**Commit bài nộp:** 1a867e0
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity IDE (Gemini 3.8 Flash) — hỗ trợ phân tích đề bài, xác định 3 lỗi trong pipeline (Silver key, Late data, CDC delete), viết code sửa lỗi trong pipeline/, triển khai cache/quarantine cho bonus B1 (pipeline/llm_label.py), chạy verify/pytest/rerun/dbt/parity và soạn thảo báo cáo REPORT.md.
**Nguồn tham khảo khác (nếu có):** Slide bài giảng Ngày 17 (Data Pipeline Engineering).

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `verify`: fail `silver_tickets has exactly one row per ticket_id` (24 rows cho 12 tickets); fail `T-91 shows its latest state` (nhận 3 tuples cả cũ lẫn mới). `pytest`: fail `test_silver_tickets_one_row_per_ticket` (assert 24 == 12). | `verify`: fail `gold_feature_daily reconciles with a full recompute` (`c50b8851affe != 8630e04a61d1`); fail `u05's offline events of 08-12 (arrived 08-15) are counted on 08-12` (nhận (2, 0) thay vì (5, 1)); fail `LOOKBACK_DAYS covers measured P99 lateness` (0 < 3). | `verify`: fail `deleted ticket T-97 is a tombstone` (vẫn còn 2 dòng, `is_deleted=False`, nguyên PII); fail snapshot `v2026-08-16` còn T-97 (1 row); fail doc chunks còn T-97 (2 chunks). `rerun_check`: FAIL lệch Gold checksum (C0 != C1 != C2 != C3). |
| **Nguyên nhân gốc** | `upsert_silver_tickets` (`pipeline/silver.py`) dùng `INSERT INTO` thuần tuý, không định danh khoá và không xét thứ tự `_lsn`, làm lặp hàng và trạng thái cũ đè/song hành trạng thái mới. | `LOOKBACK_DAYS = 0` (`pipeline/config.py`), daily run chỉ tính ngày ingest; event ngoại tuyến 08-12 của `u05` đến trễ vào 08-15 (lateness=3d) bị bỏ qua, không tính lại partition 08-12. | `ticket_changes_sql` (`pipeline/staging.py`) chỉ lấy `ticket_id` từ `after` (`null` khi `_op='d'`), mệnh đề `WHERE ticket_id IS NOT NULL` nuốt chửng bản ghi xoá, không tạo tombstone ở Silver và không lan truyền xuống Gold. |
| **Cách sửa** (file, vài dòng) | Sửa `pipeline/silver.py`: thay `INSERT INTO` bằng `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`. | Sửa `pipeline/config.py`: đổi `LOOKBACK_DAYS = 0` thành `LOOKBACK_DAYS = 3`, khớp với `math.ceil(p99)` đo từ Bronze (`p99 = 3.00` ngày) để recompute partition `[day - 3, day]`. | Sửa `pipeline/staging.py`: trích xuất `ticket_id` bằng `coalesce(after.ticket_id, before.ticket_id, key.ticket_id)`; ở Silver thành tombstone (PII null), Gold training snapshot loại qua `_op <> 'd'`, Gold RAG loại qua `NOT is_deleted`. |
| **Khái niệm trên slide** | Silver — Có khoá; Keyed Upsert / MERGE; LSN guard chống ghi đè khi out-of-order replay; Idempotent writes. | Data về muộn; Event time vs Ingest time; Lookback window = ceil(P99); Overwrite-partition theo event time. | CDC log-based (Debezium envelope before/after/op/lsn); Tombstone ở Silver giữ LSN chống hồi sinh khi replay; Xoá phải lan truyền (Deletes propagate). |

### Nhận diện ba câu chuyện trong dữ liệu seed (Baseline Checkpoint 1)

1. **T-91 cập nhật nhiều lần (Lỗi Silver — Keyed Upsert / LSN):**
   - *Câu chuyện seed:* `T-91` tạo ngày 08-10 (`low/open`), cập nhật ngày 08-14 (`high/open` — bị Kafka redeliver gửi lặp 2 bản ghi cùng offset 16), và cập nhật ngày 08-16 (`high/closed/bug`).
   - *Triệu chứng:* Bảng `silver_tickets` đang append đơn thuần (`INSERT INTO`), không có khoá định danh và không xét LSN. Dẫn đến `T-91` có 3 dòng riêng biệt, toàn bảng phình lên 24 dòng cho 12 tickets; khi chạy lại batch cũ thì trạng thái cũ có nguy cơ đè lên trạng thái mới.
2. **Sự kiện của u05 đến muộn (Lỗi Late Data — Event Time vs Ingest Time & Lookback):**
   - *Câu chuyện seed:* Khách hàng `u05` mất mạng trên tàu tối 08-12; các sự kiện click (`e-0017`, `e-0018`) và feedback 👎 (`e-0019`) cho `T-88` diễn ra ngày 08-12 nhưng mãi đến ngày 08-15 mới tới Kafka (trễ đúng 3 ngày).
   - *Triệu chứng:* Cấu hình `LOOKBACK_DAYS = 0`, pipeline chỉ xử lý dữ liệu theo ngày ingest. Khi xử lý batch 08-15, phân vùng ngày 08-12 không được tính toán lại theo `event_time`, khiến feature ngày 08-12 của `u05` chỉ có `(2 events, 1 click, 0 feedback_down)` thay vì `(5 events, 3 clicks, 1 feedback_down)`.
3. **T-97 bị xoá theo yêu cầu (Lỗi CDC Delete — Tombstone Propagation):**
   - *Câu chuyện seed:* Khách hàng `u06` tạo ticket `T-97` ngày 08-11 kèm thông tin nhạy cảm PII (email, sđt) với yêu cầu xoá tài khoản, đóng ngày 08-12. Ngày 08-15, Debezium gửi sự kiện xoá `op = 'd'` với `after = null` và kèm Kafka tombstone (`value = null`).
   - *Triệu chứng:* Mã nguồn staging chỉ parse trường `after` (vốn là `null` khi delete), khiến `silver_tickets` không ghi nhận tombstone (`is_deleted = True`, xoá thông tin cá nhân). Xoá không lan truyền xuống Gold: training snapshot mới nhất `v2026-08-16` và `gold_doc_chunks` vẫn chứa `T-97`. Khi re-run ngày 08-12, doc chunks bị ghi lặp (22 rows / 9 chunks) làm checksum Gold thay đổi liên tục.

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (chi tiết: `p50=0.00`, `p95=2.90`, `p99=3.00`, `max=3` ngày) → `LOOKBACK_DAYS = 3` (cấu hình ban đầu: `LOOKBACK_DAYS = 0`)
- `submission/checksums.txt`: PASS — Gold checksum fresh build và 3 lần re-run ngày 2026-08-12 hoàn toàn đồng nhất (C0 = C1 = C2 = C3 = `39e115c510ecdf526800eac227158a4f`).
- `make parity`: PARITY — cả hai pipeline (DuckDB Python & dbt) đều cho kết quả giống hệt nhau trên 2 bảng chung: `silver_tickets` và `gold_feature_daily`.

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: Vì `silver_tickets` là bảng thực thể cần cập nhật từng dòng theo LSN mới nhất, còn `gold_feature_daily` là bảng tổng hợp theo ngày (`event_date`) nên ghi đè toàn bộ partition trong cửa sổ lookback vừa nhanh, đơn giản và đảm bảo tính idempotent.
- Tombstone thay vì xoá hẳn hàng trong Silver: Vì lưu tombstone (`is_deleted = True` kèm LSN cao nhất và xoá trắng PII) giúp ngăn việc chạy lại (replay) các batch CDC cũ vô tình chèn lại ("hồi sinh") bản ghi đã xoá.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Vì mô hình học máy cần tính tái lập (reproducibility) tuyệt đối để audit/debug hành vi mô hình tại thời điểm huấn luyện trong quá khứ, nên snapshot cũ phải bất biến và chỉ sinh phiên bản mới khi có cập nhật.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Vì quy mô dữ liệu trong phạm vi gigabytes chạy in-process với DuckDB/dbt cho tốc độ tức thì, zero-overhead quản trị hạ tầng cụm phân tán như Spark nhưng vẫn đầy đủ tính năng SQL phân tích hiện đại.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   *Trả lời:* Đây là mâu thuẫn kinh điển giữa tính tái lập mô hình (Data Reproducibility / Lineage) và tuân thủ pháp lý (GDPR Article 17 - Right to be Forgotten / CCPA). Về mặt pháp lý, quyền riêng tư luôn có độ ưu tiên cao hơn yêu cầu kỹ thuật. Để dung hoà, ta áp dụng giải pháp phân tách danh tính bằng Cryptographic Shredding: trong training snapshot, dữ liệu cá nhân nhạy cảm được mã hoá bằng khoá riêng của từng user (`user_key`). Khi có yêu cầu xoá dữ liệu, ta chỉ cần huỷ khoá mã hoá của người dùng đó; toàn bộ dữ liệu lịch sử trong các snapshot bất biến lập tức trở thành chuỗi nhị phân vô nghĩa vĩnh viễn mà không cần ghi đè hay phá vỡ cấu trúc file snapshot cũ. Nếu bắt buộc phải loại bỏ cả văn bản, ta tạo một phiên bản snapshot mới có hậu tố sửa đổi (`v2026-08-12-redacted`) kèm biên bản kiểm toán (compliance log), và kích hoạt quy trình Machine Unlearning hoặc tái huấn luyện mô hình từ checkpoint gần nhất.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   *Trả lời:*
   - *Vị trí và kỹ thuật chốt:* Chốt chặn PII phải được đặt ngay tại cửa ngõ Staging (Bronze sang Silver) trước khi dữ liệu được lưu trữ hay phân phối cho hạ tầng downstream. Thay vì chỉ dùng Regex (chỉ bắt được mẫu có cấu trúc định dạng chuẩn như email/sđt), ta bổ sung mô hình Named Entity Recognition (NER) đa ngữ (như PhoBERT / SpaCy / Microsoft Presidio) kết hợp danh bạ từ điển tên người/địa danh (Gazetteer) để nhận diện các thực thể phi cấu trúc (`PER`, `LOC`, `ORG`).
   - *Cách đo lường:* Xây dựng tập dữ liệu benchmark kiểm thử (Golden Test Set) gồm các mẫu văn bản hỗ trợ khách hàng thực tế đã được gán nhãn PII thủ công. Đo lường bằng 2 chỉ số: (1) **Recall (Độ bao phủ PII):** phải đạt tối thượng (> 99.5%) vì rò rỉ một thực thể PII (False Negative) gây rủi ro pháp lý nghiêm trọng hơn nhiều so với che nhầm; (2) **Precision (Độ chính xác):** đảm bảo không làm mất ngữ cảnh ngữ nghĩa cần thiết cho mô hình RAG / Classifier. Định kỳ chạy quét mẫu ngẫu nhiên (sampling audit) tự động trên Silver/Gold để đảm bảo `PII Leakage Rate = 0`.

## 5. Output (dán nguyên văn)

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 4.23s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ Push-Location dbt_project; ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17; Pop-Location
02:53:14  Running with dbt=1.12.5
02:53:14  Registered adapter: duckdb=1.11.0
02:53:15  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
02:53:15  
02:53:15  Concurrency: 1 threads (target='dev')
02:53:15  
02:53:15  1 of 19 START sql view model main.stg_events ................................... [RUN]
02:53:15  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.11s]
02:53:15  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
02:53:15  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.13s]
02:53:15  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
02:53:16  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.16s]
02:53:16  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
02:53:16  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.16s]
02:53:16  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
02:53:16  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.18s]
02:53:16  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
02:53:16  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.05s]
02:53:16  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
02:53:16  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
02:53:16  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
02:53:16  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
02:53:16  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
02:53:16  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.04s]
02:53:16  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
02:53:16  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.03s]
02:53:16  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
02:53:16  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.04s]
02:53:16  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
02:53:16  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.03s]
02:53:16  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
02:53:16  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
02:53:16  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
02:53:16  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
02:53:16  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
02:53:16  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
02:53:16  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
02:53:16  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
02:53:16  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.05s]
02:53:16  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
02:53:17  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.15s]
02:53:17  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
02:53:17  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.04s]
02:53:17  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
02:53:17  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.17s]
02:53:17  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
02:53:17  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.05s]
02:53:17  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
02:53:17  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.04s]
02:53:17  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
02:53:17  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.04s]
02:53:17  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.59s]
02:53:17  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
02:53:17  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.04s]
02:53:17  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
02:53:17  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
02:53:17  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
02:53:17  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
02:53:17  
02:53:17  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.97 seconds (1.97s).
02:53:17  
02:53:17  Completed successfully
02:53:17  
02:53:17  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree

$ .\.venv\Scripts\python.exe -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

