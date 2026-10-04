# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Trần Quốc Sáng / 2A202602712
**Repo:** https://github.com/foxxiee04/K4-Track02-Day17-TranQuocSang-2A202602712-Data-Pipeline-Engineering
**Commit bài nộp:** "done"
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** ChatGPT — hỗ trợ phân tích lỗi, giải thích CDC/idempotency/late data, hướng dẫn kiểm thử và review thay đổi; toàn bộ thay đổi được kiểm tra lại bằng verify, pytest, rerun và dbt parity.
**Nguồn tham khảo khác (nếu có):** 

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có nhiều hàng cùng `ticket_id`; T-91 không đảm bảo latest state. | `gold_feature_daily` lệch full recompute; event u05 xảy ra 08-12 nhưng đến 08-15 bị bỏ sót. | T-97 đã delete nhưng vẫn còn trong current data/Gold. |
| **Nguyên nhân gốc** | Chỉ dedup trong batch nhưng ghi bằng `INSERT`; không merge theo key giữa các batch. | `LOOKBACK_DAYS=0`, chỉ rebuild ngày hiện tại nên không bắt late event. | `ticket_id` chỉ đọc từ `after`, trong khi Debezium delete có `after=null`. |
| **Cách sửa** | `pipeline/silver.py`: đổi sang `MERGE ON ticket_id`, chỉ update khi `source._lsn > target._lsn`. | `pipeline/config.py`: đặt `LOOKBACK_DAYS=3` theo `ceil(P99)`. | `pipeline/staging.py`: lấy `ticket_id = coalesce(after.ticket_id, before.ticket_id)`. |
| **Khái niệm trên slide** | CDC upsert, ordering theo LSN, idempotency. | Event time, late-arriving data, bounded lookback. | CDC delete vs Kafka tombstone, delete propagation. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE giữ đúng một current row cho mỗi entity và LSN ngăn replay cũ ghi đè; overwrite-partition giúp recompute an toàn cửa sổ event-time.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ trạng thái delete và LSN để replay thay đổi cũ không làm ticket sống lại.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: đảm bảo reproducibility và point-in-time correctness.
- DuckDB/dbt phù hợp dữ liệu lab nhỏ, chạy local nhanh và dễ kiểm chứng; Spark là không cần thiết cho quy mô này.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
- Trả lời: Snapshot immutable phục vụ reproducibility nhưng không thể được dùng để bỏ qua yêu cầu xoá PII. Trong production, tôi sẽ tách lineage/feature không định danh khỏi PII, duy trì deletion registry và cho phép purge/rebuild snapshot chịu yêu cầu pháp lý; tính "immutable" áp dụng cho lineage logic, không phải quyền giữ PII vĩnh viễn.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
- Trả lời: Tôi sẽ đặt PII gate trước khi dữ liệu text rời vùng Bronze restricted sang Silver, kết hợp regex với NER/DLP để phát hiện tên người và thực thể nhạy cảm. Tôi sẽ đo precision/recall trên tập dữ liệu gán nhãn và theo dõi leakage rate sau masking; pipeline sẽ fail nếu vượt ngưỡng cho phép.

## 5. Output (dán nguyên văn)

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

..................................                                      [100%]
34 passed in 2.91s


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


$ .\.venv\Scripts\python.exe main.py --land-only

  2026-08-10  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-11  tickets:already-landed(3)  events:already-landed(5)  transcripts:already-landed(2)
  2026-08-12  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-13  tickets:already-landed(3)  events:already-landed(7)  transcripts:already-landed(1)
  2026-08-14  tickets:already-landed(4)  events:already-landed(4)  transcripts:already-landed(1)
  2026-08-15  tickets:already-landed(4)  events:already-landed(8)  transcripts:already-landed(1)
  2026-08-16  tickets:already-landed(4)  events:already-landed(7)  transcripts:already-landed(2)


$ $env:DO_NOT_TRACK = '1'
$ Push-Location dbt_project

$ ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17

08:20:29  Running with dbt=1.12.5
08:20:29  Registered adapter: duckdb=1.11.0
08:20:29  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test

08:20:29  Concurrency: 1 threads (target='dev')

08:20:30  1 of 19 START sql view model main.stg_events ................................... [RUN]
08:20:30  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.07s]
08:20:30  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
08:20:30  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
08:20:30  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
08:20:30  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.12s]
08:20:30  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
08:20:30  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.11s]
08:20:30  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
08:20:30  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.13s]
08:20:30  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
08:20:30  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
08:20:30  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
08:20:30  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
08:20:30  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
08:20:30  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
08:20:30  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
08:20:30  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
08:20:30  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
08:20:30  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.03s]
08:20:30  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
08:20:30  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
08:20:30  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
08:20:30  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
08:20:30  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
08:20:30  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
08:20:30  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
08:20:30  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
08:20:30  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
08:20:30  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
08:20:30  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
08:20:30  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily .................. [RUN]
08:20:30  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily ............. [OK in 0.04s]
08:20:30  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily .................. [RUN]
08:20:30  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily ............. [OK in 0.03s]
08:20:30  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily .................. [RUN]
08:20:30  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily ............. [OK in 0.03s]
08:20:30  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily .................. [RUN]
08:20:31  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily ............. [OK in 0.03s]
08:20:31  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily .................. [RUN]
08:20:31  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily ............. [OK in 0.03s]
08:20:31  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily .................. [RUN]
08:20:31  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily ............. [OK in 0.03s]
08:20:31  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily .................. [RUN]
08:20:31  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily ............. [OK in 0.03s]
08:20:31  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.27s]
08:20:31  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
08:20:31  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
08:20:31  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
08:20:31  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
08:20:31  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
08:20:31  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]

08:20:31  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.25 seconds (1.25s).

08:20:31  Completed successfully

08:20:31  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ Pop-Location


$ .\.venv\Scripts\python.exe -m scripts.parity

=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
