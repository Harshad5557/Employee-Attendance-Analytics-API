# REVIEW.md

| # | Where | What is wrong | How you'd notice it | How I fixed it |
|---|---|---|---|---|
| 1 | `compute_late_minutes` | Shift start built with `punch_in.replace(...)` on a UTC/naive datetime, so the IST start time is read as UTC | Punch in 09:40 IST (04:10Z): late is 0 or hours off | Build shift start as an IST datetime for the attendance date |
| 2 | `compute_late_minutes` | Grace check is `> 10` on floored minutes, so 09:40:01 gives 10 and returns 0 | 09:40:01 must give 10 (R2) | Compare whole seconds `> 600`, then floor to minutes |
| 3 | `compute_late_minutes` | Overnight shifts: start taken on the punch-in's own date | 22:00 shift, punch-in 02:00 next day: wrong start day | Start comes from the attendance date (R1) |
| 4 | `compute_work_hours` | Python `round(x, 2)` is float and half-even; no truncation to whole seconds | 9.125 h gives 9.12 instead of 9.13 (R4) | Integer arithmetic, half-up |
| 5 | `compute_overtime` | No 30-minute minimum (R3); overnight end taken on same date; naive `punch_out` minus aware `end` raises `TypeError` (500) | `punch_out` 5 min after end gives 5 instead of 0 | Use the shift window and the `>= 30` rule |
| 6 | `punch_in` | `emp` can be `None`, then `emp["shift_start"]` raises `TypeError` (500) | Unknown `emp_code` | Return 404 |
| 7 | `punch_in` | `datetime.fromtimestamp(ms/1000)` gives server-local naive time; `if body.punched_at` treats 0 as missing; no range check, so seconds, floats and strings are accepted; sub-seconds not truncated | Send 1783312500 (seconds) or `"123"` | Strict int model in range, UTC conversion, truncate to seconds, 422 |
| 8 | `punch_in` | Date is `ts.date()` in local time, not the IST date (R1), no overnight rule | Punch at 00:30 or 23:30 IST | `attendance_date()` |
| 9 | `PunchInIn.status` | Any string accepted (`ABSENT`, `LEAVE`, `"x"`) | POST status `LEAVE` | `Literal["PRESENT","WFH","ON_DUTY"]` |
| 10 | `punch_in` | Check-then-insert race and no unique index, so parallel requests both return 201 | Fire 10 parallel punch-ins | Unique `(emp_code, date)` index; `DuplicateKeyError` gives 409 |
| 11 | `punch_in` response | Returns `id` (ObjectId string) and datetimes instead of epoch ms; `_id` must not be exposed | Look at the response | `record_to_api()` |
| 12 | `create_employee` | Same check-then-insert race, no unique index; naive `datetime.now()`; `created_at` returned as datetime; no validation of `emp_code`, email, HH:MM, `joined_on`, or `shift_start != shift_end` | Two parallel creates; `emp_code="x"` | Unique index, `EmployeeCreate` model, epoch ms |
| 13 | `list_employees` | `skip = page * page_size` skips the whole first page; `page=0` or negative gives a negative skip (500); `total` ignores the department filter; no sort so paging is unstable; no `page_size` cap | `?page=1` returns page 2's rows; `total` ignores the filter | Skip is `(page-1)*page_size`, `total` over the filter, sort by `emp_code`, `ge/le` bounds |
| 14 | `list_attendance` | Loads the whole collection and sorts and slices in Python; sorts by date only (no `emp_code` tie-break); `date_from > date_to`, bad dates and bad status unvalidated; `page=0` gives odd slices; `page_size` unbounded; exposes `id`; legacy docs lack `half_day`/`history`; datetimes not epoch ms | 100k docs: slow and memory-heavy | Query, sort, skip and limit in MongoDB; validation; serializer with defaults |
| 15 | whole file | No indexes, so every query is a COLLSCAN | `explain` | `INDEXES` created at startup |
| 16 | `/health` | Always returns ok without touching MongoDB | Stop MongoDB, `/health` still 200 | Ping MongoDB, otherwise 503 |
| 17 | `MongoClient(...)` | Not `tz_aware`, so datetimes come back naive and mix with aware IST values | `TypeError: can't subtract offset-naive and offset-aware` | `tz_aware=True` |

## Looked at and decided not a defect
- `load_dotenv()`: it does not override real environment variables, which is what the contract wants.
- `doc.pop("_id")` after `insert_one`: it only removes the id the driver added to the dict.
- 201 on creates and 409 on duplicates are the right status codes.
- The `IST` constant itself is correct.
