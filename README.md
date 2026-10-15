# Employee Attendance & Analytics API

FastAPI + MongoDB. All code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"   # or put them in a local .env
uvicorn app.main:app --port 8000
```
MongoDB 6.0+ is required (`$setWindowFields`, `$densify`). Indexes are created at startup and are idempotent.
`GET /health` returns 503 until MongoDB answers a ping.

## Where things are
- Business rules R1-R5 are small pure functions near the top (`attendance_date`, `compute_late_minutes`,
  `compute_work_hours`, `compute_overtime`, `derive`). Half-up rounding is done on exact integers / `Decimal`, because
  both Python's `round()` and MongoDB's `$round` round half to even.
- Each analytics endpoint has a pipeline builder (`*_pipeline`). `/admin/explain/{endpoint}` calls the same builder, so
  the plan it returns is the plan of the real query.

## Assumptions where the contract is silent
- Records dated before an employee's `joined_on` are not excluded from `present_days`.
- The department summary starts from `employees`; logs of employees who have not joined by the end of the month (or who
  do not exist) are ignored. The trend counts every log of a department's employees on that day.
- `moving_avg_7d` is the mean of the already rounded (4 decimals) `attendance_rate` values in the window.
- A trend range of more than 92 calendar days (both ends counted) is a 422.
- Punch-out: an already closed record gives 409 before the 24-hour / ordering checks give 422.
- `punched_at: null` is treated like an omitted `punched_at`.
- A PATCH that changes nothing is judged on every field, derived ones included. On a concurrent edit the PATCH re-reads
  and retries up to 8 times, then answers 409.

## Missing / not done
Nothing from the contract is left out.
