"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded for convenience; real environment variables win)

Layout of this single file
  1. configuration, MongoDB connection, indexes
  2. time / rounding helpers and the business rules R1-R5
  3. request models and small validation helpers
  4. employees, attendance (punch-in, punch-out, list, regularize)
  5. analytics pipelines (builders) and endpoints
  6. /admin/explain, which reuses exactly the same builders as the endpoints
"""
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date as Date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Annotated, Any, Literal, Optional

from bson import Decimal128, json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, model_validator, field_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()  # does not override variables that are already set in the environment

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration, connection, indexes
# --------------------------------------------------------------------------- #
# tz_aware=True: PyMongo then returns timezone-aware (UTC) datetimes instead of naive ones, so
# we never mix naive and aware values (the starter silently relied on the server's local zone).
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]  # R6
NON_PRESENCE = ["ABSENT", "LEAVE"]
Status = Literal["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

GRACE_SECONDS = 10 * 60  # R2
OVERTIME_MIN_MINUTES = 30  # R3
HALF_DAY_BELOW_HOURS = 4.5  # R5
MAX_SHIFT_SPAN = timedelta(hours=24)
MIN_EPOCH_MS = 100_000_000_000
MAX_EPOCH_MS = 4_102_444_800_000
MAX_SKIP = 2**62  # anything beyond this cannot be sent to MongoDB (int64); such a page is simply empty

# (collection, keys, options). Names are explicit so create_index is idempotent.
INDEXES = {
    "attendance_logs": [
        # one record per employee and day: the race-safety guarantee for punch-in, and the index behind
        # /attendance?emp_code=, the monthly summary and the per-employee $lookup in the analytics.
        ([("emp_code", ASCENDING), ("date", ASCENDING)], {"name": "uniq_emp_date", "unique": True}),
        # the default sort of GET /attendance (date desc, emp_code asc) and every month/date-range scan.
        ([("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "date_emp"}),
        # status filter + the same sort, so a rare status does not walk the whole date index.
        ([("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "status_date_emp"}),
    ],
    "employees": [
        ([("emp_code", ASCENDING)], {"name": "uniq_emp_code", "unique": True}),
        # department filter, headcount (joined_on <= day) and the sorted department listing.
        ([("department", ASCENDING), ("joined_on", ASCENDING)], {"name": "dept_joined"}),
        # headcount for "all departments" has no department to put in front.
        ([("joined_on", ASCENDING)], {"name": "joined"}),
    ],
}
_indexes_ready = False


def ensure_indexes() -> None:
    """Create every index (idempotent). A failing index is logged, it must not stop the app."""
    global _indexes_ready
    ok = True
    for coll, specs in INDEXES.items():
        for keys, opts in specs:
            try:
                db[coll].create_index(keys, **opts)
            except PyMongoError:
                ok = False
                log.exception("could not create index %s on %s", opts.get("name"), coll)
    _indexes_ready = ok


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        client.admin.command("ping")
        ensure_indexes()
    except PyMongoError:
        log.exception("MongoDB not reachable at startup; /health will report 503 until it is")
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 2. Time, rounding and the business rules
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def dt_to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:  # defensive: seeded data read through a naive client
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def opt_ms(dt: Optional[datetime]) -> Optional[int]:
    return None if dt is None else dt_to_ms(dt)


def trunc_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def now_dt() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def now_millis_dt() -> datetime:
    """Server clock at MongoDB's precision (milliseconds), so what we return equals what is stored."""
    n = datetime.now(UTC)
    return n.replace(microsecond=(n.microsecond // 1000) * 1000)


def hhmm(value: str) -> time:
    return time(int(value[:2]), int(value[3:5]))


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded "HH:MM" compares correctly as text


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> Date:
    """R1: IST calendar date of the punch-in; an overnight-shift punch-in before shift_end belongs to the previous day."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(shift_start, shift_end) and local.time() < hhmm(shift_end):
        day -= timedelta(days=1)
    return day


def shift_window(day: str, shift_start: str, shift_end: str) -> tuple[datetime, datetime]:
    """Start and end instants (IST) of the shift that belongs to attendance date `day`."""
    d = Date.fromisoformat(day)
    start = datetime.combine(d, hhmm(shift_start), tzinfo=IST)
    end = datetime.combine(d, hhmm(shift_end), tzinfo=IST)
    if end <= start:
        end += timedelta(days=1)
    return start, end


def compute_late_minutes(punch_in: datetime, day: str, shift_start: str, shift_end: str) -> int:
    """R2: strictly more than 10:00 after shift start -> whole minutes (floor) since shift start, else 0."""
    start, _ = shift_window(day, shift_start, shift_end)
    seconds = (trunc_seconds(punch_in) - start) // timedelta(seconds=1)
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600 rounded to 2 decimals HALF-UP, in exact integer arithmetic (no float rounding)."""
    seconds = (trunc_seconds(punch_out) - trunc_seconds(punch_in)) // timedelta(seconds=1)
    hundredths = (2 * seconds + 36) // 72  # floor(seconds / 36 + 1/2)
    return hundredths / 100


def compute_overtime(punch_out: datetime, day: str, shift_start: str, shift_end: str) -> int:
    """R3: whole minutes (floor) after shift end, counted only when >= 30. Overnight shifts end on the next day."""
    _, end = shift_window(day, shift_start, shift_end)
    minutes = (trunc_seconds(punch_out) - end) // timedelta(minutes=1)
    return minutes if minutes >= OVERTIME_MIN_MINUTES else 0


def derive(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime], day: str,
           shift_start: str, shift_end: str) -> dict:
    """The four API-computed fields for a record in its final state (DATA_MODEL.md table)."""
    if status not in PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, day, shift_start, shift_end)
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    hours = compute_work_hours(punch_in, punch_out)
    return {
        "work_hours": hours,
        "late_minutes": late,
        "overtime_minutes": compute_overtime(punch_out, day, shift_start, shift_end),
        "half_day": hours < HALF_DAY_BELOW_HOURS,  # R5, on the rounded value
    }


def round_half_up(value: Any, places: int) -> float:
    """R8: half-up rounding done on a Decimal (MongoDB's $round and Python's round() are half-to-even)."""
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    return float(d.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def count_weekdays(first: Date, last: Date) -> int:
    """Mon-Fri days in [first, last] (R7). Pure calendar arithmetic, no documents involved."""
    if first > last:
        return 0
    days = (last - first).days + 1
    full_weeks, rest = divmod(days, 7)
    return full_weeks * 5 + sum(1 for i in range(rest) if (first + timedelta(days=i)).weekday() < 5)


# --------------------------------------------------------------------------- #
# 3. Request models and validation helpers
# --------------------------------------------------------------------------- #
# strict=True: floats, strings and booleans are rejected instead of being coerced into an int.
EpochMs = Annotated[int, Field(strict=True, ge=MIN_EPOCH_MS, le=MAX_EPOCH_MS)]
HHMM_PATTERN = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"
DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
MONTH_RE = re.compile(r"[0-9]{4}-(0[1-9]|1[0-2])")
MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"


def unprocessable(loc: tuple, msg: str, kind: str = "value_error") -> None:
    """Raise a 422 with FastAPI's own error shape (`detail` is a list), for checks the models cannot express."""
    raise RequestValidationError([{"type": kind, "loc": loc, "msg": msg}])


def parse_date(value: str, loc: tuple) -> Date:
    if not DATE_RE.fullmatch(value):
        unprocessable(loc, "Input should be a valid date in YYYY-MM-DD format")
    try:
        return Date.fromisoformat(value)
    except ValueError:
        unprocessable(loc, "Input should be a valid calendar date")


def parse_month(value: str, loc: tuple = ("query", "month")) -> tuple[Date, Date]:
    """'YYYY-MM' -> (first day, last day)."""
    if not MONTH_RE.fullmatch(value):
        unprocessable(loc, "Input should be a month in YYYY-MM format")
    year, month = int(value[:4]), int(value[5:7])
    try:
        first = Date(year, month, 1)
    except ValueError:  # year 0000
        unprocessable(loc, "Input should be a valid month")
    nxt = Date(year + 1, 1, 1) if month == 12 else Date(year, month + 1, 1)
    return first, nxt - timedelta(days=1)


class EmployeeCreate(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HHMM_PATTERN)
    shift_end: str = Field(default="18:30", pattern=HHMM_PATTERN)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_date(cls, v: str) -> str:
        if not DATE_RE.fullmatch(v):
            raise ValueError("must be a date in YYYY-MM-DD format")
        try:
            Date.fromisoformat(v)
        except ValueError:
            raise ValueError("must be a valid calendar date") from None
        return v

    @model_validator(mode="after")
    def _shifts_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start and shift_end must differ")
        return self


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeRequest(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="before")
    @classmethod
    def _no_explicit_null(cls, data):
        # Omitted means "unchanged"; an explicit null has no meaning here (the contract types are not nullable).
        if isinstance(data, dict):
            for key in ("status", "punch_in", "punch_out"):
                if key in data and data[key] is None:
                    raise ValueError(f"{key} must not be null")
        return data


PageParam = Annotated[int, Query(ge=1)]
PageSizeParam = Annotated[int, Query(ge=1, le=100)]  # R10
MonthParam = Annotated[str, Query(pattern=MONTH_PATTERN)]


# --------------------------------------------------------------------------- #
# Serialisation: MongoDB document -> API shape (epoch ms, no _id, defaults for legacy records)
# --------------------------------------------------------------------------- #
def employee_to_api(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": opt_ms(doc.get("created_at")),
    }


def history_to_api(history: Optional[list]) -> list:
    out = []
    for entry in history or []:
        changes = {}
        for field, change in (entry.get("changes") or {}).items():
            before, after = change.get("from"), change.get("to")
            if field in ("punch_in", "punch_out"):  # BSON dates nested inside the object (easy to forget)
                before, after = opt_ms(before), opt_ms(after)
            changes[field] = {"from": before, "to": after}
        out.append({"at": opt_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"),
                    "changes": changes})
    return out


def record_to_api(doc: dict) -> dict:
    hours = doc.get("work_hours")
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": opt_ms(doc.get("punch_in")),
        "punch_out": opt_ms(doc.get("punch_out")),
        "work_hours": None if hours is None else float(hours),
        "late_minutes": int(doc.get("late_minutes") or 0),
        "overtime_minutes": int(doc.get("overtime_minutes") or 0),
        "half_day": bool(doc.get("half_day") or False),
        "history": history_to_api(doc.get("history")),
    }


def page_slice(page: int, page_size: int) -> tuple[int, int]:
    return min((page - 1) * page_size, MAX_SKIP), page_size


# --------------------------------------------------------------------------- #
# 4. Endpoints: system, employees
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    if not _indexes_ready:  # startup could not create them (MongoDB came up late): try again now
        ensure_indexes()
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeCreate):
    doc = body.model_dump()
    doc["created_at"] = now_millis_dt()
    try:
        # The unique index on emp_code is the guarantee; there is no check-then-insert.
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_to_api(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: PageParam = 1, page_size: PageSizeParam = 20):
    query = {} if department is None else {"department": department}
    skip, limit = page_slice(page, page_size)
    total = db.employees.count_documents(query)  # the filtered set, not the whole collection
    cursor = db.employees.find(query, {"_id": 0}).sort("emp_code", ASCENDING).skip(skip).limit(limit)
    return {"items": [employee_to_api(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 4b. Endpoints: attendance
# --------------------------------------------------------------------------- #
def get_employee(emp_code: str) -> dict:
    emp = db.employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInRequest):
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_dt()
    day = attendance_date(ts, emp["shift_start"], emp["shift_end"]).isoformat()
    doc = {
        "emp_code": body.emp_code,
        "date": day,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derive(body.status, ts, None, day, emp["shift_start"], emp["shift_end"]),
        "history": [],
    }
    try:
        # Race-safe: the unique (emp_code, date) index lets exactly one of N simultaneous inserts through.
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_to_api(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutRequest):
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_dt()
    # Most recent record whose punch_in is at or before the punch-out. Works across midnight because it is
    # not keyed on the calendar date of the punch-out.
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}}, sort=[("punch_in", DESCENDING)]
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    punched_in = trunc_seconds(rec["punch_in"])
    if ts <= punched_in:
        unprocessable(("body", "punched_at"), "punched_at must be after the punch-in")
    if ts - punched_in > MAX_SHIFT_SPAN:
        unprocessable(("body", "punched_at"), "punched_at is more than 24 hours after the punch-in")
    derived = derive(rec["status"], punched_in, ts, rec["date"], emp["shift_start"], emp["shift_end"])
    # Compare-and-set: only closes the record if it is still open and unchanged since we read it. Of two
    # simultaneous punch-outs exactly one matches; a concurrent regularization makes this one lose (409).
    updated = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None, "punch_in": rec["punch_in"], "status": rec["status"]},
        {"$set": {"punch_out": ts, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out or modified concurrently")
    return record_to_api(updated)


def attendance_list_query(emp_code, date_from, date_to, status, page, page_size):
    """Filter, sort, skip and limit of GET /attendance. Shared with /admin/explain so both run the same query."""
    query: dict = {}
    if emp_code is not None:
        query["emp_code"] = emp_code
    start = parse_date(date_from, ("query", "date_from")) if date_from is not None else None
    end = parse_date(date_to, ("query", "date_to")) if date_to is not None else None
    if start and end and start > end:
        unprocessable(("query", "date_from"), "date_from must not be after date_to")
    if start or end:
        query["date"] = {}
        if start:
            query["date"]["$gte"] = start.isoformat()
        if end:
            query["date"]["$lte"] = end.isoformat()
    if status is not None:
        query["status"] = status
    skip, limit = page_slice(page, page_size)
    return query, [("date", DESCENDING), ("emp_code", ASCENDING)], skip, limit


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: PageParam = 1,
    page_size: PageSizeParam = 20,
):
    query, sort, skip, limit = attendance_list_query(emp_code, date_from, date_to, status, page, page_size)
    total = db.attendance_logs.count_documents(query)
    cursor = db.attendance_logs.find(query, {"_id": 0}).sort(sort).skip(skip).limit(limit)  # paged in the database
    return {"items": [record_to_api(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


# ---- regularization ------------------------------------------------------- #
EDITABLE_FIELDS = ["status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day"]
MAX_PATCH_ATTEMPTS = 8


def plan_regularization(rec: dict, emp: dict, body: RegularizeRequest) -> tuple[dict, dict]:
    """Validate the request against the record and return (new field values, changes). Raises 422."""
    new_status = body.status or rec["status"]
    new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else rec.get("punch_in")
    new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else rec.get("punch_out")

    if new_status in NON_PRESENCE:
        if body.punch_in is not None or body.punch_out is not None:
            unprocessable(("body", "status"), f"status {new_status} clears the punch times; do not supply them")
        new_in = new_out = None
    else:
        if new_in is None:
            unprocessable(("body", "punch_in"), f"status {new_status} requires a punch_in")
        if body.punch_in is not None:
            day = attendance_date(new_in, emp["shift_start"], emp["shift_end"]).isoformat()
            if day != rec["date"]:
                unprocessable(("body", "punch_in"), f"punch_in falls on attendance date {day}, not {rec['date']}")
        if new_out is not None:
            span = trunc_seconds(new_out) - trunc_seconds(new_in)
            if span <= timedelta(0) or span > MAX_SHIFT_SPAN:
                unprocessable(("body", "punch_out"), "punch_out must be after punch_in and within 24 hours of it")

    derived = derive(new_status, new_in, new_out, rec["date"], emp["shift_start"], emp["shift_end"])
    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out, **derived}
    old = {
        "status": rec["status"],
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "work_hours": rec.get("work_hours"),
        "late_minutes": int(rec.get("late_minutes") or 0),
        "overtime_minutes": int(rec.get("overtime_minutes") or 0),
        "half_day": bool(rec.get("half_day") or False),
    }
    changes = {f: {"from": old[f], "to": new[f]} for f in EDITABLE_FIELDS if old[f] != new[f]}
    if not changes:
        unprocessable(("body",), "the request changes nothing")
    return new, changes


@app.patch("/attendance/{emp_code}/{date}")
def regularize_attendance(emp_code: str, date: str, body: RegularizeRequest):
    parse_date(date, ("path", "date"))
    emp = get_employee(emp_code)
    for _ in range(MAX_PATCH_ATTEMPTS):
        rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
        if rec is None:
            raise HTTPException(404, "no attendance record for that date")
        new, changes = plan_regularization(rec, emp, body)

        # Optimistic concurrency. The update only applies if the record is exactly as we read it: same editable
        # fields and the same number of history entries (every correction adds exactly one).
        n = len(rec.get("history") or [])
        guard = {"_id": rec["_id"], "status": rec["status"], "punch_in": rec.get("punch_in"),
                 "punch_out": rec.get("punch_out"), f"history.{n}": {"$exists": False}}
        if n:
            guard[f"history.{n - 1}"] = {"$exists": True}
        entry = {"at": now_millis_dt(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
        updated = db.attendance_logs.find_one_and_update(
            guard, {"$set": new, "$push": {"history": entry}}, return_document=ReturnDocument.AFTER
        )
        if updated is not None:
            return record_to_api(updated)
        # Someone else changed the record first: re-read and re-validate against the new state, then try again.
    raise HTTPException(409, "the record is being modified concurrently; retry")


# --------------------------------------------------------------------------- #
# 5. Analytics: pipeline builders (each is used by its endpoint AND by /admin/explain)
# --------------------------------------------------------------------------- #
# Aggregation expression fragments. All of them read the STORED derived fields (never recompute them).
_IS_PRESENT = {"$in": ["$status", PRESENCE]}
_IS_WEEKDAY = {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}}, 5]}
_PRESENT_VALUE = {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 0.5, 1]}  # half day = 0.5 (R5)
_LATE_MINUTES = {"$ifNull": ["$late_minutes", 0]}
_IS_LATE = {"$gt": [_LATE_MINUTES, 0]}


def _sum_if(cond: dict, value: Any = 1) -> dict:
    return {"$sum": {"$cond": [cond, value, 0]}}


def employee_monthly_pipeline(emp_code: str, first: Date, last: Date) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {"$group": {
            "_id": None,
            # present_days: presence status AND Mon-Fri; weekend records still feed the other totals (R7)
            "present_days": _sum_if({"$and": [_IS_PRESENT, _IS_WEEKDAY]}, _PRESENT_VALUE),
            "leave_days": _sum_if({"$eq": ["$status", "LEAVE"]}),
            "late_count": _sum_if(_IS_LATE),
            "total_late_minutes": {"$sum": _LATE_MINUTES},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


def department_summary_pipeline(first: Date, last: Date, department: Optional[str]) -> list:
    """Starts from `employees`, so people with no logs still produce a row (headcount, R9)."""
    emp_match: dict = {"joined_on": {"$lte": last.isoformat()}}
    if department is not None:
        emp_match["department"] = department
    has_hours = {"$and": [_IS_PRESENT, {"$isNumber": "$work_hours"}]}
    zero = Decimal128("0")
    return [
        {"$match": emp_match},
        {"$lookup": {
            "from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code", "as": "m",
            "pipeline": [  # one summary document per employee (or none when they have no logs)
                {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
                {"$group": {
                    "_id": None,
                    "present_days": _sum_if({"$and": [_IS_PRESENT, _IS_WEEKDAY]}, _PRESENT_VALUE),
                    # exact decimal sum of work_hours so the later mean can be rounded half-up without float drift
                    "hours_sum": {"$sum": {"$cond": [has_hours, {"$toDecimal": "$work_hours"}, zero]}},
                    "hours_n": _sum_if(has_hours),
                    "late_count": _sum_if(_IS_LATE),
                    "total_late_minutes": {"$sum": _LATE_MINUTES},
                    "leave_count": _sum_if({"$eq": ["$status", "LEAVE"]}),
                    "on_duty_count": _sum_if({"$eq": ["$status", "ON_DUTY"]}),
                }},
            ],
        }},
        # preserveNullAndEmptyArrays keeps the employees with zero logs: this is what makes headcount right
        {"$unwind": {"path": "$m", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": {"$sum": {"$ifNull": ["$m.present_days", 0]}},
            "hours_sum": {"$sum": {"$ifNull": ["$m.hours_sum", zero]}},
            "hours_n": {"$sum": {"$ifNull": ["$m.hours_n", 0]}},
            "late_count": {"$sum": {"$ifNull": ["$m.late_count", 0]}},
            "total_late_minutes": {"$sum": {"$ifNull": ["$m.total_late_minutes", 0]}},
            "leave_count": {"$sum": {"$ifNull": ["$m.leave_count", 0]}},
            "on_duty_count": {"$sum": {"$ifNull": ["$m.on_duty_count", 0]}},
        }},
        {"$sort": {"_id": 1}},
    ]


def late_leaderboard_pipeline(first: Date, last: Date, limit: int, department: Optional[str]) -> list:
    pipeline: list = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs whose employee does not exist are dropped
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipeline += [
        # $rank = standard competition ranking: ties share a rank and the next rank is skipped (1, 2, 2, 4).
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies AFTER ranking, so ties at the cutoff all stay
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name", "department": "$emp.department",
                      "total_late_minutes": 1, "late_count": 1}},
    ]
    return pipeline


def department_trend_pipeline(department: str, d_from: Date, d_to: Date) -> list:
    lower = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    upper = datetime(d_to.year, d_to.month, d_to.day, tzinfo=UTC) + timedelta(days=1)  # $densify's upper bound is exclusive
    f, t = d_from.isoformat(), d_to.isoformat()
    working_with_staff = {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]}
    return [
        {"$match": {"department": department}},
        {"$lookup": {
            "from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code", "as": "log",
            "pipeline": [
                {"$match": {"date": {"$gte": f, "$lte": t}}},
                {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1}},
            ],
        }},
        # Every employee yields at least one row (also those with no logs), so the stream handed to $densify
        # is never empty; employees without logs add zeros to the first day, which changes nothing.
        {"$unwind": {"path": "$log", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": {"$ifNull": ["$log.date", f]},
            "present_count": {"$sum": {"$cond": [
                {"$in": ["$log.status", PRESENCE]},
                {"$cond": [{"$eq": [{"$ifNull": ["$log.half_day", False]}, True]}, 0.5, 1]},
                0,
            ]}},
            "late_count": {"$sum": {"$cond": [{"$gt": [{"$ifNull": ["$log.late_minutes", 0]}, 0]}, 1, 0]}},
        }},
        {"$set": {"day": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        # Gap filling in the database: one document per calendar day in [from, to], weekends included.
        {"$densify": {"field": "day", "range": {"step": 1, "unit": "day", "bounds": [lower, upper]}}},
        {"$set": {
            "present_count": {"$ifNull": ["$present_count", 0]},
            "late_count": {"$ifNull": ["$late_count", 0]},
            "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}},
            "is_working_day": {"$lte": [{"$isoDayOfWeek": "$day"}, 5]},
        }},
        # headcount (R9): department employees that had joined on or before that day, whether or not they have logs
        {"$lookup": {
            "from": "employees", "let": {"d": "$date"}, "as": "hc",
            "pipeline": [
                {"$match": {"department": department}},
                {"$match": {"$expr": {"$lte": ["$joined_on", "$$d"]}}},
                {"$count": "n"},
            ],
        }},
        {"$set": {"headcount": {"$ifNull": [{"$arrayElemAt": ["$hc.n", 0]}, 0]}}},
        # Rates are carried as integer "ticks" of 1/10000. floor(x + 0.5) is exact half-up here, because
        # present/headcount is a rational with a small denominator and the division is correctly rounded.
        {"$set": {"rate_ticks": {"$cond": [
            working_with_staff,
            {"$floor": {"$add": [{"$divide": [{"$multiply": ["$present_count", 10000]}, "$headcount"]}, 0.5]}},
            None,
        ]}}},
        {"$setWindowFields": {
            "sortBy": {"day": 1},
            # current row and up to 6 before it, inside the requested range only; $avg ignores the null rates
            "output": {"avg_ticks": {"$avg": "$rate_ticks", "window": {"documents": [-6, 0]}}},
        }},
        {"$project": {
            "_id": 0, "date": 1, "is_working_day": 1, "headcount": 1, "present_count": 1, "late_count": 1,
            "attendance_rate": {"$cond": [{"$isNumber": "$rate_ticks"}, {"$divide": ["$rate_ticks", 10000]}, None]},
            "moving_avg_7d": {"$cond": [
                {"$isNumber": "$avg_ticks"},
                {"$divide": [{"$floor": {"$add": ["$avg_ticks", 0.5]}}, 10000]},
                None,
            ]},
        }},
        {"$sort": {"date": 1}},
    ]


# --------------------------------------------------------------------------- #
# 5b. Analytics endpoints
# --------------------------------------------------------------------------- #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthParam):
    first, last = parse_month(month)
    emp = get_employee(emp_code)
    rows = list(db.attendance_logs.aggregate(employee_monthly_pipeline(emp_code, first, last)))
    agg = rows[0] if rows else {}
    joined = Date.fromisoformat(emp["joined_on"])
    working_days = count_weekdays(max(first, joined), last)  # R7: counted from joined_on for mid-month joiners
    present = Decimal(str(agg.get("present_days", 0)))
    pct = None if working_days == 0 else round_half_up(present * 100 / working_days, 2)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": round_half_up(present, 2),
        "leave_days": int(agg.get("leave_days", 0)),
        "late_count": int(agg.get("late_count", 0)),
        "total_late_minutes": int(agg.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(agg.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: MonthParam, department: Optional[str] = None):
    first, last = parse_month(month)
    items = []
    for row in db.employees.aggregate(department_summary_pipeline(first, last, department)):
        hours_n = int(row["hours_n"])
        avg_hours = None
        if hours_n:
            avg_hours = round_half_up(row["hours_sum"].to_decimal() / hours_n, 2)  # mean over records
        items.append({
            "department": row["_id"],
            "headcount": int(row["headcount"]),
            "present_days": round_half_up(Decimal(str(row["present_days"])), 2),
            "avg_work_hours": avg_hours,
            "late_count": int(row["late_count"]),
            "total_late_minutes": int(row["total_late_minutes"]),
            "leave_count": int(row["leave_count"]),
            "on_duty_count": int(row["on_duty_count"]),
        })
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: MonthParam, limit: Annotated[int, Query(ge=1, le=50)] = 10,
                     department: Optional[str] = None):
    first, last = parse_month(month)
    rows = db.attendance_logs.aggregate(late_leaderboard_pipeline(first, last, limit, department))
    items = [{
        "rank": int(r["rank"]), "emp_code": r["emp_code"], "name": r["name"], "department": r["department"],
        "total_late_minutes": int(r["total_late_minutes"]), "late_count": int(r["late_count"]),
    } for r in rows]
    return {"month": month, "items": items}


MAX_TREND_DAYS = 92


def trend_range(date_from: str, date_to: str) -> tuple[Date, Date]:
    d_from = parse_date(date_from, ("query", "from"))
    d_to = parse_date(date_to, ("query", "to"))
    if d_to < d_from:
        unprocessable(("query", "to"), "to must not be before from")
    if (d_to - d_from).days + 1 > MAX_TREND_DAYS:
        unprocessable(("query", "to"), f"the range must not exceed {MAX_TREND_DAYS} days")
    return d_from, d_to


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, date_from: Annotated[str, Query(alias="from")],
                     date_to: Annotated[str, Query(alias="to")]):
    d_from, d_to = trend_range(date_from, date_to)
    if db.employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "department not found")
    rows = db.employees.aggregate(department_trend_pipeline(department, d_from, d_to))
    items = [{
        "date": r["date"],
        "is_working_day": bool(r["is_working_day"]),
        "headcount": int(r["headcount"]),
        "present_count": float(r["present_count"]),
        "late_count": int(r["late_count"]),
        "attendance_rate": r["attendance_rate"],
        "moving_avg_7d": r["moving_avg_7d"],
    } for r in rows]
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 6. /admin/explain: the same builders as the endpoints, run through `explain` (executionStats)
# --------------------------------------------------------------------------- #
def _require(value: Any, name: str) -> Any:
    if value is None:
        unprocessable(("query", name), "Field required", "missing")
    return value


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard",
                      "department_trend"],
    emp_code: Optional[str] = None,
    month: Annotated[Optional[str], Query(pattern=MONTH_PATTERN)] = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[str], Query(alias="from")] = None,
    to: Optional[str] = None,
    page: PageParam = 1,
    page_size: PageSizeParam = 20,
):
    if endpoint == "attendance_list":
        query, sort, skip, lim = attendance_list_query(emp_code, date_from, date_to, status, page, page_size)
        collection = "attendance_logs"
        command: dict = {"find": collection, "filter": query, "sort": dict(sort), "skip": skip, "limit": lim}
    else:
        if endpoint == "department_trend":
            d_from, d_to = trend_range(_require(from_, "from"), _require(to, "to"))
            collection, pipeline = "employees", department_trend_pipeline(_require(department, "department"),
                                                                          d_from, d_to)
        else:
            first, last = parse_month(_require(month, "month"))
            if endpoint == "employee_monthly":
                collection, pipeline = "attendance_logs", employee_monthly_pipeline(_require(emp_code, "emp_code"),
                                                                                    first, last)
            elif endpoint == "department_summary":
                collection, pipeline = "employees", department_summary_pipeline(first, last, department)
            else:
                collection, pipeline = "attendance_logs", late_leaderboard_pipeline(first, last, limit, department)
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}
    result = db.command({"explain": command, "verbosity": "executionStats"})
    # explain output holds BSON types (Int64, dates, Decimal128); turn it into plain JSON
    return {"endpoint": endpoint, "collection": collection,
            "explain": json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))}
