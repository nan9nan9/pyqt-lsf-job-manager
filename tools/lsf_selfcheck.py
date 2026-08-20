#!/usr/bin/env python3
"""실환경 자가 점검 — lsfmgr가 LSF에 대해 세운 가정이 이 사이트에서 맞는가.

라이브러리는 bjobs/bkill의 **출력 문자열**에 여러 가정을 걸고 있다. 가정이
틀리면 증상이 조용하다: 상태가 영영 안 오르거나(조회 실패로 판단 보류),
살아있는 job이 LOST로 확정되거나, kill이 확인 안 됨으로 오보된다.
테스트는 전부 가짜 LSF 상대라 **이 검증은 실환경에서만** 할 수 있다.

    python tools/lsf_selfcheck.py                       # 읽기 전용 점검
    python tools/lsf_selfcheck.py --job 12345           # 그 job으로 조회 점검
    python tools/lsf_selfcheck.py --job 12345 --kill    # kill 점검까지(죽인다!)
    python tools/lsf_selfcheck.py --bench 12345,12346   # bkill 소요 실측

아무것도 제출하지 않는다. --kill을 주지 않으면 job을 죽이지도 않는다.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lsfmgr.command import (                                    # noqa: E402
    _BKILL_RESOLVED_MSGS, _JOB_ID_RE, _LSF_TIME_FORMATS, _NO_JOB_PATTERNS,
    LsfCommand, _parse_bkill_resolved, _parse_lsf_time, _parse_run_time,
)

OK, BAD, WARN, INFO = "  OK ", " FAIL", " WARN", " info"
results = []


def report(level, title, detail=""):
    results.append((level, title))
    print(f"[{level}] {title}")
    for line in str(detail).splitlines():
        if line.strip():
            print(f"        {line}")


def run(argv, timeout=120):
    t0 = time.perf_counter()
    try:
        p = subprocess.run(argv, capture_output=True, text=True,
                           timeout=timeout)
        return p.returncode, p.stdout, p.stderr, time.perf_counter() - t0
    except subprocess.TimeoutExpired:
        return None, "", f"timeout after {timeout}s", time.perf_counter() - t0
    except OSError as e:
        return None, "", f"실행 실패: {e}", time.perf_counter() - t0


# ----------------------------------------------------------------------
def check_env():
    loc = {k: os.environ.get(k) for k in ("LC_ALL", "LANG", "LC_MESSAGES")}
    report(INFO, f"로케일 {loc}",
           "lsfmgr는 bjobs/bkill의 **영어 메시지**를 부분 문자열로 판정한다\n"
           f"  판정 대상: {_NO_JOB_PATTERNS}\n"
           "  메시지가 현지화되면 '없음'을 '장애'로 오판해 상태가 안 오른다.")


def check_bjobs_formats(cmd, job):
    """세 포맷(CORE / FULL / FULL+MC) 중 이 사이트가 뭘 받아주나."""
    sel = [str(job)] if job else ["-u", os.environ.get("USER", "")]
    for name, fmt in (("CORE", LsfCommand._BJOBS_CORE_FMT),
                      ("FULL", LsfCommand._BJOBS_FULL_FMT),
                      ("FULL+MC", LsfCommand._BJOBS_FULL_MC_FMT)):
        rc, out, err, dt = run(["bjobs", "-noheader", "-o", fmt] + sel)
        if rc is None:
            report(BAD, f"bjobs -o {name}: 실행 불가", err)
            continue
        rows = [l for l in out.splitlines() if l.strip()]
        if rc != 0 and not rows:
            lvl = WARN if name != "CORE" else BAD
            report(lvl, f"bjobs -o {name}: rc={rc}, 행 0개 ({dt:.2f}s)",
                   f"stderr: {err.strip()[:200]}\n"
                   + ("이 포맷은 자동 강등된다(다음 단계로)."
                      if name != "CORE"
                      else "CORE가 안 되면 조회 자체가 불가 — 치명적."))
            continue
        n_fields = len(rows[0].split(";")) if rows else 0
        want = {"CORE": 3, "FULL": 6, "FULL+MC": 8}[name]
        lvl = OK if n_fields == want else BAD
        report(lvl, f"bjobs -o {name}: rc={rc}, {len(rows)}행, "
                    f"필드 {n_fields}개(기대 {want}) ({dt:.2f}s)",
               f"첫 행: {rows[0][:160]}" if rows else "")
        if rows and n_fields == want:
            check_field_values(name, rows[0])


def check_field_values(name, row):
    parts = [p.strip() for p in row.split(";")]
    if len(parts) >= 4:
        rt = _parse_run_time(parts[3])
        report(OK if (rt is not None or parts[3] in ("", "-")) else BAD,
               f"  run_time 파싱: {parts[3]!r} → {rt}")
    if len(parts) >= 5:
        t = _parse_lsf_time(parts[4])
        report(OK if (t is not None or parts[4] in ("", "-")) else BAD,
               f"  start_time 파싱: {parts[4]!r} → {t}",
               "" if t or parts[4] in ("", "-") else
               f"지원 포맷: {_LSF_TIME_FORMATS}")


def check_mixed_missing_id(cmd, job):
    """핵심 가정: 없는 id가 섞이면 rc≠0인데 **찾은 행은 stdout에 나온다**."""
    if not job:
        report(WARN, "혼합 조회 점검 건너뜀 (--job 필요)")
        return
    bogus = "999999999"
    rc, out, err, dt = run(["bjobs", "-noheader", "-o",
                            LsfCommand._BJOBS_CORE_FMT, str(job), bogus])
    rows = [l for l in out.splitlines() if l.strip()]
    matched = any(p in (err + out).lower() for p in _NO_JOB_PATTERNS)
    if rows and matched:
        report(OK, f"없는 id 혼합: rc={rc}, 찾은 행 {len(rows)}개 + no-match 문구",
               "lsfmgr가 stdout을 파싱해 살아있는 job을 지킨다.")
    elif not rows:
        report(BAD, f"없는 id 혼합: rc={rc}, 행 0개",
               "이 사이트는 하나라도 없으면 **전부** 버린다 — 같은 chunk의\n"
               "살아있는 job이 미발견으로 몰려 LOST 유예로 들어간다.\n"
               "대책: chunk_size를 줄여 영향 범위를 좁힌다.")
    elif not matched:
        report(BAD, f"없는 id 혼합: no-match 문구를 못 알아봄 (rc={rc})",
               f"stderr: {err.strip()[:200]}\n"
               f"기대 문구 중 하나: {_NO_JOB_PATTERNS}\n"
               "→ 정상 응답을 '조회 장애'로 오판해 상태가 안 오른다.")


def check_finished_job_visible(cmd, job):
    """-a 없이 explicit id로 종료 job이 보이나 (CLEAN_PERIOD 안)."""
    if not job:
        return
    rc, out, _e, _dt = run(["bjobs", "-noheader", "-o", "jobid stat "
                            "delimiter=';'", str(job)])
    rows = [l for l in out.splitlines() if l.strip()]
    if rows:
        stat = rows[0].split(";")[1] if ";" in rows[0] else "?"
        report(INFO, f"job {job} 현재 상태: {stat}",
               "종료(DONE/EXIT) 상태가 보이면 -a 없이도 잘 조회된다는 뜻."
               if stat in ("DONE", "EXIT") else "")


def check_bkill_messages(cmd, job, do_kill):
    if not job:
        report(WARN, "bkill 점검 건너뜀 (--job 필요)")
        return
    if not do_kill:
        report(INFO, "bkill 점검 건너뜀 (--kill 을 주면 실제로 죽인다)",
               f"확인할 것: 'Job <{job}> is being terminated' 형태인가.\n"
               f"  lsfmgr가 해소로 인정하는 문구: {_BKILL_RESOLVED_MSGS}")
        return
    rc, out, err, dt = run(["bkill", str(job)])
    text = out + "\n" + err
    resolved = _parse_bkill_resolved(text, {str(job)})
    report(OK if str(job) in resolved else BAD,
           f"bkill {job}: rc={rc} ({dt:.2f}s) → 해소 판정={bool(resolved)}",
           f"출력: {text.strip()[:200]}\n"
           + ("" if resolved else
              "이 문구를 lsfmgr가 못 알아본다 — kill이 '미확인'으로 오보되고\n"
              f"kill_max_retry까지 재시도한다. 기대 문구: {_BKILL_RESOLVED_MSGS}"))


def bench_bkill(ids):
    """bkill 1회 소요 실측 — kill_chunk_size / kill_workers / kill_timeout_s
    를 정하는 근거."""
    ids = [i.strip() for i in ids.split(",") if i.strip()]
    rc, out, err, dt = run(["bkill"] + ids)
    per = dt / max(1, len(ids))
    report(INFO, f"bkill {len(ids)}건 소요 {dt:.2f}s (target당 {per*1000:.0f}ms)",
           f"권장: kill_chunk_size x {per:.2f}s < kill_timeout_s\n"
           f"  지금 기본(chunk 16)이면 한 호출 약 {16*per:.1f}s 예상\n"
           f"  → kill_timeout_s는 그 2~3배 여유를 두는 것이 안전\n"
           f"출력: {(out + err).strip()[:200]}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--job", help="점검에 쓸 실제 job id (조회 전용)")
    ap.add_argument("--kill", action="store_true",
                    help="--job 을 실제로 kill 해서 메시지를 확인한다")
    ap.add_argument("--bench", help="bkill 소요 실측용 id 목록 (쉼표 구분)")
    a = ap.parse_args()

    print("=" * 72)
    print("lsfmgr 실환경 자가 점검 — 가정이 이 사이트에서 맞는가")
    print("=" * 72)
    cmd = None
    check_env()
    check_bjobs_formats(cmd, a.job)
    check_mixed_missing_id(cmd, a.job)
    check_finished_job_visible(cmd, a.job)
    check_bkill_messages(cmd, a.job, a.kill)
    if a.bench:
        bench_bkill(a.bench)

    print("=" * 72)
    bad = [t for lvl, t in results if lvl == BAD]
    warn = [t for lvl, t in results if lvl == WARN]
    print(f"결과: 실패 {len(bad)} / 경고 {len(warn)} / 전체 {len(results)}")
    for t in bad:
        print(f"  FAIL  {t}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
