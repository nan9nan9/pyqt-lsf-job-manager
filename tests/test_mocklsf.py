"""MockLSF 통합 테스트.

임시 MOCKLSF_HOME 에서 스케줄러를 직접 tick 시켜(데몬 없이) 상태 전이를 검증한다.
실행: python3 -m pytest tests/ -v
"""

import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _fresh_env(tmp):
    """테스트용 환경 변수 설정 후 config 모듈을 재로드한다."""
    os.environ["MOCKLSF_HOME"] = tmp
    os.environ["MOCKLSF_SUBMIT_DELAY_MIN"] = "0"
    os.environ["MOCKLSF_SUBMIT_DELAY_MAX"] = "0"
    os.environ["MOCKLSF_PEND_MIN"] = "0"
    os.environ["MOCKLSF_PEND_MAX"] = "0"
    os.environ["MOCKLSF_RUN_MIN"] = "1"
    os.environ["MOCKLSF_RUN_MAX"] = "1"
    os.environ["MOCKLSF_SUBMIT_FAIL_RATE"] = "0"
    os.environ["MOCKLSF_EXIT_RATE"] = "0"
    os.environ["MOCKLSF_SUSPEND_RATE"] = "0"
    import importlib
    from mocklsf import config, submit, scheduler, db, formats
    importlib.reload(config)
    importlib.reload(db)
    importlib.reload(formats)
    importlib.reload(submit)
    importlib.reload(scheduler)
    return config, submit, scheduler, db


def test_submit_and_lifecycle():
    with tempfile.TemporaryDirectory() as tmp:
        config, submit, scheduler, dbmod = _fresh_env(tmp)
        database = dbmod.Database()
        opts, cmd = submit.parse_args(["-q", "normal", "sleep", "30"])
        jobs, size, limit = submit.build_jobs(database.next_job_id(), opts, cmd)
        database.insert_jobs(jobs)
        assert size == 0  # 일반 job

        sched = scheduler.Scheduler(database)
        now = time.time()
        sched.tick(now)  # pend=0 이므로 즉시 dispatch → RUN
        j = database.all_jobs()[0]
        assert j.stat == "RUN"
        assert j.exec_host in config.HOSTS

        sched.tick(now + 2)  # run=1 이므로 종료 → DONE
        j = database.all_jobs()[0]
        assert j.stat == "DONE"
        assert j.exit_code == 0


def test_array_limit():
    with tempfile.TemporaryDirectory() as tmp:
        config, submit, scheduler, dbmod = _fresh_env(tmp)
        database = dbmod.Database()
        opts, cmd = submit.parse_args(["-J", "arr[1-10]%3", "sleep", "100"])
        jobs, size, limit = submit.build_jobs(database.next_job_id(), opts, cmd)
        assert size == 10 and limit == 3
        database.insert_jobs(jobs)

        sched = scheduler.Scheduler(database)
        sched.tick(time.time())
        running = [j for j in database.all_jobs() if j.stat == "RUN"]
        assert len(running) == 3  # %3 동시 실행 제한


def test_bad_queue():
    with tempfile.TemporaryDirectory() as tmp:
        config, submit, scheduler, dbmod = _fresh_env(tmp)
        database = dbmod.Database()
        opts, cmd = submit.parse_args(["-q", "ghost", "sleep", "1"])
        try:
            submit.build_jobs(database.next_job_id(), opts, cmd)
            assert False, "should raise"
        except submit.SubmitError:
            pass


def test_array_spec_parsing():
    with tempfile.TemporaryDirectory() as tmp:
        _, submit, _, _ = _fresh_env(tmp)
        base, idx, lim = submit._parse_array_spec("job[1-5,8,10-12]%2")
        assert base == "job"
        assert idx == [1, 2, 3, 4, 5, 8, 10, 11, 12]
        assert lim == 2


def test_unknown_options_ignored():
    with tempfile.TemporaryDirectory() as tmp:
        _, submit, _, _ = _fresh_env(tmp)
        # -Is(인터렉티브)와 미지 옵션은 무시되고 command 만 남아야 한다.
        opts, cmd = submit.parse_args(
            ["-Is", "-XYZ", "-R", "rusage[mem=1000]", "myapp", "arg1"]
        )
        assert cmd == ["myapp", "arg1"]


def test_default_table_format():
    """기본 bjobs 헤더가 실제 LSF 와 바이트 단위로 일치하는지."""
    from mocklsf import formats
    expected = ("JOBID   USER    STAT  QUEUE      FROM_HOST   "
                "EXEC_HOST   JOB_NAME   SUBMIT_TIME")
    assert formats._DEFAULT_HEADER == expected


def test_jobid_not_truncated():
    """array id / 큰 job id 가 기본 출력에서 잘리지 않아야 한다 (FIX#1 회귀)."""
    with tempfile.TemporaryDirectory() as tmp:
        _fresh_env(tmp)
        from mocklsf import formats
        from mocklsf.models import Job
        j = Job(job_id=1001, user="u", command="c", queue="normal",
                from_host="h", job_name="n", submit_time=0.0, stat="RUN")
        j.array_index = 10
        j.exec_host = "hostA"
        assert formats.default_row(j).split()[0] == "1001[10]"
        j2 = Job(job_id=12345678, user="u", command="c", queue="normal",
                 from_host="h", job_name="n", submit_time=0.0, stat="RUN")
        j2.exec_host = "hostA"
        assert formats.default_row(j2).split()[0] == "12345678"


def test_bad_array_spec_raises():
    """잘못된 array 스펙은 크래시가 아니라 SubmitError (FIX#3/#5)."""
    with tempfile.TemporaryDirectory() as tmp:
        _, submit, _, _ = _fresh_env(tmp)
        for bad in ("foo[bar]", "job[1-]", "job[-5]", "job[10-1]"):
            try:
                submit._parse_array_spec(bad)
                assert False, f"{bad} should raise"
            except submit.SubmitError:
                pass
        # 정상 스펙은 통과.
        base, idx, lim = submit._parse_array_spec("ok[1-3]")
        assert idx == [1, 2, 3]


def test_guarded_update_preserves_concurrent_change():
    """스케줄러 guarded update 가 동시 변경을 덮지 않아야 한다 (FIX#1 레이스)."""
    with tempfile.TemporaryDirectory() as tmp:
        _, submit, _, dbmod = _fresh_env(tmp)
        from mocklsf.models import PEND, RUN, EXIT
        database = dbmod.Database()
        opts, cmd = submit.parse_args(["sleep", "100"])
        jobs, _, _ = submit.build_jobs(database.next_job_id(), opts, cmd,
                                       submit_time=1000.0)
        database.insert_jobs(jobs)
        j = database.jobs_in_states([PEND])[0]   # 스케줄러가 읽은 스냅샷
        # 동시 변경: 다른 프로세스가 EXIT 커밋.
        victim = database.jobs_by_id(j.job_id)[0]
        victim.stat = EXIT
        database.update_job(victim)
        # 스케줄러가 뒤늦게 RUN 으로 guarded update (prev=PEND).
        j.stat = RUN
        database.update_guarded_many([(j, PEND)])
        assert database.jobs_by_id(j.job_id)[0].stat == EXIT


def test_o_spec_embedded_delimiter():
    """-o 스펙 안의 delimiter='X' 키워드가 구분자로 동작해야 한다."""
    with tempfile.TemporaryDirectory() as tmp:
        _fresh_env(tmp)
        from mocklsf import formats
        from mocklsf.models import Job
        j = Job(job_id=1001, user="u", command="c", queue="normal",
                from_host="h", job_name="n", submit_time=0.0, stat="RUN")
        j.exec_host = "hostA"

        # 작은따옴표 / 큰따옴표 / 무따옴표 / 대시 형태 모두 동일 결과.
        for spec in ("jobid stat delimiter=';'", 'jobid stat delimiter=";"',
                     "jobid stat delimiter=;", "jobid stat -delimiter=';'"):
            out = formats.custom_format([j], spec)
            assert out == "JOBID;STAT\n1001;RUN", spec

        # 스펙 안 delimiter 가 별도 인자보다 우선한다.
        out = formats.custom_format([j], "jobid stat delimiter='|'",
                                    delimiter=";")
        assert out == "JOBID|STAT\n1001|RUN"

        # delimiter 키워드는 json 필드로 오인되지 않는다.
        import json as _json
        rec = _json.loads(formats.json_format([j], "jobid stat delimiter=';'"))
        assert list(rec["RECORDS"][0].keys()) == ["JOBID", "STAT"]


def test_extract_delimiter_helper():
    """_extract_delimiter 가 키워드를 분리하고 나머지 스펙을 보존해야 한다."""
    from mocklsf import formats
    assert formats._extract_delimiter("jobid stat delimiter='^'") == \
        ("jobid stat", "^")
    # delimiter 없으면 원본 그대로, None.
    assert formats._extract_delimiter("jobid stat queue") == \
        ("jobid stat queue", None)
    # 중간에 있어도 분리하고 앞뒤 필드를 붙여준다.
    assert formats._extract_delimiter("jobid delimiter=, stat") == \
        ("jobid stat", ",")


def test_positional_args_skips_option_values():
    """옵션 값이 job spec 으로 오인되지 않아야 한다 (FIX#2)."""
    from mocklsf import cli
    assert cli._positional_args(["-n", "5", "1001"]) == ["1001"]
    assert cli._positional_args(["-J", "myjob", "1001", "1002"]) == \
        ["1001", "1002"]
    assert cli._positional_args(["-u", "me"]) == []
    assert cli._collect_names(["-J", "worker", "-J", "other"]) == \
        ["worker", "other"]


def _reload_cli(dbmod, submit):
    """cli 를 현재(재로드된) 모듈들에 바인딩해 반환한다."""
    import importlib
    from mocklsf import cli
    importlib.reload(cli)
    return cli


def _insert(dbmod, submit, name, group=None):
    database = dbmod.Database()
    argv = ["-J", name]
    if group:
        argv += ["-g", group]
    argv += ["sleep", "100"]
    opts, cmd = submit.parse_args(argv)
    jobs, _, _ = submit.build_jobs(database.next_job_id(), opts, cmd)
    database.insert_jobs(jobs)
    database.close()


def test_bsub_stores_job_group():
    """'bsub -g <group>' 이 job 에 group 을 저장한다."""
    with tempfile.TemporaryDirectory() as tmp:
        _, submit, _, dbmod = _fresh_env(tmp)
        _insert(dbmod, submit, "jobA", group="/lsfmgr/u/jsA")
        j = dbmod.Database().all_jobs()[0]
        assert j.job_group == "/lsfmgr/u/jsA"


def test_bkill_group_scoped():
    """'bkill -g <group> 0' 은 그 group 의 job 만 종료하고 다른 group 은 보존.

    이 덕분에 lsfmgr killer 의 group-tier 가 해당 jobset 만 정확히 kill 한다.
    """
    with tempfile.TemporaryDirectory() as tmp:
        _, submit, _, dbmod = _fresh_env(tmp)
        cli = _reload_cli(dbmod, submit)
        _insert(dbmod, submit, "jobA", group="/lsfmgr/u/jsA")
        _insert(dbmod, submit, "jobB", group="/lsfmgr/u/jsB")

        rc = cli.cmd_bkill(["-g", "/lsfmgr/u/jsA", "0"])
        assert rc == 0
        by_name = {j.job_name: j.stat for j in dbmod.Database().all_jobs()}
        assert by_name["jobA"] == "EXIT"    # group A 만 종료
        assert by_name["jobB"] == "PEND"    # group B 는 생존

        # 없는 group 은 매칭 없음(255) → killer 는 fallback 신호로 해석.
        assert cli.cmd_bkill(["-g", "/lsfmgr/u/ghost", "0"]) == 255


def test_bjobs_group_filter():
    """'bjobs -g <group>' 은 해당 group 의 job 만 조회한다."""
    with tempfile.TemporaryDirectory() as tmp:
        _, submit, _, dbmod = _fresh_env(tmp)
        cli = _reload_cli(dbmod, submit)
        _insert(dbmod, submit, "jobA", group="/lsfmgr/u/jsA")
        _insert(dbmod, submit, "jobB", group="/lsfmgr/u/jsB")
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            cli.cmd_bjobs(["-a", "-noheader", "-g", "/lsfmgr/u/jsA",
                           "-o", "job_name"])
        out = buf.getvalue().split()
        assert out == ["jobA"], out


def test_bmod_moves_job_group():
    """'bmod -g <group> <id>' 이 job 의 group 을 갱신한다."""
    with tempfile.TemporaryDirectory() as tmp:
        _, submit, _, dbmod = _fresh_env(tmp)
        cli = _reload_cli(dbmod, submit)
        _insert(dbmod, submit, "jobA")            # group 없음
        j = dbmod.Database().all_jobs()[0]
        assert cli.cmd_bmod(["-g", "/lsfmgr/u/jsA", str(j.job_id)]) == 0
        moved = dbmod.Database().all_jobs()[0]
        assert moved.job_group == "/lsfmgr/u/jsA"


def test_bkill_already_finished_is_benign():
    """이미 끝난 job 을 kill 해도 오류(255)가 아니라 성공(0)이어야 한다.

    array/집합 kill 에서 일부 element 만 먼저 끝난 경우 전체가 실패로 처리되면
    lsfmgr 가 LsfCommandError 를 던지므로, '이미 종료됨' 은 성공으로 취급한다.
    """
    with tempfile.TemporaryDirectory() as tmp:
        _, submit, _, dbmod = _fresh_env(tmp)
        cli = _reload_cli(dbmod, submit)
        _insert(dbmod, submit, "jobA")
        # job 을 DONE 으로 만들어 둔다.
        db = dbmod.Database()
        j = db.all_jobs()[0]
        j.stat = "DONE"
        db.update_job(j)
        db.close()

        assert cli.cmd_bkill([str(j.job_id)]) == 0        # 이미 종료 → 성공
        # 없는 id 는 여전히 255.
        assert cli.cmd_bkill(["999999"]) == 255


def test_bkill_name_scoped_zero():
    """'bkill -J name 0' 의 '0' 은 전체 kill 이 아니라 name 범위로 한정된다.

    이 덕분에 killer 의 name-tier 가 다른 jobset 을 휩쓸지 않는다.
    """
    with tempfile.TemporaryDirectory() as tmp:
        _, submit, _, dbmod = _fresh_env(tmp)
        cli = _reload_cli(dbmod, submit)
        _insert(dbmod, submit, "jobA")
        _insert(dbmod, submit, "jobB")

        rc = cli.cmd_bkill(["-J", "jobB", "0"])
        assert rc == 0
        by_name = {j.job_name: j.stat for j in dbmod.Database().all_jobs()}
        assert by_name["jobB"] == "EXIT"    # 이름 매칭 job 만 종료
        assert by_name["jobA"] == "PEND"    # 다른 이름은 생존
