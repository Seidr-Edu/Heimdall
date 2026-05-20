#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
SCRIPT_ROOT = Path(__file__).resolve().parent
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from heimdall.images import DockerError, run_container  # noqa: E402
from heimdall.manifests.queue import load_worker_config  # noqa: E402
from heimdall.models import (  # noqa: E402
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_GENERATED_V2,
    STEP_LIDSKJALV_GENERATED_V3,
    STEP_LIDSKJALV_ORIGINAL,
)
from heimdall.utils import timestamp_utc  # noqa: E402

import retry_lidskjalv_current_batch as lidskjalv_retry  # noqa: E402


DEFAULT_RUNS_ROOT = Path("/srv/pipeline/runs")
DEFAULT_WORKER_CONFIG = Path("/srv/pipeline/worker.yaml")
DEFAULT_OUTPUT_ROOT = Path("/srv/pipeline/retries/sonar-resubmission")
DEFAULT_EXPECTED_COUNT = 45
DEFAULT_MANUAL_SCANNER_IMAGE = "sonarsource/sonar-scanner-cli:latest"
LIDSKJALV_STEPS = (
    STEP_LIDSKJALV_ORIGINAL,
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_GENERATED_V2,
    STEP_LIDSKJALV_GENERATED_V3,
)

CODEX_RUN_IDS = (
    "20260505T122153Z__ulisesbocchio_jasypt-spring-boot__2243cb80",
    "20260505T150455Z__mbechler_marshalsec__243c5aa8",
    "20260505T150537Z__frohoff_ysoserial__218bcffc",
    "20260505T150549Z__xtuhcy_gecco__e1f32e1c",
    "20260506T082045Z__happyfish100_fastdfs-client-java__4e7b6b34",
    "20260506T082105Z__JakeWharton_RxRelay__9540ecc7",
    "20260506T082129Z__amitshekhariitbhu_Android-Debug-Database__bf149df6",
    "20260506T082136Z__amitshekhariitbhu_PRDownloader__611b12c8",
    "20260506T083333Z__esoxjem_MovieGuide__dc1f20fb",
    "20260507T093402Z__19MisterX98_SeedcrackerX__8e18d20d",
    "20260507T093422Z__airbnb_native-navigation__9cf50bf9",
    "20260507T093651Z__awaitility_awaitility__4fc23ccb",
    "20260507T093656Z__DantSu_ESCPOS-ThermalPrinter-Android__f61030e4",
    "20260507T093701Z__elvishew_xLog__3faa6b27",
    "20260507T093708Z__EsotericSoftware_reflectasm__e34c5e8b",
    "20260507T093714Z__evant_binding-collection-adapter__ac7972e1",
    "20260507T093719Z__gothinkster_spring-boot-realworld-example-app__ee17e31a",
    "20260507T093724Z__Grt1228_chatgpt-java__ae761b89",
    "20260507T093731Z__j-easy_easy-rules__d4450831",
    "20260508T095107Z__jOOQ_jOOR__23e17cf3",
    "20260508T095113Z__LianjiaTech_retrofit-spring-boot-starter__24cb9eda",
    "20260508T095119Z__linkedin_kafka-monitor__043db641",
    "20260508T095125Z__macrozheng_mall-tiny__a81ec474",
    "20260508T095131Z__Meituan-Dianping_walle__f78edcf1",
    "20260508T095136Z__Mojang_brigadier__b5419b18",
    "20260508T095142Z__monkeyWie_proxyee__c2c5bb4c",
    "20260508T095147Z__mouzt_mzt-biz-log__8d3a0271",
    "20260508T095153Z__Netflix_concurrency-limits__e8df64d8",
    "20260508T095158Z__pwittchen_ReactiveNetwork__ddfde340",
    "20260508T095204Z__RikkaApps_Shizuku-API__a27f6e41",
    "20260508T095210Z__TheoKanning_openai-java__26909660",
    "20260508T095214Z__whwlsfb_JDumpSpider__24fe3186",
    "20260508T095219Z__YeautyYE_netty-websocket-spring-boot-starter__eb4d0a6b",
    "20260508T095225Z__JMCuixy_swagger2word__0da57120",
    "20260508T095230Z__eugene-khyst_postgresql-event-sourcing__90faafbb",
    "20260508T095237Z__facebook_SoLoader__d3d721fb",
    "20260508T095244Z__feiniaojin_graceful-response__cf1cd118",
    "20260508T095249Z__jenkinsci_jenkinsfile-runner__9be9c0fb",
    "20260508T095255Z__mitre_HTTP-Proxy-Servlet__c799c5e1",
    "20260508T095300Z__stealthcopter_AndroidNetworkTools__a82af8a5",
)

CLAUDE_RUN_IDS = (
    "20260510T103653Z__19MisterX98_SeedcrackerX__8e18d20d",
    "20260510T154735Z__airbnb_native-navigation__9cf50bf9",
    "20260510T154742Z__amitshekhariitbhu_Android-Debug-Database__bf149df6",
    "20260510T154753Z__amitshekhariitbhu_PRDownloader__611b12c8",
    "20260510T154759Z__awaitility_awaitility__4fc23ccb",
    "20260510T154807Z__DantSu_ESCPOS-ThermalPrinter-Android__f61030e4",
    "20260510T154813Z__elvishew_xLog__3faa6b27",
    "20260510T154820Z__EsotericSoftware_reflectasm__e34c5e8b",
    "20260510T154826Z__esoxjem_MovieGuide__dc1f20fb",
    "20260510T154832Z__evant_binding-collection-adapter__ac7972e1",
    "20260510T154845Z__frohoff_ysoserial__218bcffc",
    "20260511T105128Z__gothinkster_spring-boot-realworld-example-app__ee17e31a",
    "20260511T105134Z__Grt1228_chatgpt-java__ae761b89",
    "20260511T105140Z__happyfish100_fastdfs-client-java__4e7b6b34",
    "20260511T105145Z__j-easy_easy-rules__d4450831",
    "20260511T105150Z__JakeWharton_RxRelay__9540ecc7",
    "20260511T105155Z__jOOQ_jOOR__23e17cf3",
    "20260511T105200Z__LianjiaTech_retrofit-spring-boot-starter__24cb9eda",
    "20260511T105205Z__linkedin_kafka-monitor__043db641",
    "20260511T105211Z__macrozheng_mall-tiny__a81ec474",
    "20260511T105217Z__mbechler_marshalsec__243c5aa8",
    "20260513T080244Z__Meituan-Dianping_walle__f78edcf1",
    "20260513T080251Z__Mojang_brigadier__b5419b18",
    "20260513T080257Z__monkeyWie_proxyee__c2c5bb4c",
    "20260513T080303Z__mouzt_mzt-biz-log__8d3a0271",
    "20260513T080309Z__Netflix_concurrency-limits__e8df64d8",
    "20260513T080314Z__pwittchen_ReactiveNetwork__ddfde340",
    "20260513T080320Z__RikkaApps_Shizuku-API__a27f6e41",
    "20260513T080328Z__TheoKanning_openai-java__26909660",
    "20260513T080335Z__ulisesbocchio_jasypt-spring-boot__2243cb80",
    "20260513T080342Z__whwlsfb_JDumpSpider__24fe3186",
    "20260514T193759Z__xtuhcy_gecco__e1f32e1c",
    "20260514T193807Z__YeautyYE_netty-websocket-spring-boot-starter__eb4d0a6b",
    "20260515T132132Z__JMCuixy_swagger2word__0da57120",
    "20260515T132141Z__eugene-khyst_postgresql-event-sourcing__90faafbb",
    "20260515T132149Z__facebook_SoLoader__d3d721fb",
    "20260515T132155Z__feiniaojin_graceful-response__cf1cd118",
    "20260515T132202Z__jenkinsci_jenkinsfile-runner__9be9c0fb",
    "20260515T132208Z__mitre_HTTP-Proxy-Servlet__c799c5e1",
    "20260518T082419Z__stealthcopter_AndroidNetworkTools__a82af8a5",
)


@dataclass(frozen=True)
class MissingSonarTarget:
    agent: str
    run_id: str
    step: str
    scan_label: str
    variant: str
    follow_up_status: str | None
    follow_up_reason: str | None
    project_key: str
    project_name: str | None
    run_root: Path
    manifest_path: Path
    input_repo: Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Resubmit missing SonarCloud scans for the current Codex/Claude "
            "experiment batch using sidecar-only logs."
        )
    )
    parser.add_argument("--runs-root", type=Path, default=DEFAULT_RUNS_ROOT)
    parser.add_argument("--worker-config", type=Path, default=DEFAULT_WORKER_CONFIG)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--run-id",
        action="append",
        dest="run_ids",
        help="Limit processing to one run id. Repeatable.",
    )
    parser.add_argument(
        "--step",
        action="append",
        choices=LIDSKJALV_STEPS,
        dest="steps",
        help="Limit processing to one Lidskjalv step. Repeatable.",
    )
    parser.add_argument(
        "--expected-count",
        type=int,
        default=DEFAULT_EXPECTED_COUNT,
        help="Expected target count for the unfiltered current batch.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--manual-fallback",
        action="store_true",
        help="Run manual sonar-scanner fallback after a failed Lidskjalv replay.",
    )
    parser.add_argument(
        "--manual-only",
        action="store_true",
        help="Skip Lidskjalv replay and only run manual sonar-scanner fallback.",
    )
    parser.add_argument(
        "--manual-scanner-image",
        default=DEFAULT_MANUAL_SCANNER_IMAGE,
        help=f"Sonar scanner image for manual fallback (default: {DEFAULT_MANUAL_SCANNER_IMAGE})",
    )
    parser.add_argument(
        "--manual-timeout-sec",
        type=float,
        default=3600.0,
        help="Timeout for each manual scanner fallback attempt.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    targets = discover_targets(
        runs_root=args.runs_root,
        run_ids=tuple(args.run_ids or ()),
        steps=tuple(args.steps or LIDSKJALV_STEPS),
    )
    write_targets_manifest(args.output_root, targets)
    validate_expected_count(
        targets,
        expected_count=args.expected_count,
        filtered=bool(args.run_ids or args.steps),
    )

    if args.dry_run:
        for target in targets:
            print(format_target_line(target))
        print(f"Selected {len(targets)} missing Sonar submission target(s).")
        return 0

    require_sonar_env(manual_only=args.manual_only)
    worker_lidskjalv_image = current_lidskjalv_image(args.worker_config)
    entries: list[dict[str, object]] = []
    overall_success = True

    for target in targets:
        entry = process_target(
            target,
            output_root=args.output_root,
            worker_lidskjalv_image=worker_lidskjalv_image,
            force=args.force,
            manual_fallback=args.manual_fallback,
            manual_only=args.manual_only,
            manual_scanner_image=args.manual_scanner_image,
            manual_timeout_sec=args.manual_timeout_sec,
        )
        entries.append(entry)
        if entry.get("submission_success") is not True:
            overall_success = False

    write_invocation_summary(args.output_root, entries)
    write_residual_failures(args.output_root)
    return 0 if overall_success else 1


def discover_targets(
    *,
    runs_root: Path,
    run_ids: tuple[str, ...] = (),
    steps: tuple[str, ...] = LIDSKJALV_STEPS,
) -> list[MissingSonarTarget]:
    selected_run_ids = set(run_ids)
    selected_steps = set(steps)
    results: list[MissingSonarTarget] = []
    for agent, run_id in scoped_run_ids():
        if selected_run_ids and run_id not in selected_run_ids:
            continue
        run_root = runs_root / run_id
        if not run_root.is_dir():
            continue
        follow_up_path = run_root / "pipeline" / "outputs" / "sonar_follow_up.json"
        follow_up = load_json(follow_up_path)
        entries = mapping(follow_up.get("steps"))
        for step in LIDSKJALV_STEPS:
            if step not in selected_steps:
                continue
            entry = mapping(entries.get(step))
            if non_empty_str(entry.get("sonar_task_id")) is not None:
                continue
            target = build_target(agent, run_root, step, entry)
            if target is not None:
                results.append(target)
    return results


def build_target(
    agent: str,
    run_root: Path,
    step: str,
    follow_up_entry: Mapping[str, object],
) -> MissingSonarTarget | None:
    manifest_path = run_root / "services" / step / "config" / "manifest.yaml"
    if not manifest_path.is_file():
        return None
    manifest = lidskjalv_retry.load_yaml_file(manifest_path)
    project_key = non_empty_str(manifest.get("project_key"))
    if project_key is None:
        return None
    input_repo = lidskjalv_retry.determine_input_repo(run_root, step)
    if not input_repo.exists():
        return None
    scan_label = (
        non_empty_str(manifest.get("scan_label"))
        or non_empty_str(follow_up_entry.get("scan_label"))
        or scan_label_for_step(step)
    )
    return MissingSonarTarget(
        agent=agent,
        run_id=run_root.name,
        step=step,
        scan_label=scan_label,
        variant=variant_for_step(step),
        follow_up_status=non_empty_str(follow_up_entry.get("status")),
        follow_up_reason=non_empty_str(follow_up_entry.get("reason")),
        project_key=project_key,
        project_name=non_empty_str(manifest.get("project_name")),
        run_root=run_root,
        manifest_path=manifest_path,
        input_repo=input_repo,
    )


def process_target(
    target: MissingSonarTarget,
    *,
    output_root: Path,
    worker_lidskjalv_image: str,
    force: bool,
    manual_fallback: bool,
    manual_only: bool,
    manual_scanner_image: str,
    manual_timeout_sec: float | None,
) -> dict[str, object]:
    stable_dir = stable_step_output_dir(output_root, target)
    prior_success = load_prior_success(stable_dir)
    if prior_success is not None and not force:
        print(f"skipped_prior_success: {target.run_id} {target.step}")
        return {
            **target_summary_fields(target),
            "result": "skipped_prior_success",
            "submission_success": True,
            "stable_output_dir": str(stable_dir),
            "attempt_dir": prior_success.get("attempt_dir"),
            "sonar_task_id": prior_success.get("sonar_task_id"),
        }

    last_entry: dict[str, object] | None = None
    if not manual_only:
        last_entry = run_lidskjalv_attempt(
            target,
            output_root=output_root,
            worker_lidskjalv_image=worker_lidskjalv_image,
        )
        if last_entry.get("submission_success") is True:
            return last_entry

    if manual_fallback or manual_only:
        return run_manual_attempt(
            target,
            output_root=output_root,
            scanner_image=manual_scanner_image,
            timeout_sec=manual_timeout_sec,
            prior_result=last_entry,
        )

    assert last_entry is not None
    return last_entry


def run_lidskjalv_attempt(
    target: MissingSonarTarget,
    *,
    output_root: Path,
    worker_lidskjalv_image: str,
) -> dict[str, object]:
    stable_dir = stable_step_output_dir(output_root, target)
    attempt_dir = stable_dir / f"lidskjalv-attempt-{timestamp_slug()}"
    selection = lidskjalv_retry.StepSelection(
        run_id=target.run_id,
        step=target.step,
        source="sonar_follow_up_missing",
        status=target.follow_up_status or "missing-sonar-task-id",
        reason=target.follow_up_reason,
        project_key=target.project_key,
        run_root=target.run_root,
    )
    try:
        context = lidskjalv_retry.build_replay_context(selection)
        context = replace(
            context,
            requested_image_ref=worker_lidskjalv_image,
            requested_image_source="worker_config_current",
            configured_image_ref=worker_lidskjalv_image,
        )
        summary = dict(
            lidskjalv_retry.execute_replay_attempt(
                attempt_dir=attempt_dir,
                context=context,
                output_root=output_root,
            )
        )
    except Exception as exc:  # pragma: no cover - operational failure capture
        summary = {
            **target_summary_fields(target),
            "result": "lidskjalv_preflight_failed",
            "submission_success": False,
            "stable_output_dir": str(stable_dir),
            "attempt_dir": str(attempt_dir),
            "error": str(exc) or exc.__class__.__name__,
            "finished_at": timestamp_utc(),
        }
        ensure_directory(attempt_dir)

    summary.update(
        {
            "attempt_type": "lidskjalv",
            "agent": target.agent,
            "variant": target.variant,
            "scan_label": target.scan_label,
        }
    )
    write_json(attempt_dir / "summary.json", summary)
    return summary


def run_manual_attempt(
    target: MissingSonarTarget,
    *,
    output_root: Path,
    scanner_image: str,
    timeout_sec: float | None,
    prior_result: Mapping[str, object] | None,
) -> dict[str, object]:
    stable_dir = stable_step_output_dir(output_root, target)
    attempt_dir = stable_dir / f"manual-attempt-{timestamp_slug()}"
    workspace = attempt_dir / "workspace"
    docker_log_path = attempt_dir / "docker.log"
    ensure_directory(attempt_dir)
    if workspace.exists():
        shutil.rmtree(workspace)
    shutil.copytree(target.input_repo, workspace, symlinks=True)

    report_task_path = workspace / ".scannerwork" / "report-task.txt"
    result = "manual_submission_failed"
    sonar_task_id: str | None = None
    error: str | None = None
    visibility_preflight: dict[str, object] | None = None
    try:
        visibility_preflight = ensure_public_sonar_project(target)
        run_container(
            scanner_image,
            {
                "SONAR_HOST_URL": os.environ["SONAR_HOST_URL"],
                "SONAR_TOKEN": os.environ["SONAR_TOKEN"],
            },
            [(workspace, "/usr/src", False)],
            output_path=docker_log_path,
            timeout_sec=timeout_sec,
            timeout_reason="manual-sonar-timeout",
            command_args=manual_scanner_args(target),
        )
        task = parse_report_task(report_task_path)
        sonar_task_id = non_empty_str(task.get("ceTaskId"))
        reported_key = non_empty_str(task.get("projectKey"))
        if sonar_task_id is not None and reported_key in {None, target.project_key}:
            result = "submission_success"
        else:
            error = "manual scanner did not emit a matching ceTaskId/projectKey"
    except Exception as exc:  # pragma: no cover - operational failure capture
        error = str(exc) or exc.__class__.__name__

    summary: dict[str, object] = {
        **target_summary_fields(target),
        "attempt_type": "manual",
        "result": result,
        "submission_success": result == "submission_success",
        "stable_output_dir": str(stable_dir),
        "attempt_dir": str(attempt_dir),
        "input_repo": str(target.input_repo),
        "workspace": str(workspace),
        "scanner_image": scanner_image,
        "docker_log_path": str(docker_log_path),
        "report_task_path": str(report_task_path),
        "sonar_task_id": sonar_task_id,
        "visibility_preflight": visibility_preflight,
        "prior_result": dict(prior_result) if prior_result is not None else None,
        "error": error,
        "finished_at": timestamp_utc(),
    }
    write_json(attempt_dir / "summary.json", summary)
    print(f"{result}: {target.run_id} {target.step} project_key={target.project_key}")
    return summary


def ensure_public_sonar_project(target: MissingSonarTarget) -> dict[str, object]:
    organization = os.environ.get("SONAR_ORGANIZATION")
    if not organization:
        return {"status": "skipped", "reason": "missing-sonar-organization"}
    project_key = target.project_key
    project_name = target.project_name or project_key
    show = sonar_api_request("GET", "/api/components/show", {"component": project_key})
    if show["ok"] is True:
        visibility = non_empty_str(mapping(show.get("json")).get("visibility"))
        update = sonar_api_request(
            "POST",
            "/api/projects/update_visibility",
            {"project": project_key, "visibility": "public"},
        )
        return {
            "status": "updated_existing" if update["ok"] else "update_failed",
            "previous_visibility": visibility,
            "update": update,
        }
    create = sonar_api_request(
        "POST",
        "/api/projects/create",
        {
            "organization": organization,
            "project": project_key,
            "name": project_name,
            "visibility": "public",
        },
    )
    if create["ok"] is True:
        return {"status": "created_public", "create": create}
    if str(create.get("http_status")) == "400":
        update = sonar_api_request(
            "POST",
            "/api/projects/update_visibility",
            {"project": project_key, "visibility": "public"},
        )
        return {
            "status": "updated_after_create_conflict"
            if update["ok"]
            else "update_failed",
            "create": create,
            "update": update,
        }
    return {"status": "create_failed", "create": create}


def sonar_api_request(
    method: str, path: str, params: Mapping[str, str]
) -> dict[str, object]:
    host_url = os.environ["SONAR_HOST_URL"].rstrip("/")
    token = os.environ["SONAR_TOKEN"]
    encoded = urllib.parse.urlencode(params).encode("utf-8")
    url = f"{host_url}{path}"
    data = encoded if method == "POST" else None
    if method == "GET":
        url = f"{url}?{encoded.decode('utf-8')}"
    auth = base64.b64encode(f"{token}:".encode("utf-8")).decode("ascii")
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Basic {auth}",
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8", errors="replace")
            return {
                "ok": True,
                "http_status": response.status,
                "json": parse_optional_json(body),
            }
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "http_status": exc.code,
            "error": detail.strip() or exc.reason,
        }
    except OSError as exc:
        return {"ok": False, "http_status": None, "error": str(exc)}


def parse_optional_json(text: str) -> object:
    stripped = text.strip()
    if not stripped:
        return {}
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return {"raw": stripped}


def manual_scanner_args(target: MissingSonarTarget) -> list[str]:
    args = [
        f"-Dsonar.projectKey={target.project_key}",
        f"-Dsonar.projectName={target.project_name or target.project_key}",
        "-Dsonar.sources=.",
        "-Dsonar.scm.disabled=true",
        "-Dsonar.java.binaries=.",
        "-Dsonar.working.directory=/usr/src/.scannerwork",
        "-Dsonar.scanner.metadataFilePath=/usr/src/.scannerwork/report-task.txt",
    ]
    organization = os.environ.get("SONAR_ORGANIZATION")
    if organization:
        args.append(f"-Dsonar.organization={organization}")
    return args


def parse_report_task(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def load_prior_success(stable_dir: Path) -> Mapping[str, object] | None:
    if not stable_dir.is_dir():
        return None
    for summary_path in sorted(
        stable_dir.glob("*attempt-*/summary.json"), reverse=True
    ):
        summary = load_optional_json(summary_path)
        if summary.get("submission_success") is True and non_empty_str(
            summary.get("sonar_task_id")
        ):
            return summary
    return None


def write_targets_manifest(
    output_root: Path, targets: Sequence[MissingSonarTarget]
) -> None:
    write_json(
        output_root / "targets.json",
        {
            "schema_version": "heimdall_sonar_resubmission_targets.v1",
            "generated_at": timestamp_utc(),
            "target_count": len(targets),
            "targets": [target_summary_fields(target) for target in targets],
        },
    )


def write_invocation_summary(
    output_root: Path, entries: Sequence[Mapping[str, object]]
) -> None:
    write_json(
        output_root / f"invocation-{timestamp_slug()}.json",
        {
            "schema_version": "heimdall_sonar_resubmission_invocation.v1",
            "finished_at": timestamp_utc(),
            "entries": list(entries),
        },
    )


def write_residual_failures(output_root: Path) -> None:
    failures: list[Mapping[str, object]] = []
    for summary_path in sorted(output_root.glob("*/*/*attempt-*/summary.json")):
        summary = load_optional_json(summary_path)
        if summary.get("submission_success") is True:
            continue
        failures.append(summary)
    write_json(
        output_root / "residual_failures.json",
        {
            "schema_version": "heimdall_sonar_resubmission_residual_failures.v1",
            "generated_at": timestamp_utc(),
            "failure_count": len(failures),
            "failures": failures,
        },
    )


def validate_expected_count(
    targets: Sequence[MissingSonarTarget], *, expected_count: int, filtered: bool
) -> None:
    if filtered:
        return
    if len(targets) != expected_count:
        raise RuntimeError(
            f"Expected {expected_count} missing Sonar target(s), found {len(targets)}"
        )


def current_lidskjalv_image(worker_config_path: Path) -> str:
    config = load_worker_config(worker_config_path)
    image_ref = config.images.lidskjalv.strip()
    if not image_ref:
        raise RuntimeError(
            f"Worker config does not define images.lidskjalv: {worker_config_path}"
        )
    return image_ref


def require_sonar_env(*, manual_only: bool) -> None:
    required = ["SONAR_HOST_URL", "SONAR_TOKEN", "SONAR_ORGANIZATION"]
    if manual_only:
        required = ["SONAR_HOST_URL", "SONAR_TOKEN"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            f"Missing required Sonar environment variable(s): {', '.join(missing)}"
        )


def scoped_run_ids() -> list[tuple[str, str]]:
    return [
        *(("codex", run_id) for run_id in CODEX_RUN_IDS),
        *(("claude", run_id) for run_id in CLAUDE_RUN_IDS),
    ]


def stable_step_output_dir(output_root: Path, target: MissingSonarTarget) -> Path:
    return output_root / target.run_id / target.step


def target_summary_fields(target: MissingSonarTarget) -> dict[str, object]:
    return {
        "agent": target.agent,
        "run_id": target.run_id,
        "step": target.step,
        "scan_label": target.scan_label,
        "variant": target.variant,
        "follow_up_status": target.follow_up_status,
        "follow_up_reason": target.follow_up_reason,
        "project_key": target.project_key,
        "project_name": target.project_name,
        "manifest_path": str(target.manifest_path),
        "input_repo": str(target.input_repo),
    }


def format_target_line(target: MissingSonarTarget) -> str:
    return (
        f"{target.agent}\t{target.run_id}\t{target.step}\t{target.variant}\t"
        f"{target.follow_up_status or ''}\t{target.follow_up_reason or ''}\t"
        f"{target.project_key}\t{target.input_repo}"
    )


def scan_label_for_step(step: str) -> str:
    if step == STEP_LIDSKJALV_GENERATED:
        return "generated"
    if step == STEP_LIDSKJALV_GENERATED_V2:
        return "generated-v2"
    if step == STEP_LIDSKJALV_GENERATED_V3:
        return "generated-v3"
    return "original"


def variant_for_step(step: str) -> str:
    if step == STEP_LIDSKJALV_GENERATED:
        return "generated"
    if step == STEP_LIDSKJALV_GENERATED_V2:
        return "v2"
    if step == STEP_LIDSKJALV_GENERATED_V3:
        return "v3"
    return "original"


def load_json(path: Path) -> Mapping[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise RuntimeError(f"Expected JSON object: {path}")
    return payload


def load_optional_json(path: Path) -> Mapping[str, object]:
    if not path.is_file():
        return {}
    try:
        return load_json(path)
    except (OSError, json.JSONDecodeError):
        return {}


def mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def non_empty_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def timestamp_slug() -> str:
    return timestamp_utc().replace("-", "").replace(":", "").removesuffix("Z") + "Z"


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except DockerError as exc:
        print(f"Docker error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
