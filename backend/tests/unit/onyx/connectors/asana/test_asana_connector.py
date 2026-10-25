"""Tests for Asana connector configuration parsing."""

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, NamedTuple
from unittest.mock import MagicMock, patch

import pytest

from onyx.connectors.asana.asana_api import AsanaAPI
from onyx.connectors.asana.connector import AsanaConnector


class _AsanaTestSetup(NamedTuple):
    api: AsanaAPI
    stories_api: MagicMock
    tasks_api: MagicMock


# A poll window that starts after the 2026-01-01 timestamps the shared task
# factory uses.
_WINDOW_START_DATE = "2026-02-01T00:00:00+00:00"
_WINDOW_START_SECONDS = datetime(2026, 2, 1, tzinfo=timezone.utc).timestamp()
_RECENT_DATE = "2026-02-02T00:00:00+00:00"


@pytest.mark.parametrize(
    "project_ids,expected",
    [
        (None, None),
        ("", None),
        ("   ", None),
        (" 123 ", ["123"]),
        (" 123 , , 456 , ", ["123", "456"]),
    ],
)
def test_asana_connector_project_ids_normalization(
    project_ids: str | None, expected: list[str] | None
) -> None:
    connector = AsanaConnector(
        asana_workspace_id=" 1153293530468850 ",
        asana_project_ids=project_ids,
        asana_team_id=" 1210918501948021 ",
    )

    assert connector.workspace_id == "1153293530468850"
    assert connector.project_ids_to_index == expected
    assert connector.asana_team_id == "1210918501948021"


@pytest.mark.parametrize(
    "team_id,expected",
    [
        (None, None),
        ("", None),
        ("   ", None),
        (" 1210918501948021 ", "1210918501948021"),
    ],
)
def test_asana_connector_team_id_normalization(
    team_id: str | None, expected: str | None
) -> None:
    connector = AsanaConnector(
        asana_workspace_id="1153293530468850",
        asana_project_ids=None,
        asana_team_id=team_id,
    )

    assert connector.asana_team_id == expected


def _make_task_data(gid: str, name: str | None = None) -> dict[str, Any]:
    """Minimal Asana task payload covering the fields the connector reads."""
    return {
        "gid": gid,
        "name": name or f"task-{gid}",
        "notes": "",
        "created_by": None,
        "created_at": "2026-01-01T00:00:00+00:00",
        "due_on": None,
        "completed_at": None,
        "modified_at": "2026-01-01T00:00:00+00:00",
        "permalink_url": f"https://app.asana.com/0/{gid}",
    }


def _build_api_with_mocks(
    project_to_tasks: dict[str, list[dict[str, Any]]],
    *,
    team_gid: str | None = None,
    project_metadata: dict[str, dict[str, Any]] | None = None,
    parent_to_subtasks: dict[str, list[dict[str, Any]]] | None = None,
    full_tasks: dict[str, dict[str, Any]] | None = None,
    subtask_list_errors: dict[str, Exception] | None = None,
    full_task_errors: dict[str, Exception] | None = None,
) -> _AsanaTestSetup:
    """Construct an AsanaAPI with all SDK clients replaced by mocks.

    `project_to_tasks` maps each project gid to the tasks `get_tasks_for_project`
    will return when asked for that project. `project_metadata` lets a test
    override per-project metadata returned by `get_project` (the default is a
    public, team-bound, unarchived project). `team_gid` configures the API's
    team filter.

    Subtask walks read compact listings from `parent_to_subtasks` and full
    records from `full_tasks` (keyed by task gid). `subtask_list_errors` and
    `full_task_errors` make the corresponding SDK calls raise for a gid.

    Returns the api plus the stories/tasks api mocks so tests can introspect
    call counts without fighting `ty` type inference on the original SDK
    attributes.
    """
    with patch("onyx.connectors.asana.asana_api.asana"):
        api = AsanaAPI(api_token="token", workspace_gid="ws", team_gid=team_gid)

    project_api = MagicMock()
    tasks_api = MagicMock()
    stories_api = MagicMock()
    users_api = MagicMock()

    api.project_api = project_api
    api.tasks_api = tasks_api
    api.stories_api = stories_api
    api.users_api = users_api

    default_metadata: dict[str, Any] = {
        "name": "p",
        "team": {"gid": "T1"},
        "archived": False,
        "privacy_setting": "public",
    }
    metadata_overrides = project_metadata or {}
    subtask_overrides = parent_to_subtasks or {}
    full_task_overrides = full_tasks or {}
    subtask_list_error_overrides = subtask_list_errors or {}
    full_task_error_overrides = full_task_errors or {}

    project_api.get_projects.return_value = iter(
        [{"gid": gid} for gid in project_to_tasks]
    )

    def _get_project(gid: str, **_kwargs: Any) -> dict[str, Any]:
        return {**default_metadata, **metadata_overrides.get(gid, {})}

    project_api.get_project.side_effect = _get_project

    def _get_tasks_for_project(
        gid: str, *_args: Any, **_kwargs: Any
    ) -> Iterator[dict[str, Any]]:
        return iter(project_to_tasks.get(gid, []))

    def _get_subtasks_for_task(
        gid: str, *_args: Any, **_kwargs: Any
    ) -> Iterator[dict[str, Any]]:
        if gid in subtask_list_error_overrides:
            raise subtask_list_error_overrides[gid]
        return iter(subtask_overrides.get(gid, []))

    def _get_task(gid: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        if gid in full_task_error_overrides:
            raise full_task_error_overrides[gid]
        return full_task_overrides[gid]

    tasks_api.get_tasks_for_project.side_effect = _get_tasks_for_project
    tasks_api.get_subtasks_for_task.side_effect = _get_subtasks_for_task
    tasks_api.get_task.side_effect = _get_task
    stories_api.get_stories_for_task.return_value = iter([])

    return _AsanaTestSetup(api=api, stories_api=stories_api, tasks_api=tasks_api)


def _compact_subtask(gid: str, modified_at: str = _RECENT_DATE) -> dict[str, Any]:
    """Compact payload from GET /tasks/{gid}/subtasks with modified_at opted in."""
    return {"gid": gid, "modified_at": modified_at}


def _full_subtask(
    gid: str,
    parent_gid: str,
    *,
    modified_at: str = _RECENT_DATE,
    parent_name: str | None = None,
    in_project: bool = False,
) -> dict[str, Any]:
    """Full task payload for a subtask, with its direct parent opted in."""
    data = _make_task_data(gid)
    data["modified_at"] = modified_at
    data["parent"] = {
        "gid": parent_gid,
        "name": parent_name or f"task-{parent_gid}",
        "permalink_url": f"https://app.asana.com/0/{parent_gid}",
    }
    data["projects"] = [{"gid": "P1"}] if in_project else []
    return data


def test_get_tasks_dedupes_task_appearing_in_multiple_projects() -> None:
    """An Asana task in N projects is yielded once per poll, and the expensive
    `_fetch_and_add_comments` call only fires for unique tasks."""
    setup = _build_api_with_mocks(
        {
            "P1": [_make_task_data("X"), _make_task_data("Y")],
            "P2": [_make_task_data("X"), _make_task_data("Z")],
        }
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="2026-01-01T00:00:00+00:00",
            start_seconds=0.0,
        )
    )

    assert [t.id for t in yielded] == ["X", "Y", "Z"]
    assert setup.api.task_count == 3
    # Comments fetched only for unique tasks; the duplicate X is skipped before
    # `_fetch_and_add_comments` runs.
    assert setup.stories_api.get_stories_for_task.call_count == 3
    fetched_gids = [
        call.args[0] for call in setup.stories_api.get_stories_for_task.call_args_list
    ]
    assert sorted(fetched_gids) == ["X", "Y", "Z"]


def test_get_tasks_no_duplicates_unchanged() -> None:
    """When projects don't share tasks, every task is yielded and counters
    reflect zero duplicates."""
    setup = _build_api_with_mocks(
        {
            "P1": [_make_task_data("A"), _make_task_data("B")],
            "P2": [_make_task_data("C")],
        }
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="2026-01-01T00:00:00+00:00",
            start_seconds=0.0,
        )
    )

    assert [t.id for t in yielded] == ["A", "B", "C"]
    assert setup.api.task_count == 3
    assert setup.stories_api.get_stories_for_task.call_count == 3


def test_get_tasks_processes_teamless_project_when_no_team_filter() -> None:
    """A workspace-level project (team=None) must still be indexed when no
    team filter is configured. The earlier `if not team_gid: return` skip
    silently dropped these and produced 0/0 syncs in production."""
    setup = _build_api_with_mocks(
        {"P1": [_make_task_data("A")]},
        team_gid=None,
        project_metadata={"P1": {"team": None}},
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="2026-01-01T00:00:00+00:00",
            start_seconds=0.0,
        )
    )

    assert [t.id for t in yielded] == ["A"]


def test_get_tasks_processes_teamless_projects_when_explicitly_allowlisted() -> None:
    """Customer's exact scenario from the Slack thread: three projects are
    explicitly listed in `asana_project_ids`, every one of them returns
    team=None from Asana, and the connector also has a `team_gid` configured.
    Before the fix, every project was dropped at the `no team` skip and the
    sync reported success with 0 documents."""
    setup = _build_api_with_mocks(
        {
            "P1": [_make_task_data("A")],
            "P2": [_make_task_data("B")],
            "P3": [_make_task_data("C")],
        },
        team_gid="T_CONFIGURED",
        project_metadata={
            "P1": {"team": None},
            "P2": {"team": None},
            "P3": {"team": None},
        },
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=["P1", "P2", "P3"],
            start_date="2026-01-01T00:00:00+00:00",
            start_seconds=0.0,
        )
    )

    assert [t.id for t in yielded] == ["A", "B", "C"]


def test_get_tasks_skips_private_project_in_wrong_team() -> None:
    """When a team filter is configured and the user did not allowlist a
    private project that belongs to a different team, the connector must
    still skip it — the team filter is the user's scope-narrowing tool."""
    setup = _build_api_with_mocks(
        {
            "P_IN": [_make_task_data("A")],
            "P_OUT": [_make_task_data("B")],
        },
        team_gid="T_CONFIGURED",
        project_metadata={
            "P_IN": {"team": {"gid": "T_CONFIGURED"}, "privacy_setting": "private"},
            "P_OUT": {"team": {"gid": "T_OTHER"}, "privacy_setting": "private"},
        },
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="2026-01-01T00:00:00+00:00",
            start_seconds=0.0,
        )
    )

    assert [t.id for t in yielded] == ["A"]


def test_get_tasks_processes_private_wrong_team_project_when_allowlisted() -> None:
    """An explicit allowlist overrides the team filter even for private
    projects — the user knows what they want."""
    setup = _build_api_with_mocks(
        {"P_OUT": [_make_task_data("B")]},
        team_gid="T_CONFIGURED",
        project_metadata={
            "P_OUT": {"team": {"gid": "T_OTHER"}, "privacy_setting": "private"},
        },
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=["P_OUT"],
            start_date="2026-01-01T00:00:00+00:00",
            start_seconds=0.0,
        )
    )

    assert [t.id for t in yielded] == ["B"]


def test_get_tasks_skips_private_teamless_project_with_team_filter_and_no_allowlist() -> (
    None
):
    """A private, teamless project cannot satisfy the configured team filter
    (None is never equal to a real team gid), so it must be skipped when the
    user did not explicitly allowlist it. Pins the corner that emerges from
    the new compound skip condition so future refactors of that branch don't
    silently flip its behavior."""
    setup = _build_api_with_mocks(
        {"P_NULL": [_make_task_data("A")]},
        team_gid="T_CONFIGURED",
        project_metadata={
            "P_NULL": {"team": None, "privacy_setting": "private"},
        },
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="2026-01-01T00:00:00+00:00",
            start_seconds=0.0,
        )
    )

    assert yielded == []


def test_get_tasks_processes_public_teamless_project_with_team_filter_and_no_allowlist() -> (
    None
):
    """Public projects are not subject to the team filter — only the
    `private` branch applies it. A teamless public project must therefore
    still index when no allowlist is supplied, even with a team filter
    configured. Pins the symmetric counterpart of the private skip above."""
    setup = _build_api_with_mocks(
        {"P_NULL": [_make_task_data("A")]},
        team_gid="T_CONFIGURED",
        project_metadata={
            "P_NULL": {"team": None, "privacy_setting": "public"},
        },
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="2026-01-01T00:00:00+00:00",
            start_seconds=0.0,
        )
    )

    assert [t.id for t in yielded] == ["A"]


def test_get_tasks_skips_archived_project_even_when_allowlisted() -> None:
    """The archived filter is unconditional — even an explicit allowlist
    shouldn't pull tasks from an archived project (they are read-only and
    typically already indexed)."""
    setup = _build_api_with_mocks(
        {"P_ARCHIVED": [_make_task_data("X")]},
        project_metadata={"P_ARCHIVED": {"archived": True}},
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=["P_ARCHIVED"],
            start_date="2026-01-01T00:00:00+00:00",
            start_seconds=0.0,
        )
    )

    assert yielded == []


def test_subtasks_disabled_by_default_in_config() -> None:
    """The typed config must opt in; existing connectors keep the old
    behavior when the key is absent from their stored config."""
    from onyx.connectors.asana.config import AsanaConnectorConfig

    config = AsanaConnectorConfig(asana_workspace_id="w")

    assert config.index_subtasks is False


def test_connector_passes_subtask_flag_and_window_to_api() -> None:
    """The connector forwards both the toggle and the poll window start (in
    seconds) so the API layer can filter subtasks client-side."""
    with patch("onyx.connectors.asana.connector.asana_api.AsanaAPI") as asana_api_cls:
        client = MagicMock()
        client.get_tasks.return_value = iter([])
        asana_api_cls.return_value = client

        connector = AsanaConnector(asana_workspace_id="w", index_subtasks=True)
        connector.load_credentials({"asana_api_token_secret": "token"})

        list(connector.poll_source(start=1234.0, end=None))

    _, kwargs = client.get_tasks.call_args
    assert kwargs["include_subtasks"] is True
    assert kwargs["start_seconds"] == 1234.0


def test_connector_defaults_to_no_subtask_fetching() -> None:
    """With the toggle off the subtasks endpoint is never called and the
    project query keeps its modified_since filter."""
    setup = _build_api_with_mocks(
        {"P1": [_make_task_data("A")]},
        parent_to_subtasks={"A": [_compact_subtask("S1")]},
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="2026-01-01T00:00:00+00:00",
            start_seconds=0.0,
        )
    )

    assert [t.id for t in yielded] == ["A"]
    setup.tasks_api.get_subtasks_for_task.assert_not_called()
    setup.tasks_api.get_task.assert_not_called()
    sent_opts = setup.tasks_api.get_tasks_for_project.call_args.args[1]
    assert sent_opts["modified_since"] == "2026-01-01T00:00:00+00:00"


def test_subtasks_walked_at_every_depth_and_tagged_with_parent_and_project() -> None:
    """Subtasks at every depth become documents, including subtasks that are
    not project members. Their text carries the direct parent title/link and
    their metadata stays on the root project so access grouping is unchanged."""
    setup = _build_api_with_mocks(
        {"P1": [_make_task_data("A")]},
        parent_to_subtasks={
            "A": [_compact_subtask("S1")],
            "S1": [_compact_subtask("S2")],
            "S2": [],
        },
        full_tasks={
            "S1": _full_subtask("S1", "A", parent_name="Parent A"),
            "S2": _full_subtask("S2", "S1"),
        },
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="1970-01-01T00:00:00+00:00",
            start_seconds=0.0,
            include_subtasks=True,
        )
    )

    by_id = {task.id: task for task in yielded}
    assert list(by_id) == ["A", "S1", "S2"]

    s1 = by_id["S1"]
    assert s1.project_gid == "P1"
    assert s1.parent_name == "Parent A"
    assert s1.parent_link == "https://app.asana.com/0/A"
    assert "Parent task: Parent A" in s1.text
    assert "Parent task link: https://app.asana.com/0/A" in s1.text
    assert s1.link == "https://app.asana.com/0/S1"

    s2 = by_id["S2"]
    assert s2.parent_name == "task-S1"
    assert s2.parent_link == "https://app.asana.com/0/S1"
    assert s2.project_gid == "P1"


def test_subtask_also_in_project_is_indexed_once() -> None:
    """A task that is both a project member and a parent's subtask produces a
    single document and pays for a single comments fetch, regardless of which
    path sees it first."""
    project_copy = _make_task_data("S1")
    project_copy["parent"] = {
        "gid": "A",
        "name": "Parent A",
        "permalink_url": "https://app.asana.com/0/A",
    }
    setup = _build_api_with_mocks(
        {"P1": [_make_task_data("A"), project_copy]},
        parent_to_subtasks={"A": [_compact_subtask("S1")], "S1": []},
        full_tasks={"S1": _full_subtask("S1", "A")},
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="1970-01-01T00:00:00+00:00",
            start_seconds=0.0,
            include_subtasks=True,
        )
    )

    assert [t.id for t in yielded] == ["A", "S1"]
    # The project listing already owned S1, so the subtree walk must not
    # re-fetch it as a full task.
    setup.tasks_api.get_task.assert_not_called()
    assert setup.stories_api.get_stories_for_task.call_count == 2
    assert "Parent task: Parent A" in yielded[1].text


def test_new_subtask_found_when_parent_unchanged() -> None:
    """The parent project task is older than the poll window (so its document
    is not re-emitted), but its subtree is still walked: a changed subtask is
    indexed and a changed grandchild below an unchanged subtask is found too."""
    old_parent = _make_task_data("A")
    old_parent["modified_at"] = "2026-01-01T00:00:00+00:00"
    setup = _build_api_with_mocks(
        {"P1": [old_parent]},
        parent_to_subtasks={
            "A": [_compact_subtask("S1", modified_at="2026-01-01T00:00:00+00:00")],
            "S1": [_compact_subtask("S2")],
            "S2": [],
        },
        full_tasks={"S2": _full_subtask("S2", "S1")},
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date=_WINDOW_START_DATE,
            start_seconds=_WINDOW_START_SECONDS,
            include_subtasks=True,
        )
    )

    assert [t.id for t in yielded] == ["S2"]
    listed_parents = [
        call.args[0] for call in setup.tasks_api.get_subtasks_for_task.call_args_list
    ]
    assert listed_parents == ["A", "S1", "S2"]
    # The unchanged subtask never paid for a full fetch.
    assert [call.args[0] for call in setup.tasks_api.get_task.call_args_list] == ["S2"]


def test_subtask_read_failure_keeps_other_subtasks() -> None:
    """A failing get_task for one subtask is counted and logged; siblings and
    other branches keep indexing and the failed task emits no document (its
    existing index entry stays untouched)."""
    setup = _build_api_with_mocks(
        {"P1": [_make_task_data("A")]},
        parent_to_subtasks={
            "A": [_compact_subtask("S_BAD"), _compact_subtask("S_OK")],
            "S_BAD": [],
            "S_OK": [],
        },
        full_tasks={"S_OK": _full_subtask("S_OK", "A")},
        full_task_errors={"S_BAD": RuntimeError("403 forbidden")},
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="1970-01-01T00:00:00+00:00",
            start_seconds=0.0,
            include_subtasks=True,
        )
    )

    assert [t.id for t in yielded] == ["A", "S_OK"]
    assert setup.api.api_error_count == 1


def test_subtask_listing_failure_does_not_stop_walk() -> None:
    """When listing one parent's children fails, the run records the error and
    still processes sibling project tasks and their subtrees."""
    setup = _build_api_with_mocks(
        {"P1": [_make_task_data("A"), _make_task_data("B")]},
        parent_to_subtasks={"A": [], "B": [_compact_subtask("S1")]},
        full_tasks={"S1": _full_subtask("S1", "B")},
        subtask_list_errors={"A": RuntimeError("500 server error")},
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="1970-01-01T00:00:00+00:00",
            start_seconds=0.0,
            include_subtasks=True,
        )
    )

    assert [t.id for t in yielded] == ["A", "B", "S1"]
    assert setup.api.api_error_count == 1


def test_subtask_listing_consumed_without_hard_limit() -> None:
    """The walk iterates the SDK generator to exhaustion (the SDK handles
    pagination), so a parent with more subtasks than one API page yields all
    of them. Pins against anyone adding an implicit cap."""
    subtask_count = 150
    gids = [f"S{i}" for i in range(subtask_count)]
    setup = _build_api_with_mocks(
        {"P1": [_make_task_data("A")]},
        parent_to_subtasks={
            "A": [_compact_subtask(gid) for gid in gids],
            **{gid: [] for gid in gids},
        },
        full_tasks={gid: _full_subtask(gid, "A") for gid in gids},
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date="1970-01-01T00:00:00+00:00",
            start_seconds=0.0,
            include_subtasks=True,
        )
    )

    assert len(yielded) == subtask_count + 1
    assert setup.tasks_api.get_task.call_count == subtask_count


def test_missing_compact_modified_at_fetches_then_filters() -> None:
    """A compact listing without modified_at cannot be filtered, so the full
    record is fetched and the poll window is applied to it: an old subtask is
    not emitted even though its enumeration lacked a timestamp."""
    recent_parent = _make_task_data("A")
    recent_parent["modified_at"] = _RECENT_DATE
    setup = _build_api_with_mocks(
        {"P1": [recent_parent]},
        parent_to_subtasks={
            "A": [{"gid": "S_OLD"}],
            "S_OLD": [],
        },
        full_tasks={
            "S_OLD": _full_subtask(
                "S_OLD",
                "A",
                modified_at="2026-01-01T00:00:00+00:00",
            )
        },
    )

    yielded = list(
        setup.api.get_tasks(
            project_gids=None,
            start_date=_WINDOW_START_DATE,
            start_seconds=_WINDOW_START_SECONDS,
            include_subtasks=True,
        )
    )

    assert [t.id for t in yielded] == ["A"]
    setup.tasks_api.get_task.assert_called_once()
