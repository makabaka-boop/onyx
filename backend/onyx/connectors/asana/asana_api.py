import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any, Dict

import asana

from onyx.utils.logger import setup_logger

logger = setup_logger()

_TASK_OPTIMIZATION_FIELDS = (
    "name,memberships,memberships.project,completed_at,completed_by,created_at,"
    "created_by,custom_fields,dependencies,due_at,due_on,external,html_notes,liked,likes,"
    "modified_at,notes,num_hearts,parent,parent.name,parent.permalink_url,projects,"
    "resource_subtype,resource_type,start_on,workspace,permalink_url"
)
# Subtask listings return compact task objects; only modified_at is needed to
# decide which subtasks changed in the poll window.
_SUBTASK_ENUMERATION_FIELDS = "modified_at"


# https://github.com/Asana/python-asana/tree/master?tab=readme-ov-file#documentation-for-api-endpoints
class AsanaTask:
    def __init__(
        self,
        id: str,
        title: str,
        text: str,
        link: str,
        last_modified: datetime,
        created_at: datetime,
        project_gid: str,
        project_name: str,
        parent_name: str | None = None,
        parent_link: str | None = None,
    ) -> None:
        self.id = id
        self.title = title
        self.text = text
        self.link = link
        self.last_modified = last_modified
        self.created_at = created_at
        self.project_gid = project_gid
        self.project_name = project_name
        self.parent_name = parent_name
        self.parent_link = parent_link

    def __str__(self) -> str:
        return f"ID: {self.id}\nTitle: {self.title}\nLast modified: {self.last_modified}\nText: {self.text}"


class AsanaAPI:
    def __init__(
        self, api_token: str, workspace_gid: str, team_gid: str | None
    ) -> None:
        self._user = None
        self.workspace_gid = workspace_gid
        self.team_gid = team_gid

        self.configuration = asana.Configuration()
        self.api_client = asana.ApiClient(self.configuration)
        self.tasks_api = asana.TasksApi(self.api_client)
        self.stories_api = asana.StoriesApi(self.api_client)
        self.users_api = asana.UsersApi(self.api_client)
        self.project_api = asana.ProjectsApi(self.api_client)
        self.workspaces_api = asana.WorkspacesApi(self.api_client)

        self.api_error_count = 0
        self.configuration.access_token = api_token
        self.task_count = 0

    def get_tasks(
        self,
        project_gids: list[str] | None,
        start_date: str,
        start_seconds: float,
        include_subtasks: bool = False,
    ) -> Iterator[AsanaTask]:
        """Get all tasks from the projects with the given gids that were modified since the given date.
        If project_gids is None, get all tasks from all projects in the workspace.
        When include_subtasks is True, every in-scope project task roots a full
        subtree walk; subtasks form their own documents even when their parent
        task did not change in the window."""
        logger.info("Starting to fetch Asana projects")
        projects = self.project_api.get_projects(
            opts={
                "workspace": self.workspace_gid,
                "opt_fields": "gid,name,archived,modified_at",
            }
        )
        run_started_seconds = int(time.mktime(datetime.now().timetuple()))
        projects_list = []
        project_count = 0
        for project_info in projects:
            project_gid = project_info["gid"]
            if project_gids is None or project_gid in project_gids:
                projects_list.append(project_gid)
            else:
                logger.debug(
                    "Skipping project: %s - not in accepted project_gids", project_gid
                )
            project_count += 1
            if project_count % 100 == 0:
                logger.info("Processed %s projects", project_count)

        logger.info("Found %s projects to process", len(projects_list))
        # Asana tasks can belong to multiple projects and thus tasks
        # can get reported multiple times. Shared with subtree walks so a
        # subtask that is also a project member is indexed exactly once.
        seen_task_gids: set[str] = set()
        for project_gid in projects_list:
            for task in self._get_tasks_for_project(
                project_gid,
                start_date,
                start_seconds,
                seen_task_gids,
                project_gids,
                include_subtasks,
                run_started_seconds,
            ):
                yield task
        logger.info("Completed fetching %s tasks from Asana", self.task_count)
        if self.api_error_count > 0:
            logger.warning(
                "Encountered %s API errors during task fetching", self.api_error_count
            )

    def _get_tasks_for_project(
        self,
        project_gid: str,
        start_date: str,
        start_seconds: float,
        seen_task_gids: set[str],
        project_gids: list[str] | None,
        include_subtasks: bool,
        run_started_seconds: int,
    ) -> Iterator[AsanaTask]:
        project = self.project_api.get_project(project_gid, opts={})
        project_name = project.get("name", project_gid)
        team = project.get("team") or {}
        team_gid = team.get("gid")

        if project.get("archived"):
            logger.info("Skipping archived project: %s (%s)", project_name, project_gid)
            return
        # The team filter narrows the workspace-wide sync. When the user has
        # picked specific projects via `project_gids`, respect that choice
        # regardless of team membership — those projects were explicitly
        # opted in upstream by the project_gid filter in get_tasks.
        if (
            project.get("privacy_setting") == "private"
            and self.team_gid
            and team_gid != self.team_gid
            and project_gids is None
        ):
            logger.info(
                "Skipping private project not in configured team: %s (%s)",
                project_name,
                project_gid,
            )
            return

        simple_start_date = start_date.split(".")[0].split("+")[0]
        logger.info(
            "Fetching tasks%s for project: %s (%s)",
            f" modified since {simple_start_date}" if not include_subtasks else "",
            project_name,
            project_gid,
        )

        opts: Dict[str, str] = {"opt_fields": _TASK_OPTIMIZATION_FIELDS}
        # With subtasks enabled the full project list is needed every run:
        # new subtasks must be found even when their parent task is unchanged,
        # so the poll window is applied client-side instead of in the query.
        if not include_subtasks:
            opts["modified_since"] = start_date
        tasks_from_api = self.tasks_api.get_tasks_for_project(project_gid, opts)

        subtask_roots: list[str] = []
        for data in tasks_from_api:
            gid = data["gid"]
            if gid in seen_task_gids:
                logger.debug(
                    "Skipping duplicate Asana task %s (already yielded for another project)",
                    gid,
                )
                continue
            seen_task_gids.add(gid)

            self.task_count += 1
            if self.task_count % 10 == 0:
                end_seconds = time.mktime(datetime.now().timetuple())
                runtime_seconds = end_seconds - run_started_seconds
                if runtime_seconds > 0:
                    logger.info(
                        "Processed %s tasks in %s seconds (%s tasks/second)",
                        self.task_count,
                        format(runtime_seconds, ".0f"),
                        format(self.task_count / runtime_seconds, ".2f"),
                    )

            logger.debug("Processing Asana task: %s", data["name"])

            if include_subtasks:
                subtask_roots.append(gid)
                if not self._modified_since(data.get("modified_at"), start_seconds):
                    logger.debug(
                        "Skipping unchanged Asana project task %s outside poll window",
                        gid,
                    )
                    continue

            task = self._build_task(data, project_gid, project_name)
            if task is not None:
                yield task

        if include_subtasks and subtask_roots:
            yield from self._iter_subtasks(
                subtask_roots,
                project_gid,
                project_name,
                seen_task_gids,
                start_seconds,
            )

    def _build_task(
        self, data: Dict[str, Any], project_gid: str, project_name: str
    ) -> AsanaTask | None:
        """Builds the index document for a task. Returns None and records the
        error when the task cannot be read, so the caller can keep going and
        the task keeps its existing index entry."""
        gid = data["gid"]
        try:
            text = self._construct_task_text(data)
            text += self._fetch_and_add_comments(gid)

            last_modified_date = self.format_date(data["modified_at"])
            text += f"Last modified: {last_modified_date}\n"

            parent = data.get("parent") or {}
            return AsanaTask(
                id=gid,
                title=data["name"],
                text=text,
                link=data["permalink_url"],
                last_modified=datetime.fromisoformat(data["modified_at"]),
                created_at=datetime.fromisoformat(data["created_at"]),
                project_gid=project_gid,
                project_name=project_name,
                parent_name=parent.get("name"),
                parent_link=parent.get("permalink_url"),
            )
        except Exception:
            logger.error(
                "Error processing task %s in project %s",
                gid,
                project_gid,
                exc_info=True,
            )
            self.api_error_count += 1
            return None

    def _iter_subtasks(
        self,
        root_gids: list[str],
        project_gid: str,
        project_name: str,
        seen_task_gids: set[str],
        start_seconds: float,
    ) -> Iterator[AsanaTask]:
        """Yields changed subtasks at every depth below root_gids.

        The tree is walked with an explicit stack because Asana allows deep
        nesting. Every node is enumerated regardless of its modified time so
        descendants of unchanged tasks stay discoverable, but only subtasks
        modified in the poll window pay for a full task fetch and comments.
        Read failures on one node never stop the rest of the walk.
        """
        pending = list(root_gids)
        while pending:
            parent_gid = pending.pop()
            try:
                subtasks = iter(
                    self.tasks_api.get_subtasks_for_task(
                        parent_gid,
                        opts={"opt_fields": _SUBTASK_ENUMERATION_FIELDS},
                    )
                )
            except Exception as e:
                logger.error(
                    "Failed to list subtasks of Asana task %s: %s",
                    parent_gid,
                    e,
                    exc_info=True,
                )
                self.api_error_count += 1
                continue

            while True:
                try:
                    compact = next(subtasks)
                except StopIteration:
                    break
                except Exception as e:
                    # A failed pagination call loses the remainder of this
                    # parent's listing; other branches must still be walked.
                    logger.error(
                        "Failed to read subtasks of Asana task %s: %s",
                        parent_gid,
                        e,
                        exc_info=True,
                    )
                    self.api_error_count += 1
                    break

                gid = compact["gid"]
                if gid in seen_task_gids:
                    logger.debug(
                        "Skipping duplicate Asana subtask %s (already processed)",
                        gid,
                    )
                    continue
                seen_task_gids.add(gid)
                # Descendants are walked even when this node is unchanged or
                # unreadable: new subtasks can sit below any task.
                pending.append(gid)

                if not self._modified_since(compact.get("modified_at"), start_seconds):
                    continue

                try:
                    data = self.tasks_api.get_task(
                        gid, opts={"opt_fields": _TASK_OPTIMIZATION_FIELDS}
                    )
                    # Re-check on the full record in case the compact listing
                    # omitted modified_at.
                    if not self._modified_since(data.get("modified_at"), start_seconds):
                        continue
                except Exception as e:
                    logger.error(
                        "Failed to read Asana subtask %s of task %s: %s",
                        gid,
                        parent_gid,
                        e,
                        exc_info=True,
                    )
                    self.api_error_count += 1
                    continue

                self.task_count += 1
                logger.debug("Processing Asana subtask: %s", data.get("name", gid))
                task = self._build_task(data, project_gid, project_name)
                if task is not None:
                    yield task

    def _modified_since(self, modified_at: str | None, start_seconds: float) -> bool:
        """True when modified_at is at or after the poll window start. A
        missing timestamp cannot be filtered safely, so returns True and lets
        the caller decide after a full fetch."""
        if not modified_at:
            return True
        return datetime.fromisoformat(modified_at).timestamp() >= start_seconds

    def _construct_task_text(self, data: Dict) -> str:
        text = f"{data['name']}\n\n"

        if data["notes"]:
            text += f"{data['notes']}\n\n"

        parent = data.get("parent") or {}
        if parent.get("gid"):
            text += f"Parent task: {parent.get('name') or parent['gid']}\n"
            if parent.get("permalink_url"):
                text += f"Parent task link: {parent['permalink_url']}\n"
            text += "\n"

        if data["created_by"] and data["created_by"]["gid"]:
            creator = self.get_user(data["created_by"]["gid"])["name"]
            created_date = self.format_date(data["created_at"])
            text += f"Created by: {creator} on {created_date}\n"

        if data["due_on"]:
            due_date = self.format_date(data["due_on"])
            text += f"Due date: {due_date}\n"

        if data["completed_at"]:
            completed_date = self.format_date(data["completed_at"])
            text += f"Completed on: {completed_date}\n"

        text += "\n"
        return text

    def _fetch_and_add_comments(self, task_gid: str) -> str:
        text = ""
        stories_opts: Dict[str, str] = {}
        story_start = time.time()
        stories = self.stories_api.get_stories_for_task(task_gid, stories_opts)

        story_count = 0
        comment_count = 0

        for story in stories:
            story_count += 1
            if story["resource_subtype"] == "comment_added":
                comment = self.stories_api.get_story(
                    story["gid"], opts={"opt_fields": "text,created_by,created_at"}
                )
                commenter = self.get_user(comment["created_by"]["gid"])["name"]
                text += f"Comment by {commenter}: {comment['text']}\n\n"
                comment_count += 1

        story_duration = time.time() - story_start
        logger.debug(
            "Processed %s stories (including %s comments) in %s seconds",
            story_count,
            comment_count,
            format(story_duration, ".2f"),
        )

        return text

    def get_user(self, user_gid: str) -> Dict:
        if self._user is not None:
            return self._user
        self._user = self.users_api.get_user(user_gid, {"opt_fields": "name,email"})

        if not self._user:
            logger.warning(
                "Unable to fetch user information for user_gid: %s", user_gid
            )
            return {"name": "Unknown"}
        return self._user

    def format_date(self, date_str: str) -> str:
        date = datetime.fromisoformat(date_str)
        return time.strftime("%Y-%m-%d", date.timetuple())

    def get_time(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
