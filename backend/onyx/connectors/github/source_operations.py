"""Source operations for the GitHub connector.

This is the only module in ``github/`` and in the EE GitHub permission sync
that imports PyGithub. PyGithub objects make network calls when a caller reads
an attribute that the first response did not have. So the operations call the
REST API through PyGithub's requester (auth, base URL, retries) and return
plain data (``github/models.py``), never PyGithub objects.

SDK errors leave the gateway as ``GitHubApiError``, and rate limits (primary
and secondary) as ``GitHubRateLimitError``. Generator operations translate
errors raised while the caller iterates.
"""

import base64
import functools
import inspect
import json
import re
import threading
import urllib.parse
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, ParamSpec, TypeVar, cast

from github import Auth, Github, GithubException, RateLimitExceededException
from github.GithubRetry import GithubRetry
from github.Requester import Requester
from urllib3.response import BaseHTTPResponse
from urllib3.util.retry import Retry

from onyx.configs.constants import DocumentSource
from onyx.connectors.capabilities import CredentialCapability
from onyx.connectors.cross_connector_utils.server_wait import bound_server_wait
from onyx.connectors.exceptions import SourceApiError
from onyx.connectors.github.models import (
    GitHubBranch,
    GitHubCursorItem,
    GitHubDiscussion,
    GitHubDiscussionCategory,
    GitHubDiscussionComment,
    GitHubDiscussionPage,
    GitHubDiscussionRef,
    GitHubFileContent,
    GitHubGitTree,
    GitHubIssue,
    GitHubOrganizationRef,
    GitHubPullRequest,
    GitHubRepository,
    GitHubTokenInfo,
    GitHubTokenKind,
    GitHubTreeEntry,
    GitHubUser,
)
from onyx.connectors.github.utils import resolve_github_base_url
from onyx.connectors.models import ConnectorMissingCredentialError
from onyx.connectors.source_operations import (
    OperationConsumes,
    SourceOperations,
    source_operation,
)
from onyx.utils.logger import setup_logger

logger = setup_logger()

_P = ParamSpec("_P")
_R = TypeVar("_R")

GITHUB_ACCESS_TOKEN_KEY = "github_access_token"

# The largest page GitHub allows.
_PER_PAGE = 100
_RATE_LIMITED_STATUS = 429
_FINE_GRAINED_TOKEN_PREFIX = "github_pat_"
_CLASSIC_TOKEN_PREFIX = "ghp_"
_SCOPES_HEADER = "x-oauth-scopes"
_ACCEPTED_SCOPES_HEADER = "x-accepted-oauth-scopes"
_ACCEPTED_PERMISSIONS_HEADER = "x-accepted-github-permissions"
_RATE_LIMIT_REMAINING_HEADER = "x-ratelimit-remaining"
_RETRY_AFTER_HEADER = "retry-after"
_SSO_HEADER = "x-github-sso"

_ALL_CAPABILITIES = frozenset(CredentialCapability)
_CURSOR_FALLBACK = (
    "Used only when GitHub refuses offset pages for a large listing; it reads "
    "the same endpoint as the offset listing that the checks probe."
)


class BoundedGithubRetry(GithubRetry):
    """GithubRetry whose server-requested waits go through bound_server_wait."""

    def increment(self, *args: Any, **kwargs: Any) -> Retry:
        retry = super().increment(*args, **kwargs)
        # GithubRetry replaces get_backoff_time with a wait until the rate
        # limit resets.
        backoff = bound_server_wait(retry.get_backoff_time(), "github")
        retry.get_backoff_time = lambda: backoff  # ty: ignore[invalid-assignment]
        return retry

    def get_retry_after(self, response: BaseHTTPResponse) -> float | None:
        retry_after = super().get_retry_after(response)
        if retry_after is None:
            return None
        return bound_server_wait(retry_after, "github")


class GitHubApiError(SourceApiError):
    """A GitHub API call returned an error response.

    ``github_message`` is the ``message`` of the error body. The two
    ``accepted_*`` values come from the response headers: the OAuth scopes
    (classic token) or the fine-grained permissions that the endpoint accepts.
    ``sso_required`` is True when a SAML organization has not authorized the
    token.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None,
        github_message: str | None = None,
        accepted_scopes: str | None = None,
        accepted_permissions: str | None = None,
        sso_required: bool = False,
        method: str | None = None,
        url: str | None = None,
        body: str | None = None,
    ) -> None:
        super().__init__(
            message, status_code=status_code, method=method, url=url, body=body
        )
        self.github_message = github_message
        self.accepted_scopes = accepted_scopes
        self.accepted_permissions = accepted_permissions
        self.sso_required = sso_required


class GitHubRateLimitError(GitHubApiError):
    """GitHub rate limited the call, after the client's own retries.
    ``secondary`` is True for a secondary (abuse) rate limit."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None,
        github_message: str | None = None,
        secondary: bool,
        method: str | None = None,
        url: str | None = None,
        body: str | None = None,
    ) -> None:
        super().__init__(
            message,
            status_code=status_code,
            github_message=github_message,
            method=method,
            url=url,
            body=body,
        )
        self.secondary = secondary


class GitHubDiscussionsUnavailableError(GitHubApiError):
    """Discussions are turned off for the repository, or the server's GraphQL
    schema has no discussions support. The crawl treats it like the 404 a
    disabled issues or pull requests listing returns: it skips discussions for
    the repository instead of failing the run."""


def _header(headers: Any, name: str) -> str | None:
    if not isinstance(headers, dict):
        return None
    value: Any = headers.get(name)
    return str(value) if value is not None else None


def _github_message(error: GithubException) -> str | None:
    data: Any = error.data
    if isinstance(data, dict) and isinstance(data.get("message"), str):
        return data["message"]
    return error.message


def _body(error: GithubException) -> str | None:
    data: Any = error.data
    if data is None:
        return None
    return data if isinstance(data, str) else json.dumps(data)


def _translate(
    error: GithubException, *, method: str | None = None, url: str | None = None
) -> GitHubApiError:
    """The gateway's error for an SDK error. ``method`` and ``url`` are the
    request, when the caller knows it."""
    headers: Any = error.headers
    github_message: str | None = _github_message(error)
    lowered: str = (github_message or "").lower()
    if (
        isinstance(error, RateLimitExceededException)
        or error.status == _RATE_LIMITED_STATUS
        or (
            error.status == 403
            and _header(headers, _RATE_LIMIT_REMAINING_HEADER) == "0"
        )
    ):
        return GitHubRateLimitError(
            str(error),
            status_code=error.status,
            github_message=github_message,
            method=method,
            url=url,
            body=_body(error),
            secondary=(
                Requester.isSecondaryRateLimitError(lowered)
                or _header(headers, _RETRY_AFTER_HEADER) is not None
            ),
        )
    return GitHubApiError(
        str(error),
        status_code=error.status,
        github_message=github_message,
        accepted_scopes=_header(headers, _ACCEPTED_SCOPES_HEADER),
        accepted_permissions=_header(headers, _ACCEPTED_PERMISSIONS_HEADER),
        sso_required=bool(_header(headers, _SSO_HEADER)),
        method=method,
        url=url,
        body=_body(error),
    )


@contextmanager
def _github_errors() -> Iterator[None]:
    try:
        yield
    except GithubException as error:
        raise _translate(error) from error


def _translated(func: Callable[_P, _R]) -> Callable[_P, _R]:
    """Raises the gateway's errors in place of the SDK's. A generator operation
    is wrapped for the whole iteration, since its calls run lazily."""
    if inspect.isgeneratorfunction(func):

        @functools.wraps(func)
        def generator(*args: Any, **kwargs: Any) -> Iterator[Any]:
            with _github_errors():
                yield from func(*args, **kwargs)

        return cast(Callable[_P, _R], generator)

    @functools.wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        with _github_errors():
            return func(*args, **kwargs)

    return wrapper


def _next_link(headers: dict[str, Any]) -> str | None:
    """The ``next`` URL of a ``Link`` header, as PyGithub parses it."""
    link: Any = headers.get("link")
    if not isinstance(link, str):
        return None
    for part in link.split(", "):
        url, rel, *_rest = part.split("; ")
        if rel == 'rel="next"':
            return url[1:-1]
    return None


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def _token_kind(token: str, scopes: list[str] | None) -> GitHubTokenKind:
    if token.startswith(_FINE_GRAINED_TOKEN_PREFIX):
        return GitHubTokenKind.FINE_GRAINED
    if scopes is not None or token.startswith(_CLASSIC_TOKEN_PREFIX):
        return GitHubTokenKind.CLASSIC
    return GitHubTokenKind.OTHER


def _logins(
    listing: Iterator[tuple[dict[str, Any], str | None]], limit: int | None
) -> list[str]:
    """The logins of a user listing, up to ``limit``."""
    logins: list[str] = []
    for raw, _next_url in listing:
        logins.append(raw["login"])
        if limit is not None and len(logins) >= limit:
            break
    return logins


def _login(actor: Any) -> str | None:
    """The login of a GraphQL actor (``author { login }``). GraphQL returns
    null for an author that deleted their account."""
    if not isinstance(actor, dict):
        return None
    login: Any = actor.get("login")
    return login if isinstance(login, str) else None


def _listing_params(state: str) -> dict[str, Any]:
    """The query of a pull request or issue listing: newest update first."""
    return {"state": state, "sort": "updated", "direction": "desc"}


# Discussions are only available through the GraphQL API. The REST API root of
# a GitHub Enterprise Server is /api/v3, while its GraphQL endpoint is
# /api/graphql.
_GHES_REST_SUFFIX = "/api/v3"
_GHES_GRAPHQL_SUFFIX = "/api/graphql"
_GRAPHQL_PATH = "/graphql"
# Discussions listed per checkpoint batch; comments and replies per page.
_DISCUSSIONS_PER_PAGE = 50
_GRAPHQL_PAGE_SIZE = 100

_LIST_DISCUSSIONS_QUERY = """
query OnyxListDiscussions($owner: String!, $name: String!, $first: Int!, $after: String) {
  repository(owner: $owner, name: $name) {
    discussions(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { endCursor hasNextPage }
      nodes {
        number
        title
        url
        createdAt
        updatedAt
        author { login }
        category { name slug }
      }
    }
  }
}
"""

_DISCUSSION_COMMENT_FIELDS = """
  id
  databaseId
  url
  body
  createdAt
  updatedAt
  author { login }
  replyTo { databaseId }
"""

_DISCUSSION_COMMENTS_QUERY = (
    """
query OnyxDiscussionComments($owner: String!, $name: String!, $number: Int!,
                              $commentsFirst: Int!, $commentsAfter: String) {
  repository(owner: $owner, name: $name) {
    discussion(number: $number) {
      number
      title
      body
      url
      createdAt
      updatedAt
      author { login }
      category { name slug }
      answerChosenAt
      answerChosenBy { login }
      answer { id }
      comments(first: $commentsFirst, after: $commentsAfter) {
        pageInfo { endCursor hasNextPage }
        nodes {
"""
    + _DISCUSSION_COMMENT_FIELDS
    + """
          replies(first: """
    + str(_GRAPHQL_PAGE_SIZE)
    + """) {
            pageInfo { endCursor hasNextPage }
            nodes {
"""
    + _DISCUSSION_COMMENT_FIELDS
    + """
            }
          }
        }
      }
    }
  }
}
"""
)

_COMMENT_REPLIES_QUERY = (
    """
query OnyxCommentReplies($commentId: ID!, $first: Int!, $after: String) {
  node(id: $commentId) {
    ... on DiscussionComment {
      replies(first: $first, after: $after) {
        pageInfo { endCursor hasNextPage }
        nodes {
"""
    + _DISCUSSION_COMMENT_FIELDS
    + """
        }
      }
    }
  }
}
"""
)

_DISCUSSIONS_DISABLED_RE = re.compile(
    r"discussions? (?:are|is) not enabled|"
    r"field ['\"]discussions?['\"] .*doesn't exist",
    re.IGNORECASE,
)
_GRAPHQL_RATE_LIMIT_RE = re.compile(r"rate limit|abuse", re.IGNORECASE)
_GRAPHQL_SECONDARY_RATE_LIMIT_RE = re.compile(
    r"secondary rate limit|abuse", re.IGNORECASE
)


def _split_repo_full_name(repo_full_name: str) -> tuple[str, str]:
    owner, _, name = repo_full_name.partition("/")
    return owner, name


def _discussion_category(raw: Any) -> GitHubDiscussionCategory | None:
    if not isinstance(raw, dict):
        return None
    name: Any = raw.get("name")
    if not isinstance(name, str):
        return None
    slug: Any = raw.get("slug")
    return GitHubDiscussionCategory(
        name=name, slug=slug if isinstance(slug, str) else None
    )


def _discussion_ref(raw: dict[str, Any]) -> GitHubDiscussionRef:
    return GitHubDiscussionRef(
        number=raw["number"],
        title=raw.get("title") or "",
        url=raw["url"],
        created_at=raw.get("createdAt"),
        updated_at=raw.get("updatedAt"),
        author_login=_login(raw.get("author")),
        category=_discussion_category(raw.get("category")),
    )


def _discussion_comment(raw: dict[str, Any]) -> GitHubDiscussionComment:
    """One comment with the replies the current page carries."""
    reply_to: Any = raw.get("replyTo")
    replies_connection: Any = raw.get("replies")
    reply_nodes: list[Any] = (
        replies_connection.get("nodes")
        if isinstance(replies_connection, dict)
        and isinstance(replies_connection.get("nodes"), list)
        else []
    )
    return GitHubDiscussionComment(
        id=raw["id"],
        database_id=raw.get("databaseId"),
        url=raw.get("url"),
        body=raw.get("body"),
        created_at=raw.get("createdAt"),
        updated_at=raw.get("updatedAt"),
        author_login=_login(raw.get("author")),
        reply_to_id=(
            reply_to.get("databaseId") if isinstance(reply_to, dict) else None
        ),
        replies=[
            _discussion_comment(reply)
            for reply in reply_nodes
            if isinstance(reply, dict) and reply.get("id")
        ],
    )


def _discussion_replies_page_info(raw: dict[str, Any]) -> tuple[str | None, bool]:
    """The replies paging state of a comment node of a comments page."""
    replies_connection: Any = raw.get("replies")
    page_info: Any = (
        replies_connection.get("pageInfo")
        if isinstance(replies_connection, dict)
        else None
    )
    if not isinstance(page_info, dict):
        return None, False
    end_cursor: Any = page_info.get("endCursor")
    return (
        end_cursor if isinstance(end_cursor, str) else None,
        bool(page_info.get("hasNextPage")),
    )


def _graphql_error_messages(errors: Any) -> list[str]:
    if not isinstance(errors, list):
        return []
    return [
        error["message"]
        for error in errors
        if isinstance(error, dict) and isinstance(error.get("message"), str)
    ]


def _is_discussions_unavailable(errors: Any) -> bool:
    return any(
        bool(_DISCUSSIONS_DISABLED_RE.search(message))
        for message in _graphql_error_messages(errors)
    )


def _graphql_rate_limit(errors: Any) -> GitHubRateLimitError | None:
    messages = _graphql_error_messages(errors)
    if not any(_GRAPHQL_RATE_LIMIT_RE.search(message) for message in messages):
        return None
    secondary = any(
        bool(_GRAPHQL_SECONDARY_RATE_LIMIT_RE.search(message)) for message in messages
    )
    detail = "; ".join(messages)
    return GitHubRateLimitError(
        detail,
        status_code=None,
        github_message=detail,
        secondary=secondary,
        method="POST",
    )


class GitHubSourceOperations(SourceOperations):
    source = DocumentSource.GITHUB
    sdk_modules = ("github",)
    # The gateway reads only the credential.
    config_keys = frozenset()

    def _thread_clients(self) -> threading.local:
        """This gateway's clients, one per thread. ``dict.setdefault`` is
        atomic under the GIL, so no guard lock is needed."""
        return vars(self).setdefault("_clients", threading.local())

    def _token(self) -> str:
        token: Any = self.credentials_provider.get_credentials().get(
            GITHUB_ACCESS_TOKEN_KEY
        )
        if not isinstance(token, str) or not token:
            raise ConnectorMissingCredentialError("GitHub (no access token)")
        return token

    def _client(self) -> Github:
        """This thread's client. Threads must not share one: PyGithub keeps
        each request's URL and headers on one connection object between
        sending the request and reading the response."""
        clients: threading.local = self._thread_clients()
        cached: Github | None = vars(clients).get("client")
        if cached is not None:
            return cached
        credentials: dict[str, Any] = self.credentials_provider.get_credentials()
        base_url: str | None = resolve_github_base_url(credentials)
        kwargs: dict[str, Any] = {"base_url": base_url} if base_url else {}
        client = Github(
            auth=Auth.Token(self._token()),
            per_page=_PER_PAGE,
            retry=BoundedGithubRetry(),
            **kwargs,
        )
        clients.client = client
        return client

    def _get(
        self, url: str, params: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], Any]:
        """One GET through PyGithub's requester. Returns the response headers
        (lowercase names) and the decoded JSON."""
        requester: Requester = self._client().requester
        try:
            return requester.requestJsonAndCheck("GET", url, parameters=params)
        except GithubException as e:
            absolute: str = (
                url
                if url.startswith(("http://", "https://"))
                else requester.base_url + url
            )
            raise _translate(e, method="GET", url=absolute) from e

    def _paginate(
        self,
        url: str,
        params: dict[str, Any] | None,
        *,
        skip: int = 0,
    ) -> Iterator[tuple[dict[str, Any], str | None]]:
        """Yields each raw item of a listing with the URL of the page after
        its page. Follows the ``Link`` header, as PyGithub does. ``params``
        apply to the first request only: next URLs carry their own query."""
        next_url: str | None = url
        request_params: dict[str, Any] | None = (
            {**(params or {}), "per_page": _PER_PAGE} if params is not None else None
        )
        skipped: int = 0
        while next_url is not None:
            headers, data = self._get(next_url, request_params)
            page: list[Any] = data if isinstance(data, list) else []
            next_url = _next_link(headers) if page else None
            request_params = None
            for raw in page:
                if raw is None:
                    continue
                if skipped < skip:
                    skipped += 1
                    continue
                yield raw, next_url

    def _list_page(
        self, url: str, params: dict[str, Any], page: int
    ) -> list[dict[str, Any]]:
        """One offset page of a listing, as PyGithub's ``get_page``: page 0
        sends no ``page`` parameter."""
        page_params: dict[str, Any] = {**params, "per_page": _PER_PAGE}
        if page != 0:
            page_params["page"] = page + 1
        _headers, data = self._get(url, page_params)
        return [raw for raw in (data if isinstance(data, list) else []) if raw]

    def _graphql_url(self) -> str:
        """The GraphQL endpoint. A GitHub Enterprise Server serves it at
        /api/graphql, next to the /api/v3 REST root."""
        base_url: str = self._client().requester.base_url
        if base_url.endswith(_GHES_REST_SUFFIX):
            return base_url[: -len(_GHES_REST_SUFFIX)] + _GHES_GRAPHQL_SUFFIX
        return base_url + _GRAPHQL_PATH

    def _graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """One GraphQL call. Returns the ``data`` object.

        GraphQL answers HTTP 200 with an ``errors`` array for a rejected
        field (a turned-off feature, a missing grant, the rate limit), so the
        HTTP success status is not enough.
        """
        url: str = self._graphql_url()
        try:
            _headers, data = self._client().requester.requestJsonAndCheck(
                "POST", url, input={"query": query, "variables": variables}
            )
        except GithubException as error:
            translated: GitHubApiError = _translate(error, method="POST", url=url)
            # GitHub answers a secondary GraphQL rate limit with HTTP 402.
            if error.status == 402 and not isinstance(translated, GitHubRateLimitError):
                raise GitHubRateLimitError(
                    str(error),
                    status_code=error.status,
                    github_message=_github_message(error),
                    secondary=True,
                    method="POST",
                    url=url,
                    body=_body(error),
                ) from error
            raise translated from error

        if not isinstance(data, dict):
            raise GitHubApiError(
                "GitHub GraphQL returned no JSON object",
                status_code=None,
                method="POST",
                url=url,
            )
        errors: Any = data.get("errors")
        if errors:
            if _is_discussions_unavailable(errors):
                detail = "; ".join(_graphql_error_messages(errors))
                raise GitHubDiscussionsUnavailableError(
                    detail,
                    status_code=None,
                    github_message=detail,
                    method="POST",
                    url=url,
                )
            rate_limit_error: GitHubRateLimitError | None = _graphql_rate_limit(errors)
            if rate_limit_error is not None:
                rate_limit_error.url = url
                raise rate_limit_error
            detail = "; ".join(_graphql_error_messages(errors)) or str(errors)
            raise GitHubApiError(
                detail,
                status_code=None,
                github_message=detail,
                method="POST",
                url=url,
            )
        payload: Any = data.get("data")
        if not isinstance(payload, dict):
            raise GitHubApiError(
                "GitHub GraphQL returned no data",
                status_code=None,
                method="POST",
                url=url,
            )
        return payload

    def _list_repositories(self, url: str, limit: int | None) -> list[GitHubRepository]:
        repositories: list[GitHubRepository] = []
        for raw, _next_url in self._paginate(url, {}):
            repositories.append(GitHubRepository.from_raw(raw))
            if limit is not None and len(repositories) >= limit:
                break
        return repositories

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.CREDENTIAL,
    )
    @_translated
    def get_token_info(self) -> GitHubTokenInfo:
        """The account the token acts as (``GET /user``), the token kind and,
        for a classic token, its OAuth scopes."""
        headers, data = self._get("/user")
        scopes_header: str | None = _header(headers, _SCOPES_HEADER)
        scopes: list[str] | None = (
            [scope.strip() for scope in scopes_header.split(",") if scope.strip()]
            if scopes_header is not None
            else None
        )
        return GitHubTokenInfo(
            login=data["login"],
            kind=_token_kind(self._token(), scopes),
            scopes=scopes,
        )

    @source_operation(
        capabilities=_ALL_CAPABILITIES,
        consumes=OperationConsumes.CREDENTIAL,
        untested=(
            "Called only after a rate-limit error, to wait until the limit "
            "resets; it changes no verdict."
        ),
    )
    @_translated
    def get_rate_limit_reset(self) -> datetime:
        """When the core rate limit resets (``GET /rate_limit``)."""
        _headers, data = self._get("/rate_limit")
        reset: int = data["resources"]["core"]["reset"]
        return datetime.fromtimestamp(reset, tz=timezone.utc)

    @source_operation(
        capabilities=_ALL_CAPABILITIES,
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def get_organization(self, *, login: str) -> GitHubOrganizationRef:
        """An organization (``GET /orgs/{login}``). Raises ``GitHubApiError``
        (404) when ``login`` is not an organization."""
        _headers, data = self._get(f"/orgs/{_quote(login)}")
        return GitHubOrganizationRef(id=data["id"], login=data["login"])

    @source_operation(
        capabilities=_ALL_CAPABILITIES,
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def list_organization_repositories(
        self, *, org_login: str, limit: int | None = None
    ) -> list[GitHubRepository]:
        """The organization's repositories that the token can see
        (``GET /orgs/{org}/repos``). ``limit`` stops the listing early."""
        return self._list_repositories(f"/orgs/{_quote(org_login)}/repos", limit)

    @source_operation(
        capabilities=_ALL_CAPABILITIES,
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def list_user_repositories(
        self, *, login: str, limit: int | None = None
    ) -> list[GitHubRepository]:
        """A user's repositories (``GET /users/{login}/repos``). GitHub lists
        only public repositories here, also for the token's own user."""
        return self._list_repositories(f"/users/{_quote(login)}/repos", limit)

    @source_operation(
        capabilities=_ALL_CAPABILITIES,
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def get_repository(self, *, owner: str, name: str) -> GitHubRepository:
        """One repository (``GET /repos/{owner}/{name}``). GitHub answers 404
        for a private repository that the token cannot see."""
        _headers, data = self._get(f"/repos/{_quote(owner)}/{_quote(name)}")
        return GitHubRepository.from_raw(data)

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
        untested=(
            "Re-reads, by id, a repository that a listing returned during "
            "indexing; the repository checks probe the same access."
        ),
    )
    @_translated
    def get_repository_by_id(self, *, repo_id: int) -> GitHubRepository:
        """One repository by id (``GET /repositories/{id}``)."""
        _headers, data = self._get(f"/repositories/{repo_id}")
        return GitHubRepository.from_raw(data)

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def list_root_contents(self, *, repo_full_name: str) -> list[str]:
        """The names in the repository's root directory
        (``GET /repos/{repo}/contents/``)."""
        _headers, data = self._get(f"/repos/{repo_full_name}/contents/")
        entries: list[Any] = data if isinstance(data, list) else [data]
        return [
            str(entry.get("name"))
            for entry in entries
            if isinstance(entry, dict) and entry.get("name")
        ]

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def get_branch(self, *, repo_full_name: str, branch: str) -> GitHubBranch:
        """One branch. Raises ``GitHubApiError`` (404) when it does not
        exist."""
        _headers, data = self._get(
            f"/repos/{repo_full_name}/branches/{urllib.parse.quote(branch)}"
        )
        return GitHubBranch(name=data["name"])

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def list_pull_requests(
        self, *, repo_full_name: str, state: str, page: int
    ) -> list[GitHubPullRequest]:
        """One offset page of pull requests, newest update first. GitHub
        answers 422 when a large listing needs cursor pagination."""
        return [
            GitHubPullRequest.from_raw(raw, detailed=False)
            for raw in self._list_page(
                f"/repos/{repo_full_name}/pulls", _listing_params(state), page
            )
        ]

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
        untested=_CURSOR_FALLBACK,
    )
    @_translated
    def iter_pull_requests_by_cursor(
        self,
        *,
        repo_full_name: str,
        state: str,
        cursor_url: str | None = None,
        skip: int = 0,
    ) -> Iterator[GitHubCursorItem[GitHubPullRequest]]:
        """Pull requests by cursor pagination, from ``cursor_url`` or from the
        first page. ``skip`` drops that many items first."""
        for raw, next_url in self._paginate(
            cursor_url or f"/repos/{repo_full_name}/pulls",
            None if cursor_url else _listing_params(state),
            skip=skip,
        ):
            yield GitHubCursorItem(
                item=GitHubPullRequest.from_raw(raw, detailed=False),
                next_url=next_url,
            )

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def get_pull_request(
        self, *, repo_full_name: str, number: int
    ) -> GitHubPullRequest:
        """One pull request with the fields a listing leaves out (merged,
        commits, changed files, merged by)."""
        _headers, data = self._get(f"/repos/{repo_full_name}/pulls/{number}")
        return GitHubPullRequest.from_raw(data, detailed=True)

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def list_issues(
        self, *, repo_full_name: str, state: str, page: int
    ) -> list[GitHubIssue]:
        """One offset page of issues (pull requests included), newest update
        first."""
        return [
            GitHubIssue.from_raw(raw, repo_full_name=repo_full_name, detailed=False)
            for raw in self._list_page(
                f"/repos/{repo_full_name}/issues", _listing_params(state), page
            )
        ]

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
        untested=_CURSOR_FALLBACK,
    )
    @_translated
    def iter_issues_by_cursor(
        self,
        *,
        repo_full_name: str,
        state: str,
        cursor_url: str | None = None,
        skip: int = 0,
    ) -> Iterator[GitHubCursorItem[GitHubIssue]]:
        """Issues by cursor pagination, from ``cursor_url`` or from the first
        page. ``skip`` drops that many items first."""
        for raw, next_url in self._paginate(
            cursor_url or f"/repos/{repo_full_name}/issues",
            None if cursor_url else _listing_params(state),
            skip=skip,
        ):
            yield GitHubCursorItem(
                item=GitHubIssue.from_raw(
                    raw, repo_full_name=repo_full_name, detailed=False
                ),
                next_url=next_url,
            )

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def get_issue(self, *, repo_full_name: str, number: int) -> GitHubIssue:
        """One issue with the fields a listing leaves out (closed by)."""
        _headers, data = self._get(f"/repos/{repo_full_name}/issues/{number}")
        return GitHubIssue.from_raw(data, repo_full_name=repo_full_name, detailed=True)

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def list_discussions_page(
        self, *, repo_full_name: str, after: str | None = None
    ) -> GitHubDiscussionPage:
        """One cursor page of discussions, newest activity first.

        Discussions are only available through GraphQL. Raises
        ``GitHubDiscussionsUnavailableError`` when the repository has
        discussions turned off or the server does not support them.
        """
        owner, name = _split_repo_full_name(repo_full_name)
        payload: dict[str, Any] = self._graphql(
            _LIST_DISCUSSIONS_QUERY,
            {
                "owner": owner,
                "name": name,
                "first": _DISCUSSIONS_PER_PAGE,
                "after": after,
            },
        )
        repository: Any = payload.get("repository")
        if not isinstance(repository, dict):
            raise GitHubApiError(
                f"Cannot read repository {repo_full_name}",
                status_code=404,
            )
        discussions_connection: Any = repository.get("discussions")
        if not isinstance(discussions_connection, dict):
            # The token can read the repository but not its discussions.
            raise GitHubApiError(
                f"Cannot read discussions of {repo_full_name}",
                status_code=None,
            )
        page_info: Any = discussions_connection.get("pageInfo") or {}
        end_cursor: Any = page_info.get("endCursor")
        nodes: Any = discussions_connection.get("nodes")
        return GitHubDiscussionPage(
            discussions=[
                _discussion_ref(node)
                for node in (nodes if isinstance(nodes, list) else [])
                if isinstance(node, dict) and node.get("number") is not None
            ],
            end_cursor=end_cursor if isinstance(end_cursor, str) else None,
            has_next_page=bool(page_info.get("hasNextPage")),
        )

    def _discussion_comment_with_all_replies(
        self, raw_comment: dict[str, Any]
    ) -> GitHubDiscussionComment:
        """One top-level comment with the replies of every page.

        The comments query carries only the first replies page. The remaining
        replies are read through the comment node's ``replies`` connection.
        """
        comment: GitHubDiscussionComment = _discussion_comment(raw_comment)
        end_cursor, has_next_page = _discussion_replies_page_info(raw_comment)
        while has_next_page:
            replies_payload: dict[str, Any] = self._graphql(
                _COMMENT_REPLIES_QUERY,
                {
                    "commentId": comment.id,
                    "first": _GRAPHQL_PAGE_SIZE,
                    "after": end_cursor,
                },
            )
            node: Any = replies_payload.get("node")
            replies_connection: Any = (
                node.get("replies") if isinstance(node, dict) else None
            )
            if not isinstance(replies_connection, dict):
                break
            reply_nodes: Any = replies_connection.get("nodes")
            comment.replies.extend(
                _discussion_comment(reply)
                for reply in (reply_nodes if isinstance(reply_nodes, list) else [])
                if isinstance(reply, dict) and reply.get("id")
            )
            replies_page_info: Any = replies_connection.get("pageInfo") or {}
            next_cursor: Any = replies_page_info.get("endCursor")
            end_cursor = next_cursor if isinstance(next_cursor, str) else None
            has_next_page = bool(replies_page_info.get("hasNextPage"))
        return comment

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def get_discussion(self, *, repo_full_name: str, number: int) -> GitHubDiscussion:
        """One discussion with every comment and reply, paging comments and
        the replies of each comment to the end.

        Raises ``GitHubApiError`` (404) for a missing discussion and
        ``GitHubDiscussionsUnavailableError`` when discussions are turned off.
        """
        owner, name = _split_repo_full_name(repo_full_name)
        comments: list[GitHubDiscussionComment] = []
        core: dict[str, Any] | None = None
        answer_id: str | None = None
        answer_chosen_at: Any = None
        answer_chosen_by_login: str | None = None
        comments_after: str | None = None
        while True:
            payload: dict[str, Any] = self._graphql(
                _DISCUSSION_COMMENTS_QUERY,
                {
                    "owner": owner,
                    "name": name,
                    "number": number,
                    "commentsFirst": _GRAPHQL_PAGE_SIZE,
                    "commentsAfter": comments_after,
                },
            )
            repository: Any = payload.get("repository")
            discussion: Any = (
                repository.get("discussion") if isinstance(repository, dict) else None
            )
            if not isinstance(discussion, dict):
                raise GitHubApiError(
                    f"Discussion #{number} not found in {repo_full_name}",
                    status_code=404,
                )
            if core is None:
                core = discussion
            # The answer fields sit on the discussion object, so every
            # comments page repeats them; read them on every page because the
            # chosen answer comment can be on a later page.
            answer: Any = discussion.get("answer")
            if isinstance(answer, dict) and isinstance(answer.get("id"), str):
                answer_id = answer["id"]
            answer_chosen_at = discussion.get("answerChosenAt")
            answer_chosen_by_login = _login(discussion.get("answerChosenBy"))
            comments_connection: Any = discussion.get("comments")
            if not isinstance(comments_connection, dict):
                break
            comment_nodes: Any = comments_connection.get("nodes")
            for raw_comment in comment_nodes if isinstance(comment_nodes, list) else []:
                if not isinstance(raw_comment, dict) or not raw_comment.get("id"):
                    continue
                comments.append(self._discussion_comment_with_all_replies(raw_comment))
            comments_page_info: Any = comments_connection.get("pageInfo") or {}
            if not comments_page_info.get("hasNextPage"):
                break
            next_cursor: Any = comments_page_info.get("endCursor")
            comments_after = next_cursor if isinstance(next_cursor, str) else None
            if comments_after is None:
                break

        if core is None:
            raise GitHubApiError(
                f"Discussion #{number} not found in {repo_full_name}",
                status_code=404,
            )
        if answer_id is not None:
            for comment in comments:
                if comment.id == answer_id:
                    comment.is_accepted_answer = True
        ref: GitHubDiscussionRef = _discussion_ref(core)
        return GitHubDiscussion(
            **ref.model_dump(),
            body=core.get("body"),
            comments=comments,
            answer_comment_id=answer_id,
            answer_chosen_at=answer_chosen_at,
            answer_chosen_by_login=answer_chosen_by_login,
        )

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def get_git_tree(self, *, repo_full_name: str, ref: str) -> GitHubGitTree:
        """The recursive tree of a branch, tag or commit. GitHub answers 404
        for an unknown ref and 409 for an empty repository."""
        _headers, data = self._get(
            f"/repos/{repo_full_name}/git/trees/{urllib.parse.quote(ref)}",
            {"recursive": 1},
        )
        return GitHubGitTree(
            entries=[
                GitHubTreeEntry(
                    path=entry["path"], type=entry["type"], size=entry.get("size")
                )
                for entry in data.get("tree") or []
            ],
            truncated=bool(data.get("truncated")),
        )

    @source_operation(
        capabilities={CredentialCapability.INDEXING},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def get_file_content(
        self, *, repo_full_name: str, path: str, ref: str
    ) -> GitHubFileContent:
        """The content of one file. Raises ``ValueError`` for a directory."""
        _headers, data = self._get(
            f"/repos/{repo_full_name}/contents/{urllib.parse.quote(path)}",
            {"ref": ref},
        )
        if not isinstance(data, dict):
            raise ValueError(f"Expected a file at {path}, got a directory")
        encoding: Any = data.get("encoding")
        content: Any = data.get("content")
        response_path: Any = data.get("path")
        html_url: Any = data.get("html_url")
        return GitHubFileContent(
            content=(
                base64.b64decode(bytearray(content, "utf-8"))
                if encoding == "base64" and isinstance(content, str)
                else None
            ),
            encoding=encoding if isinstance(encoding, str) else None,
            path=response_path if isinstance(response_path, str) else None,
            html_url=html_url if isinstance(html_url, str) else None,
        )

    @source_operation(
        capabilities={CredentialCapability.EXTERNAL_GROUP_SYNC},
        consumes=OperationConsumes.CREDENTIAL,
    )
    @_translated
    def get_user(self, *, login: str) -> GitHubUser:
        """A user's profile (``GET /users/{login}``), with the public email.
        Indexing also reads the name and email of authors, but skips a user it
        cannot read, so only group sync needs this."""
        _headers, data = self._get(f"/users/{_quote(login)}")
        return GitHubUser(
            login=data["login"], name=data.get("name"), email=data.get("email")
        )

    @source_operation(
        capabilities={CredentialCapability.EXTERNAL_GROUP_SYNC},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def list_repository_collaborators(
        self, *, repo_full_name: str, limit: int | None = None
    ) -> list[str]:
        """The logins of everyone with access to the repository, from any
        grant (``GET /repos/{repo}/collaborators``). GitHub requires push
        access to the repository. ``limit`` stops the listing early."""
        return _logins(
            self._paginate(f"/repos/{repo_full_name}/collaborators", {}), limit
        )

    @source_operation(
        capabilities={CredentialCapability.EXTERNAL_GROUP_SYNC},
        consumes=OperationConsumes.BOTH,
    )
    @_translated
    def list_organization_members(
        self, *, org_login: str, limit: int | None = None
    ) -> list[str]:
        """The logins of the organization's members
        (``GET /orgs/{org}/members?filter=all``). Without ``read:org`` or
        membership, GitHub lists only the public members. ``limit`` stops the
        listing early."""
        return _logins(
            self._paginate(f"/orgs/{_quote(org_login)}/members", {"filter": "all"}),
            limit,
        )
