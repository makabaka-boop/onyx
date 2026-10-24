"""Discussions indexing: GraphQL paging at the gateway, document assembly and
failure handling at the connector, and targeted reindex."""

import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from onyx.connectors.credentials_provider import OnyxStaticCredentialsProvider
from onyx.connectors.github.connector import (
    GithubConnector,
    GithubConnectorStage,
    parse_github_document_id,
)
from onyx.connectors.github.models import (
    GitHubDiscussion,
    GitHubDiscussionCategory,
    GitHubDiscussionComment,
    GitHubDiscussionPage,
    GitHubDiscussionRef,
    GitHubDocKind,
    GitHubRepository,
    SerializedRepository,
)
from onyx.connectors.github.source_operations import (
    GitHubApiError,
    GitHubDiscussionsUnavailableError,
    GitHubRateLimitError,
    GitHubSourceOperations,
)
from onyx.connectors.models import (
    ConnectorFailure,
    Document,
    DocumentFailure,
    SlimDocument,
    TextSection,
)
from tests.unit.onyx.connectors.utils import (
    load_everything_from_checkpoint_connector,
    load_everything_from_checkpoint_connector_from_checkpoint,
)

_REPO = "test-org/test-repo"
_DISCUSSION_URL = f"https://github.com/{_REPO}/discussions"


def _ref(number: int, **overrides: Any) -> GitHubDiscussionRef:
    values: dict[str, Any] = {
        "title": f"Discussion {number}",
        "url": f"{_DISCUSSION_URL}/{number}",
        "created_at": datetime(2023, 6, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2023, 6, 1, tzinfo=timezone.utc),
        "author_login": "octocat",
    }
    values.update(overrides)
    return GitHubDiscussionRef(number=number, **values)


def _discussion_checkpoint(connector: GithubConnector, repo: GitHubRepository) -> Any:
    checkpoint = connector.build_dummy_checkpoint()
    checkpoint.cached_repo_ids = []
    checkpoint.cached_repo = SerializedRepository(id=repo.id, raw_data=repo.raw_data)
    checkpoint.stage = GithubConnectorStage.DISCUSSIONS
    return checkpoint


def _flatten(outputs: list[Any]) -> list[Document | ConnectorFailure]:
    return [item for output in outputs for item in output.items]


# --- document assembly ---


def test_discussions_become_documents_with_body_comments_replies_and_answer(
    build_github_connector: Callable[..., GithubConnector],
    mock_source_operations: MagicMock,
    create_repo: Callable[..., GitHubRepository],
    create_discussion: Callable[..., GitHubDiscussion],
) -> None:
    repo = create_repo()
    mock_source_operations.get_repository.return_value = repo
    connector = build_github_connector(
        include_prs=False, include_issues=False, include_discussions=True
    )
    chosen_at = datetime(2023, 6, 2, tzinfo=timezone.utc)
    question = GitHubDiscussionComment(
        id="C_1",
        database_id=10,
        url=f"{_DISCUSSION_URL}/7?comment=10",
        body="How do I reset the cache?",
        created_at=datetime(2023, 6, 1, tzinfo=timezone.utc),
        updated_at=datetime(2023, 6, 1, tzinfo=timezone.utc),
        author_login="asker",
        replies=[
            GitHubDiscussionComment(
                id="R_1",
                database_id=11,
                body="Did you try turning it off and on?",
                created_at=datetime(2023, 6, 1, 1, tzinfo=timezone.utc),
                updated_at=datetime(2023, 6, 1, 1, tzinfo=timezone.utc),
                author_login="reply-er",
            )
        ],
    )
    answer = GitHubDiscussionComment(
        id="C_2",
        database_id=20,
        url=f"{_DISCUSSION_URL}/7?comment=20",
        body="Call cache.reset()",
        created_at=datetime(2023, 6, 2, tzinfo=timezone.utc),
        updated_at=datetime(2023, 6, 2, tzinfo=timezone.utc),
        author_login="expert",
        is_accepted_answer=True,
    )
    discussion = create_discussion(
        number=7,
        title="How do I reset the cache?",
        body="The cache seems stuck.",
        author_login="asker",
        category=GitHubDiscussionCategory(name="Q&A", slug="q-a"),
        comments=[question, answer],
        answer_comment_id="C_2",
        answer_chosen_at=chosen_at,
        answer_chosen_by_login="expert",
    )
    mock_source_operations.list_discussions_page.return_value = GitHubDiscussionPage(
        discussions=[_ref(7)]
    )

    outputs = load_everything_from_checkpoint_connector(connector, 0, time.time())
    docs = [item for item in _flatten(outputs) if isinstance(item, Document)]
    assert len(docs) == 1
    doc = docs[0]

    assert doc.id == discussion.url
    assert doc.semantic_identifier == "7: How do I reset the cache?"
    assert doc.doc_updated_at == datetime(2023, 1, 1, tzinfo=timezone.utc)

    # Access control follows the repository, like pull requests and issues.
    assert doc.external_access is None
    assert doc.doc_metadata is not None
    assert doc.doc_metadata["repo"] == _REPO
    assert doc.doc_metadata["hierarchy"]["object_type"] == "discussion"
    assert doc.doc_metadata["hierarchy"]["source_path"] == [
        "test-org",
        "test-repo",
        "discussions",
    ]

    assert doc.metadata["object_type"] == "Discussion"
    assert doc.metadata["id"] == "7"
    assert doc.metadata["repo"] == _REPO
    assert doc.metadata["category"] == "Q&A"
    assert doc.metadata["accepted_answer"] == "True"
    assert doc.metadata["user"]  # author resolved

    # Body, top-level comment, nested reply and the answer marker are indexed.
    assert doc.sections[0].link == discussion.url
    assert doc.sections[0].text == "The cache seems stuck."
    comment_text = "\n".join(
        section.text for section in doc.sections[1:] if isinstance(section, TextSection)
    )
    assert "@asker" in comment_text
    assert "How do I reset the cache?" in comment_text
    assert "@reply-er" in comment_text
    assert "Did you try turning it off and on?" in comment_text
    assert "[ACCEPTED ANSWER]" in comment_text
    assert "Call cache.reset()" in comment_text
    assert "@expert" in comment_text


# --- pagination ---


def test_discussion_listing_pages_resume_from_the_graphql_cursor(
    build_github_connector: Callable[..., GithubConnector],
    mock_source_operations: MagicMock,
    create_repo: Callable[..., GitHubRepository],
    create_discussion: Callable[..., GitHubDiscussion],
) -> None:
    repo = create_repo()
    mock_source_operations.get_repository.return_value = repo
    connector = build_github_connector(
        include_prs=False, include_issues=False, include_discussions=True
    )
    first = create_discussion(number=1)
    second = create_discussion(number=2)
    mock_source_operations.list_discussions_page.side_effect = [
        GitHubDiscussionPage(
            discussions=[_ref(1)], end_cursor="cursor-1", has_next_page=True
        ),
        GitHubDiscussionPage(discussions=[_ref(2)], has_next_page=False),
    ]

    outputs = load_everything_from_checkpoint_connector(connector, 0, time.time())
    docs = [item for item in _flatten(outputs) if isinstance(item, Document)]
    assert [doc.id for doc in docs] == [first.url, second.url]

    list_calls = mock_source_operations.list_discussions_page.call_args_list
    assert list_calls[0].kwargs == {"repo_full_name": _REPO, "after": None}
    assert list_calls[1].kwargs == {"repo_full_name": _REPO, "after": "cursor-1"}

    # The cursor is held while pages remain and cleared when drained.
    assert outputs[1].next_checkpoint.discussion_cursor == "cursor-1"
    assert outputs[-1].next_checkpoint.discussion_cursor is None


# --- failures ---


def test_a_failed_discussion_read_reports_failure_without_a_document(
    build_github_connector: Callable[..., GithubConnector],
    mock_source_operations: MagicMock,
    create_repo: Callable[..., GitHubRepository],
    create_discussion: Callable[..., GitHubDiscussion],
) -> None:
    mock_source_operations.get_repository.return_value = create_repo()
    connector = build_github_connector(
        include_prs=False, include_issues=False, include_discussions=True
    )
    second = create_discussion(number=2)

    def get_discussion(*, repo_full_name: str, number: int) -> GitHubDiscussion:
        assert repo_full_name
        if number == 1:
            raise GitHubApiError("GitHub is on fire", status_code=502)
        return second

    mock_source_operations.get_discussion.side_effect = get_discussion
    mock_source_operations.list_discussions_page.return_value = GitHubDiscussionPage(
        discussions=[_ref(1), _ref(2)]
    )

    outputs = load_everything_from_checkpoint_connector(connector, 0, time.time())
    items = _flatten(outputs)
    docs = [item for item in items if isinstance(item, Document)]
    failures = [item for item in items if isinstance(item, ConnectorFailure)]

    # No document is emitted for the failed discussion, so its old document
    # keeps its previous content.
    assert [doc.id for doc in docs] == [second.url]
    assert len(failures) == 1
    failed_document = failures[0].failed_document
    assert isinstance(failed_document, DocumentFailure)
    assert failed_document.document_id == _ref(1).url
    assert failed_document.document_link == _ref(1).url


def test_a_repo_with_discussions_turned_off_skips_the_stage(
    build_github_connector: Callable[..., GithubConnector],
    mock_source_operations: MagicMock,
    create_repo: Callable[..., GitHubRepository],
) -> None:
    mock_source_operations.get_repository.return_value = create_repo()
    connector = build_github_connector(
        include_prs=False, include_issues=False, include_discussions=True
    )
    mock_source_operations.list_discussions_page.side_effect = (
        GitHubDiscussionsUnavailableError(
            "Discussions are not enabled for this repository",
            status_code=None,
        )
    )

    outputs = load_everything_from_checkpoint_connector(connector, 0, time.time())

    assert _flatten(outputs) == []
    assert outputs[-1].next_checkpoint.has_more is False
    mock_source_operations.get_discussion.assert_not_called()


def test_discussions_are_not_read_when_the_toggle_is_off(
    build_github_connector: Callable[..., GithubConnector],
    mock_source_operations: MagicMock,
    create_repo: Callable[..., GitHubRepository],
) -> None:
    mock_source_operations.get_repository.return_value = create_repo()
    connector = build_github_connector(
        include_prs=False, include_issues=False, include_discussions=False
    )

    outputs = load_everything_from_checkpoint_connector(connector, 0, time.time())

    assert _flatten(outputs) == []
    mock_source_operations.list_discussions_page.assert_not_called()


# --- time window ---


def test_discussions_older_than_the_window_stop_the_stage(
    build_github_connector: Callable[..., GithubConnector],
    mock_source_operations: MagicMock,
    create_repo: Callable[..., GitHubRepository],
) -> None:
    repo = create_repo()
    connector = build_github_connector(
        include_prs=False, include_issues=False, include_discussions=True
    )
    old = _ref(1, updated_at=datetime(2023, 1, 1, tzinfo=timezone.utc))
    mock_source_operations.list_discussions_page.return_value = GitHubDiscussionPage(
        discussions=[old]
    )
    checkpoint = _discussion_checkpoint(connector, repo)

    # A poll in 2024 must not read a discussion last updated in 2023.
    start = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
    outputs = load_everything_from_checkpoint_connector_from_checkpoint(
        connector, start, time.time(), checkpoint
    )

    assert _flatten(outputs) == []
    mock_source_operations.get_discussion.assert_not_called()
    assert outputs[-1].next_checkpoint.stage == GithubConnectorStage.FILES


# --- slim documents ---


def test_slim_docs_include_discussion_ids(
    build_github_connector: Callable[..., GithubConnector],
    mock_source_operations: MagicMock,
    create_repo: Callable[..., GitHubRepository],
) -> None:
    mock_source_operations.get_repository.return_value = create_repo()
    connector = build_github_connector(
        include_prs=False, include_issues=False, include_discussions=True
    )
    mock_source_operations.list_discussions_page.return_value = GitHubDiscussionPage(
        discussions=[_ref(42)]
    )

    batches = list(connector.retrieve_all_slim_docs())
    ids = [
        doc.id for batch in batches for doc in batch if isinstance(doc, SlimDocument)
    ]

    assert f"{_DISCUSSION_URL}/42" in ids


# --- document id parsing ---


@pytest.mark.parametrize(
    "doc_id",
    [
        f"https://github.com/{_REPO}/discussions/12",
        f"https://github.com/{_REPO}/discussions/1",
    ],
)
def test_parse_discussion_document_id(doc_id: str) -> None:
    parsed = parse_github_document_id(doc_id)
    assert parsed is not None
    assert parsed.kind == GitHubDocKind.DISCUSSIONS
    assert parsed.owner == "test-org"
    assert parsed.repo == "test-repo"
    assert parsed.number == int(doc_id.rsplit("/", 1)[-1])


@pytest.mark.parametrize(
    "doc_id",
    [
        f"https://github.com/{_REPO}/discussions/",
        f"https://github.com/{_REPO}/discussions/abc",
        f"https://github.com/{_REPO}/pull/1",
    ],
)
def test_parse_rejects_non_discussion_shapes(doc_id: str) -> None:
    parsed = parse_github_document_id(doc_id)
    if doc_id.endswith("/pull/1"):
        assert parsed is not None and parsed.kind == GitHubDocKind.PULL_REQUEST
    else:
        assert parsed is None


# --- targeted reindex ---


def _reindex_failures(doc_ids: list[str]) -> list[ConnectorFailure]:
    return [
        ConnectorFailure(
            failed_document=DocumentFailure(document_id=doc_id, document_link=doc_id),
            failure_message="synthetic",
        )
        for doc_id in doc_ids
    ]


def test_reindex_rebuilds_a_discussion_document(
    build_github_connector: Callable[..., GithubConnector],
    mock_source_operations: MagicMock,
    create_repo: Callable[..., GitHubRepository],
    create_discussion: Callable[..., GitHubDiscussion],
) -> None:
    mock_source_operations.get_repository.return_value = create_repo()
    discussion = create_discussion(number=9)
    connector = build_github_connector(
        include_prs=False, include_issues=False, include_discussions=True
    )

    results = list(connector.reindex(_reindex_failures([discussion.url])))

    assert len(results) == 1
    assert isinstance(results[0], Document)
    assert results[0].id == discussion.url
    mock_source_operations.get_discussion.assert_called_once_with(
        repo_full_name=_REPO, number=9
    )


def test_reindex_refuses_a_discussion_when_the_toggle_is_off(
    build_github_connector: Callable[..., GithubConnector],
    mock_source_operations: MagicMock,
) -> None:
    connector = build_github_connector(
        include_prs=False, include_issues=False, include_discussions=False
    )

    results = list(connector.reindex(_reindex_failures([f"{_DISCUSSION_URL}/9"])))

    assert len(results) == 1
    assert isinstance(results[0], ConnectorFailure)
    assert "discussions are not indexed" in results[0].failure_message
    mock_source_operations.get_repository.assert_not_called()
    mock_source_operations.get_discussion.assert_not_called()


def test_reindex_reports_turned_off_discussions(
    build_github_connector: Callable[..., GithubConnector],
    mock_source_operations: MagicMock,
    create_repo: Callable[..., GitHubRepository],
) -> None:
    mock_source_operations.get_repository.return_value = create_repo()
    mock_source_operations.get_discussion.side_effect = (
        GitHubDiscussionsUnavailableError("disabled", status_code=None)
    )
    connector = build_github_connector(
        include_prs=False, include_issues=False, include_discussions=True
    )

    results = list(connector.reindex(_reindex_failures([f"{_DISCUSSION_URL}/9"])))

    assert len(results) == 1
    assert isinstance(results[0], ConnectorFailure)
    assert "turned off" in results[0].failure_message


# --- gateway: GraphQL ---


def _gql_gateway() -> tuple[GitHubSourceOperations, MagicMock]:
    gateway = GitHubSourceOperations(
        credentials_provider=OnyxStaticCredentialsProvider(
            None, "github", {"github_access_token": "ghp_test"}
        )
    )
    client = MagicMock()
    client.requester.base_url = "https://api.github.com"
    gateway._thread_clients().client = client
    return gateway, client.requester.requestJsonAndCheck


def _graphql_body(data: Any, errors: list[dict[str, str]] | None = None) -> tuple:
    body: dict[str, Any] = {"data": data}
    if errors is not None:
        body["errors"] = errors
    return ({}, body)


def _comment_node(
    node_id: str,
    *,
    login: str = "octocat",
    body: str = "comment",
    replies: list[dict[str, Any]] | None = None,
    replies_page: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": node_id,
        "databaseId": int(node_id[-1]),
        "url": f"https://github.com/{_REPO}/discussions/7",
        "body": body,
        "createdAt": "2023-06-01T00:00:00Z",
        "updatedAt": "2023-06-01T00:00:00Z",
        "author": {"login": login},
        "replyTo": None,
        "replies": {
            "pageInfo": replies_page
            or {
                "endCursor": None,
                "hasNextPage": False,
            },
            "nodes": replies or [],
        },
    }


def _discussion_core(
    comments: list[dict[str, Any]],
    *,
    comments_page: dict[str, Any],
    answer_id: str | None = None,
) -> dict[str, Any]:
    return {
        "repository": {
            "discussion": {
                "number": 7,
                "title": "Title",
                "body": "Body",
                "url": f"{_DISCUSSION_URL}/7",
                "createdAt": "2023-06-01T00:00:00Z",
                "updatedAt": "2023-06-02T00:00:00Z",
                "author": {"login": "asker"},
                "category": {"name": "Q&A", "slug": "q-a"},
                "answerChosenAt": "2023-06-03T00:00:00Z",
                "answerChosenBy": {"login": "expert"},
                "answer": {"id": answer_id} if answer_id else None,
                "comments": {"pageInfo": comments_page, "nodes": comments},
            }
        }
    }


def test_gateway_lists_one_discussions_page() -> None:
    gateway, request = _gql_gateway()
    request.return_value = _graphql_body(
        {
            "repository": {
                "discussions": {
                    "pageInfo": {
                        "endCursor": "abc",
                        "hasNextPage": True,
                    },
                    "nodes": [
                        {
                            "number": 7,
                            "title": "Title",
                            "url": f"{_DISCUSSION_URL}/7",
                            "createdAt": "2023-06-01T00:00:00Z",
                            "updatedAt": "2023-06-02T00:00:00Z",
                            "author": {"login": "asker"},
                            "category": {"name": "Q&A", "slug": "q-a"},
                        }
                    ],
                }
            }
        }
    )

    page = gateway.list_discussions_page(repo_full_name=_REPO, after=None)

    assert page.has_next_page is True
    assert page.end_cursor == "abc"
    assert len(page.discussions) == 1
    ref = page.discussions[0]
    assert ref.number == 7
    assert ref.author_login == "asker"
    assert ref.category is not None and ref.category.slug == "q-a"

    verb, url = request.call_args.args[:2]
    assert verb == "POST"
    assert url == "https://api.github.com/graphql"
    variables = request.call_args.kwargs["input"]["variables"]
    assert variables == {
        "owner": "test-org",
        "name": "test-repo",
        "first": 50,
        "after": None,
    }


def test_gateway_get_discussion_pages_comments_and_nested_replies() -> None:
    gateway, request = _gql_gateway()
    reply_one = _comment_node("R_1", login="r1", body="reply one")
    reply_two = _comment_node("R_2", login="r2", body="reply two")
    request.side_effect = [
        # Comments page 1: one comment whose replies span two pages.
        _graphql_body(
            _discussion_core(
                [
                    _comment_node(
                        "C_1",
                        login="asker",
                        body="question",
                        replies=[reply_one],
                        replies_page={
                            "endCursor": "reply-cursor",
                            "hasNextPage": True,
                        },
                    )
                ],
                comments_page={
                    "endCursor": "comments-cursor",
                    "hasNextPage": True,
                },
            )
        ),
        # The remaining replies of C_1.
        _graphql_body(
            {
                "node": {
                    "replies": {
                        "pageInfo": {"endCursor": None, "hasNextPage": False},
                        "nodes": [reply_two],
                    }
                }
            }
        ),
        # Comments page 2: the chosen answer, no more replies or pages.
        _graphql_body(
            _discussion_core(
                [
                    _comment_node(
                        "C_2",
                        login="expert",
                        body="the answer",
                    )
                ],
                comments_page={"endCursor": None, "hasNextPage": False},
                answer_id="C_2",
            )
        ),
    ]

    discussion = gateway.get_discussion(repo_full_name=_REPO, number=7)

    assert [comment.id for comment in discussion.comments] == ["C_1", "C_2"]
    top_comment = discussion.comments[0]
    assert [reply.id for reply in top_comment.replies] == ["R_1", "R_2"]
    answer_comment = discussion.comments[1]
    assert answer_comment.is_accepted_answer is True
    assert top_comment.is_accepted_answer is False
    assert discussion.answer_comment_id == "C_2"
    assert discussion.answer_chosen_by_login == "expert"

    calls = request.call_args_list
    assert calls[0].kwargs["input"]["variables"]["commentsAfter"] is None
    assert calls[1].kwargs["input"]["variables"] == {
        "commentId": "C_1",
        "first": 100,
        "after": "reply-cursor",
    }
    assert calls[2].kwargs["input"]["variables"]["commentsAfter"] == ("comments-cursor")


def test_gateway_maps_disabled_discussions_to_unavailable_error() -> None:
    gateway, request = _gql_gateway()
    request.return_value = (
        {},
        {
            "data": {"repository": None},
            "errors": [{"message": "Discussions are not enabled for this repository"}],
        },
    )

    with pytest.raises(GitHubDiscussionsUnavailableError):
        gateway.list_discussions_page(repo_full_name=_REPO)


def test_gateway_raises_api_error_for_a_graphql_errors_array() -> None:
    gateway, request = _gql_gateway()
    request.return_value = (
        {},
        {"data": None, "errors": [{"message": "Something went wrong"}]},
    )

    with pytest.raises(GitHubApiError) as excinfo:
        gateway.list_discussions_page(repo_full_name=_REPO)
    assert not isinstance(excinfo.value, GitHubRateLimitError)
    assert "Something went wrong" in str(excinfo.value)


def test_gateway_maps_graphql_rate_limit_message() -> None:
    gateway, request = _gql_gateway()
    request.return_value = (
        {},
        {"data": None, "errors": [{"message": "API rate limit exceeded"}]},
    )

    with pytest.raises(GitHubRateLimitError):
        gateway.list_discussions_page(repo_full_name=_REPO)


@pytest.mark.parametrize(
    "rest_root,expected",
    [
        ("https://api.github.com", "https://api.github.com/graphql"),
        (
            "https://ghes.example.com/api/v3",
            "https://ghes.example.com/api/graphql",
        ),
    ],
)
def test_graphql_endpoint_for_github_and_ghes(rest_root: str, expected: str) -> None:
    gateway = GitHubSourceOperations(
        credentials_provider=OnyxStaticCredentialsProvider(
            None, "github", {"github_access_token": "ghp_test"}
        )
    )
    client = MagicMock()
    client.requester.base_url = rest_root
    gateway._thread_clients().client = client

    assert gateway._graphql_url() == expected
