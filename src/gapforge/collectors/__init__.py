"""Async evidence collectors."""

from gapforge.collectors.base import Collector, collect_isolated
from gapforge.collectors.github import GitHubCollector
from gapforge.collectors.hacker_news import HackerNewsCollector
from gapforge.collectors.reddit import RedditCollector

__all__ = [
    "Collector",
    "GitHubCollector",
    "HackerNewsCollector",
    "RedditCollector",
    "collect_isolated",
]
