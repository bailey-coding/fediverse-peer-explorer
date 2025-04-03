#!/usr/bin/env python3
import argparse
import asyncio
from json import JSONDecodeError
import sqlite3
from dataclasses import dataclass
from datetime import datetime, UTC, timedelta
from subprocess import check_output
from typing import Optional

from itertools import batched
from enum import StrEnum, auto
from typing import Optional

from aiohttp import (
    ClientConnectionError,
    ClientConnectorError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
    ServerDisconnectedError,
)
from aiohttp.client_exceptions import (
    ClientConnectorDNSError,
    ClientConnectorCertificateError,
)
from aiohttp_client_cache import CachedSession, SQLiteBackend
from tabulate import tabulate


class FetchError(StrEnum):
    DNS = auto()
    CERTIFICATE = auto()
    TIMEOUT = auto()
    REDIRECT = auto()
    OTHER = auto()
    UNSPECIFIED = auto()


class Instance:
    checks = list[dict[str, str]]
    pass


EXAMPLE_CHECKS = [
    {
        "checked_at": datetime.now(tz=UTC),
        "success": True,
        "software": "pixelfed",
        "version": "0.12.5",
    },
    {
        "checked_at": datetime.now(tz=UTC),
        "success": False,
        "error_type": FetchError.DNS,
        "error": "Yada yada",
    },
]


try:
    VERSION = check_output("git", "rev-parse", "--short", "HEAD")
except Exception:
    VERSION = "unknown"


@dataclass
class FediverseInstance:
    domain: str
    last_updated_at: float
    fetch_error_count: int = 0
    software: Optional[str] = None
    version: Optional[str] = None
    error: Optional[str] = None
    last_error_type: Optional[FetchError] = None


async def fetch(session: ClientSession, peer: str, semaphore) -> FediverseInstance:
    async with semaphore:
        try:
            url = f"https://{peer}/.well-known/nodeinfo"
            # print(f'Fetching data for {url}')
            # TODO: Verify the initial domain doesn't redirect
            async with session.get(url) as response:
                response.raise_for_status()
                node_info_links = await response.json()
            links = [
                link
                for link in node_info_links["links"]
                if link["rel"]
                in (
                    "http://nodeinfo.diaspora.software/ns/schema/1.0",
                    "http://nodeinfo.diaspora.software/ns/schema/1.1",
                    "http://nodeinfo.diaspora.software/ns/schema/2.0",
                    "http://nodeinfo.diaspora.software/ns/schema/2.1",
                )
            ]
            # if len(links) == 0:
            #     raise Exception(node_info_links["links"])
            # TODO: Verify the link is same domain (and that it doesn't resolve
            # to a network-internal resource).
            url = links[0]["href"]
            # print(f'Fetching data for {url}')
            async with session.get(url) as response:
                response.raise_for_status()
                node_info = await response.json()
                return FediverseInstance(
                    peer,
                    last_updated_at=datetime.now(UTC).timestamp(),
                    software=node_info["software"]["name"],
                    version=node_info["software"]["version"],
                )
        except (
            ClientConnectorCertificateError,
            ClientConnectorDNSError,
            ClientConnectorError,
            ClientConnectionError,
            ClientResponseError,
            JSONDecodeError,
            ServerDisconnectedError,
            TimeoutError,
        ) as e:
            # TODO: If error, check if it's a 404 or 500/network error, as these may
            # indicate that the server is no longer existent.
            if any(i in peer for i in ("pix", "photo")):
                print(f"Error fetching {peer}: {type(e)} {e}")
            return FediverseInstance(
                peer,
                last_updated_at=datetime.now(UTC).timestamp(),
                error=f"Error fetching {peer}: {type(e)} {e}",
                last_error_type=e.__class__.__name__,
            )
        except Exception as e:
            if any(i in peer for i in ("pix", "photo")):
                print(f"Error fetching {peer}: {type(e)} {e}")
            raise


async def update(args):
    domain = args.domain
    con = sqlite3.connect("db.sqlite")
    con.row_factory = sqlite3.Row
    with con:
        cur = con.cursor()
        cur.execute("""SELECT domain, error, last_updated_at FROM instances""")
        existing_rows = cur.fetchall()
    existing_domains = {row["domain"]: row for row in existing_rows}
    results = []

    async with CachedSession(
        cache=SQLiteBackend("demo_cache", expire_after=60 * 60 * 6),
        timeout=ClientTimeout(total=15, sock_connect=15),
        headers={
            "User-Agent": f"Fediverse Peer Explorer/{VERSION} ({SERVER_SOFTWARE}; +https://{domain}/) (like Mastodon)"
        },
    ) as session:
        all_peers = await (
            await session.get(f"https://{domain}/api/v1/instance/peers")
        ).json()
        semaphore = asyncio.Semaphore(100)
        six_hours_ago = datetime.now(UTC) - timedelta(hours=6)
        twenty_four_hours_ago = datetime.now(UTC) - timedelta(hours=24)
        processed = 0
        BATCH_SIZE = 1000
        for peers in batched(all_peers, BATCH_SIZE):
            print(f"Processing {processed}-{processed + BATCH_SIZE}/{len(all_peers)}")
            processed += BATCH_SIZE
            tasks = []
            fetched_peers = []
            for p in peers:
                if p in existing_domains:
                    data = existing_domains[p]
                    if (
                        data["error"]
                        and datetime.fromtimestamp(data["last_updated_at"], tz=UTC)
                        > twenty_four_hours_ago
                    ) or datetime.fromtimestamp(
                        data["last_updated_at"], tz=UTC
                    ) > six_hours_ago:
                        continue
                fetched_peers.append(p)
                tasks.append(fetch(session, p, semaphore))

            task_results = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )
            for idx, p in enumerate(fetched_peers):
                if not isinstance(task_results[idx], Exception):
                    results.append(task_results[idx])
                if not any(i in p for i in ("pix", "photo")):
                    continue
                if isinstance(task_results[idx], Exception):
                    print("=== UNHANDLED ERROR / EXCEPTION ===")
                    print(
                        "domain={}, exception_class={}, exception={}".format(
                            p, task_results[idx].__class__.__name__, task_results[idx]
                        )
                    )
                    print("===================================")
            with con:
                res = con.executemany(
                    """INSERT INTO instances
                        (domain, software_name, software_version, error, last_updated_at, fetch_error_count, last_error_type)
                    VALUES (:domain, :software, :version, :error, :last_updated_at, :fetch_error_count, :last_error_type)
                    ON CONFLICT (domain)
                        DO UPDATE
                        SET software_name=excluded.software_name,
                            software_version=excluded.software_version,
                            error=excluded.error,
                            fetch_error_count=fetch_error_count+excluded.fetch_error_count,
                            last_error_type=excluded.last_error_type,
                            last_updated_at=excluded.last_updated_at""",
                    (r.__dict__ for r in results),
                )
                con.commit()
                print(f"Updated {res.rowcount} rows")
    con.close()
    await query(args)


async def query(args):
    con = sqlite3.connect("db.sqlite")
    con.row_factory = sqlite3.Row
    with con:
        cur = con.cursor()
        cur.execute(
            """SELECT domain, software_name, software_version, strftime('%Y-%m-%d', datetime(last_updated_at, 'unixepoch')) FROM instances WHERE software_name = 'pixelfed'"""
        )
        results = cur.fetchall()
    print(
        tabulate(
            sorted(
                results,
                key=lambda x: x["software_version"],
            ),
            headers={
                "domain": "domain",
                "last_updated_at": "last_updated_at",
                # "software_name": "software",
                "software_version": "version",
                # "error": "error"
            },
            tablefmt="github",
        )
    )


async def main():
    parser = argparse.ArgumentParser(prog="fediverse-peer-explorer")
    # parser.add_argument('--foo', action='store_true', help='foo help')
    subparsers = parser.add_subparsers(help="when does this show?", required=True)
    parser_query = subparsers.add_parser("query", help="print data from DB")
    parser_query.set_defaults(func=query)
    # parser_query.add_argument('bar', type=int, help='bar help')
    parser_update = subparsers.add_parser(
        "update", help="fetch peer software + versions"
    )
    parser_update.add_argument(
        "domain", type=str, help="domain where we'll fetch peers from"
    )
    parser_update.set_defaults(func=update)
    args = parser.parse_args()
    await args.func(args)


def outer_main():
    asyncio.run(main())


if __name__ == "__main__":
    outer_main()
